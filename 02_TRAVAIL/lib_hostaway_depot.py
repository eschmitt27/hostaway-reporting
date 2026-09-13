"""Source Hostaway « dépôt GitHub » — le MÊME moteur d'extraction, un autre transport.

POURQUOI CE MODULE EXISTE
Les identifiants Hostaway de cette maison vivent dans les *Secrets* d'un dépôt GitHub, pas sur le
poste. Un pipeline GitHub Actions (`.github/workflows/pipeline.yml`, branche `main`) interroge
l'API trois fois par jour et **commite** le résultat dans le dépôt, sous forme de fichiers TSV.
Ce pipeline fonctionne ; il n'y a aucune raison d'en construire un second, et de très bonnes
raisons de ne pas le faire : deux extracteurs pour une même API finissent toujours par se
comporter différemment, et l'un des deux ment sans qu'on sache lequel.

CE QUE CE MODULE EST — ET CE QU'IL N'EST PAS
Ce n'est PAS un second extracteur. C'est un **transport** : il présente exactement la même surface
que `HostawayClient` (`count_reservations`, `get_reservations_page`, `get_reservation_detail`,
`get_listings`, `get_tasks`), en lisant des fichiers versionnés au lieu d'appeler HTTP. Tout ce
qui suit dans `lot1_hostaway_extract.py` — normalisation des canaux, calcul du payout H1/H2/H3,
coût de ménage daté, détection d'anomalies, écriture de la couche RAW — est INCHANGÉ et partagé.
Une seule chaîne métier ; seule la manière dont les faits arrivent change.

AUCUNE RÈGLE MÉTIER N'EST LUE ICI
Le dépôt publie aussi `listing_constants.csv` (colonnes `CoutMenage`, `TauxCommission`) et
`hostaway_reporting_final.tsv` (colonne `TotalPayout`). Ce module les IGNORE délibérément. Le coût
de ménage et le taux de commission sont des décisions de gestion : ils vivent dans les
référentiels de l'application, datés et versionnés, et c'est le moteur qui calcule le payout. Les
importer d'ici créerait une seconde vérité, muette, plus récente en apparence, et fausse le jour
où l'une des deux change. Le dépôt fournit des FAITS Hostaway ; l'application détient les RÈGLES.

TÂCHES DE MÉNAGE
Le pipeline publie aussi `cleaning_tasks_hostaway.tsv` (`/v1/tasks`). Ce fichier est FACULTATIF
pour la disponibilité du dépôt (les réservations n'en dépendent pas) ; `etat()` dit s'il est
publié. Absent, `get_tasks()` lève, avec un message explicite.

CE QUE LE DÉPÔT NE PORTE PAS
La fiche des annonces (`/v1/listings`) n'est pas publiée. Et un fichier de tâches absent n'est
jamais rendu comme une liste vide : « non fourni par cette source » n'est pas « aucune tâche ». C'est la même
distinction que `RateLimitEpuise` fait déjà pour un débit saturé, et elle a la même raison d'être —
un ensemble vide rendu par erreur fait disparaître des données sans la moindre alerte.
"""
from __future__ import annotations

import csv
import io
import json
import subprocess
from pathlib import Path

# Fichiers publiés par le pipeline GitHub, sur la branche de données.
FICHIER_RESERVATIONS = "reservations_hostaway.tsv"
FICHIER_FINANCE_FIELDS = "finance_fields_hostaway.tsv"
# Tâches de ménage (`/v1/tasks`, H6) — publiées par `extract_cleaning_tasks.py`. OPTIONNEL pour la
# disponibilité du dépôt : son absence n'empêche pas de lire les réservations, elle fait seulement
# refuser `get_tasks()` avec un motif explicite.
FICHIER_CLEANING_TASKS = "cleaning_tasks_hostaway.tsv"

# Le rapport final agrégé du pipeline. Volontairement NON lu : il porte `TotalPayout`, `CoutMenage`
# et `TauxCommission`, qui sont des conclusions, pas des faits. Nommé ici pour que le lecteur sache
# qu'il a été vu et écarté, et non oublié.
FICHIER_RAPPORT_AGREGE = "hostaway_reporting_final.tsv"
FICHIER_CONSTANTES = "listing_constants.csv"

REMOTE_DEFAUT = "origin"
BRANCHE_DEFAUT = "main"

MODE = "DEPOT_GITHUB"


class DepotIndisponible(RuntimeError):
    """Le dépôt de données n'a pas pu être lu.

    Type distinct d'un jeu de données vide : « je n'ai pas pu lire » ne doit jamais se confondre
    avec « il n'y a rien ». La confusion entre les deux est ce qui produit des mois à zéro euro
    sans qu'aucune erreur ne subsiste pour l'expliquer.
    """


class DonneeNonFournieParCetteSource(RuntimeError):
    """La source est lisible, mais ne publie pas cette donnée-là.

    Rendre une liste vide serait affirmer une absence qu'on n'a pas constatée.
    """


def _git(racine: Path, *arguments: str, binaire: bool = False):
    """Exécute une commande git dans le dépôt et rend sa sortie.

    `git` est utilisé plutôt qu'un téléchargement HTTP : le dépôt de données EST le dépôt du
    projet (même `origin`), donc l'authentification qui fonctionne déjà pour le code fonctionne
    pour la donnée. Aucun jeton supplémentaire, aucun secret à stocker sur le poste.
    """
    proc = subprocess.run(
        ["git", *arguments], cwd=str(racine), capture_output=True, timeout=300)
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", errors="replace").strip().splitlines()
        raise DepotIndisponible(
            f"git {' '.join(arguments[:2])} a échoué : {detail[-1] if detail else 'sans message'}")
    return proc.stdout if binaire else proc.stdout.decode("utf-8", errors="replace")


def _lire_tsv(octets: bytes, separateur: str = "\t") -> list[dict]:
    """Lignes d'un fichier délimité, en dictionnaires.

    `utf-8-sig` : le pipeline écrit avec BOM (`encoding="utf-8-sig"` dans ses scripts). Sans cela
    la première colonne s'appellerait « ﻿reservationId » et toutes les lectures par nom
    échoueraient silencieusement sur elle seule.
    """
    texte = octets.decode("utf-8-sig", errors="replace")
    return list(csv.DictReader(io.StringIO(texte), delimiter=separateur))


def _txt(v) -> str:
    return "" if v is None else str(v).strip()


def _nombre(v):
    """Valeur numérique, ou None. Une cellule vide n'est pas un zéro."""
    s = _txt(v)
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


def _identifiant(v):
    """Identifiant Hostaway dans le TYPE que l'API rend : un entier.

    Un fichier délimité n'a qu'un type, le texte. Rendre `listingMapId` sous forme de chaîne
    paraît anodin et ne l'est pas : le moteur compare cet identifiant aux `hostaway_listing_id` du
    référentiel des logements, qui sont des entiers. `"481998" in {481998, …}` est faux, et
    l'extraction d'essai a effectivement classé 1 589 réservations « listing orphelin » — une
    alarme de masse, purement typographique, sur des logements parfaitement connus.

    Rendre le type de l'API, et non celui du fichier, est donc la seule manière d'obtenir du
    moteur le même verdict quelle que soit la provenance.
    """
    s = _txt(v)
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return s


# ── Lecture de l'état du dépôt ──────────────────────────────────────────────────────────────────

def etat(racine, *, remote: str = REMOTE_DEFAUT, branche: str = BRANCHE_DEFAUT,
         rafraichir: bool = True) -> dict:
    """Ce que le dépôt publie AUJOURD'HUI : commit, date de production, fichiers présents.

    `rafraichir` déclenche un `git fetch` : sans lui, on lirait l'état connu du poste, qui peut
    dater de plusieurs jours — exactement le genre de fraîcheur mensongère qu'on veut éviter.
    """
    racine = Path(racine)
    erreur = ""
    if rafraichir:
        try:
            _git(racine, "fetch", remote, branche)
        except DepotIndisponible as exc:
            # Le réseau peut manquer ; l'état local reste lisible et honnête, à condition de dire
            # qu'il n'a pas pu être rafraîchi.
            erreur = str(exc)

    reference = f"{remote}/{branche}"
    try:
        ligne = _git(racine, "log", "-1", "--format=%H%x09%cI", reference).strip()
    except DepotIndisponible as exc:
        return {"disponible": False, "reference": reference, "erreur": erreur or str(exc)}
    if not ligne:
        return {"disponible": False, "reference": reference,
                "erreur": erreur or f"Aucun commit sur {reference}."}

    commit, _, horodatage = ligne.partition("\t")
    try:
        presents = set(_git(racine, "ls-tree", "--name-only", reference).split())
    except DepotIndisponible as exc:
        return {"disponible": False, "reference": reference, "commit": commit,
                "erreur": erreur or str(exc)}

    manquants = [f for f in (FICHIER_RESERVATIONS, FICHIER_FINANCE_FIELDS) if f not in presents]
    return {
        "disponible": not manquants,
        "reference": reference,
        "remote": remote,
        "branche": branche,
        "commit": commit,
        "commit_court": commit[:10],
        # Date à laquelle le pipeline a PRODUIT ces données — distincte de l'heure à laquelle cette
        # installation les importera. Confondre les deux, c'est mentir sur la fraîcheur.
        "source_horodatage": horodatage.strip(),
        "fichiers_manquants": manquants,
        # Tâches de ménage publiées dans CE commit ? Indépendant de `disponible` : un dépôt sans
        # tâches reste lisible pour les réservations.
        "cleaning_tasks_publiees": FICHIER_CLEANING_TASKS in presents,
        "rafraichi": not erreur,
        "erreur": erreur or ("Fichiers absents du dépôt : " + ", ".join(manquants)
                             if manquants else ""),
    }


# ── La source proprement dite ───────────────────────────────────────────────────────────────────

class SourceDepotGitHub:
    """Présente le dépôt de données comme un client Hostaway.

    Même surface que `HostawayClient`, donc `lot1_hostaway_extract.py` n'a pas à savoir lequel des
    deux il manipule : c'est ce qui garantit qu'aucune règle métier ne se dédouble.
    """

    def __init__(self, racine, log, *, remote: str = REMOTE_DEFAUT,
                 branche: str = BRANCHE_DEFAUT, rafraichir: bool = True):
        self._racine = Path(racine)
        self._log = log
        self._etat = etat(self._racine, remote=remote, branche=branche, rafraichir=rafraichir)
        if not self._etat.get("disponible"):
            raise DepotIndisponible(
                self._etat.get("erreur") or "Dépôt de données Hostaway illisible.")
        self._reference = self._etat["reference"]
        self._reservations: list[dict] | None = None
        self._finance_par_reservation: dict[str, list[dict]] | None = None

    # ── Métadonnées ──────────────────────────────────────────────────────────────────────────

    @property
    def etat_source(self) -> dict:
        return dict(self._etat)

    @property
    def source_ref(self) -> str:
        return self._etat["commit"]

    @property
    def source_horodatage(self) -> str:
        return self._etat["source_horodatage"]

    # ── Chargement paresseux ─────────────────────────────────────────────────────────────────

    def _fichier(self, nom: str) -> bytes:
        return _git(self._racine, "show", f"{self._reference}:{nom}", binaire=True)

    def _charger(self) -> None:
        if self._reservations is not None:
            return
        brut = _lire_tsv(self._fichier(FICHIER_RESERVATIONS))
        self._reservations = [r for r in brut if _txt(r.get("reservationId"))]
        self._log.info(
            f"  Dépôt {self._etat['commit_court']} : {len(self._reservations)} réservations "
            f"publiées le {self._etat['source_horodatage']}.")

        champs = _lire_tsv(self._fichier(FICHIER_FINANCE_FIELDS))
        par_reservation: dict[str, list[dict]] = {}
        ignores = 0
        for ligne in champs:
            # `isDeleted` : un champ financier supprimé côté Hostaway ne doit pas peser dans un
            # calcul de payout. Le pipeline publie la colonne ; l'ignorer reviendrait à ressusciter
            # des montants que la plateforme a retirés.
            if _txt(ligne.get("isDeleted")) in ("1", "1.0", "True", "true"):
                ignores += 1
                continue
            nom = _txt(ligne.get("name"))
            reservation = _txt(ligne.get("reservationId"))
            if not nom or not reservation:
                continue
            par_reservation.setdefault(reservation, []).append({
                "name": nom,
                "value": _nombre(ligne.get("value")),
                "currency": "EUR",
            })
        self._finance_par_reservation = par_reservation
        self._log.info(
            f"  Dépôt : champs financiers pour {len(par_reservation)} réservations"
            + (f" ({ignores} champ(s) supprimé(s) écarté(s))." if ignores else "."))

    # ── Mise en forme « comme l'API » ────────────────────────────────────────────────────────

    def _reservation_api(self, ligne: dict) -> dict:
        """Une ligne du dépôt, présentée comme la liste `/v1/reservations` la présenterait.

        Les noms de champs sont ceux de l'API — ce sont d'ailleurs ceux que le pipeline a
        recopiés. Aucune valeur n'est inventée : un champ que le dépôt ne porte pas reste absent,
        et le moteur en aval le traite comme absent, ce qu'il est.
        """
        identifiant = _txt(ligne.get("reservationId"))
        return {
            "id": _identifiant(identifiant),
            "listingMapId": _identifiant(ligne.get("listingMapId")),
            "listingName": _txt(ligne.get("listingName")) or None,
            # `resolve_channel` essaie `source` puis `channelName` : le dépôt ne publie que le
            # second, et c'est celui que l'API renseigne pour ce compte (constaté : airbnbOfficial,
            # bookingcom, vrboical, direct).
            "channelName": _txt(ligne.get("channelName")) or None,
            "channelId": _identifiant(ligne.get("channelId")),
            "reservationId": _txt(ligne.get("reservationCode")) or None,
            "guestName": _txt(ligne.get("guestName")) or None,
            "arrivalDate": _txt(ligne.get("arrivalDate")) or None,
            "departureDate": _txt(ligne.get("departureDate")) or None,
            "nights": _nombre(ligne.get("nights")),
            "numberOfGuests": _nombre(ligne.get("numberOfGuests")),
            "status": _txt(ligne.get("status")) or None,
            "paymentStatus": _txt(ligne.get("paymentStatus")) or None,
            "totalPrice": _nombre(ligne.get("totalPrice")),
            "currency": _txt(ligne.get("currency")) or None,
            "reservationDate": _txt(ligne.get("reservationDate")) or None,
            "insertedOn": _txt(ligne.get("insertedOn")) or None,
            "updatedOn": _txt(ligne.get("updatedOn")) or None,
            "latestActivityOn": _txt(ligne.get("latestActivityOn")) or None,
            # Champs financiers rattachés : c'est par eux que le calcul de payout du moteur trouve
            # `airbnbPayoutSum`, `totalPriceFromChannel`, `cityTax`, `otaPaymentProcessingFee` et
            # `hostChannelFee` — tous présents dans le jeu publié.
            "financeField": list(self._finance_par_reservation.get(identifiant, [])),
        }

    # ── Surface identique à HostawayClient ───────────────────────────────────────────────────

    def count_reservations(self, date_from: str) -> int:
        """Nombre de réservations publiées.

        AUCUN FILTRE DE DATE N'EST APPLIQUÉ, et `date_from` est donc ignoré volontairement. Le
        pipeline publie l'ensemble complet ; le paramètre `dateFrom` de l'API filtre selon une
        sémantique que ce dépôt ne documente pas (ni la date d'arrivée ni la date de départ ne
        reproduisent le compte observé côté API). Reproduire ce filtre au jugé écarterait des
        réservations sans le dire. Un surensemble est sans danger — l'aval travaille par mois
        sélectionné — là où un sous-ensemble erroné est indétectable.
        """
        self._charger()
        return len(self._reservations)

    def get_reservations_page(self, date_from: str, offset: int) -> list:
        self._charger()
        from app.adapters.hostaway_client import PAGE_SIZE
        return [self._reservation_api(l)
                for l in self._reservations[offset:offset + PAGE_SIZE]]

    def get_reservation_detail(self, res_id) -> dict:
        """Vue la plus détaillée que le dépôt possède pour une réservation.

        Le pipeline n'appelle pas `/v1/reservations/{id}` : il n'existe donc pas de charge utile
        plus riche que la ligne de liste, enrichie de ses champs financiers. C'est cette vue qui
        est rendue — pas un objet fabriqué, et pas non plus un dictionnaire vide qui ferait croire
        à une réservation introuvable.
        """
        self._charger()
        cible = _txt(res_id)
        for ligne in self._reservations:
            if _txt(ligne.get("reservationId")) == cible:
                return self._reservation_api(ligne)
        raise DonneeNonFournieParCetteSource(
            f"Réservation {cible} absente du dépôt {self._etat['commit_court']}.")

    def get_listings(self) -> list:
        """Annonces DÉDUITES des réservations : identifiant et nom, rien de plus.

        Le pipeline n'appelle pas `/v1/listings`. Ce qu'on sait avec certitude, c'est que ces
        annonces existent et comment elles s'appellent, parce que des réservations les désignent.
        Tout le reste — ville, statut d'activité, statuts d'export Airbnb/Booking — reste NULL.
        Écrire « actif = OUI » faute de mieux transformerait une ignorance en affirmation, et rien
        dans l'application ne pourrait plus distinguer les deux.

        Sans conséquence métier : `hostaway_listings` n'alimente aucun calcul (le parc réel est
        `ref_logements`), elle sert de trace de ce que la plateforme exposait.
        """
        self._charger()
        from app.adapters.hostaway_client import row_hash, now_utc

        vues: dict = {}
        for ligne in self._reservations:
            identifiant = _identifiant(ligne.get("listingMapId"))
            if identifiant is not None and identifiant not in vues:
                vues[identifiant] = _txt(ligne.get("listingName"))
        return [{
            "id": identifiant,
            "listingMapId": identifiant,
            "name": nom or None,
            "internalListingName": None,
            "city": None,
            "specialStatus": None,
            "airbnbExportStatus": None,
            "bookingcomExportStatus": None,
            "_deduit_des_reservations": True,
            "_extrait_le": now_utc(),
            "_row_hash": row_hash(identifiant, "depot"),
        } for identifiant, nom in vues.items()]

    def get_tasks(self, date_from: str) -> list:
        """Tâches de ménage publiées (`cleaning_tasks_hostaway.tsv`), présentées comme `/v1/tasks`.

        AUCUN FILTRE DE DATE, comme `count_reservations` : le pipeline applique déjà son propre
        `dateFrom`, et rejouer ce filtre au jugé écarterait des tâches sans le dire — l'aval
        travaille par mois. Un fichier ABSENT n'est pas « aucune tâche » : il lève.
        """
        if not self._etat.get("cleaning_tasks_publiees"):
            raise DonneeNonFournieParCetteSource(
                f"Les tâches de ménage ne sont pas publiées dans le dépôt "
                f"{self._etat['commit_court']} ({FICHIER_CLEANING_TASKS} absent). Rendre une "
                "liste vide ferait passer « non fourni » pour « aucune tâche ».")
        vues: dict = {}
        for ligne in _lire_tsv(self._fichier(FICHIER_CLEANING_TASKS)):
            identifiant = _identifiant(ligne.get("id"))
            if identifiant is None or identifiant in vues:
                continue
            vues[identifiant] = {
                "id": identifiant,
                "reservationId": _identifiant(ligne.get("reservationId")),
                "listingMapId": _identifiant(ligne.get("listingMapId")),
                "title": _txt(ligne.get("title")) or None,
                "status": _txt(ligne.get("status")) or None,
                "taskType": _txt(ligne.get("taskType")) or None,
                "type": _txt(ligne.get("type")) or None,
                "canStartFrom": _txt(ligne.get("canStartFrom")) or None,
                "shouldEndBy": _txt(ligne.get("shouldEndBy")) or None,
                "assigneeUserId": _identifiant(ligne.get("assigneeUserId")),
            }
        self._log.info(f"  Dépôt {self._etat['commit_court']} : {len(vues)} tâches de ménage "
                       "publiées.")
        return list(vues.values())

    # ── Trace ────────────────────────────────────────────────────────────────────────────────

    def resume(self) -> str:
        return json.dumps({
            "source": MODE,
            "reference": self._reference,
            "commit": self.source_ref,
            "produit_le": self.source_horodatage,
        }, ensure_ascii=False)
