"""Relevé propriétaire — ÉCRAN ÉCONOMIQUE (§13-§15).

CE QUE CET ÉCRAN EST, ET CE QU'IL N'EST PAS
Il répond à une seule question : **ce parc a-t-il bien travaillé ce mois-ci ?** Combien de séjours,
combien de nuits, remplies à quel taux, à quel prix moyen, pour quel net.

Il ne traite donc ni créances, ni règlements, ni compensation, ni aucun geste de trésorerie. Ces
sujets existent, ils ont leurs écrans, et les mélanger ici produisait un document que personne ne
pouvait lire d'un bout à l'autre : on y cherchait une performance et on y trouvait un état de
compte.

AUCUN CHIFFRE N'EST RECALCULÉ ICI
Le total perçu, la commission et le net viennent de `lot10_net_reglement`, la sortie du moteur.
Les compter à nouveau depuis les réservations donnerait un second résultat — proche, jamais
identique — et personne ne saurait lequel fait foi. Ce service AGRÈGE et met en forme ; il ne
décide de rien.

Ce qu'il calcule, ce sont les indicateurs d'activité que le moteur ne produit pas, et dont les
formules sont posées explicitement :

    durée moyenne          = Σ nuits / nombre de réservations
    voyageurs moyens       = Σ voyageurs connus / nombre de réservations PORTANT la donnée
    taux de remplissage    = nuits occupées / nuits commercialisables
    ADR                    = Σ total perçu / Σ nuits occupées
    ADR net propriétaire   = Σ net propriétaire / Σ nuits occupées

VOYAGEURS MOYENS : LE DÉNOMINATEUR N'EST PAS LE MÊME
Il ne compte que les réservations qui PORTENT un nombre de voyageurs. Diviser par le total ferait
baisser la moyenne à chaque donnée manquante, et une donnée absente deviendrait un séjour à zéro
voyageur. L'écran dit sur combien de réservations la moyenne est établie.

NUITS COMMERCIALISABLES
Somme, logement par logement, des jours du mois où le logement était SOUS GESTION — pas les jours
du mois multipliés par le nombre de logements. Un logement entré en gestion le 15 n'était pas
commercialisable du 1er au 14, et l'inclure ferait apparaître un taux de remplissage artificiellement
bas, qu'aucune action ne pourrait corriger.
"""
from __future__ import annotations

from calendar import monthrange
from datetime import date
from typing import Any

from app.db.connection import get_db

ST_OK = "OK"
ST_SANS_DONNEES = "SANS_DONNEES"
ST_INDISPONIBLE = "INDISPONIBLE"

# Une réservation ne compte dans l'activité que si elle est retenue au résultat réel. Les exclues
# (annulations sans payout, séjours propriétaire) existent, mais compter leurs nuits ferait monter
# un taux de remplissage sans qu'un euro soit entré.
IMPACT_RETENU = "OUI"


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _f(v: Any) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _arrondi(v: float, n: int = 2) -> float:
    return round(v + 0.0, n)


def mois_valide(mois: str) -> bool:
    m = _txt(mois)
    if len(m) != 7 or m[4] != "-":
        return False
    try:
        annee, numero = int(m[:4]), int(m[5:])
    except ValueError:
        return False
    return 1 <= numero <= 12 and 2000 <= annee <= 2100


def mois_courant() -> str:
    return date.today().strftime("%Y-%m")


def _bornes(mois: str) -> tuple[date, date]:
    annee, numero = int(mois[:4]), int(mois[5:])
    return date(annee, numero, 1), date(annee, numero, monthrange(annee, numero)[1])


# ── Nuits commercialisables ─────────────────────────────────────────────────────────────────────

def _jours_sous_gestion(ligne: dict[str, Any], debut: date, fin: date) -> int:
    """Jours du mois où ce logement était effectivement sous gestion.

    Les bornes de la période de gestion sont intersectées avec le mois. Une période sans date de
    fin est ouverte ; une période sans date de début est traitée comme couvrant tout le mois — on
    ne fabrique pas une date d'entrée qui n'est pas écrite.
    """
    def _date(valeur: Any, defaut: date) -> date:
        texte = _txt(valeur)[:10]
        if not texte:
            return defaut
        try:
            return date.fromisoformat(texte)
        except ValueError:
            return defaut

    d = max(_date(ligne.get("date_debut"), debut), debut)
    f = min(_date(ligne.get("date_fin"), fin), fin)
    return max((f - d).days + 1, 0)


def nuits_commercialisables(proprietaire_id: str, mois: str, *, db_path=None) -> dict[str, Any]:
    """Capacité du parc du propriétaire sur le mois, logement par logement."""
    from app.moteurs.charges_engine import gestion_active_pour_mois

    debut, fin = _bornes(mois)
    conn = get_db(db_path)
    try:
        lignes = [dict(r) for r in conn.execute(
            "SELECT logement_id, proprietaire_id, date_debut, date_fin, statut_gestion "
            "FROM ref_gestion_logements_hist WHERE proprietaire_id = ?", (proprietaire_id,))]
    finally:
        conn.close()

    par_logement: dict[str, int] = {}
    for ligne in lignes:
        if not gestion_active_pour_mois(ligne, mois):
            continue
        identifiant = _txt(ligne.get("logement_id"))
        if not identifiant:
            continue
        jours = _jours_sous_gestion(ligne, debut, fin)
        # Deux périodes de gestion peuvent se succéder dans le même mois (changement de contrat) :
        # leurs jours s'additionnent, plafonnés à la durée du mois.
        par_logement[identifiant] = min(par_logement.get(identifiant, 0) + jours,
                                        (fin - debut).days + 1)
    return {"par_logement": par_logement, "total": sum(par_logement.values()),
            "nb_logements": len(par_logement), "jours_du_mois": (fin - debut).days + 1}


# ── Activité, depuis les réservations résolues ──────────────────────────────────────────────────

def _reservations(proprietaire_id: str, mois: str, *, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        dataset = conn.execute(
            "SELECT dataset_id FROM reservations_datasets WHERE etape = 'RESOLUES' AND actif = 1 "
            "ORDER BY id DESC LIMIT 1").fetchone()
        if dataset is None:
            return []
        return [dict(r) for r in conn.execute(
            "SELECT logement_id, canal, nuits, guest_count, montant_retenu, payout_calcule, "
            "menage_retenu, date_arrivee, date_depart, impact_resultat_reel, statut_controle "
            "FROM reservations_resolues WHERE dataset_id = ? AND mois = ? AND proprietaire_id = ?",
            (dataset["dataset_id"], mois, proprietaire_id))]
    finally:
        conn.close()


def _agreger_activite(lignes: list[dict[str, Any]]) -> dict[str, Any]:
    retenues = [l for l in lignes if _txt(l.get("impact_resultat_reel")).upper() == IMPACT_RETENU]
    nb = len(retenues)
    nuits = sum(int(_f(l.get("nuits"))) for l in retenues)

    # Dénominateur PROPRE aux voyageurs : seules les réservations qui portent la donnée.
    avec_voyageurs = [l for l in retenues if l.get("guest_count") not in (None, "", 0)]
    voyageurs = sum(int(_f(l.get("guest_count"))) for l in avec_voyageurs)

    canaux: dict[str, dict[str, Any]] = {}
    for ligne in retenues:
        canal = _txt(ligne.get("canal")) or "INCONNU"
        agrege = canaux.setdefault(canal, {"canal": canal, "reservations": 0, "nuits": 0,
                                           "montant": 0.0})
        agrege["reservations"] += 1
        agrege["nuits"] += int(_f(ligne.get("nuits")))
        agrege["montant"] = _arrondi(agrege["montant"] + _f(ligne.get("montant_retenu")))

    for agrege in canaux.values():
        agrege["part_reservations"] = _arrondi(100 * agrege["reservations"] / nb, 1) if nb else 0.0
        agrege["part_nuits"] = _arrondi(100 * agrege["nuits"] / nuits, 1) if nuits else 0.0

    return {
        "nb_reservations": nb,
        "nuits_occupees": nuits,
        "duree_moyenne": _arrondi(nuits / nb, 1) if nb else None,
        "voyageurs_moyens": _arrondi(voyageurs / len(avec_voyageurs), 1) if avec_voyageurs else None,
        "nb_reservations_avec_voyageurs": len(avec_voyageurs),
        "nb_reservations_sans_voyageurs": nb - len(avec_voyageurs),
        "canaux": sorted(canaux.values(), key=lambda c: (-c["nuits"], c["canal"])),
        "nb_exclues": len(lignes) - nb,
    }


# ── Économie, depuis la sortie du moteur ────────────────────────────────────────────────────────

def _economie(proprietaire_id: str, mois: str, *, db_path=None) -> dict[str, Any]:
    """Total perçu, ménages, commission, net — LUS, jamais recalculés.

    LA LECTURE PASSE PAR LE READER, ET C'EST ESSENTIEL.
    `lot10_net_reglement` conserve TOUS les runs du moteur : six coexistent sur cette base. Une
    requête directe sur (mois, propriétaire) additionne donc six calculs du même mois — un premier
    essai rendait 12 046 € là où le mois vaut 1 204 €, avec des ADR à quatre chiffres qui seuls ont
    trahi l'erreur. `proprietaires_reglements_reader` filtre sur le run ACTIF, désigné par
    `lot10_runs.actif`. C'est le même run que toutes les autres lectures Lot10 : cet écran ne peut
    donc pas diverger des Créances ou du Compte propriétaire — ils lisent le même calcul.
    """
    from app.readers import proprietaires_reglements_reader as lot10

    lignes = [l for l in lot10.net_reglement(db_path=db_path).lignes
              if _txt(l.get("mois")) == _txt(mois)
              and _txt(l.get("proprietaire_id")) == _txt(proprietaire_id)]
    lignes.sort(key=lambda l: _txt(l.get("logement_id")))

    return {
        "par_logement": lignes,
        "total_percu": _arrondi(sum(_f(l.get("total_payout_mois")) for l in lignes)),
        "menages": _arrondi(sum(_f(l.get("total_menage_mois")) for l in lignes)),
        "commission": _arrondi(sum(_f(l.get("total_commission_mois")) for l in lignes)),
        "preparation_canape": _arrondi(
            sum(_f(l.get("total_preparation_canape_mois")) for l in lignes)),
        "net_proprietaire": _arrondi(
            sum(_f(l.get("net_proprietaire_avant_charge_mois")) for l in lignes)),
    }


# ── Le relevé ───────────────────────────────────────────────────────────────────────────────────

def releve(proprietaire_id: str, mois: str, *, db_path=None) -> dict[str, Any]:
    """Relevé de performance d'un propriétaire pour un mois."""
    from app.services import referentiel_service as ref

    identifiant = _txt(proprietaire_id)
    periode = _txt(mois)
    if not identifiant or not mois_valide(periode):
        return {"status": ST_INDISPONIBLE, "message": "Propriétaire ou mois non renseigné.",
                "proprietaire_id": identifiant, "mois": periode}

    activite = _agreger_activite(_reservations(identifiant, periode, db_path=db_path))
    economie = _economie(identifiant, periode, db_path=db_path)
    capacite = nuits_commercialisables(identifiant, periode, db_path=db_path)

    nuits = activite["nuits_occupees"]
    commercialisables = capacite["total"]

    #: Un mois non écoulé ne se juge pas comme un mois clos : ses réservations à venir ne sont pas
    #: encore des nuits réalisées, et son chiffre bougera encore. L'écran le signale au lieu de
    #: présenter un résultat partiel comme définitif.
    provisoire = periode >= mois_courant()

    par_logement = _detail_par_logement(activite, economie, capacite, identifiant, periode,
                                        db_path=db_path)

    resultat = {
        "status": ST_OK if (activite["nb_reservations"] or economie["par_logement"])
                  else ST_SANS_DONNEES,
        "proprietaire_id": identifiant,
        "proprietaire": ref.libelle_proprietaire(identifiant, db_path=db_path),
        "mois": periode,
        "provisoire": provisoire,
        **activite,
        **economie,
        "nuits_commercialisables": commercialisables,
        "nb_logements": capacite["nb_logements"],
        "jours_du_mois": capacite["jours_du_mois"],
        "taux_remplissage": _arrondi(100 * nuits / commercialisables, 1)
                            if commercialisables else None,
        "adr": _arrondi(economie["total_percu"] / nuits) if nuits else None,
        "adr_net_proprietaire": _arrondi(economie["net_proprietaire"] / nuits) if nuits else None,
        "logements": par_logement,
        "formules": FORMULES,
    }

    # UN TAUX ABSENT DOIT DIRE POURQUOI.
    #
    # Sans dénominateur, aucun taux de remplissage n'est calculable — et afficher « — » laisse
    # croire à un détail d'affichage. Le cas rencontré est tout autre : des séjours et des recettes
    # existent sur un mois où AUCUNE période de gestion n'est ouverte. C'est une contradiction dans
    # les données, pas une case vide, et elle appelle une décision humaine.
    if commercialisables == 0 and (nuits or economie["total_percu"]):
        resultat["capacite_inconnue"] = (
            "Aucune période de gestion ouverte sur ce mois pour ce propriétaire, alors que des "
            "séjours et des recettes y figurent. Le taux de remplissage n'a donc pas de "
            "dénominateur. À vérifier : la date de fin de gestion, ou le rattachement de ces "
            "réservations.")
    return resultat


def _detail_par_logement(activite: dict[str, Any], economie: dict[str, Any],
                         capacite: dict[str, Any], proprietaire_id: str, mois: str,
                         *, db_path=None) -> list[dict[str, Any]]:
    """Le même relevé, logement par logement. Aucun code technique n'y figure : un identifiant
    `LOG_0004` ne dit rien à qui lit le document."""
    from app.services import referentiel_service as ref

    lignes = _reservations(proprietaire_id, mois, db_path=db_path)
    par_logement: dict[str, dict[str, Any]] = {}
    for ligne in lignes:
        if _txt(ligne.get("impact_resultat_reel")).upper() != IMPACT_RETENU:
            continue
        identifiant = _txt(ligne.get("logement_id"))
        agrege = par_logement.setdefault(identifiant, {"reservations": 0, "nuits": 0})
        agrege["reservations"] += 1
        agrege["nuits"] += int(_f(ligne.get("nuits")))

    economie_par_logement = {_txt(l["logement_id"]): l for l in economie["par_logement"]}
    identifiants = sorted(set(par_logement) | set(economie_par_logement)
                          | set(capacite["par_logement"]))

    resultat = []
    for identifiant in identifiants:
        activite_log = par_logement.get(identifiant, {"reservations": 0, "nuits": 0})
        economie_log = economie_par_logement.get(identifiant, {})
        commercialisables = capacite["par_logement"].get(identifiant, 0)
        nuits = activite_log["nuits"]
        percu = _f(economie_log.get("total_payout_mois"))
        net = _f(economie_log.get("net_proprietaire_avant_charge_mois"))
        resultat.append({
            "logement_id": identifiant,
            "logement": ref.nom_logement(identifiant, db_path=db_path),
            "reservations": activite_log["reservations"],
            "nuits": nuits,
            "nuits_commercialisables": commercialisables,
            "taux_remplissage": _arrondi(100 * nuits / commercialisables, 1)
                                if commercialisables else None,
            "total_percu": _arrondi(percu),
            "menages": _arrondi(_f(economie_log.get("total_menage_mois"))),
            "commission": _arrondi(_f(economie_log.get("total_commission_mois"))),
            "net_proprietaire": _arrondi(net),
            "adr": _arrondi(percu / nuits) if nuits else None,
            "adr_net_proprietaire": _arrondi(net / nuits) if nuits else None,
        })
    return resultat


#: Chaque indicateur dit COMMENT il est obtenu. Un taux qu'on ne sait pas reconstituer ne se
#: discute pas : il se subit.
FORMULES: dict[str, dict[str, str]] = {
    "nb_reservations": {
        "libelle": "Réservations",
        "formule": "Séjours retenus au résultat réel du mois",
        "source": "reservations_resolues"},
    "nuits_occupees": {
        "libelle": "Nuits occupées",
        "formule": "Σ nuits des réservations retenues",
        "source": "reservations_resolues"},
    "duree_moyenne": {
        "libelle": "Durée moyenne des séjours",
        "formule": "Σ nuits ÷ nombre de réservations",
        "source": "reservations_resolues"},
    "voyageurs_moyens": {
        "libelle": "Voyageurs moyens",
        "formule": "Σ voyageurs connus ÷ nombre de réservations PORTANT la donnée",
        "source": "reservations_resolues"},
    "taux_remplissage": {
        "libelle": "Taux de remplissage",
        "formule": "Nuits occupées ÷ nuits commercialisables (jours réellement sous gestion)",
        "source": "reservations_resolues + ref_gestion_logements_hist"},
    "total_percu": {
        "libelle": "Total perçu",
        "formule": "Σ des payouts encaissés du mois",
        "source": "lot10_net_reglement.total_payout_mois"},
    "commission": {
        "libelle": "Commission",
        "formule": "Commission de conciergerie calculée par le moteur",
        "source": "lot10_net_reglement.total_commission_mois"},
    "net_proprietaire": {
        "libelle": "Net propriétaire",
        "formule": "Total perçu − ménages − commission (avant charge fixe et refacturations)",
        "source": "lot10_net_reglement.net_proprietaire_avant_charge_mois"},
    "adr": {
        "libelle": "ADR",
        "formule": "Total perçu ÷ nuits occupées",
        "source": "calculé"},
    "adr_net_proprietaire": {
        "libelle": "ADR net propriétaire",
        "formule": "Net propriétaire ÷ nuits occupées",
        "source": "calculé"},
}


# ── Mois disponibles, pour le sélecteur ─────────────────────────────────────────────────────────

def mois_disponibles(proprietaire_id: str = "", *, db_path=None) -> list[str]:
    from app.readers import proprietaires_reglements_reader as lot10

    cible = _txt(proprietaire_id)
    mois = {_txt(l.get("mois")) for l in lot10.net_reglement(db_path=db_path).lignes
            if (not cible or _txt(l.get("proprietaire_id")) == cible) and _txt(l.get("mois"))}
    return sorted(mois, reverse=True)


def proprietaires_du_mois(mois: str, *, db_path=None) -> list[dict[str, Any]]:
    """Propriétaires ayant une activité sur le mois, avec leur nom — jamais leur code."""
    from app.services import referentiel_service as ref

    from app.readers import proprietaires_reglements_reader as lot10

    identifiants = sorted({_txt(l.get("proprietaire_id"))
                           for l in lot10.net_reglement(db_path=db_path).lignes
                           if _txt(l.get("mois")) == _txt(mois) and _txt(l.get("proprietaire_id"))})
    return [{"proprietaire_id": i, "libelle": ref.libelle_proprietaire(i, db_path=db_path)}
            for i in identifiants]
