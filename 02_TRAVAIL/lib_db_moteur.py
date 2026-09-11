"""Acces SQLite pour les lots moteur — resolution de base et lecture/ecriture des datasets.

POURQUOI CETTE LIB
`lot8b` avait deja etabli la regle de resolution de la base : --db, puis PILOTAGE_DB_PATH, puis
APP_DATA_DIR/app.db, et JAMAIS de defaut vers la base reelle. Lot4bis, Lot4ter et Lot4quater ont le
meme besoin. La recopier trois fois garantirait qu'une des copies derive ; elle vit donc ici.

AUCUN DEFAUT VERS LA BASE REELLE
C'est la garantie principale. Un lot moteur lance sans precision ne doit pas tomber par accident sur
la base de production : il rend None et l'appelant refuse, plutot que d'ecrire quelque part au hasard.

PAS D'IMPORT DE L'APPLICATION
Les lots tournent sous l'interpreteur qui porte pandas ; l'application, sous un autre. Importer
`05_APPLICATION` depuis un lot lierait les deux environnements. On parle donc SQL directement, sur le
schema que les migrations ont cree.
"""
from __future__ import annotations

import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

SOURCE_SQLITE = "SQLITE"
SOURCE_EXCEL = "EXCEL"
SOURCE_AUTO = "AUTO"
SOURCES = (SOURCE_SQLITE, SOURCE_EXCEL, SOURCE_AUTO)

ETAPE_CALCULEES = "CALCULEES"
ETAPE_RESOLUES = "RESOLUES"

ST_SUCCES = "SUCCES"
ST_PARTIEL = "PARTIEL"
ST_ECHEC = "ECHEC"

# ── Statuts de facture qui engagent l'economie ──────────────────────────────────────────────────
# Une facture fournisseur RECUE n'est pas une charge economique : tant qu'un humain ne l'a pas
# validee, son montant ne doit alimenter ni le cout reel (lot6e) ni le cout complet (lot6f), donc ni
# TYPE_FLUX_014/018/019 dans lot9. Le defaut inverse a reellement existe : les requetes
# `facture_lignes_menage JOIN factures` ne filtraient que `type_ligne`, si bien qu'une ligne d'une
# facture A_CONTROLER traversait toute la chaine economique.
#
# SOURCE DE VERITE : `05_APPLICATION/app/services/factures_service.STATUTS_COMPTABLES`.
# Recopie ici et non importee : cette lib documente plus haut qu'un lot moteur n'importe jamais
# l'application (interpreteurs distincts). La copie est verrouillee par un test de synchronisation
# qui echoue si les deux ensembles divergent.
#
# VALIDEE seule ne suffirait pas : une facture PARTIELLEMENT_REGLEE ou REGLEE a ete validee puis
# payee, elle est economiquement reelle. Ne garder que VALIDEE ferait disparaitre du resultat une
# charge pourtant reglee.
STATUTS_FACTURE_COMPTABLES = ("VALIDEE", "PARTIELLEMENT_REGLEE", "REGLEE")


# ── EXCLUSION DU CALCUL ECONOMIQUE — vocabulaire canonique ────────────────────────────────────────
#
# CE QUE CECI REMPLACE : le code d'impact `HR` (« hors resultat »).
#
# `HR` disait « cette ligne ne compte ni au resultat reel ni en comptabilite ». Mais un CODE
# D'IMPACT decrit COMMENT une somme pese sur l'economie ; il ne peut pas dire qu'une ligne est
# HORS de cette economie. Les deux idees se confondaient dans un meme champ, et `HR` a fini par
# recouvrir TROIS realites sans rapport :
#   · une depense sans effet nulle part          -> ce n'etait pas une charge ; code supprime ;
#   · une reservation sans vente (sejour proprietaire, annulation, logement hors parc) ;
#   · une ligne de SUIVI associe (lot7), qui trace sans jamais rien produire.
#
# Les deux dernieres ne sont pas des impacts : ce sont des EXCLUSIONS. Elles sont donc dites par
# un statut et un motif, pas par un code d'impact. Une ligne exclue porte `code_impact = NULL` :
# elle n'a pas d'impact « neutre », elle n'a pas d'impact du tout.
#
# L'exclusion etait DEJA portee par `statut_controle` (`EXCLU_RESULTAT` depuis l'origine, partage
# par lot4bis, lot5, lot7 et la saisie HH) : `HR` n'en etait qu'une seconde ecriture, redondante.
# `lot4bis` le montrait sans le dire — il forcait deja l'impact a NON/NON des que le statut valait
# `EXCLU_RESULTAT`, quel que soit le code. Ce qui manquait etait le MOTIF, jusqu'ici noye dans un
# commentaire libre et donc non requetable.
STATUT_EXCLU_RESULTAT = "EXCLU_RESULTAT"
STATUT_EXCLU_LEGACY = "EXCLU_LEGACY"
#: Statuts qui SORTENT une ligne du calcul economique. Ils ne disent pas « a verifier » : la
#: decision est prise, elle est justifiee, et elle est definitive pour la periode.
STATUTS_EXCLUSION = (STATUT_EXCLU_RESULTAT, STATUT_EXCLU_LEGACY)

MOTIF_OWNERSTAY = "OWNERSTAY"
MOTIF_STATUT_HORS_PERIMETRE = "STATUT_HOSTAWAY_HORS_PERIMETRE"
MOTIF_HORS_PARC_TECHNIQUE = "HORS_PARC_TECHNIQUE"
MOTIF_STATUT_PARC_INVALIDE = "STATUT_PARC_INVALIDE"
MOTIF_LEGACY_SANS_ARCHIVE = "LEGACY_SANS_ARCHIVE_ORIGINE"
MOTIF_SUIVI_ASSOCIE = "SUIVI_ASSOCIE"
MOTIFS_EXCLUSION = (
    MOTIF_OWNERSTAY,               # sejour du proprietaire : occupation reelle, aucune vente
    MOTIF_STATUT_HORS_PERIMETRE,   # statut Hostaway hors {new, modified} (annulee, etc.)
    MOTIF_HORS_PARC_TECHNIQUE,     # logement marque hors parc
    MOTIF_STATUT_PARC_INVALIDE,    # statut de parc vide ou invalide
    MOTIF_LEGACY_SANS_ARCHIVE,     # mois de bascule sans archive economique d'origine
    MOTIF_SUIVI_ASSOCIE,           # ligne de suivi associe (lot7) : trace, ne produit rien
)


def est_exclue(ligne) -> bool:
    """La ligne est-elle hors du calcul economique ?

    Lit le STATUT, pas le code d'impact : c'est le statut qui porte la decision. Une ligne peut
    etre exclue sans motif renseigne (donnee anterieure a la migration 0079) — elle reste exclue.
    """
    statut = ligne.get("statut_controle") if hasattr(ligne, "get") else None
    return str(statut or "").strip().upper() in STATUTS_EXCLUSION


def motif_exclusion_pour(source, statut_controle=None, code_anomalie=None):
    """Motif canonique deduit de ce que les moteurs savent deja d'une ligne exclue.

    Rend `None` quand la ligne n'est pas exclue — l'appelant ecrit alors NULL, ce qui se lit
    « cette ligne participe au calcul ». Ne devine jamais un motif pour une ligne incluse.
    """
    src = str(source or "").strip().upper()
    ano = str(code_anomalie or "").strip().upper()
    if src.startswith("OWNERSTAY"):
        return MOTIF_OWNERSTAY
    if src == MOTIF_STATUT_HORS_PERIMETRE or ano == MOTIF_STATUT_HORS_PERIMETRE:
        return MOTIF_STATUT_HORS_PERIMETRE
    if src == MOTIF_HORS_PARC_TECHNIQUE or ano == MOTIF_HORS_PARC_TECHNIQUE:
        return MOTIF_HORS_PARC_TECHNIQUE
    if ano == MOTIF_STATUT_PARC_INVALIDE:
        return MOTIF_STATUT_PARC_INVALIDE
    if ano == MOTIF_LEGACY_SANS_ARCHIVE:
        return MOTIF_LEGACY_SANS_ARCHIVE
    if str(statut_controle or "").strip().upper() in STATUTS_EXCLUSION:
        # Exclue pour une raison que les champs ne nomment pas : on le dit plutot que d'inventer.
        return MOTIF_LEGACY_SANS_ARCHIVE if statut_controle == STATUT_EXCLU_LEGACY else None
    return None


# ── CONTROLE D'UNE CHARGE — ce qui entre dans les calculs ────────────────────────────────────────
#
# Une charge NON VALIDEE n'alimente aucun calcul operationnel. C'est la regle par defaut, et elle
# vaut pour le cout complet menage, le resultat analytique, les flux unifies et les calculs
# mensuels : tant qu'un humain n'a pas dit oui, la depense existe comme SAISIE, pas comme COUT.
#
# Le depot portait DEUX mots pour le meme etat, ecrits dans la MEME colonne :
#   · `VALIDE`   — pose par l'ecran « Charges a controler » (`charges_validation_service`) ;
#                  c'est le seul que lot9 ingere, et le seul present dans `flux_unifies` ;
#   · `CONFORME` — pose par le bouton « Valider la charge » de la fiche charge.
# Une charge validee depuis la fiche devenait donc `CONFORME`, que lot9 ignorait : l'ecran disait
# « conforme » et la depense restait invisible a l'economie, sans erreur ni avertissement. Le mot
# retenu est `VALIDE`, parce que c'est celui que la chaine economique lit deja (migration 0080).
STATUT_CHARGE_A_CONTROLER = "A_CONTROLER"
STATUT_CHARGE_VALIDE = "VALIDE"
STATUT_CHARGE_ANOMALIE = "ANOMALIE"
STATUT_CHARGE_REJETE = "REJETE"
#: Le SEUL statut qui fait entrer une charge dans un calcul.
STATUTS_CHARGE_CALCULEE = (STATUT_CHARGE_VALIDE,)


def charge_entre_dans_les_calculs(charge) -> bool:
    """La charge alimente-t-elle les calculs ? `statut_controle = VALIDE`, et rien d'autre.

    Une colonne vide vaut A_CONTROLER — « pas encore controlee », jamais « implicitement bonne ».
    C'est la lecture fail-closed : l'absence d'avis ne vaut pas accord.
    """
    if charge is None:
        return False
    lire = charge.get if hasattr(charge, "get") else (lambda k, d=None: d)
    if str(lire("statut", "ACTIVE") or "ACTIVE").strip().upper() == "ANNULEE":
        return False
    return str(lire("statut_controle") or "").strip().upper() in STATUTS_CHARGE_CALCULEE


def impacts_reservation(code_impact, *, motif_exclusion=None, statut_controle=None):
    """(impact_resultat_reel, impact_resultat_comptable) d'une ligne de reservation.

    UNE seule derivation, partagee par les deux branches de lot4bis (Hostaway et hors Hostaway),
    qui en portaient chacune leur version. Elles disaient deja la meme chose, mais rien ne le
    garantissait : la branche HA forcait NON/NON sur `EXCLU_RESULTAT`, la branche HH ne le faisait
    pas et s'en remettait entierement au code `HR`.

    Une ligne EXCLUE rend NON/NON — elle ne pese sur rien, et c'est le motif qui le dit, plus un
    code d'impact. Un code inconnu rend A_CONTROLER/A_CONTROLER : il est vu, pas neutralise.
    """
    if motif_exclusion or str(statut_controle or "").strip().upper() in STATUTS_EXCLUSION:
        return "NON", "NON"
    code = str(code_impact or "").strip().upper()
    if code == "IC":
        return "OUI", "OUI"
    if code == "HC":
        return "OUI", "NON"
    return "A_CONTROLER", "A_CONTROLER"


def filtre_sql_factures_comptables(alias: str = "f") -> str:
    """Fragment SQL restreignant une jointure `factures` aux statuts qui engagent l'economie.

    Rendu en litteraux plutot qu'en parametres : les appelants concatenent ce fragment dans des
    requetes qui portent deja leurs propres parametres positionnels, et melanger les deux est la
    meilleure facon d'introduire un decalage silencieux. Les valeurs sont des constantes internes,
    jamais une entree utilisateur.
    """
    valeurs = ", ".join(f"'{s}'" for s in STATUTS_FACTURE_COMPTABLES)
    return f"{alias}.statut IN ({valeurs})"


def maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def chemin_db(argument: str | None = None) -> Path | None:
    """Base applicative a interroger. Resolue a l'appel, jamais figee a l'import.

    Priorite : argument explicite > PILOTAGE_DB_PATH > APP_DATA_DIR/app.db. None si rien n'est
    designe — l'appelant doit alors refuser, pas deviner.
    """
    if argument:
        return Path(argument)
    env = os.environ.get("PILOTAGE_DB_PATH")
    if env:
        return Path(env)
    data = os.environ.get("APP_DATA_DIR")
    if data:
        return Path(data) / "app.db"
    return None


def ouvrir(chemin: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(chemin))
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# Identifiants Hostaway : TEXTE en base, ENTIERS cote moteur.
#
# Stocker un identifiant externe en texte est correct — le comparer numeriquement n'a pas de sens et
# un zero non significatif serait perdu. Mais tous les index du moteur ont ete construits sur les
# valeurs que le classeur fournissait, c'est-a-dire des entiers : mapping des logements, index des
# payouts, rapprochement live/historique. Une cle texte n'y est jamais trouvee, et l'echec est
# SILENCIEUX — le payout apparait simplement absent, et un revenu disparait sans erreur.
#
# On restitue donc le type attendu a la frontiere, sans changer la logique du moteur.
IDS_HOSTAWAY = ("reservation_id", "reservation_id_hostaway", "listingMapId", "listing_map_id")

PREFIXE_HA = "RES-HA-"
PREFIXE_HH = "RES-HH-"
PREFIXE_LEGACY = "RES-LEGACY-"


def cle_reservation_ha(reservation_id) -> str | None:
    """Identite stable d'une reservation Hostaway : derivee de son identifiant, jamais de sa
    position dans une liste. None si aucun identifiant n'est disponible (ne devrait pas arriver
    pour une reservation venant reellement de l'API)."""
    if reservation_id in (None, ""):
        return None
    return f"{PREFIXE_HA}{reservation_id}"


def cle_reservation_hh(reservation_hh_id) -> str | None:
    """Identite stable d'une reservation hors Hostaway : reservation_hh_id est deja l'identifiant
    opaque persistant attribue a la saisie (ex. RESHH-2026-05-001), on le reutilise tel quel."""
    if reservation_hh_id in (None, ""):
        return None
    return f"{PREFIXE_HH}{reservation_hh_id}"


def entier_si_possible(valeur):
    """Entier quand la valeur en est un, sinon la valeur telle quelle."""
    if valeur is None or isinstance(valeur, int):
        return valeur
    texte = str(valeur).strip()
    if not texte:
        return valeur
    try:
        return int(texte)
    except ValueError:
        return valeur


def traduire(ligne: dict[str, Any], correspondance: dict[str, str] | None = None) -> dict[str, Any]:
    """Renomme les cles d'une ligne SQLite vers le vocabulaire moteur, types d'identifiants compris."""
    correspondance = correspondance or {}
    traduite = {correspondance.get(k, k): v for k, v in ligne.items()}
    for cle in IDS_HOSTAWAY:
        if cle in traduite:
            traduite[cle] = entier_si_possible(traduite[cle])
    return traduite


def table_presente(conn: sqlite3.Connection, nom: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (nom,)).fetchone())


def verifier(chemin: Path | None, tables: Sequence[str]) -> tuple[sqlite3.Connection | None, str]:
    """Ouvre la base et verifie que les tables attendues existent.

    Rend (connexion, message). Connexion None = inutilisable, et le message dit pourquoi : base non
    designee, introuvable, ou migration non appliquee. Trois causes distinctes, trois corrections
    differentes — les confondre ferait chercher au mauvais endroit.
    """
    if chemin is None:
        return None, "aucune base applicative designee (--db / PILOTAGE_DB_PATH / APP_DATA_DIR)"
    if not chemin.exists():
        return None, "base applicative introuvable : %s" % chemin
    conn = ouvrir(chemin)
    manquantes = [t for t in tables if not table_presente(conn, t)]
    if manquantes:
        conn.close()
        return None, "tables absentes (migration non appliquee) : %s" % ", ".join(manquantes)
    return conn, str(chemin)


def lignes(conn: sqlite3.Connection, table: str, colonnes: Sequence[str],
           ou: str = "", args: Sequence[Any] = (), ordre: str = "id") -> list[dict[str, Any]]:
    sql = "SELECT %s FROM %s" % (", ".join(colonnes), table)
    if ou:
        sql += " WHERE " + ou
    sql += " ORDER BY " + ordre
    return [dict(zip(colonnes, r)) for r in conn.execute(sql, tuple(args)).fetchall()]


# ── Extraction Hostaway courante ────────────────────────────────────────────────────────────────

def extraction_utilisable(conn: sqlite3.Connection) -> str:
    """Derniere extraction Hostaway exploitable, ou chaine vide.

    Une extraction ECHEC est ecartee : ses lignes sont un fragment. Une extraction PARTIELLE est
    retenue — incomplete n'est pas fausse — et c'est a l'appelant de le signaler.
    """
    if not table_presente(conn, "hostaway_extractions"):
        return ""
    r = conn.execute(
        "SELECT extraction_id FROM hostaway_extractions WHERE statut IN ('SUCCES','PARTIEL') "
        "ORDER BY date_debut DESC, id DESC LIMIT 1").fetchone()
    return r[0] if r else ""


# ── Datasets de reservations ────────────────────────────────────────────────────────────────────

def ouvrir_dataset(conn: sqlite3.Connection, etape: str, *, extraction_id: str = "",
                   run_id: str = "") -> str:
    """Cree un dataset et le rend COURANT pour son etape.

    L'ancien jeu courant est desactive, pas supprime : un recalcul doit rester comparable au
    precedent, et effacer le passe interdirait de constater ce qui a change.
    """
    dataset_id = "RDS-" + uuid.uuid4().hex[:12].upper()
    conn.execute("UPDATE reservations_datasets SET actif = 0 WHERE etape = ? AND actif = 1",
                 (etape,))
    conn.execute(
        "INSERT INTO reservations_datasets (dataset_id, etape, extraction_id, run_id, date_calcul, "
        "actif) VALUES (?,?,?,?,?,1)",
        (dataset_id, etape, extraction_id or None, run_id or None, maintenant()))
    return dataset_id


def cloturer_dataset(conn: sqlite3.Connection, dataset_id: str, *, table: str,
                     statut: str = ST_SUCCES, message: str = "") -> int:
    """Fige le compteur du dataset, lu en base plutot que transmis par l'appelant."""
    nb = conn.execute("SELECT COUNT(*) FROM %s WHERE dataset_id = ?" % table,
                      (dataset_id,)).fetchone()[0]
    conn.execute(
        "UPDATE reservations_datasets SET nb_lignes = ?, statut = ?, message = ? "
        "WHERE dataset_id = ?", (nb, statut, message or None, dataset_id))
    return nb


def dataset_courant(conn: sqlite3.Connection, etape: str) -> str:
    if not table_presente(conn, "reservations_datasets"):
        return ""
    r = conn.execute(
        "SELECT dataset_id FROM reservations_datasets WHERE etape = ? AND actif = 1", (etape,)
    ).fetchone()
    return r[0] if r else ""


def ecrire_lignes(conn: sqlite3.Connection, table: str, colonnes: Sequence[str],
                  dataset_id: str, lignes_a_ecrire: Iterable[dict[str, Any]],
                  alias: dict[str, str] | None = None) -> int:
    """Insere les lignes d'un dataset. `alias` traduit un nom de colonne moteur vers la base.

    Le moteur nomme `guestCount`, la base `guest_count` : la traduction vit ici pour qu'aucune des
    deux couches n'impose son style a l'autre.
    """
    alias = alias or {}
    lignes_a_ecrire = list(lignes_a_ecrire)
    if not lignes_a_ecrire:
        return 0
    trous = ", ".join(["?"] * (len(colonnes) + 1))
    conn.executemany(
        "INSERT OR REPLACE INTO %s (dataset_id, %s) VALUES (%s)"
        % (table, ", ".join(colonnes), trous),
        [(dataset_id, *(l.get(alias.get(c, c)) for c in colonnes)) for l in lignes_a_ecrire])
    return len(lignes_a_ecrire)


# ── Rapprochement ménages externes <-> Hostaway (VUE_ECART_HOSTAWAY, ex-Lot6c) ───────────────────
#
# CE QUE CECI REMPLACE : l'onglet `VUE_ECART_HOSTAWAY` de `MASTER_FACT_MEN_MenagesExternes.xlsx`,
# calcule par `lot6c_menages_externes.py` a partir d'un classeur Hostaway (LOT6A) et d'un MASTER
# construit depuis des PDF relus a chaque run.
#
# Les deux sources sont deja en SQLite, alimentees par des services DEJA live :
#   · `menages_taches_enrichies` (0038)   — comptage Hostaway ("realise" x compte_comme_menage=OUI),
#     la MEME table que lit deja lot6d pour son propre rapprochement ;
#   · `facture_lignes_menage` (0037)      — lignes de ménage externe, alimentees par
#     `facture_menage_pdf_service.importer()` (deja en production, PDF -> SQLite direct, sans
#     passer par lot6c).
#
# La regle de classement (4 codes, memes seuils, memes libelles) est reprise TELLE QUELLE de
# `lot6c_menages_externes.py`. Elle vivait AUSSI, en double, dans le service applicatif
# `menages_ecarts_service._classer` : cette fonction est desormais la SEULE definition — le service
# applicatif l'importe (cf. sa propre fin de fichier) plutot que de la reecrire.
CODE_ECART_RAPPROCHE = "MENAGE_EXTERNE_RAPPROCHE_HOSTAWAY"
CODE_ECART_HORS_HA = "MENAGE_EXTERNE_LOGEMENT_HORS_HA"
CODE_ECART_HA_SANS_FACTURE = "MENAGE_HA_SANS_FACTURE_EXTERNE"
CODE_ECART_VOLUME = "MENAGE_EXTERNE_ECART_HOSTAWAY"


def classer_ecart_menage_externe(nb_ext: float, nb_ha: float) -> str:
    """Code de rapprochement pour un couple (mois, logement). `""` = rien a comparer.

    Codes NEUTRES : un ecart signale une verification, jamais une accusation prestataire.
    """
    if nb_ext == 0 and nb_ha == 0:
        return ""
    if nb_ext > 0 and nb_ha == 0:
        return CODE_ECART_HORS_HA
    if nb_ext == 0 and nb_ha > 0:
        return CODE_ECART_HA_SANS_FACTURE
    return CODE_ECART_RAPPROCHE if (nb_ext - nb_ha) == 0 else CODE_ECART_VOLUME


def calculer_ecarts_menages_externes(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Rapprochement mois x logement : ménages EXTERNES facturés vs ménages Hostaway REALISES.

    Rend une ligne par couple portant une activite (aucune ligne "rien a comparer"), avec les
    memes noms de colonnes que l'ancien onglet VUE_ECART_HOSTAWAY — aucun consommateur n'a besoin
    de changer de vocabulaire pour lire cette sortie.
    """
    ha: dict[tuple[str, str], dict[str, Any]] = {}
    if table_presente(conn, "menages_taches_enrichies"):
        cur = conn.execute(
            "SELECT mois, logement_id, proprietaire_id "
            "FROM menages_taches_enrichies "
            "WHERE statut_menage = 'réalisé' AND compte_comme_menage = 'OUI' "
            "AND logement_id IS NOT NULL AND logement_id <> ''")
        for mois, logement_id, proprietaire_id in cur.fetchall():
            cle = (str(mois)[:7], logement_id)
            e = ha.setdefault(cle, {"nb_ha": 0, "prop": None})
            e["nb_ha"] += 1
            if proprietaire_id and not e["prop"]:
                e["prop"] = proprietaire_id

    ext: dict[tuple[str, str], dict[str, Any]] = {}
    if table_presente(conn, "facture_lignes_menage"):
        cur = conn.execute(
            "SELECT f.date_facture, l.logement_id, f.fournisseur_id_opaque, "
            "COALESCE(d.quantite, 1) "
            "FROM facture_lignes_menage l "
            "JOIN factures f ON f.facture_id_opaque = l.facture_id_opaque "
            "LEFT JOIN facture_lignes_menage_detail d ON d.ligne_id_opaque = l.ligne_id_opaque "
            "WHERE l.type_ligne = 'MENAGE_EXTERNE' "
            "AND l.logement_id IS NOT NULL AND l.logement_id <> ''")
        for date_facture, logement_id, prestataire_id, quantite in cur.fetchall():
            mois = str(date_facture or "")[:7]
            if not mois:
                continue
            cle = (mois, logement_id)
            e = ext.setdefault(cle, {"nb_ext": 0.0, "prestataires": set()})
            e["nb_ext"] += quantite or 0
            if prestataire_id:
                e["prestataires"].add(str(prestataire_id))

    lignes: list[dict[str, Any]] = []
    for cle in sorted(set(ha) | set(ext)):
        mois, logement_id = cle
        nb_ha = ha.get(cle, {}).get("nb_ha", 0) or 0
        nb_ext = ext.get(cle, {}).get("nb_ext", 0) or 0
        code = classer_ecart_menage_externe(nb_ext, nb_ha)
        if not code:
            continue
        prop = (ext.get(cle) or {}).get("prop") or ha.get(cle, {}).get("prop")
        lignes.append({
            "mois": mois, "logement_id": logement_id, "proprietaire_id": prop,
            "prestataires_factures": ",".join(sorted(ext.get(cle, {}).get("prestataires", ()))),
            "nombre_menages_facture": nb_ext, "nombre_menages_hostaway": nb_ha,
            "ecart": nb_ext - nb_ha, "code_controle": code,
        })
    return lignes
