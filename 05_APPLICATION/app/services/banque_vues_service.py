"""Vues de lecture Banque — SQLite uniquement (APP-4A).

Ce module rend, depuis la base, les mêmes JEUX DE COLONNES que les onglets du classeur
`BANQUE_LOT8_IMPORT.xlsx` produisaient : `NORM_Banque`, `CTRL_A_CONTROLER`, `IA_Classification`,
`RAPPROCH_AIRBNB_ATTENTE`, `RAPPROCH_PROPRIETAIRES_ATTENTE`, `CTRL_RAPPROCHEMENT_8C`,
`LOG_Traitement`.

POURQUOI CONSERVER LE CONTRAT DE COLONNES
Une dizaine de routes, services et gabarits lisent ces clés. Les renommer en même temps qu'on change
de source rendrait impossible de savoir laquelle des deux modifications casse un affichage. Le
vocabulaire reste donc celui du moteur ; seule la provenance change. Le renommage, s'il a lieu, sera
un travail distinct et vérifiable seul.

CE QUI N'EST PAS REPRIS, ET POURQUOI
`commentaire` portait dans le classeur la mention « Relevé du … », propre au fichier reçu. Le
mouvement en base ne la porte pas, et la reconstituer depuis le nom du fichier d'import serait une
approximation présentée comme une donnée. La colonne existe donc, vide.

AUCUN REPLI EXCEL
Sans mouvement en base, ces fonctions rendent une liste vide et l'appelant l'apprend par
`initialisee()`. Lire le classeur « en attendant » ferait afficher, après un cut-over bancaire, des
mouvements de l'ancienne banque comme s'ils étaient courants.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db
from app.services import banque_attentes_service as att
from app.services import banque_classification_service as cls
from app.services import banque_mouvements_service as bq

# Codes d'état — jamais confondus avec « zéro mouvement ».
ETAT_OK = "OK"
ETAT_NON_INITIALISEE = "BANQUE_NON_INITIALISEE"
ETAT_NON_CLASSEE = "BANQUE_NON_CLASSEE"
ETAT_VIDE = "VIDE"

MESSAGES = {
    ETAT_NON_INITIALISEE: ("Aucun mouvement bancaire en base : importez un relevé depuis "
                           "l'application avant de consulter cet écran."),
    ETAT_NON_CLASSEE: ("Des mouvements sont importés mais aucune classification n'a été exécutée : "
                       "lancez la classification."),
}


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def initialisee(*, db_path=None) -> bool:
    """Vrai dès qu'un mouvement existe en base. Distinct de « classée »."""
    return bq.compter(db_path=db_path) > 0


def etat(*, db_path=None) -> str:
    if not initialisee(db_path=db_path):
        return ETAT_NON_INITIALISEE
    if not cls.derniere_execution(db_path=db_path):
        return ETAT_NON_CLASSEE
    return ETAT_OK


# ── NORM_Banque ─────────────────────────────────────────────────────────────────────────────────

COLONNES_NORM = ("mouvement_id", "ROW_HASH", "import_id", "ligne_source", "date_operation",
                 "date_valeur", "libelle", "libelle_brut", "montant", "sens", "devise",
                 "compte_id", "tiers_detecte", "categorie", "type_flux_id", "code_impact",
                 "source_classification", "source_economique", "statut_controle", "niveau_risque",
                 "codes_anomalie", "date_integration", "commentaire", "statut_classification",
                 "niveau_anomalie", "regle_id_appliquee")


def mouvements_normalises(*, bank_account_id: str = "", classification_run_id: str = "",
                          db_path=None) -> list[dict[str, Any]]:
    """Mouvements enrichis de leur classification courante.

    Un mouvement importé mais non encore classé APPARAÎT, ses colonnes de classification vides. Le
    masquer laisserait croire que la banque ne l'a jamais envoyé.
    """
    mvts = bq.mouvements(bank_account_id=bank_account_id, db_path=db_path)
    if not mvts:
        return []

    run = classification_run_id or cls.derniere_execution(db_path=db_path)
    classifs = {c["mouvement_id_opaque"]: c
                for c in cls.classifications(classification_run_id=run, db_path=db_path)}
    sigs = cls.signaux(classification_run_id=run, db_path=db_path)
    dates_import = _dates_import(db_path=db_path)
    surcharges = overrides_actifs(db_path=db_path)

    lignes = []
    for m in mvts:
        mid = m["mouvement_id_opaque"]
        c = classifs.get(mid, {})
        s = sigs.get(mid, {})
        lignes.append({
            "mouvement_id": mid,
            # 16 premiers caractères, comme le ROW_HASH du classeur : de quoi rapprocher deux
            # extractions sans exposer l'empreinte complète.
            "ROW_HASH": _txt(m.get("fingerprint"))[:16],
            "import_id": m.get("import_id"),
            "ligne_source": m.get("ligne_source"),
            "date_operation": m.get("date_operation"),
            "date_valeur": m.get("date_valeur"),
            "libelle": bq.normaliser_libelle(m.get("libelle_brut")),
            "libelle_brut": m.get("libelle_brut"),
            "montant": m.get("montant"),
            "sens": m.get("sens"),
            "devise": m.get("devise"),
            "compte_id": m.get("bank_account_id"),
            "tiers_detecte": c.get("tiers_detecte"),
            "categorie": c.get("categorie"),
            "type_flux_id": c.get("type_flux_id"),
            "code_impact": c.get("code_impact"),
            # Ce mouvement a-t-il été classé par une règle déterministe. Vide s'il ne l'est pas
            # encore : « pas de classification » n'est pas « classification d'origine inconnue ».
            "source_classification": cls.SOURCE_REGLE if c else None,
            "source_economique": c.get("source_economique"),
            "statut_controle": c.get("statut_controle"),
            "niveau_risque": c.get("niveau_risque"),
            "codes_anomalie": s.get("codes_anomalie") or None,
            "date_integration": dates_import.get(_txt(m.get("import_id")), ""),
            "commentaire": "",
            "statut_classification": c.get("statut_classification"),
            "niveau_anomalie": s.get("niveau_anomalie") or None,
            "regle_id_appliquee": c.get("regle_id"),
        })
        _appliquer_override(lignes[-1], surcharges.get(mid))
    return lignes


# ── Décisions humaines ──────────────────────────────────────────────────────────────────────────
#
# Une décision humaine PRIME sur la règle : c'est la raison d'être de l'écran de contrôle. Le
# classeur exprimait cela en réécrivant des cellules dans une copie ; en base, la décision vit dans
# `banque_overrides` (migration 0006) et la vue l'applique à la lecture. La classification d'origine
# reste donc intacte et consultable — on peut toujours dire ce que la règle avait proposé.

COLONNES_SURCHARGEES = ("categorie", "type_flux_id", "statut_controle")

SOURCE_DECISION_HUMAINE = "DECISION_HUMAINE"


def overrides_actifs(*, db_path=None) -> dict[str, dict[str, Any]]:
    """{mouvement_id: décision} pour les décisions actives."""
    from app.services.banque_mouvements_service import _table_presente
    conn = get_db(db_path)
    try:
        if not _table_presente(conn, "banque_overrides"):
            return {}
        rows = conn.execute(
            "SELECT mouvement_id_interne, categorie_validee, type_flux_id, statut_controle "
            "FROM banque_overrides WHERE actif=1 ORDER BY id").fetchall()
    finally:
        conn.close()
    return {_txt(r[0]): {"categorie": _txt(r[1]), "type_flux_id": _txt(r[2]),
                         "statut_controle": _txt(r[3])} for r in rows if _txt(r[0])}


def _appliquer_override(ligne: dict[str, Any], override: dict[str, Any] | None) -> None:
    """Applique une décision sur une ligne de vue, champ par champ.

    Un champ vide dans la décision ne remplace rien : décider d'une catégorie ne dit rien du type de
    flux, et écraser ce dernier par du vide effacerait une information que personne n'a contestée.
    """
    if not override:
        return
    for col in COLONNES_SURCHARGEES:
        if override.get(col):
            ligne[col] = override[col]
    ligne["source_classification"] = SOURCE_DECISION_HUMAINE


def _dates_import(*, db_path=None) -> dict[str, str]:
    return {_txt(i["import_id"]): _txt(i.get("date_import"))[:10]
            for i in bq.imports(db_path=db_path)}


# ── CTRL_A_CONTROLER ────────────────────────────────────────────────────────────────────────────

COLONNES_CTRL = ("mouvement_id", "ligne_source", "date_operation", "date_valeur", "libelle",
                 "montant", "sens", "code_controle", "severite", "description", "statut_controle")


def controles_a_controler(*, classification_run_id: str = "", db_path=None
                          ) -> list[dict[str, Any]]:
    """Constats de contrôle, chacun replacé dans son mouvement."""
    constats = cls.controles(classification_run_id=classification_run_id, db_path=db_path)
    if not constats:
        return []
    par_id = {m["mouvement_id_opaque"]: m for m in bq.mouvements(db_path=db_path)}
    lignes = []
    for c in constats:
        m = par_id.get(c["mouvement_id_opaque"])
        if m is None:
            # Un constat dont le mouvement a disparu est une incohérence : on l'expose au lieu de
            # l'écarter en silence.
            m = {}
        lignes.append({
            "mouvement_id": c["mouvement_id_opaque"],
            "ligne_source": m.get("ligne_source"),
            "date_operation": m.get("date_operation"),
            "date_valeur": m.get("date_valeur"),
            "libelle": m.get("libelle_brut"),
            "montant": m.get("montant"),
            "sens": m.get("sens"),
            "code_controle": c["code_controle"],
            "severite": c["severite"],
            "description": c["description"],
            "statut_controle": c["statut_controle"],
        })
    return lignes


# ── IA_Classification ───────────────────────────────────────────────────────────────────────────

COLONNES_IA = ("mouvement_id", "date_operation", "libelle", "montant", "sens",
               "categorie_proposee", "score_confiance", "commentaire_ia", "statut_ia")

COMMENTAIRE_IA = "Aucune regle deterministe applicable - classification automatique indisponible"
STATUT_IA_ATTENTE = "EN_ATTENTE_IA"


def a_classer_par_ia(*, classification_run_id: str = "", db_path=None) -> list[dict[str, Any]]:
    """Mouvements qu'aucune règle déterministe n'a su classer.

    `categorie_proposee` et `score_confiance` restent vides : aucune proposition n'est calculée ici,
    et en inventer une donnerait à un écran l'apparence d'une décision prise.
    """
    lignes = []
    for l in mouvements_normalises(classification_run_id=classification_run_id, db_path=db_path):
        if l["statut_classification"] != cls.CLASS_A_ENVOYER_IA:
            continue
        lignes.append({
            "mouvement_id": l["mouvement_id"], "date_operation": l["date_operation"],
            "libelle": l["libelle_brut"], "montant": l["montant"], "sens": l["sens"],
            "categorie_proposee": None, "score_confiance": None,
            "commentaire_ia": COMMENTAIRE_IA, "statut_ia": STATUT_IA_ATTENTE,
        })
    return lignes


# ── RAPPROCH_*_ATTENTE ──────────────────────────────────────────────────────────────────────────

COLONNES_RAPPRO_PLATEFORME = ("mouvement_id", "date_operation", "libelle", "montant_banque",
                              "sens", "reference_airbnb", "statut_rapprochement",
                              "methode_rapprochement", "lot_rapprochement", "date_rapprochement",
                              "commentaire")

COLONNES_RAPPRO_PROPRIETAIRES = ("mouvement_id", "date_operation", "libelle", "montant_banque",
                                 "sens", "proprietaire_id", "nature_presumee",
                                 "statut_rapprochement", "prerequis_rapprochement",
                                 "lot_rapprochement", "date_rapprochement", "commentaire")

LOT_PLATEFORME = "LOT8C"
NATURE_PRESUMEE = "ACOMPTE_OU_REVERSEMENT_A_QUALIFIER"


def _index_mouvements(db_path=None) -> dict[str, dict[str, Any]]:
    return {m["mouvement_id_opaque"]: m for m in bq.mouvements(db_path=db_path)}


def attentes_plateforme(*, db_path=None) -> list[dict[str, Any]]:
    par_id = _index_mouvements(db_path)
    lignes = []
    for motif in (att.ATTENTE_EXPORT_PLATEFORME, att.ATTENTE_AJUSTEMENT):
        for a in att.attentes(motif=motif, db_path=db_path):
            m = par_id.get(a["mouvement_id_opaque"], {})
            lignes.append({
                "mouvement_id": a["mouvement_id_opaque"],
                "date_operation": m.get("date_operation"),
                "libelle": m.get("libelle_brut"),
                "montant_banque": a["montant_rapproche"],
                "sens": m.get("sens"),
                "reference_airbnb": a["criteres"].get("reference") or None,
                "statut_rapprochement": motif,
                # Aucune méthode n'a rapproché quoi que ce soit : c'est une attente, pas un résultat.
                "methode_rapprochement": None,
                "lot_rapprochement": LOT_PLATEFORME,
                "date_rapprochement": None,
                "commentaire": a["commentaire"],
            })
    return lignes


def attentes_proprietaires(*, db_path=None) -> list[dict[str, Any]]:
    par_id = _index_mouvements(db_path)
    lignes = []
    for a in att.attentes(motif=att.ATTENTE_SAISIE_ACOMPTE, db_path=db_path):
        m = par_id.get(a["mouvement_id_opaque"], {})
        lignes.append({
            "mouvement_id": a["mouvement_id_opaque"],
            "date_operation": m.get("date_operation"),
            "libelle": m.get("libelle_brut"),
            "montant_banque": a["montant_rapproche"],
            "sens": m.get("sens"),
            "proprietaire_id": a["criteres"].get("tiers_detecte") or None,
            # Présumée, et nommée comme telle : la nature économique réelle vient d'une saisie
            # humaine, jamais d'une déduction sur un libellé bancaire.
            "nature_presumee": NATURE_PRESUMEE,
            "statut_rapprochement": att.ATTENTE_SAISIE_ACOMPTE,
            "prerequis_rapprochement": "Mouvement de trésorerie propriétaire déclaré",
            "lot_rapprochement": LOT_PLATEFORME,
            "date_rapprochement": None,
            "commentaire": a["commentaire"],
        })
    return lignes


# ── CTRL_RAPPROCHEMENT_8C ───────────────────────────────────────────────────────────────────────

COLONNES_CTRL_8C = ("code_controle", "severite", "nb_lignes", "total_montant_eur", "description",
                    "statut", "lot", "date_controle", "action_requise")

_DESCRIPTIONS_8C = {
    att.ATTENTE_EXPORT_PLATEFORME: (
        "Versements de plateforme sans export détaillé",
        "Obtenir l'export de la plateforme ; aucun rapprochement à une réservation n'est possible."),
    att.ATTENTE_AJUSTEMENT: (
        "Versements de plateforme d'un montant d'ajustement",
        "Vérifier la nature de l'ajustement avant toute écriture."),
    att.ATTENTE_SAISIE_ACOMPTE: (
        "Encaissements propriétaires sans mouvement de trésorerie déclaré",
        "Saisir le mouvement de trésorerie correspondant."),
}


def controles_rapprochement(*, db_path=None) -> list[dict[str, Any]]:
    """Synthèse des files d'attente, un constat par motif. Les motifs à zéro sont omis."""
    synth = att.synthese(db_path=db_path)
    lignes = []
    for motif, chiffres in synth.items():
        if not chiffres["nb"]:
            continue
        description, action = _DESCRIPTIONS_8C.get(motif, (motif, ""))
        lignes.append({
            "code_controle": motif, "severite": att.ST_PROPOSE,
            "nb_lignes": chiffres["nb"], "total_montant_eur": chiffres["total"],
            "description": description, "statut": "OUVERT", "lot": LOT_PLATEFORME,
            "date_controle": None, "action_requise": action,
        })
    return lignes


# ── LOG_Traitement ──────────────────────────────────────────────────────────────────────────────

COLONNES_LOG = ("run_id", "import_id", "etape", "timestamp", "nb_lignes_lues", "nb_lignes_brut",
                "nb_lignes_norm", "nb_a_controler", "nb_doublons", "commentaire", "format_source")


def journal_traitement(*, db_path=None) -> list[dict[str, Any]]:
    """Un enregistrement par import, tel que la base l'a journalisé."""
    return [{
        "run_id": i.get("run_id"),
        "import_id": i["import_id"],
        "etape": "IMPORT_BANCAIRE",
        "timestamp": i.get("date_import"),
        "nb_lignes_lues": i.get("nb_lignes"),
        "nb_lignes_brut": i.get("nb_lignes"),
        "nb_lignes_norm": i.get("nb_inseres"),
        "nb_a_controler": i.get("nb_a_controler"),
        "nb_doublons": i.get("nb_doublons"),
        "commentaire": f"Source {i.get('source_type')} — {i.get('source_filename') or 'API'}",
        "format_source": i.get("source_type"),
    } for i in bq.imports(db_path=db_path)]


# ── Comptes ─────────────────────────────────────────────────────────────────────────────────────

def comptes(*, db_path=None) -> list[str]:
    """Comptes présents en base, dans un ordre stable."""
    conn = get_db(db_path)
    try:
        return [r[0] for r in conn.execute(
            "SELECT DISTINCT bank_account_id FROM banque_mouvements "
            "ORDER BY bank_account_id")]
    finally:
        conn.close()
