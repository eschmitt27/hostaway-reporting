"""Service ménages — rapprochement d'affichage (APP-2a).

Ce service ORCHESTRE des sorties moteur déjà calculées. Il ne rejoue aucune règle.

Ce qu'il fait :
- lit les sorties Lot6a/6b/6c/6d/6e/6f/11 via le reader ;
- filtre, trie, pagine, et rapproche par la clé du moteur (mois × logement × intervenant) ;
- agrège pour l'affichage (sommes de colonnes déjà produites par le moteur, comptages
  de lignes) — jamais une règle métier ;
- explique une ligne en montrant les 4 flux côte à côte et leur source.

Ce qu'il ne fait jamais :
- fusionner les flux Hostaway / interne / externe ;
- valoriser un ménage à partir des données de coût Hostaway ;
- déduire qu'un ménage est interne ou externe sans source explicite ;
- fabriquer un statut : le statut vient toujours du moteur (`statut_controle`) ;
- écrire dans un fichier Excel ou un MASTER.

Seule écriture autorisée : l'outrepassage tracé (motif + horodatage) dans SQLite.
Il ne modifie aucune source ; il est affiché à part du statut moteur, jamais à sa place.
"""
import csv
import io
from datetime import datetime
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.readers import menages_reader as reader
from app.readers.menages_reader import to_mois, to_nombre, to_texte

TAILLE_PAGE = 25


# ---------------------------------------------------------------------------
# Cache applicatif — invalidation publique contrôlée
# ---------------------------------------------------------------------------

def invalidate_menages_cache() -> None:
    """Vide le cache de lecture ménages (MASTER + référentiel propriétaires).

    Fonction publique unique appelée par l'action « Actualiser l'affichage » : les routes
    ne manipulent jamais directement les structures privées du reader. Ne relit rien tout
    de suite ; la prochaine lecture rouvrira les fichiers et reflètera l'état du disque.
    """
    reader.vider_cache()


# ---------------------------------------------------------------------------
# Factures de ménage externes — informations d'affichage (PDF)
# ---------------------------------------------------------------------------

def load_pdf_externes_info() -> dict[str, Any]:
    """Infos sur les factures de ménage externes (Lot6c), reflétant le MASTER produit.

    Le mode d'extraction est LU dans le MASTER (source_document) : PDF_AUTOMATIQUE (extraction
    réelle des PDF par lot6c/lib_menages_externes_pdf) ou SAISIE_MANUELLE_SECOURS (transcription
    figée, secours). Le diagnostic par fichier vient de l'onglet DIAGNOSTIC_PDF, produit par le
    moteur — l'application ne parse pas les PDF elle-même (aucune règle métier dupliquée).
    """
    dossier: Path = cfg.MENAGES_PDF_DIR
    pdfs = sorted(p.name for p in dossier.glob("*.pdf")) if dossier.exists() else []

    src = reader.externes()
    prestataires: list[str] = []
    periodes: set[str] = set()
    fichiers_source: set[str] = set()
    nb_lignes = 0
    if src.etat.disponible:
        nb_lignes = len(src.lignes)
        for r in src.lignes:
            nom = to_texte(r.get("nom_prestataire"))
            if nom and nom != "INCONNU":
                prestataires.append(nom)
            m = to_mois(r.get("mois"))
            if m:
                periodes.add(m)
            f = to_texte(r.get("nom_fichier_source"))
            if f:
                fichiers_source.add(f)

    mode = reader.mode_extraction_externes()
    diag_src = reader.diagnostic_pdf()
    diagnostic = []
    nb_reconnus = nb_non_supportes = nb_erreur = nb_doublon = 0
    for r in diag_src.lignes:
        statut = to_texte(r.get("statut_extraction"))
        doublon = to_texte(r.get("doublon_de"))
        diagnostic.append({
            "fichier": to_texte(r.get("nom_fichier")),
            "format": to_texte(r.get("format_detecte")),
            "statut": statut,
            "numero_facture": to_texte(r.get("numero_facture")),
            "montant_total": to_nombre(r.get("montant_total")),
            "ecart": to_nombre(r.get("ecart_reconciliation")),
            "nb_lignes": to_nombre(r.get("nb_lignes")),
            "doublon_de": doublon,
            "anomalies": to_texte(r.get("anomalies")),
        })
        if doublon:
            nb_doublon += 1
        elif statut == "OK":
            nb_reconnus += 1
        elif statut == "NON_SUPPORTE":
            nb_non_supportes += 1
        else:
            nb_erreur += 1

    return {
        "dossier_relatif": cfg.MENAGES_PDF_DIR_REL,
        "dossier_present": dossier.exists(),
        "pdf_presents": pdfs,
        "nb_pdf_presents": len(pdfs),
        "fichiers_transcrits": sorted(fichiers_source),
        "nb_fichiers_transcrits": len(fichiers_source),
        "prestataires_detectes": sorted(set(prestataires)),
        "periode_couverte": sorted(periodes),
        "nb_lignes_master": nb_lignes,
        "derniere_extraction": src.etat.derniere_maj,
        "mode_extraction": mode,
        "extraction_automatique": mode == "PDF_AUTOMATIQUE",
        "diagnostic_disponible": diag_src.etat.disponible,
        "diagnostic": diagnostic,
        "nb_pdf_reconnus": nb_reconnus,
        "nb_pdf_non_supportes": nb_non_supportes,
        "nb_pdf_erreur": nb_erreur,
        "nb_pdf_doublon": nb_doublon,
        "note_extraction": _note_mode(mode),
        "etat_source": src.etat,
    }


# ---------------------------------------------------------------------------
# Actualisation réelle — statut affiché sur l'écran principal (mission Ménages, bouton unique)
# ---------------------------------------------------------------------------

def _nb_factures_menage_a_controler(db_path=None) -> int:
    """Factures ménage externe (issues d'un import PDF) encore au statut A_CONTROLER."""
    conn = get_db(db_path)
    try:
        if not conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='facture_pdf_diagnostics'"
        ).fetchone():
            return 0
        return conn.execute(
            "SELECT COUNT(*) FROM factures WHERE statut = 'A_CONTROLER' AND facture_id_opaque IN "
            "(SELECT DISTINCT facture_id_opaque FROM facture_pdf_diagnostics "
            "WHERE facture_id_opaque IS NOT NULL)"
        ).fetchone()[0]
    finally:
        conn.close()


def charger_etat_actualisation(db_path=None) -> dict[str, Any]:
    """Tout ce que l'écran Ménages doit afficher pour le bouton « Actualiser les ménages » —
    une seule lecture, jamais une exécution. Combine : l'état RÉEL du dataset MENAGES tel que
    l'orchestrateur le voit (`orchestrateur_service.etat_datasets`), l'aperçu du dossier PDF
    (`menages_pdf_import_service.apercu`, lecture seule), et les mois réellement disponibles."""
    from app.services import orchestrateur_service as orch
    from app.services import menages_pdf_import_service as pdf_svc
    from app.services.menages_recalcul_service import _mois_disponibles

    dataset = next((d for d in orch.etat_datasets(db_path=db_path) if d["dataset"] == "MENAGES"), None)
    pdf = pdf_svc.apercu(db_path=db_path)
    mois = _mois_disponibles(Path(db_path) if db_path else Path(cfg.DB_PATH))
    verrous = orch._verrous_actifs(db_path)
    return {
        "dataset": dataset,
        "derniere_actualisation": dataset.get("calcule_le") if dataset else None,
        "statut_dataset": dataset.get("statut") if dataset else None,
        "pdf": pdf,
        "nb_a_controler": _nb_factures_menage_a_controler(db_path=db_path),
        "mois_disponibles": mois,
        "en_cours": bool(verrous),
    }


def _note_mode(mode: str | None) -> str:
    if mode == "PDF_AUTOMATIQUE":
        return ("Extraction AUTOMATIQUE : les factures sont lues directement depuis les PDF déposés "
                "(texte natif, lib_menages_externes_pdf). Déposer un nouveau PDF d'un format reconnu "
                "suffit ; relancer la chaîne régénère les lignes.")
    if mode == "SAISIE_MANUELLE_SECOURS":
        return ("Extraction en mode SECOURS : aucun PDF exploitable trouvé lors de la dernière "
                "génération ; la transcription figée a été utilisée. Déposer les PDF puis relancer.")
    return ("Mode d'extraction inconnu pour ce MASTER (généré avant l'extraction PDF, ou source "
            "indisponible). Relancer la chaîne pour produire le diagnostic PDF.")

# Marqueurs d'identification incomplète produits par le moteur (Lot6d).
# On les filtre, on ne les invente pas.
INTERVENANTS_NON_IDENTIFIES = ("NON_ATTRIBUE", "ASSIGNEE_NON_MAPPE")

STATUTS_A_CONTROLER = ("A_CONTROLER", "BLOQUANT")

TRIS = {
    "anomalie": "Anomalies d'abord",
    "logement": "Logement",
    "intervenant": "Intervenant",
    "ecart": "Écart",
}

# Explications LISIBLES des codes de contrôle du moteur. Affichage uniquement :
# le code et le statut restent produits par le moteur, jamais réécrits ici.
EXPLICATIONS_CONTROLE: dict[str, str] = {
    "MENAGE_TOTAL_ECART_HOSTAWAY":
        "Le total déclaré (interne + externe) ne couvre pas les tâches Hostaway réalisées.",
    "MENAGE_ECART_NOMBRE":
        "Le nombre de ménages déclarés ne correspond pas au nombre de tâches Hostaway réalisées.",
    "MENAGE_M04_NON_ALIMENTE":
        "Des tâches Hostaway internes existent mais aucune déclaration M04 n'a été trouvée.",
    "MENAGE_PRESTATAIRE_ECART_HOSTAWAY":
        "Le prestataire a facturé plus de ménages que de tâches Hostaway réalisées.",
    "MENAGE_ASSIGNEE_NON_MAPPE":
        "La tâche Hostaway porte un intervenant (assigneeUserId) inconnu du référentiel.",
    "TASK_FUTURE_SANS_INTERVENANT_ASSIGNE":
        "Une tâche Hostaway d'un mois ouvert n'est pas encore rattachée à un intervenant.",
    "TASK_NON_ASSIGNEE_HISTORIQUE_IGNOREE":
        "Tâche Hostaway non assignée sur un mois clôturé — ignorée pour le blocage (historique).",
    "CONFLIT_TITLE_ASSIGNEE":
        "Le titre de la tâche Hostaway et son intervenant assigné ne concordent pas.",
    "IDENTIFICATION_INCOMPLETE":
        "Le moteur n'a pas su rattacher cette ligne à un logement ou un intervenant du référentiel.",
}


def explication_controle(code: str) -> str:
    """Libellé lisible d'un code de contrôle, ou chaîne vide si inconnu (jamais d'invention)."""
    return EXPLICATIONS_CONTROLE.get(str(code or "").strip(), "")


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Périodes, filtres
# ---------------------------------------------------------------------------

def load_available_periods() -> list[str]:
    """Mois présents dans le rapprochement moteur, du plus récent au plus ancien."""
    mois = {to_mois(r.get("mois")) for r in reader.rapprochement().lignes}
    return sorted((m for m in mois if m), reverse=True)


def periode_par_defaut() -> str:
    periodes = load_available_periods()
    return periodes[0] if periodes else ""


_MOIS_LIBELLES = {
    "01": "janvier", "02": "février", "03": "mars", "04": "avril", "05": "mai", "06": "juin",
    "07": "juillet", "08": "août", "09": "septembre", "10": "octobre", "11": "novembre",
    "12": "décembre",
}


def libelle_mois(mois: str) -> str:
    """"2026-07" -> "juillet 2026" — pour le bouton "Actualiser <mois affiché>"."""
    brut = str(mois or "").strip()
    if len(brut) >= 7 and brut[4] == "-":
        libelle = _MOIS_LIBELLES.get(brut[5:7])
        if libelle:
            return f"{libelle} {brut[:4]}"
    return brut


def load_filter_options(mois: str = "") -> dict[str, list[dict[str, str]]]:
    """Options de filtre, restreintes au mois actif quand il est fourni."""
    lignes = [r for r in reader.rapprochement().lignes
              if not mois or to_mois(r.get("mois")) == mois]

    def _options(cle_id: str, cle_libelle: str) -> list[dict[str, str]]:
        vus: dict[str, str] = {}
        for r in lignes:
            ident = to_texte(r.get(cle_id))
            if not ident:
                continue
            vus.setdefault(ident, to_texte(r.get(cle_libelle)) or ident)
        return [{"id": k, "libelle": v} for k, v in sorted(vus.items())]

    def _options_proprietaires() -> list[dict[str, str]]:
        """Filtre propriétaire : valeur = PROP_XXXX, libellé = « Prénom NOM » (référentiel)."""
        ids = {to_texte(r.get("proprietaire_id")) for r in lignes if to_texte(r.get("proprietaire_id"))}
        return [{"id": pid, "libelle": reader.libelle_proprietaire(pid)} for pid in sorted(ids)]

    types = sorted({to_texte(r.get("type_intervenant")) for r in lignes if to_texte(r.get("type_intervenant"))})
    statuts = sorted({to_texte(r.get("statut_controle")) for r in lignes if to_texte(r.get("statut_controle"))})

    return {
        "periodes": [{"id": m, "libelle": m} for m in load_available_periods()],
        "logements": _options("logement_id", "nom_appartement"),
        "proprietaires": _options_proprietaires(),
        "intervenants": _options("intervenant_id", "nom_intervenant"),
        "types": [{"id": t, "libelle": t} for t in types],
        "statuts": [{"id": s, "libelle": s} for s in statuts],
    }


# ---------------------------------------------------------------------------
# Lignes de rapprochement
# ---------------------------------------------------------------------------

def _identification_incomplete(row: dict[str, Any]) -> bool:
    """Vrai si le moteur n'a pas su rattacher la ligne (marqueurs Lot6d)."""
    return (
        to_texte(row.get("intervenant_id")) in INTERVENANTS_NON_IDENTIFIES
        or not to_texte(row.get("logement_id"))
        or not to_texte(row.get("type_intervenant"))
    )


def _vue_ligne(row: dict[str, Any], overrides: dict[str, dict],
               couts: dict[tuple, dict], gains: dict[tuple, dict]) -> dict[str, Any]:
    """Une ligne prête à afficher : les 4 flux restent des champs distincts."""
    cle = _cle(row)
    override = overrides.get(_cle_override(*cle))
    gp = gains.get(cle)
    cc = couts.get(cle)

    statut_moteur = to_texte(row.get("statut_controle"))
    return {
        "mois": cle[0],
        "logement_id": cle[1],
        "intervenant_id": cle[2],
        "nom_appartement": to_texte(row.get("nom_appartement")),
        "proprietaire_id": to_texte(row.get("proprietaire_id")),
        "proprietaire_libelle": reader.libelle_proprietaire(to_texte(row.get("proprietaire_id"))),
        "nom_intervenant": to_texte(row.get("nom_intervenant")),
        "type_intervenant": to_texte(row.get("type_intervenant")),
        # Bloc A — jamais déduit.
        "attendu": None,
        # Bloc B — comptage opérationnel Hostaway.
        "hostaway_realise": to_nombre(row.get("nb_menages_tasks_hostaway_completed")),
        # Bloc C — déclaré interne.
        "interne_declare": to_nombre(row.get("nb_menages_declares_interne_m04")),
        # Bloc D — facturé externe.
        "externe_facture": to_nombre(row.get("nb_menages_declares_externe")),
        "total_declares": to_nombre(row.get("total_menages_declares")),
        "ecart": to_nombre(row.get("ecart")),
        "statut_moteur": statut_moteur,
        "code_controle": to_texte(row.get("code_controle")),
        "commentaire": to_texte(row.get("commentaire")),
        "source_mapping_hostaway": to_texte(row.get("source_mapping_hostaway")),
        # Coûts — pris tels quels dans les MASTER de calcul, jamais recalculés.
        # Deux notions distinctes du moteur, jamais confondues :
        #   coût réel (Lot6e)    = heures internes M04 OU montant facturé ;
        #   coût complet (Lot6f) = coût réel + quotes-parts des charges ménage.
        "cout_standard": to_nombre((gp or {}).get("cout_standard_total")),
        "cout_reel": to_nombre((gp or {}).get("cout_reel_total")),
        "methode_cout_reel": to_texte((gp or {}).get("methode_cout_reel")),
        "cout_complet": to_nombre((cc or {}).get("cout_complet_total")),
        "gain_perte": to_nombre((gp or {}).get("ecart_total")),
        "identification_incomplete": _identification_incomplete(row),
        "override": override,
        "statut_effectif": "JUSTIFIE" if (override or {}).get("statut_override") == "JUSTIFIE" else statut_moteur,
    }


def _tri_cle(vue: dict[str, Any], tri: str):
    ecart = abs(vue["ecart"] or 0)
    a_controler = 0 if vue["statut_effectif"] in STATUTS_A_CONTROLER else 1
    if tri == "logement":
        return (vue["logement_id"], vue["intervenant_id"])
    if tri == "intervenant":
        return (vue["nom_intervenant"] or vue["intervenant_id"], vue["logement_id"])
    if tri == "ecart":
        return (-ecart, vue["logement_id"])
    return (a_controler, -ecart, vue["logement_id"])


def _match_vue(v: dict[str, Any], mois: str, logement_id: str, proprietaire_id: str,
               intervenant_id: str, type_intervenant: str, statut: str,
               ecart_seul: bool, identification_incomplete: bool) -> bool:
    """Prédicat de filtre partagé — liste paginée ET export CSV, pour qu'ils ne divergent jamais."""
    if mois and v["mois"] != mois:
        return False
    if logement_id and v["logement_id"] != logement_id:
        return False
    if proprietaire_id and v["proprietaire_id"] != proprietaire_id:
        return False
    if intervenant_id and v["intervenant_id"] != intervenant_id:
        return False
    if type_intervenant and v["type_intervenant"] != type_intervenant:
        return False
    if statut and v["statut_effectif"] != statut:
        return False
    if ecart_seul and (v["ecart"] or 0) == 0:
        return False
    if identification_incomplete and not v["identification_incomplete"]:
        return False
    return True


def load_reconciliation_rows(
    mois: str = "",
    logement_id: str = "",
    proprietaire_id: str = "",
    intervenant_id: str = "",
    type_intervenant: str = "",
    statut: str = "",
    ecart_seul: bool = False,
    identification_incomplete: bool = False,
    tri: str = "anomalie",
    page: int = 1,
) -> dict[str, Any]:
    """Lignes filtrées, triées, paginées. Statuts et écarts viennent du moteur."""
    source = reader.rapprochement()
    if not source.etat.disponible:
        return {
            "status": "SOURCE_INDISPONIBLE",
            "etat_source": source.etat,
            "rows": [],
            "page": 1, "pages": 1, "count_total": 0, "count_filtre": 0,
            "read_at": _now(),
        }

    overrides = _load_all_overrides()
    gains = {_cle(r): r for r in reader.gainperte().lignes}
    couts = {_cle(r): r for r in reader.cout_complet().lignes}

    vues = [_vue_ligne(r, overrides, couts, gains) for r in source.lignes]

    filtrees = sorted(
        (v for v in vues if _match_vue(
            v, mois, logement_id, proprietaire_id, intervenant_id,
            type_intervenant, statut, ecart_seul, identification_incomplete)),
        key=lambda v: _tri_cle(v, tri),
    )

    pages = max(1, (len(filtrees) + TAILLE_PAGE - 1) // TAILLE_PAGE)
    page = min(max(1, page), pages)
    debut = (page - 1) * TAILLE_PAGE

    return {
        "status": "OK",
        "etat_source": source.etat,
        "rows": filtrees[debut:debut + TAILLE_PAGE],
        "page": page,
        "pages": pages,
        "count_total": len(vues),
        "count_filtre": len(filtrees),
        "read_at": _now(),
    }


_CSV_COLONNES = [
    ("periode", "période"), ("logement_id", "logement"), ("nom_appartement", "nom_logement"),
    ("proprietaire_id", "propriétaire_id"), ("proprietaire_libelle", "propriétaire"),
    ("intervenant_id", "intervenant"),
    ("nom_intervenant", "nom_intervenant"), ("type_intervenant", "type"),
    ("attendu", "attendu"), ("hostaway_realise", "hostaway_realise"),
    ("interne_declare", "interne_declare"), ("externe_facture", "externe_facture"),
    ("ecart", "ecart"), ("cout_reel", "cout_reel"), ("cout_complet", "cout_complet"),
    ("statut_moteur", "statut_moteur"), ("statut_effectif", "statut_applicatif"),
    ("code_controle", "code_anomalie"),
]


def export_reconciliation_csv(
    mois: str = "", logement_id: str = "", proprietaire_id: str = "",
    intervenant_id: str = "", type_intervenant: str = "", statut: str = "",
    ecart_seul: bool = False, identification_incomplete: bool = False,
    tri: str = "anomalie",
) -> str:
    """Export CSV en mémoire de la vue filtrée (mêmes filtres que la liste, sans pagination).

    Colonnes métier uniquement : aucun chemin interne, aucune donnée bancaire ni voyageur.
    """
    if not mois:
        mois = periode_par_defaut()
    vues = [
        v for v in _toutes_les_vues(mois)
        if _match_vue(v, mois, logement_id, proprietaire_id, intervenant_id,
                      type_intervenant, statut, ecart_seul, identification_incomplete)
    ]
    vues.sort(key=lambda v: _tri_cle(v, tri if tri in TRIS else "anomalie"))

    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\n")
    w.writerow([entete for _, entete in _CSV_COLONNES])
    for v in vues:
        ligne = dict(v)
        ligne["periode"] = v["mois"]
        w.writerow(["" if ligne.get(cle) is None else ligne.get(cle) for cle, _ in _CSV_COLONNES])
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Synthèse
# ---------------------------------------------------------------------------

def a_controler(vue: dict[str, Any]) -> bool:
    """Une ligne à contrôler : statut moteur à contrôler, OU identification incomplète.

    Même critère partout — la carte de synthèse et l'écran « À contrôler » comptent
    exactement le même ensemble de lignes.
    """
    return vue["statut_effectif"] in STATUTS_A_CONTROLER or vue["identification_incomplete"]


def load_summary(mois: str = "") -> dict[str, Any]:
    """Cartes de synthèse. Sommes de colonnes moteur ; aucune règle rejouée."""
    source = reader.rapprochement()
    lignes = [r for r in source.lignes if not mois or to_mois(r.get("mois")) == mois]
    couts = reader.cout_complet()
    couts_mois = [r for r in couts.lignes if not mois or to_mois(r.get("mois")) == mois]

    def _somme(rows: list[dict], colonne: str) -> float | int | None:
        valeurs = [to_nombre(r.get(colonne)) for r in rows]
        connues = [v for v in valeurs if v is not None]
        return sum(connues) if connues else None

    nb_a_controler = sum(1 for v in _toutes_les_vues(mois) if a_controler(v))

    etats = reader.etats_sources()
    sources_manquantes = [e for e in etats if e.etat not in (reader.ETAT_OK, reader.ETAT_NON_ALIMENTE)]

    if not source.etat.disponible or sources_manquantes:
        etat_global = "SOURCE_INCOMPLETE"
    elif nb_a_controler:
        etat_global = "A_CONTROLER"
    else:
        etat_global = "CONFORME"

    return {
        "mois": mois,
        "etat_global": etat_global,
        "attendu": None,                                   # bloc A non alimenté
        "attendu_etat": reader.attendu(),
        "hostaway_realise": _somme(lignes, "nb_menages_tasks_hostaway_completed"),
        "interne_declare": _somme(lignes, "nb_menages_declares_interne_m04"),
        "externe_facture": _somme(lignes, "nb_menages_declares_externe"),
        "total_declares": _somme(lignes, "total_menages_declares"),
        "a_controler": nb_a_controler,
        # Somme des coûts complets produits par Lot6f. Distinct du coût réel (Lot6e).
        "cout_complet_total": _somme(couts_mois, "cout_complet_total"),
        "cout_reel_total": _somme(
            [r for r in reader.gainperte().lignes
             if not mois or to_mois(r.get("mois")) == mois],
            "cout_reel_total",
        ),
        "cout_complet_disponible": couts.etat.disponible,
        "nb_lignes": len(lignes),
        "etats_sources": etats,
        "sources_manquantes": sources_manquantes,
        "read_at": _now(),
    }


def load_dernier_calcul() -> dict[str, Any]:
    """Bloc « Dernier calcul Ménages » — dates de production, sources, anomalies.

    Ne lance rien. La commande affichée est celle qu'exécuterait le serveur.
    """
    etats = reader.etats_sources()
    dates = [e.derniere_maj for e in etats if e.derniere_maj]
    controles = reader.controles_rapprochement()
    nb_anomalies = sum(
        int(to_nombre(r.get("nb")) or 0)
        for r in controles.lignes
        if to_texte(r.get("niveau")) in STATUTS_A_CONTROLER
    )
    return {
        "derniere_generation": max(dates) if dates else None,
        "etats_sources": etats,
        "nb_anomalies": nb_anomalies,
        "commande": "python 02_TRAVAIL/run_menages_pipeline.py",
        "etapes": [
            "lot6b_m04_menages_internes.py",
            "lot6d_rapprochement_menages.py",
            "lot6e_gainperte_menages.py",
            "lot6f_cout_complet_menages.py",
        ],
        "recalcul_active": False,
        "read_at": _now(),
    }


def load_dashboard(mois: str = "", **filtres: Any) -> dict[str, Any]:
    """Écran principal : synthèse + lignes + dernier calcul + fraîcheur."""
    if not mois:
        mois = periode_par_defaut()
    page = int(filtres.pop("page", 1) or 1)
    liste = load_reconciliation_rows(mois=mois, page=page, **filtres)
    # Import différé : évite tout couplage d'import au chargement du module.
    from app.services import menages_recalcul_service as recalc
    return {
        "mois": mois,
        "mois_libelle": libelle_mois(mois),
        "summary": load_summary(mois),
        "liste": liste,
        "options": load_filter_options(mois),
        "dernier_calcul": load_dernier_calcul(),
        "fraicheur": recalc.etat_fraicheur(),
        "pdf_externes": load_pdf_externes_info(),
        "tris": TRIS,
        "applied": {"mois": mois, "page": page, **filtres},
    }


# ---------------------------------------------------------------------------
# Anomalies — écran « À contrôler »
# ---------------------------------------------------------------------------

def load_anomalies(mois: str = "") -> dict[str, Any]:
    """Anomalies produites par le moteur. Aucune anomalie n'est inventée ici."""
    liste = load_reconciliation_rows(mois=mois, page=1)
    lignes_ko = [] if liste["status"] != "OK" else [
        v for v in _toutes_les_vues(mois) if a_controler(v)
    ]

    controles = reader.controles_rapprochement()
    controles_moteur = [
        {
            "code": to_texte(r.get("code_controle")),
            "niveau": to_texte(r.get("niveau")),
            "nb": to_nombre(r.get("nb")),
            "exemple": to_texte(r.get("exemple")),
            "source": reader.SOURCE_RAPPROCHEMENT,
        }
        for r in controles.lignes
    ]

    lot11 = reader.controles_lot11()
    controles_lot11 = [
        {
            "code": to_texte(r.get("code_controle")),
            "niveau": to_texte(r.get("severity")),
            "message": to_texte(r.get("message")),
            "commentaire": to_texte(r.get("commentaire")),
            "statut_resolution": to_texte(r.get("statut_resolution")),
            "source": reader.SOURCE_LOT11,
        }
        for r in lot11.lignes
        if to_texte(r.get("source_module")).upper().startswith("MENAGE")
    ]

    etats = reader.etats_sources()
    return {
        "mois": mois,
        "status": liste["status"],
        "etat_source": liste["etat_source"],
        "lignes": sorted(lignes_ko, key=lambda v: _tri_cle(v, "anomalie")),
        "controles_moteur": controles_moteur,
        "controles_lot11": controles_lot11,
        "sources_manquantes": [e for e in etats
                               if e.etat not in (reader.ETAT_OK, reader.ETAT_NON_ALIMENTE)],
        "etats_sources": etats,
        "read_at": _now(),
    }


def _toutes_les_vues(mois: str = "") -> list[dict[str, Any]]:
    source = reader.rapprochement()
    if not source.etat.disponible:
        return []
    overrides = _load_all_overrides()
    gains = {_cle(r): r for r in reader.gainperte().lignes}
    couts = {_cle(r): r for r in reader.cout_complet().lignes}
    return [
        _vue_ligne(r, overrides, couts, gains)
        for r in source.lignes
        if not mois or to_mois(r.get("mois")) == mois
    ]


# ---------------------------------------------------------------------------
# Fiche de rapprochement
# ---------------------------------------------------------------------------

def load_reconciliation_detail(mois: str, logement_id: str,
                               intervenant_id: str) -> dict[str, Any] | None:
    """Fiche complète. None si la ligne n'existe pas au rapprochement (→ 404)."""
    source = reader.rapprochement()
    if not source.etat.disponible:
        return {
            "status": "SOURCE_INDISPONIBLE",
            "etat_source": source.etat,
            "mois": mois, "logement_id": logement_id, "intervenant_id": intervenant_id,
            "read_at": _now(),
        }

    cle = (str(mois).strip(), str(logement_id).strip(), str(intervenant_id).strip())
    brute = next((r for r in source.lignes if _cle(r) == cle), None)
    if brute is None:
        return None

    overrides = _load_all_overrides()
    gains = {_cle(r): r for r in reader.gainperte().lignes}
    couts = {_cle(r): r for r in reader.cout_complet().lignes}
    vue = _vue_ligne(brute, overrides, couts, gains)

    from app.services import menages_declarations_service as declarations

    return {
        "status": "OK",
        "mois": cle[0], "logement_id": cle[1], "intervenant_id": cle[2],
        "vue": vue,
        "attendu": _bloc_attendu(cle),
        "hostaway": _bloc_hostaway(cle),
        "interne": _bloc_interne(cle),
        "externe": _bloc_externe(cle),
        "cout": _bloc_cout(cle, gains, couts),
        "anomalies": _bloc_anomalies(vue),
        "tracabilite": _bloc_tracabilite(),
        "override": vue["override"],
        "declaration_extra": declarations.declaration_extra(*cle),
        "declaration_historique": declarations.historique(*cle),
        "declaration_modifiable": bool(_bloc_interne(cle)["lignes"]) and not declarations.mois_cloture(cle[0]),
        "read_at": _now(),
    }


def _bloc_attendu(cle: tuple[str, str, str]) -> dict[str, Any]:
    """Bloc A. Le comptage Hostaway planifié n'est PAS un attendu métier : on le dit."""
    comptage = reader.hostaway_comptage()
    ligne = next(
        (r for r in comptage.lignes
         if to_mois(r.get("mois")) == cle[0] and to_texte(r.get("logement_id")) == cle[1]),
        None,
    )
    return {
        "etat": reader.attendu(),
        "comptage_hostaway": {
            "planifiees": to_nombre((ligne or {}).get("nb_taches_total")),
            "realisees": to_nombre((ligne or {}).get("nb_menages_realises")),
            "pending": to_nombre((ligne or {}).get("nb_menages_pending")),
            "annulees": to_nombre((ligne or {}).get("nb_menages_annules")),
        } if ligne else None,
        "etat_comptage": comptage.etat,
    }


def _bloc_hostaway(cle: tuple[str, str, str]) -> dict[str, Any]:
    """Bloc B. Tâches du logement sur le mois.

    Le rattachement tâche → intervenant repose sur le mapping assigneeUserId du
    moteur (REF_Intervenants) ; il n'est pas rejoué ici. Les tâches listées sont
    donc celles du logement et du mois, toutes assignations confondues.
    """
    source = reader.hostaway_taches()
    taches = [
        {
            "task_id": to_texte(r.get("task_id")),
            "reservation_id": to_texte(r.get("reservation_id")),
            "date_prevue": to_texte(r.get("scheduled_date"))[:10],
            "titre": to_texte(r.get("title")),
            "status": to_texte(r.get("status")),
            "statut_menage": to_texte(r.get("statut_menage")),
            "type_ligne": to_texte(r.get("type_ligne_menage_lib")),
            "compte_comme_menage": to_texte(r.get("compte_comme_menage")),
            "code_anomalie": to_texte(r.get("code_anomalie")),
        }
        for r in source.lignes
        if to_mois(r.get("mois")) == cle[0] and to_texte(r.get("logement_id")) == cle[1]
    ]
    return {
        "etat": source.etat,
        "taches": sorted(taches, key=lambda t: t["date_prevue"]),
        "portee": "Toutes les tâches du logement sur le mois (le rattachement à "
                  "l'intervenant est fait par le moteur, pas par l'application).",
    }


def _bloc_interne(cle: tuple[str, str, str]) -> dict[str, Any]:
    """Bloc C. Déclarations M04 exactement au grain de la ligne."""
    source = reader.internes()
    lignes = [
        {
            "nom_intervenant": to_texte(r.get("nom_intervenant")),
            "type_intervenant": to_texte(r.get("type_intervenant")),
            "nb_menages": to_nombre(r.get("nb_menages")),
            "nb_heures": to_nombre(r.get("nb_heures")),
            "cout_lavage_attribue": to_nombre(r.get("cout_lavage_attribue")),
            "statut_controle": to_texte(r.get("statut_controle")),
            "code_controle": to_texte(r.get("code_controle")),
            "mois_saisie": to_texte(r.get("mois_saisie")),
        }
        for r in source.lignes
        if _cle(r) == cle
    ]
    return {"etat": source.etat, "lignes": lignes, "source": reader.SOURCE_INTERNES}


def _bloc_externe(cle: tuple[str, str, str]) -> dict[str, Any]:
    """Bloc D. Lignes de facture. Le montant vient de la facture, jamais d'Hostaway."""
    source = reader.externes()
    lignes = [
        {
            "menage_externe_id": to_texte(r.get("menage_externe_id")),
            "facture_id": to_texte(r.get("facture_id")),
            "nom_fichier_source": to_texte(r.get("nom_fichier_source")),
            "nom_prestataire": to_texte(r.get("nom_prestataire")),
            "date_facture": to_texte(r.get("date_facture")),
            "date_menage": to_texte(r.get("date_menage")),
            "precision_date_menage": to_texte(r.get("precision_date_menage")),
            "nombre_menages": to_nombre(r.get("nombre_menages")),
            "montant_ligne_ttc": to_nombre(r.get("montant_ligne_ttc")),
            "statut_controle": to_texte(r.get("statut_controle")),
            "niveau_anomalie": to_texte(r.get("niveau_anomalie")),
            "code_anomalie": to_texte(r.get("code_anomalie")),
            "date_absente": not to_texte(r.get("date_menage")),
        }
        for r in source.lignes
        if to_mois(r.get("mois")) == cle[0]
        and to_texte(r.get("logement_id")) == cle[1]
        and to_texte(r.get("prestataire_id")) == cle[2]
    ]
    return {"etat": source.etat, "lignes": lignes, "source": reader.SOURCE_EXTERNES}


def _bloc_cout(cle: tuple[str, str, str], gains: dict[tuple, dict],
               couts: dict[tuple, dict]) -> dict[str, Any]:
    gp = gains.get(cle) or {}
    cc = couts.get(cle) or {}
    return {
        "etat_gainperte": reader.gainperte().etat,
        "etat_coutcomplet": reader.cout_complet().etat,
        "cout_standard_total": to_nombre(gp.get("cout_standard_total")),
        "cout_reel_total": to_nombre(gp.get("cout_reel_total")),
        "methode_cout_reel": to_texte(gp.get("methode_cout_reel")),
        "nb_heures": to_nombre(gp.get("nb_heures")),
        "gain_perte": to_nombre(gp.get("ecart_total")),
        "statut_ecart": to_texte(gp.get("statut_ecart")),
        "cout_direct_total": to_nombre(cc.get("cout_direct_total")),
        "quote_part_local": to_nombre(cc.get("quote_part_local")),
        "quote_part_courses": to_nombre(cc.get("quote_part_courses")),
        "quote_part_lavage": to_nombre(cc.get("quote_part_lavage")),
        "quote_part_consommables": to_nombre(cc.get("quote_part_consommables")),
        "cout_complet_total": to_nombre(cc.get("cout_complet_total")),
        "cout_complet_unitaire": to_nombre(cc.get("cout_complet_unitaire")),
    }


def _bloc_anomalies(vue: dict[str, Any]) -> list[dict[str, str]]:
    """Anomalies de la ligne, telles que codées par le moteur."""
    anomalies = []
    if vue["code_controle"]:
        anomalies.append({
            "code": vue["code_controle"],
            "niveau": vue["statut_moteur"],
            "resume": vue["commentaire"] or "Anomalie signalée par le moteur de rapprochement.",
            "explication": explication_controle(vue["code_controle"]),
            "source": reader.SOURCE_RAPPROCHEMENT,
        })
    if vue["identification_incomplete"]:
        anomalies.append({
            "code": "IDENTIFICATION_INCOMPLETE",
            "niveau": "A_CONTROLER",
            "resume": "Le moteur n'a pas pu rattacher cette ligne à un intervenant "
                      "ou à un logement du référentiel.",
            "explication": explication_controle("IDENTIFICATION_INCOMPLETE"),
            "source": reader.SOURCE_RAPPROCHEMENT,
        })
    return anomalies


def _bloc_tracabilite() -> dict[str, Any]:
    """Noms de fichiers uniquement — jamais de chemin absolu en interface."""
    return {
        "sources": reader.etats_sources(),
        "moteur": "Pipeline ménages (Lot6b → Lot6d → Lot6e → Lot6f)",
    }


# ---------------------------------------------------------------------------
# Compatibilité APP-2 (routes et tests d'origine)
# ---------------------------------------------------------------------------

def load_list(mois: str = "", logement_id: str = "", type_intervenant: str = "",
              statut_controle: str = "") -> dict[str, Any]:
    """Ancienne API de liste. Conservée : les tests APP-2 s'appuient dessus."""
    read_at = _now()
    if not reader.rapprochement_available():
        return {
            "status": "ERROR",
            "error_message": f"Source introuvable : {reader.SOURCE_RAPPROCHEMENT}.",
            "source": reader.SOURCE_RAPPROCHEMENT,
            "read_at": read_at, "rows": [], "filters": _empty_filters(), "applied": {},
        }

    rows = reader.read_tableau_comparaison()
    if not rows:
        return {
            "status": "ERROR",
            "error_message": f"Source vide ou illisible : {reader.SOURCE_RAPPROCHEMENT}.",
            "source": reader.SOURCE_RAPPROCHEMENT,
            "read_at": read_at, "rows": [], "filters": _empty_filters(), "applied": {},
        }

    filters = {
        "mois": sorted({to_mois(r.get("mois")) for r in rows if to_mois(r.get("mois"))}),
        "logements": sorted({to_texte(r.get("logement_id")) for r in rows if to_texte(r.get("logement_id"))}),
        "types": sorted({to_texte(r.get("type_intervenant")) for r in rows if to_texte(r.get("type_intervenant"))}),
        "statuts": sorted({to_texte(r.get("statut_controle")) for r in rows if to_texte(r.get("statut_controle"))}),
    }

    overrides = _load_all_overrides()

    def _match(r: dict) -> bool:
        if mois and to_mois(r.get("mois")) != mois:
            return False
        if logement_id and to_texte(r.get("logement_id")) != logement_id:
            return False
        if type_intervenant and to_texte(r.get("type_intervenant")) != type_intervenant:
            return False
        if statut_controle and _effective_statut(r, overrides) != statut_controle:
            return False
        return True

    filtered = [_enrich_override(r, overrides) for r in rows if _match(r)]

    return {
        "status": "OK",
        "error_message": None,
        "source": reader.SOURCE_RAPPROCHEMENT,
        "read_at": read_at,
        "rows": filtered,
        "count_total": len(rows),
        "count_affiches": len(filtered),
        "filters": filters,
        "applied": {
            "mois": mois, "logement_id": logement_id,
            "type_intervenant": type_intervenant, "statut_controle": statut_controle,
        },
    }


def load_detail(mois: str, logement_id: str, intervenant_id: str) -> dict[str, Any] | None:
    """Ancienne API de fiche, conservée pour les tests APP-2."""
    if not reader.rapprochement_available():
        return {
            "status": "ERROR",
            "error_message": f"Source introuvable : {reader.SOURCE_RAPPROCHEMENT}.",
            "mois": mois, "logement_id": logement_id, "intervenant_id": intervenant_id,
            "read_at": _now(),
        }

    ligne = reader.find_ligne(mois, logement_id, intervenant_id)
    if ligne is None:
        return None

    gp = reader.find_gainperte(mois, logement_id, intervenant_id) if reader.gainperte_available() else None
    override = _load_override(mois, logement_id, intervenant_id)
    cle_ov = {_cle_override(mois, logement_id, intervenant_id): override} if override else {}

    return {
        "status": "OK",
        "error_message": None,
        "mois": mois, "logement_id": logement_id, "intervenant_id": intervenant_id,
        "ligne": ligne,
        "gainperte": gp,
        "override": override,
        "statut_effectif": _effective_statut(ligne, cle_ov),
        "source_rapprochement": reader.SOURCE_RAPPROCHEMENT,
        "source_gainperte": reader.SOURCE_GAINPERTE if gp is not None else None,
        "gainperte_disponible": reader.gainperte_available(),
        "read_at": _now(),
    }


# ---------------------------------------------------------------------------
# Outrepassage — SQLite uniquement, jamais une source
# ---------------------------------------------------------------------------

def enregistrer_outrepassage(mois: str, logement_id: str, intervenant_id: str,
                             motif: str) -> dict[str, Any]:
    """Trace un motif d'outrepassage dans SQLite. N'écrit jamais dans un MASTER.

    Le statut moteur reste affiché tel quel ; l'outrepassage est une annotation
    tracée, pas une réécriture de la vérité métier.
    """
    motif = str(motif).strip()
    if not motif:
        return {"ok": False, "error": "Le motif est obligatoire."}

    # cfg.DB_PATH est lu À CHAUD : le défaut de get_db() est figé à l'import et
    # pointerait sur la base réelle même quand un test isole cfg.DB_PATH.
    conn = get_db(cfg.DB_PATH)
    try:
        conn.execute(
            """INSERT INTO menage_overrides (mois, logement_id, intervenant_id, motif, statut_override)
               VALUES (?, ?, ?, ?, 'JUSTIFIE')
               ON CONFLICT(mois, logement_id, intervenant_id)
               DO UPDATE SET motif=excluded.motif,
                             ts=strftime('%Y-%m-%dT%H:%M:%SZ','now'),
                             statut_override='JUSTIFIE'
            """,
            (mois, logement_id, intervenant_id, motif),
        )
        conn.execute(
            "INSERT INTO audit_events (action, details) VALUES (?, ?)",
            ("MENAGE_OVERRIDE",
             f"mois={mois} logement={logement_id} intervenant={intervenant_id} motif={motif[:200]}"),
        )
        conn.commit()
    finally:
        conn.close()

    return {"ok": True, "mois": mois, "logement_id": logement_id,
            "intervenant_id": intervenant_id}


# ---------------------------------------------------------------------------
# Helpers privés
# ---------------------------------------------------------------------------

def _cle(row: dict[str, Any]) -> tuple[str, str, str]:
    return (to_mois(row.get("mois")), to_texte(row.get("logement_id")),
            to_texte(row.get("intervenant_id")))


def _cle_override(mois: str, logement_id: str, intervenant_id: str) -> str:
    return f"{mois}|{logement_id}|{intervenant_id}"


def _load_all_overrides() -> dict[str, dict]:
    # cfg.DB_PATH est lu À CHAUD : le défaut de get_db() est figé à l'import et
    # pointerait sur la base réelle même quand un test isole cfg.DB_PATH.
    conn = get_db(cfg.DB_PATH)
    try:
        rows = conn.execute(
            "SELECT mois, logement_id, intervenant_id, motif, statut_override, ts "
            "FROM menage_overrides"
        ).fetchall()
    finally:
        conn.close()
    return {_cle_override(r["mois"], r["logement_id"], r["intervenant_id"]): dict(r) for r in rows}


def _load_override(mois: str, logement_id: str, intervenant_id: str) -> dict | None:
    # cfg.DB_PATH est lu À CHAUD : le défaut de get_db() est figé à l'import et
    # pointerait sur la base réelle même quand un test isole cfg.DB_PATH.
    conn = get_db(cfg.DB_PATH)
    try:
        row = conn.execute(
            "SELECT mois, logement_id, intervenant_id, motif, statut_override, ts "
            "FROM menage_overrides WHERE mois=? AND logement_id=? AND intervenant_id=?",
            (mois, logement_id, intervenant_id),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def _effective_statut(row: dict, overrides: dict) -> str:
    cle = _cle_override(*_cle(row))
    if (overrides.get(cle) or {}).get("statut_override") == "JUSTIFIE":
        return "JUSTIFIE"
    return to_texte(row.get("statut_controle"))


def _enrich_override(row: dict, overrides: dict) -> dict:
    enriched = dict(row)
    enriched["override"] = overrides.get(_cle_override(*_cle(row)))
    enriched["statut_effectif"] = _effective_statut(row, overrides)
    return enriched


def _empty_filters() -> dict[str, list]:
    return {"mois": [], "logements": [], "types": [], "statuts": []}
