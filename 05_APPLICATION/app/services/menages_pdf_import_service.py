"""Import par lot des factures PDF ménage externe (dossier surveillé → SQLite, zéro Excel).

N'EXTRAIT NI NE DÉCIDE RIEN ICI : boucle sur `facture_menage_pdf_service.importer()`, déjà réel,
déjà testé, déjà idempotent (`factures_service.creer` refuse un doublon fournisseur+référence via
`E_DOUBLON_CERTAIN`). Ce module se contente de scanner `cfg.MENAGES_PDF_DIR`, d'appeler l'import
PDF par PDF, et de CLASSER ce qui s'est réellement passé pour chacun — jamais une invention.

Si l'écriture réelle des factures est désactivée (`FACTURES_REAL_WRITE_ENABLED=False`, gardé par
`RECETTE_MODE` — jamais retouché ici), chaque PDF ressort en `ECRITURE_DESACTIVEE` : détecté, mais
non importé, sans qu'aucune facture ne soit fabriquée pour faire illusion.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import facture_menage_pdf_service as pdf_import
from app.services import factures_service as fact

STATUT_IMPORTEE = "IMPORTEE"
STATUT_DEJA_IMPORTEE = "DEJA_IMPORTEE"
STATUT_EXTRACTION_ECHOUEE = "EXTRACTION_ECHOUEE"
STATUT_ECRITURE_DESACTIVEE = "ECRITURE_DESACTIVEE"
STATUT_ERREUR = "ERREUR"
STATUT_NOUVEAU = "NOUVEAU"

_CODES_DOUBLON = (fact.E_DOUBLON_CERTAIN, "E_DOUBLON_CERTAIN")


def _est_doublon(res: dict[str, Any]) -> bool:
    """`factures_service.creer` remonte le doublon au premier niveau (`code`) OU imbriqué dans
    `erreurs` (validation groupée, cf. `valider()`) — les deux formes existent selon le chemin
    emprunté, jamais un seul des deux ici."""
    if res.get("code") in _CODES_DOUBLON:
        return True
    return any(e.get("code") in _CODES_DOUBLON for e in (res.get("erreurs") or []))


def lister_pdf(*, dossier: Path | None = None) -> list[Path]:
    """PDF actuellement présents dans le dossier surveillé, triés par nom."""
    d = Path(dossier) if dossier else cfg.MENAGES_PDF_DIR
    return sorted(d.glob("*.pdf")) if d.exists() else []


def _deja_traite(nom_fichier: str, *, db_path=None) -> bool:
    """Un PDF est « déjà traité » si une tentative d'import a produit une facture pour ce fichier
    (`facture_pdf_diagnostics.facture_id_opaque IS NOT NULL`) — jamais un simple sha256 de fichier,
    qui confondrait un PDF renommé avec un PDF réellement nouveau (l'extracteur, lui, dédoublonne
    sur fournisseur+référence, pas sur le nom du fichier)."""
    conn = get_db(db_path)
    try:
        if not conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='facture_pdf_diagnostics'"
        ).fetchone():
            return False
        return conn.execute(
            "SELECT 1 FROM facture_pdf_diagnostics "
            "WHERE nom_fichier = ? AND facture_id_opaque IS NOT NULL LIMIT 1",
            (nom_fichier,),
        ).fetchone() is not None
    finally:
        conn.close()


def apercu(*, dossier: Path | None = None, db_path=None) -> dict[str, Any]:
    """État LECTURE SEULE du dossier — pour l'écran, sans rien importer."""
    d = Path(dossier) if dossier else cfg.MENAGES_PDF_DIR
    pdfs = lister_pdf(dossier=d)
    details = [
        {"nom_fichier": p.name,
         "statut": STATUT_DEJA_IMPORTEE if _deja_traite(p.name, db_path=db_path) else STATUT_NOUVEAU}
        for p in pdfs
    ]
    nb_nouveaux = sum(1 for d_ in details if d_["statut"] == STATUT_NOUVEAU)
    return {
        "dossier": str(d),
        "dossier_present": d.exists(),
        "nb_detectes": len(pdfs),
        "nb_nouveaux": nb_nouveaux,
        "nb_deja_importes": len(pdfs) - nb_nouveaux,
        "details": details,
        "ecriture_active": bool(cfg.FACTURES_REAL_WRITE_ENABLED
                                and cfg.FACTURES_REAL_WRITE_CONFIRMATION_ENABLED),
    }


def importer_nouveaux(*, acteur: str = "", dossier: Path | None = None,
                      db_path=None) -> dict[str, Any]:
    """Importe chaque PDF du dossier. Idempotent : un PDF déjà importé ne crée jamais de doublon
    (dédoublonnage délégué à `factures_service.creer`, pas réimplémenté ici)."""
    pdfs = lister_pdf(dossier=dossier)
    details: list[dict[str, Any]] = []
    nb_importees = nb_deja = nb_echecs = nb_desactive = 0
    for p in pdfs:
        res = pdf_import.importer(p, acteur=acteur, db_path=db_path)
        code = res.get("code")
        if res.get("ok"):
            statut = STATUT_IMPORTEE
            nb_importees += 1
        elif _est_doublon(res):
            statut = STATUT_DEJA_IMPORTEE
            nb_deja += 1
        elif code == pdf_import.E_EXTRACTION_ECHOUEE:
            statut = STATUT_EXTRACTION_ECHOUEE
            nb_echecs += 1
        elif code == "E_FLAGS_DESACTIVES":
            statut = STATUT_ECRITURE_DESACTIVEE
            nb_desactive += 1
        else:
            statut = STATUT_ERREUR
            nb_echecs += 1
        details.append({"nom_fichier": p.name, "statut": statut, "resultat": res})
    return {
        "ok": nb_echecs == 0,
        "nb_detectes": len(pdfs),
        "nb_importees": nb_importees,
        "nb_deja_importees": nb_deja,
        "nb_extraction_echouee": nb_echecs,
        "nb_ecriture_desactivee": nb_desactive,
        "details": details,
    }
