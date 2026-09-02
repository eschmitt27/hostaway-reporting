"""Import par lot des factures PDF ménage externe (dossier surveillé → SQLite, zéro Excel).

N'EXTRAIT NI NE DÉCIDE RIEN ICI : boucle sur `facture_menage_pdf_service.importer()`, déjà réel,
déjà testé, déjà idempotent (`factures_service.creer` refuse un doublon fournisseur+référence via
`E_DOUBLON_CERTAIN`). Ce module se contente de scanner `cfg.MENAGES_PDF_DIR`, d'appeler l'import
PDF par PDF, et de CLASSER ce qui s'est réellement passé pour chacun — jamais une invention.

Créer une facture au statut À CONTRÔLER est une écriture OPÉRATIONNELLE (niveau A,
`cfg.ECRITURE_OPERATIONNELLE_ENABLED`) : elle fonctionne en production normale, sans `RECETTE_MODE`.
Un PDF déposé est un document reçu, pas une dette comptabilisée — la comptabilité ne commence
qu'après validation humaine explicite, qui reste gardée au niveau B (`FACTURES_REAL_WRITE_*`).
Si ce niveau A était désactivé, chaque PDF ressortirait en `ECRITURE_DESACTIVEE` : détecté, mais non
importé, sans qu'aucune facture ne soit fabriquée pour faire illusion.
"""
from __future__ import annotations

import hashlib
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
STATUT_REMPLACEE = "REMPLACEE"  # même nom de fichier, contenu changé -> V1 ANNULEE, V2 créée

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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _hash_connu(nom_fichier: str, *, db_path=None) -> str | None:
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT sha256 FROM menages_pdf_fichiers_hash WHERE nom_fichier = ?", (nom_fichier,),
        ).fetchone()
        return row["sha256"] if row else None
    finally:
        conn.close()


def _enregistrer_hash(nom_fichier: str, sha256: str, *, db_path=None) -> None:
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO menages_pdf_fichiers_hash (nom_fichier, sha256, date_maj) "
            "VALUES (?,?,strftime('%Y-%m-%dT%H:%M:%SZ','now')) "
            "ON CONFLICT(nom_fichier) DO UPDATE SET sha256=excluded.sha256, date_maj=excluded.date_maj",
            (nom_fichier, sha256))
        conn.commit()
    finally:
        conn.close()


def _deja_traite(nom_fichier: str, *, db_path=None) -> bool:
    """Un PDF est « déjà traité, sans changement » si une tentative d'import a déjà produit une
    facture pour ce fichier ET que son contenu (sha256) n'a pas changé depuis. Un même nom de
    fichier dont le CONTENU a changé (PDF remplacé sur place, §E3) n'est PAS considéré déjà traité :
    il redevient éligible à un import, qui déclenchera alors le remplacement V1->V2 côté
    `facture_menage_pdf_service` si le montant diffère, ou sera refusé comme doublon sinon."""
    conn = get_db(db_path)
    try:
        if not conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='facture_pdf_diagnostics'"
        ).fetchone():
            return False
        deja = conn.execute(
            "SELECT 1 FROM facture_pdf_diagnostics "
            "WHERE nom_fichier = ? AND facture_id_opaque IS NOT NULL LIMIT 1",
            (nom_fichier,),
        ).fetchone() is not None
    finally:
        conn.close()
    if not deja:
        return False
    return True  # la distinction contenu-changé se fait dans apercu()/importer_nouveaux() (hash)


def _contenu_change(p: Path, *, db_path=None) -> bool:
    connu = _hash_connu(p.name, db_path=db_path)
    return connu is not None and connu != _sha256(p)


def _statut_apercu(p: Path, *, db_path=None) -> str:
    if not _deja_traite(p.name, db_path=db_path):
        return STATUT_NOUVEAU
    return STATUT_NOUVEAU if _contenu_change(p, db_path=db_path) else STATUT_DEJA_IMPORTEE


def apercu(*, dossier: Path | None = None, db_path=None) -> dict[str, Any]:
    """État LECTURE SEULE du dossier — pour l'écran, sans rien importer."""
    d = Path(dossier) if dossier else cfg.MENAGES_PDF_DIR
    pdfs = lister_pdf(dossier=d)
    details = [
        {"nom_fichier": p.name, "statut": _statut_apercu(p, db_path=db_path)}
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
        # Niveau A : c'est ce qui gouverne réellement l'import d'un PDF en facture À CONTRÔLER.
        "ecriture_active": bool(getattr(cfg, "ECRITURE_OPERATIONNELLE_ENABLED", False)),
        # Niveau B, indicatif : la comptabilisation reste gardée, bien après cet import.
        "comptabilisation_active": bool(cfg.FACTURES_REAL_WRITE_ENABLED
                                        and cfg.FACTURES_REAL_WRITE_CONFIRMATION_ENABLED),
    }


def importer_nouveaux(*, acteur: str = "", dossier: Path | None = None,
                      db_path=None) -> dict[str, Any]:
    """Importe chaque PDF du dossier. Idempotent : un PDF déjà importé ne crée jamais de doublon
    (dédoublonnage délégué à `factures_service.creer`, pas réimplémenté ici)."""
    pdfs = lister_pdf(dossier=dossier)
    details: list[dict[str, Any]] = []
    nb_importees = nb_deja = nb_echecs = nb_desactive = nb_remplacees = 0
    for p in pdfs:
        if _deja_traite(p.name, db_path=db_path) and not _contenu_change(p, db_path=db_path):
            details.append({"nom_fichier": p.name, "statut": STATUT_DEJA_IMPORTEE, "resultat": {"ok": True}})
            nb_deja += 1
            continue
        res = pdf_import.importer(p, acteur=acteur, db_path=db_path)
        code = res.get("code")
        if res.get("ok"):
            statut = STATUT_REMPLACEE if res.get("remplacement_de") else STATUT_IMPORTEE
            if statut == STATUT_REMPLACEE:
                nb_remplacees += 1
            else:
                nb_importees += 1
            _enregistrer_hash(p.name, _sha256(p), db_path=db_path)
        elif _est_doublon(res):
            statut = STATUT_DEJA_IMPORTEE
            nb_deja += 1
            _enregistrer_hash(p.name, _sha256(p), db_path=db_path)
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
    # Mois impactés = mission "recalcul mensuel ciblé" §8 : seuls les PDF réellement
    # importés/remplacés produisent un mois à recalculer, jamais les déjà-importés/échecs.
    mois_impactes = sorted({
        d["resultat"].get("mois_impacte") for d in details
        if d["statut"] in (STATUT_IMPORTEE, STATUT_REMPLACEE) and d["resultat"].get("mois_impacte")
    })
    return {
        "ok": nb_echecs == 0,
        "nb_detectes": len(pdfs),
        "nb_importees": nb_importees,
        "nb_deja_importees": nb_deja,
        "nb_remplacees": nb_remplacees,
        "nb_extraction_echouee": nb_echecs,
        "nb_ecriture_desactivee": nb_desactive,
        "mois_impactes": mois_impactes,
        "details": details,
    }
