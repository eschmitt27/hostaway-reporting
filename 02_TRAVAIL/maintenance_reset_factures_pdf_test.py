# -*- coding: utf-8 -*-
"""MAINTENANCE DE PHASE DE TEST — remise à zéro des factures fournisseurs issues des PDF.

    python maintenance_reset_factures_pdf_test.py --confirmer [--db app.db]

À N'UTILISER QUE TANT QUE L'APPLICATION N'EST PAS EN PRODUCTION. Ce script efface des factures
fournisseurs de TEST pour pouvoir remesurer l'extraction sur un corpus propre. Il n'a aucune
vocation opérationnelle : en production, une facture se supprime par la règle A (mois ouvert) ou
se contrepasse (règle B), jamais par un script.

CE QU'IL FAIT
  · sauvegarde la base AVANT toute écriture ;
  · supprime les factures fournisseurs À CONTRÔLER (ou BROUILLON) issues du pipeline PDF
    (`source = PDF_EXTRACTION`), avec leurs artefacts provisoires — lignes, ventilations et leurs
    parts, classifications, diagnostics, événements, empreintes de fichiers — en réutilisant
    `factures_service.supprimer`, donc exactement le même nettoyage que le parcours normal ;
  · écrit un journal de l'opération à côté de la sauvegarde, puisque la trace en base disparaît
    avec la facture.

LA SEULE RÈGLE LEVÉE, ET RIEN D'AUTRE
`supprimer()` refuse une facture dont le MOIS EST CLÔTURÉ. Ici, et ici seulement, ce verrou est
neutralisé le temps de l'opération : les factures de test des mois clos sont des imports de
recette, pas des pièces comptables (aucune n'a d'écriture ni de règlement — le script le vérifie).
AUCUN statut de clôture n'est modifié : les mois restent clos, et la règle reprend ses droits dès
la fin du script.

LES AUTRES VERROUS RESTENT ACTIFS
  · une facture VALIDÉE / RÉGLÉE n'est jamais supprimée — le script s'arrête s'il en trouve une ;
  · une facture ayant produit une écriture, une dette ou un règlement n'est jamais supprimée ;
  · rien d'autre n'est touché : factures propriétaires, réservations, charges, écritures sans
    lien, propriétaires, logements, référentiels et mappings logements confirmés restent intacts.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

ICI = Path(__file__).resolve().parent
for p in (ICI, ICI.parent / "05_APPLICATION"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

MOTIF = ("Maintenance de phase de test : remise à zéro des imports PDF avant mesure du moteur "
         "d'extraction (aucune pièce comptable, aucune clôture modifiée)")

SOURCE_PDF = "PDF_EXTRACTION"


def _horodatage() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sauvegarder(db_path: Path, dossier: Path) -> Path:
    dossier.mkdir(parents=True, exist_ok=True)
    cible = dossier / f"app_avant_maintenance_factures_{_horodatage()}.db"
    shutil.copy2(db_path, cible)
    return cible


def executer(*, db_path=None, dossier_sauvegardes: Path | None = None,
             acteur: str = "maintenance-test") -> dict:
    import app.config as cfg
    from app.services import factures_service as fact

    base = Path(db_path or cfg.DB_PATH)
    sauvegarde = sauvegarder(base, Path(dossier_sauvegardes or cfg.BACKUPS_DIR))

    toutes = fact.lister(db_path=db_path)
    protegees = [f for f in toutes
                 if f["statut"] not in (fact.ST_BROUILLON, fact.ST_A_CONTROLER, fact.ST_ANNULEE)]
    if protegees:
        return {"ok": False, "code": "FACTURE_NON_SUPPRIMABLE", "sauvegarde": str(sauvegarde),
                "message": "Des factures fournisseurs ne sont pas À CONTRÔLER : rien n'est supprimé.",
                "factures": [{"facture_id_opaque": f["facture_id_opaque"],
                              "facture_ref": f["facture_ref"], "statut": f["statut"]}
                             for f in protegees]}

    cibles = [f for f in toutes if str(f.get("source") or "") == SOURCE_PDF]
    engagees = [f for f in cibles
                if not fact.consequences_constatees(f["facture_id_opaque"],
                                                    db_path=db_path)["reversible"]]
    if engagees:
        return {"ok": False, "code": "CONSEQUENCES_CONSTATEES", "sauvegarde": str(sauvegarde),
                "message": "Des factures ont produit une écriture ou un règlement : rien n'est supprimé.",
                "factures": [f["facture_ref"] for f in engagees]}

    # ── LA SEULE LEVÉE : le verrou « mois clôturé », le temps de l'opération ────────────────────
    # Restauré dans le `finally`, quoi qu'il arrive. Aucun statut de clôture n'est écrit.
    garde_origine = fact._mois_est_cloture
    fact._mois_est_cloture = lambda mois, db_path=None: False
    supprimees, refus = [], []
    try:
        for f in cibles:
            res = fact.supprimer(f["facture_id_opaque"], motif=MOTIF, acteur=acteur,
                                 db_path=db_path)
            trace = {"facture_id_opaque": f["facture_id_opaque"], "facture_ref": f["facture_ref"],
                     "facture_ref_source": f.get("facture_ref_source"),
                     "mois": f.get("mois_concerne"), "montant_ttc": f["montant_ttc"],
                     "fichier_source": f.get("fichier_source"), "statut": f["statut"]}
            if res.get("ok"):
                supprimees.append({**trace, "tables_nettoyees": res.get("tables_nettoyees")})
            else:
                refus.append({**trace, "code": res.get("code"), "message": res.get("message")})
    finally:
        fact._mois_est_cloture = garde_origine

    journal = {
        "operation": "MAINTENANCE_TEST_RESET_FACTURES_PDF",
        "horodatage": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "acteur": acteur, "motif": MOTIF, "sauvegarde": str(sauvegarde),
        "nb_supprimees": len(supprimees), "nb_refus": len(refus),
        "supprimees": supprimees, "refus": refus,
    }
    chemin = Path(sauvegarde).with_name(
        f"maintenance_reset_factures_{_horodatage()}.json")
    chemin.write_text(json.dumps(journal, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"ok": not refus, "sauvegarde": str(sauvegarde), "journal": str(chemin),
            "nb_supprimees": len(supprimees), "nb_refus": len(refus), "refus": refus,
            "restant": len(fact.lister(db_path=db_path))}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--confirmer", action="store_true",
                    help="obligatoire : confirme qu'on est bien en phase de TEST")
    ap.add_argument("--db", default=None)
    a = ap.parse_args(argv)
    if not a.confirmer:
        print("Refus : opération de maintenance de TEST. Relancer avec --confirmer.")
        return 2
    sys.stdout.reconfigure(encoding="utf-8")
    resultat = executer(db_path=a.db)
    print(json.dumps(resultat, ensure_ascii=False, indent=2))
    return 0 if resultat.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
