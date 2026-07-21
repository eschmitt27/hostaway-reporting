"""Recette fonctionnelle séparée APP-3E — 26 scénarios reproductibles sur base SQLite isolée.

Aucune écriture réelle, aucun virement, aucune donnée bancaire. Produit un rapport Markdown
(données initiales / action / attendu / obtenu / statut / preuve). Usage :

    PYTHONUTF8=1 python recette_fonctionnelle_app3e.py [chemin_rapport.md]

La base est un fichier temporaire jetable ; les fichiers métier réels ne sont jamais touchés.
"""
from __future__ import annotations

import sys
import tempfile
from datetime import datetime
from pathlib import Path

# Base isolée AVANT tout import applicatif qui lirait cfg.DB_PATH.
_DB = Path(tempfile.mkdtemp(prefix="recette_app3e_")) / "recette.db"
import app.config as cfg  # noqa: E402
cfg.DB_PATH = _DB

from app.db.connection import apply_migrations, get_db  # noqa: E402
from app.services import charges_affectations_service as aff  # noqa: E402
from app.services import charges_controles_service as ctrl  # noqa: E402
from app.services import fournisseurs_referentiel_service as frs  # noqa: E402
from app.services import fournisseur_rattachements_service as fl  # noqa: E402
from app.services import proprietaires_releve_cycle_service as cycle_svc  # noqa: E402
from app.services import proprietaires_paiement_service as pay  # noqa: E402

apply_migrations(_DB)

_RESULTATS: list[dict] = []


def scenario(num, nom, initial, action, attendu, obtenu, preuve):
    ok = attendu == obtenu
    _RESULTATS.append({
        "num": num, "nom": nom, "initial": initial, "action": action,
        "attendu": attendu, "obtenu": obtenu, "statut": "OK" if ok else "ECHEC", "preuve": preuve,
    })


def run():
    # 1. Propriétaire simple — affectation d'une charge à un fournisseur actif.
    f1 = frs.creer("Ménage Pro", "MENAGE", db_path=_DB)
    a1 = aff.affecter("CHG-R1", fournisseur_id_opaque=f1["fournisseur_id_opaque"], logement_id="L1",
                      proprietaire_id="PROP1", mois="2026-01", nature="menage", db_path=_DB)
    scenario(1, "propriétaire simple", "1 fournisseur actif, 1 charge",
             "affecter la charge", "CHA- opaque créé", a1["affectation_id_opaque"][:3] + "- opaque créé",
             a1["affectation_id_opaque"])

    # 2. Propriétaire multi-logements — deux charges/logements.
    aff.affecter("CHG-R2a", proprietaire_id="PROP2", mois="2026-01", logement_id="L2", db_path=_DB)
    aff.affecter("CHG-R2b", proprietaire_id="PROP2", mois="2026-01", logement_id="L3", db_path=_DB)
    rows = aff.lister_par_proprietaire_mois("PROP2", "2026-01", db_path=_DB)
    scenario(2, "propriétaire multi-logements", "2 charges, 2 logements",
             "lister par propriétaire/mois", 2, len(rows), f"{len(rows)} affectations")

    # 3. Relevé sans anomalie — contrôle conforme.
    r3 = ctrl.evaluer({"montant": 10.0, "mois": "2026-01", "logement_id": "L1", "proprietaire_id": "PROP1"},
                      a1, db_path=_DB)
    scenario(3, "relevé sans anomalie", "charge affectée, fournisseur actif",
             "évaluer contrôles", True, r3["conforme"], f"bloquants={r3['bloquants']}")

    # 4. Charge sans fournisseur.
    a4 = aff.affecter("CHG-R4", logement_id="L1", nature="menage", db_path=_DB)
    r4 = ctrl.evaluer({"montant": 10.0, "mois": "2026-01", "logement_id": "L1", "proprietaire_id": "P"},
                      a4, db_path=_DB)
    scenario(4, "charge sans fournisseur", "affectation sans fournisseur",
             "évaluer", True, "CHARGE_FOURNISSEUR_ABSENT" in r4["bloquants"], str(r4["bloquants"]))

    # 5. Fournisseur inactif.
    f5 = frs.creer("Inactif SARL", "MAINTENANCE", db_path=_DB)
    frs.desactiver(f5, version_attendue=f5["version"], db_path=_DB)
    a5 = aff.affecter("CHG-R5", fournisseur_id_opaque=f5["fournisseur_id_opaque"], logement_id="L1",
                      nature="maintenance", db_path=_DB)
    r5 = ctrl.evaluer({"montant": 10.0, "mois": "2026-01", "logement_id": "L1", "proprietaire_id": "P"},
                      a5, db_path=_DB)
    scenario(5, "fournisseur inactif", "fournisseur désactivé puis charge affectée",
             "évaluer", True, "CHARGE_FOURNISSEUR_INACTIF" in r5["bloquants"], str(r5["bloquants"]))

    # 6. Fournisseur changé sur un logement (association historisée).
    f6a = frs.creer("Ancien Maint", "MAINTENANCE", db_path=_DB)
    f6b = frs.creer("Nouveau Maint", "MAINTENANCE", db_path=_DB)
    as6 = fl.associer(f6a["fournisseur_id_opaque"], "L9", type_prestation="MAINTENANCE",
                      date_debut="2026-01-01", db_path=_DB)
    as6b = fl.changer_fournisseur("L9", as6, f6b["fournisseur_id_opaque"], date_bascule="2026-07-01",
                                  type_prestation="MAINTENANCE", db_path=_DB)
    ancien = fl.charger_par_opaque(as6["association_id_opaque"], db_path=_DB)
    scenario(6, "fournisseur changé sur un logement", "association ouverte L9",
             "changer_fournisseur au 01/07", "fermé + nouveau ouvert",
             ("fermé" if ancien["date_fin"] else "ouvert") + (" + nouveau ouvert" if as6b["date_fin"] is None else ""),
             f"ancien.date_fin={ancien['date_fin']}, nouveau.date_fin={as6b['date_fin']}")

    # 7. Payout incomplet (montant moteur absent) — bloque prêt à payer.
    codes7 = pay.controler_passage_pret_a_payer(
        cycle_etat="VALIDE", derive=False, statut_app5c_compatible=True, statut_moteur_compatible=True,
        montant_moteur_disponible=False, proprietaire_connu=True, source_obligatoire_disponible=True)
    scenario(7, "payout incomplet", "montant moteur indisponible",
             "contrôle passage prêt à payer", True, "DONNEE_MOTEUR_INDISPONIBLE" in codes7, str(codes7))

    # 8. Annulation indemnisée (charge = ajustement avec motif) — pas de blocage AJUSTEMENT_SANS_MOTIF.
    r8 = ctrl.evaluer({"montant": -50.0, "mois": "2026-01", "logement_id": "L1", "proprietaire_id": "P",
                       "est_ajustement": True, "motif": "annulation indemnisée"}, None)
    scenario(8, "annulation indemnisée", "ajustement avec motif",
             "évaluer", True, "AJUSTEMENT_SANS_MOTIF" not in r8["bloquants"], str(r8["bloquants"]))

    # 9. Charge refacturable sans justificatif — informatif, jamais bloquant.
    f9 = frs.creer("Refact SARL", "FOURNITURE", db_path=_DB)
    a9 = aff.affecter("CHG-R9", fournisseur_id_opaque=f9["fournisseur_id_opaque"], logement_id="L1",
                      nature="fourniture", refacturable=True, db_path=_DB)
    r9 = ctrl.evaluer({"montant": 10.0, "mois": "2026-01", "logement_id": "L1", "proprietaire_id": "P"},
                      a9, db_path=_DB)
    scenario(9, "charge refacturable", "refacturable sans justificatif",
             "évaluer", True,
             "CHARGE_REFACTURABLE_NON_JUSTIFIEE" in r9["informatifs"] and
             "CHARGE_REFACTURABLE_NON_JUSTIFIEE" not in r9["bloquants"],
             f"info={r9['informatifs']}, bloq={r9['bloquants']}")

    # 10. AirCover (charge divergence moteur) — informatif.
    r10 = ctrl.evaluer({"montant": 10.0, "mois": "2026-01", "logement_id": "L1", "proprietaire_id": "P",
                        "divergence_moteur": True}, None)
    scenario(10, "AirCover / divergence moteur", "divergence signalée",
             "évaluer", True, "CHARGE_DIVERGENCE_MOTEUR" in r10["informatifs"], str(r10["informatifs"]))

    # 11-13. Acompte / reversement / ajustement → dérive du snapshot.
    opaque = "REG-recette01"
    c = cycle_svc.creer_ou_charger(opaque, db_path=_DB)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=_DB)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=_DB)
    c = cycle_svc.valider(c, {"acomptes": [], "reversements": [], "ajustements": []},
                          version_attendue=c["version"], db_path=_DB)
    d11 = cycle_svc.detecter_derive(c, {"acomptes": [{"m": 1}], "reversements": [], "ajustements": []})
    scenario(11, "acompte ajouté", "snapshot figé sans acompte",
             "détecter dérive après ajout acompte", True, d11["derive"], str(d11["champs_modifies"]))
    d12 = cycle_svc.detecter_derive(c, {"acomptes": [], "reversements": [{"m": 1}], "ajustements": []})
    scenario(12, "reversement ajouté", "snapshot figé sans reversement",
             "détecter dérive", True, d12["derive"], str(d12["champs_modifies"]))
    d13 = cycle_svc.detecter_derive(c, {"acomptes": [], "reversements": [], "ajustements": [{"m": 1}]})
    scenario(13, "ajustement ajouté", "snapshot figé sans ajustement",
             "détecter dérive", True, d13["derive"], str(d13["champs_modifies"]))

    # 14. Création du snapshot — empreinte non vide.
    scenario(14, "création du snapshot", "relevé validé",
             "lire empreinte snapshot", True, bool(c["snapshot_empreinte"]),
             f"empreinte={c['snapshot_empreinte'][:12]}…")

    # 15-16. Évolution ultérieure + détection de dérive (statut moteur).
    d16 = cycle_svc.detecter_derive(c, {"statut_moteur": "OUVERT"})
    scenario(15, "évolution ultérieure des données", "snapshot figé",
             "modifier statut moteur", True, d16["derive"], str(d16["champs_modifies"]))
    scenario(16, "détection de dérive", "statut moteur changé",
             "détecter dérive", True, "statut_moteur" in d16["champs_modifies"], str(d16["champs_modifies"]))

    # 17. Réouverture (cycle).
    c = cycle_svc.rouvrir(c, "données évoluées", version_attendue=c["version"], db_path=_DB)
    scenario(17, "réouverture", "relevé validé + dérive",
             "rouvrir avec motif", cycle_svc.ETAT_ROUVERT, c["etat_cycle"], f"état={c['etat_cycle']}")

    # 18. Préfacture (document préparatoire) — pas de numéro légal. Contrôle structurel.
    import app.services.proprietaires_service as legacy
    src = Path(legacy.__file__).read_text(encoding="utf-8")
    scenario(18, "préfacture préparatoire", "code du service relevé",
             "chercher un numéro légal définitif", False, "numero_legal" in src.lower(),
             "aucun champ numero_legal")

    # 19-21. Préparation du règlement : contrôler → prêt à payer → payé.
    p = pay.creer_ou_charger(opaque, db_path=_DB)
    p = pay.demarrer_controle(p, version_attendue=p["version"], db_path=_DB)
    scenario(19, "passage à contrôler", "paiement NON_PREPARE",
             "démarrer contrôle", pay.ST_A_CONTROLER, p["statut_paiement"], p["statut_paiement"])
    p = pay.marquer_pret_a_payer(p, version_attendue=p["version"], db_path=_DB)
    scenario(20, "passage prêt à payer", "aucun bloquant",
             "marquer prêt à payer", pay.ST_PRET_A_PAYER, p["statut_paiement"], p["statut_paiement"])
    p = pay.marquer_paye(p, reference_interne="vu sur relevé du 05/01", version_attendue=p["version"], db_path=_DB)
    scenario(21, "marquage comme payé", "paiement prêt à payer",
             "marquer payé (déclaratif)", pay.ST_MARQUE_COMME_PAYE, p["statut_paiement"], p["statut_paiement"])

    # 22. Preuve qu'aucun virement n'est déclenché — aucun appel réseau dans le service paiement.
    src_pay = Path(pay.__file__).read_text(encoding="utf-8").lower()
    reseau = any(t in src_pay for t in ("import requests", "import httpx", "urllib.request", "socket.", "sepa"))
    scenario(22, "aucun virement déclenché", "service paiement",
             "chercher un appel réseau/bancaire", False, reseau, "aucun import réseau/SEPA")

    # 23. Export préparatoire — mention non-ordre bancaire, aucune donnée bancaire.
    from app.services.proprietaires_releve_export_service import _cellule_sure
    ref_dangereuse = _cellule_sure("=CMD()")
    scenario(23, "export préparatoire", "référence avec amorce de formule",
             "neutraliser l'injection CSV", True, ref_dangereuse.startswith("'"),
             f"cellule={ref_dangereuse!r}")

    # 24. Tentative de doublon d'affectation.
    doublon_ok = False
    try:
        aff.affecter("CHG-R1", db_path=_DB)   # charge déjà affectée en scénario 1
    except aff.AffectationRefusee:
        doublon_ok = True
    scenario(24, "tentative de doublon", "charge CHG-R1 déjà affectée",
             "réaffecter la même charge", True, doublon_ok, "AffectationRefusee levée")

    # 25. Source indisponible → contrôle bloque.
    r25 = ctrl.evaluer({}, None, source_etat="ABSENTE")
    scenario(25, "source indisponible", "source de charges absente",
             "évaluer", True, "CHARGE_SOURCE_ABSENTE" in r25["bloquants"], str(r25["bloquants"]))

    # 26. Redémarrage — relecture DB : le statut de paiement persiste.
    p_relu = pay.charger(opaque, db_path=_DB)
    scenario(26, "redémarrage", "paiement MARQUE_COMME_PAYE en base",
             "relire depuis la base (redémarrage simulé)", pay.ST_MARQUE_COMME_PAYE,
             p_relu["statut_paiement"], p_relu["statut_paiement"])


def rapport_markdown() -> str:
    total = len(_RESULTATS)
    ok = sum(1 for r in _RESULTATS if r["statut"] == "OK")
    lignes = [
        "# Recette fonctionnelle APP-3E — 26 scénarios",
        "",
        f"Généré le {datetime.now().isoformat(timespec='seconds')} — base isolée `{_DB}`.",
        f"**Résultat : {ok}/{total} OK.**",
        "",
        "| N° | Scénario | Données initiales | Action | Attendu | Obtenu | Statut | Preuve |",
        "| -: | -------- | ----------------- | ------ | ------- | ------ | ------ | ------ |",
    ]
    for r in _RESULTATS:
        lignes.append(
            f"| {r['num']} | {r['nom']} | {r['initial']} | {r['action']} | `{r['attendu']}` | "
            f"`{r['obtenu']}` | {r['statut']} | {r['preuve']} |")
    return "\n".join(lignes) + "\n"


if __name__ == "__main__":
    run()
    sortie = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("recette_fonctionnelle_app3e_rapport.md")
    sortie.write_text(rapport_markdown(), encoding="utf-8")
    echecs = [r for r in _RESULTATS if r["statut"] != "OK"]
    print(f"Recette fonctionnelle : {len(_RESULTATS) - len(echecs)}/{len(_RESULTATS)} OK -> {sortie}")
    if echecs:
        for r in echecs:
            print(f"  ECHEC #{r['num']} {r['nom']} : attendu={r['attendu']} obtenu={r['obtenu']}")
        sys.exit(1)
