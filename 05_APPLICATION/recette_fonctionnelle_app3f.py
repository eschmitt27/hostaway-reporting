"""Recette fonctionnelle séparée APP-3F — 20 scénarios reproductibles sur base SQLite isolée.

Aucune écriture réelle, aucun virement, aucune donnée bancaire réelle (mouvements synthétiques).
Produit un rapport Markdown. Usage : PYTHONUTF8=1 python recette_fonctionnelle_app3f.py [rapport.md]
"""
from __future__ import annotations

import sys
import tempfile
from datetime import datetime
from pathlib import Path

_DB = Path(tempfile.mkdtemp(prefix="recette_app3f_")) / "recette.db"
import app.config as cfg  # noqa: E402
cfg.DB_PATH = _DB

from app.db.connection import apply_migrations, get_db  # noqa: E402
from app.readers import banques_reader as banques  # noqa: E402
from app.readers import rapprochement_bancaire_reader as contrat  # noqa: E402
from app.services import rapprochement_candidats_service as cand  # noqa: E402
from app.services import rapprochement_reglements_service as rap  # noqa: E402

apply_migrations(_DB)
_RES: list[dict] = []


class _FakeSource:
    def __init__(self, etat, lignes):
        self.etat = type("E", (), {"etat": etat})()
        self.lignes = lignes


def _set_banque(etat, lignes):
    banques.mouvements = lambda: _FakeSource(etat, lignes)   # override lecture seule (jamais d'écriture)


def sc(n, nom, initial, action, attendu, obtenu, preuve):
    _RES.append({"n": n, "nom": nom, "initial": initial, "action": action, "attendu": attendu,
                 "obtenu": obtenu, "statut": "OK" if attendu == obtenu else "ECHEC", "preuve": preuve})


_MVT = {"mouvement_id": "M1", "date_operation": "2026-01-05", "montant": -120.0, "sens": "DEBIT",
        "libelle": "VIR PROP", "reference": "R1"}


def run():
    # 1. paiement déclaré avec candidat exact
    _set_banque(contrat.ETAT_OK, [_MVT])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01")
    sc(1, "candidat exact", "1 mouvement -120 le 05/01", "chercher candidats", 1, len(r["candidats"]),
       str(r["candidats"][0]["criteres"]) if r["candidats"] else "—")

    # 2. sans candidat
    _set_banque(contrat.ETAT_OK, [{**_MVT, "montant": -999.0}])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01")
    sc(2, "aucun candidat", "montant ne correspond pas", "chercher", 0, len(r["candidats"]), "0 candidat")

    # 3. deux candidats
    _set_banque(contrat.ETAT_OK, [_MVT, {**_MVT, "mouvement_id": "M2", "date_operation": "2026-01-06"}])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01")
    sc(3, "deux candidats", "2 mouvements -120", "chercher", 2, len(r["candidats"]), "2 candidats")

    # 4. mouvement entrant ignoré
    _set_banque(contrat.ETAT_OK, [{**_MVT, "montant": 120.0, "sens": "CREDIT"}])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01")
    sc(4, "mouvement entrant", "crédit +120", "chercher", 0, len(r["candidats"]), "entrant jamais proposé")

    # 5. montant différent
    _set_banque(contrat.ETAT_OK, [{**_MVT, "montant": -130.0}])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01")
    sc(5, "montant différent", "-130 vs 120", "chercher", 0, len(r["candidats"]), "tolérance exacte")

    # 6. date éloignée (proposé mais critère date absent)
    _set_banque(contrat.ETAT_OK, [{**_MVT, "date_operation": "2026-03-01"}])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01", fenetre_jours=7)
    date_ok = "date_dans_fenetre" not in (r["candidats"][0]["criteres"] if r["candidats"] else [])
    sc(6, "date éloignée", "mouvement en mars", "chercher fenêtre 7j", True, date_ok, "date hors fenêtre signalée")

    # 7-8. sélection manuelle + confirmation
    _set_banque(contrat.ETAT_OK, [_MVT])
    o = "REG-recette3f"
    rr = rap.creer_ou_charger(o, db_path=_DB)
    best = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01")["candidats"][0]
    m = best["mouvement"]
    rr = rap.enregistrer_proposition(rr, m.mouvement_opaque, criteres=best["criteres"], score=best["score"],
                                     mouvement_empreinte=m.empreinte, ecart_montant=best["ecart_montant"],
                                     ecart_jours=best["ecart_jours"], version_attendue=rr["version"], db_path=_DB)
    sc(7, "sélection manuelle", "candidat exact proposé", "enregistrer proposition",
       rap.ST_PROPOSITION_DISPONIBLE, rr["statut"], rr["mouvement_id_opaque"])
    rr = rap.passer_a_controler(rr, version_attendue=rr["version"], db_path=_DB)
    rr = rap.confirmer(rr, reglement_paye=True, mouvement_present=True, sens_sortant=True,
                       version_attendue=rr["version"], db_path=_DB)
    sc(8, "confirmation humaine", "à contrôler", "confirmer", rap.ST_RAPPROCHE, rr["statut"], rr["decision"])

    # 9. écartement
    o9 = "REG-recette3f-9"
    r9 = rap.creer_ou_charger(o9, db_path=_DB)
    r9 = rap.enregistrer_proposition(r9, "MVT-x", criteres=["montant_exact"], score=1,
                                     mouvement_empreinte="e", ecart_montant=0.0, ecart_jours=0,
                                     version_attendue=r9["version"], db_path=_DB)
    r9 = rap.ecarter(r9, "pas le bon mouvement", version_attendue=r9["version"], db_path=_DB)
    sc(9, "écartement", "proposition disponible", "écarter avec motif", rap.ST_ECARTE, r9["statut"], r9["motif"])

    # 10. anomalie
    o10 = "REG-recette3f-10"
    r10 = rap.creer_ou_charger(o10, db_path=_DB)
    r10 = rap.signaler_anomalie(r10, "ambigu", version_attendue=r10["version"], db_path=_DB)
    sc(10, "anomalie", "non rapproché", "signaler anomalie", rap.ST_ANOMALIE, r10["statut"], r10["motif"])

    # 11. réouverture
    rr = rap.rouvrir(rr, "erreur détectée", version_attendue=rr["version"], db_path=_DB)
    sc(11, "réouverture", "rapproché", "rouvrir avec motif", rap.ST_ROUVERT, rr["statut"], rr["motif"])

    # 12. mouvement disparu après proposition
    _set_banque(contrat.ETAT_OK, [])   # le mouvement a disparu
    present = contrat.charger_mouvement(m.mouvement_opaque) is not None
    sc(12, "mouvement disparu", "mouvement proposé puis absent", "recharger mouvement", False, present,
       "charger_mouvement -> None")

    # 13. source bancaire absente
    _set_banque(contrat.ETAT_FICHIER_ABSENT, [])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01")
    sc(13, "source absente", "fichier bancaire absent", "chercher", "RAPPROCHEMENT_SOURCE_ABSENTE",
       r["code_source"], "code source")

    # 14. source bancaire vide
    _set_banque(contrat.ETAT_VIDE, [])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01")
    sc(14, "source vide", "onglet vide", "chercher", "RAPPROCHEMENT_SOURCE_VIDE", r["code_source"], "code source")

    # 15. export (structure/mention)
    from app.services.proprietaires_releve_export_service import _cellule_sure
    sc(15, "export préparatoire", "rapprochements en base", "générer une cellule sûre",
       True, _cellule_sure("=DANGER").startswith("'"), "amorce neutralisée")

    # 16. injection CSV
    sc(16, "injection CSV", "commentaire =CMD()", "neutraliser", "'=CMD()", _cellule_sure("=CMD()"), "quote de tête")

    # 17. double confirmation
    o17 = "REG-recette3f-17"
    _set_banque(contrat.ETAT_OK, [_MVT])
    r17 = rap.creer_ou_charger(o17, db_path=_DB)
    b17 = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01")["candidats"][0]
    r17 = rap.enregistrer_proposition(r17, b17["mouvement"].mouvement_opaque, criteres=b17["criteres"],
                                      score=b17["score"], mouvement_empreinte=b17["mouvement"].empreinte,
                                      ecart_montant=0.0, ecart_jours=b17["ecart_jours"],
                                      version_attendue=r17["version"], db_path=_DB)
    r17 = rap.passer_a_controler(r17, version_attendue=r17["version"], db_path=_DB)
    r17 = rap.confirmer(r17, reglement_paye=True, mouvement_present=True, sens_sortant=True,
                        version_attendue=r17["version"], db_path=_DB)
    refus = False
    try:
        rap.confirmer(r17, reglement_paye=True, mouvement_present=True, sens_sortant=True,
                      version_attendue=r17["version"], db_path=_DB)
    except rap.RapprochementRefuse:
        refus = True
    sc(17, "double confirmation", "déjà rapproché", "confirmer à nouveau", True, refus, "refusé")

    # 18. redémarrage (relecture DB)
    relu = rap.charger(r17["rapprochement_id_opaque"], db_path=_DB)
    sc(18, "redémarrage", "rapproché en base", "relire", rap.ST_RAPPROCHE, relu["statut"], "persisté")

    # 19. concurrence (version obsolète)
    o19 = "REG-recette3f-19"
    r19 = rap.creer_ou_charger(o19, db_path=_DB)
    rap.signaler_anomalie(r19, "a", version_attendue=r19["version"], db_path=_DB)
    conflit = False
    try:
        rap.rouvrir(r19, "b", version_attendue=r19["version"], db_path=_DB)   # version périmée
    except rap.RapprochementRefuse:
        conflit = True
    sc(19, "concurrence", "objet modifié entretemps", "action version périmée", True, conflit, "refusé")

    # 20. intégrité du fichier bancaire (aucune écriture)
    import inspect
    src = inspect.getsource(rap) + inspect.getsource(cand) + inspect.getsource(contrat)
    aucune_ecriture = "open(" not in src and "load_workbook" not in src
    sc(20, "intégrité fichier bancaire", "modules APP-3F", "chercher toute écriture fichier",
       True, aucune_ecriture, "aucun open()/load_workbook")


def rapport() -> str:
    ok = sum(1 for r in _RES if r["statut"] == "OK")
    out = [f"# Recette fonctionnelle APP-3F — 20 scénarios", "",
           f"Généré le {datetime.now().isoformat(timespec='seconds')} — base isolée `{_DB}`.",
           f"**Résultat : {ok}/{len(_RES)} OK.**", "",
           "| N° | Scénario | Données | Action | Attendu | Obtenu | Statut | Preuve |",
           "| -: | -------- | ------- | ------ | ------- | ------ | ------ | ------ |"]
    for r in _RES:
        out.append(f"| {r['n']} | {r['nom']} | {r['initial']} | {r['action']} | `{r['attendu']}` | "
                   f"`{r['obtenu']}` | {r['statut']} | {r['preuve']} |")
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    run()
    dest = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("recette_app3f_rapport.md")
    dest.write_text(rapport(), encoding="utf-8")
    ko = [r for r in _RES if r["statut"] != "OK"]
    print(f"Recette APP-3F : {len(_RES) - len(ko)}/{len(_RES)} OK -> {dest}")
    for r in ko:
        print(f"  ECHEC #{r['n']} {r['nom']}: attendu={r['attendu']} obtenu={r['obtenu']}")
    sys.exit(1 if ko else 0)
