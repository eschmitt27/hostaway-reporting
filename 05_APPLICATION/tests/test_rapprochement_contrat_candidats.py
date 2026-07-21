"""APP-3F — Contrat du reader bancaire (masquage/état) + recherche de candidats + contrôles.

Fixtures synthétiques (jamais de données bancaires réelles). Le fichier est monkeypatché en lecture
seule via `banques_reader.mouvements`.
"""
import pytest

from app.readers import rapprochement_bancaire_reader as contrat
from app.services import rapprochement_candidats_service as cand


class _FakeSource:
    def __init__(self, etat, lignes):
        self.etat = type("E", (), {"etat": etat})()
        self.lignes = lignes


def _patch_mouvements(monkeypatch, etat, lignes):
    from app.readers import banques_reader as b
    monkeypatch.setattr(b, "mouvements", lambda: _FakeSource(etat, lignes))


_MVT = {"mouvement_id": "M001", "date_operation": "2026-01-05", "montant": -120.0, "sens": "DEBIT",
        "libelle": "VIR PROP DUPONT", "reference": "REF-2026-01"}


# ── Contrat du reader ────────────────────────────────────────────────────────

def test_01_contrat_expose_opaque_et_masque(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_OK, [_MVT])
    s = contrat.source()
    assert s.disponible and len(s.mouvements) == 1
    m = s.mouvements[0]
    assert m.mouvement_opaque.startswith("MVT-")
    assert m.sens == "DEBIT" and m.montant == -120.0 and m.mois == "2026-01"
    assert m.empreinte and m.reference_normalisee == "REF202601"


def test_02_contrat_ne_fuit_aucune_donnee_bancaire(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_OK, [
        {**_MVT, "compte_id": "CM_02211_00021321603", "libelle_brut": "IBAN FR76 1234"}])
    m = contrat.source().mouvements[0]
    champs = vars(m)
    for interdit in ("iban", "rib", "bic", "compte", "libelle_brut", "numero"):
        assert not any(interdit in k.lower() for k in champs), f"champ {interdit} exposé"
    # aucune valeur ne contient l'IBAN brut ni le compte
    assert "FR76" not in str(champs) and "02211" not in str(champs)


def test_03_source_absente(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_FICHIER_ABSENT, [])
    s = contrat.source()
    assert not s.disponible and s.mouvements == []


def test_04_source_vide(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_VIDE, [])
    assert contrat.source().disponible is False


def test_05_schema_invalide(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_ILLISIBLE, [])
    assert contrat.source().disponible is False


def test_06_mouvement_invalide_sans_id_ignore(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_OK, [{"date_operation": "2026-01-05", "montant": -1}])
    assert contrat.source().mouvements == []   # pas de mouvement_id -> ignoré, jamais un crash


def test_07_charger_mouvement_disparu(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_OK, [_MVT])
    assert contrat.charger_mouvement("MVT-inexistant") is None


def test_08_empreinte_change_si_mouvement_modifie(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_OK, [_MVT])
    e1 = contrat.source().mouvements[0].empreinte
    _patch_mouvements(monkeypatch, contrat.ETAT_OK, [{**_MVT, "montant": -999.0}])
    e2 = contrat.source().mouvements[0].empreinte
    assert e1 != e2


# ── Recherche de candidats ───────────────────────────────────────────────────

def test_09_candidat_exact(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_OK, [_MVT])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01")
    assert len(r["candidats"]) == 1
    c = r["candidats"][0]
    assert "montant_exact" in c["criteres"] and "sens_sortant" in c["criteres"]
    assert "mois_compatible" in c["criteres"] and "date_dans_fenetre" in c["criteres"]


def test_10_candidat_reference_compatible(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_OK, [_MVT])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01",
                                reference_interne="ref 2026 01")
    assert "reference_compatible" in r["candidats"][0]["criteres"]


def test_11_aucun_candidat_si_entrant(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_OK, [{**_MVT, "montant": 120.0, "sens": "CREDIT"}])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01")
    assert r["candidats"] == []   # mouvement entrant jamais proposé


def test_12_aucun_candidat_si_montant_different(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_OK, [{**_MVT, "montant": -130.0}])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01")
    assert r["candidats"] == []   # tolérance exacte : montant différent non proposé


def test_13_plusieurs_candidats(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_OK, [
        {**_MVT, "mouvement_id": "M001"}, {**_MVT, "mouvement_id": "M002", "date_operation": "2026-01-06"}])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01")
    assert len(r["candidats"]) == 2


def test_14_date_hors_fenetre_exclut(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_OK, [{**_MVT, "date_operation": "2026-03-01"}])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01",
                                fenetre_jours=7)
    # montant exact + sortant -> proposé, mais date_dans_fenetre absent des critères
    assert len(r["candidats"]) == 1 and "date_dans_fenetre" not in r["candidats"][0]["criteres"]


def test_15_mouvement_deja_rapproche_exclu(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_OK, [_MVT])
    opq = contrat.source().mouvements[0].mouvement_opaque
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01",
                                mouvements_deja_rapproches={opq})
    assert r["candidats"] == []


def test_16_source_absente_code(monkeypatch):
    _patch_mouvements(monkeypatch, contrat.ETAT_FICHIER_ABSENT, [])
    r = cand.chercher_candidats(montant_declare=120.0, date_declaree="2026-01-05", mois="2026-01")
    assert r["code_source"] == "RAPPROCHEMENT_SOURCE_ABSENTE" and r["candidats"] == []


def test_17_fenetre_documentee_dans_le_service():
    import inspect
    src = inspect.getsource(cand)
    assert "FENETRE_JOURS_DEFAUT" in src and "TOLERANCE_MONTANT_EXACT" in src


# ── Contrôles ────────────────────────────────────────────────────────────────

def test_18_controle_reglement_non_paye_bloque():
    r = cand.evaluer_controles(source_etat_code=None, reglement_paye=False, reglement_annule=False,
                               nb_candidats=1, mouvement_present=True)
    assert "RAPPROCHEMENT_REGLEMENT_NON_PAYE" in r["bloquants"] and not r["confirmable"]


def test_19_controle_reglement_annule_bloque():
    r = cand.evaluer_controles(source_etat_code=None, reglement_paye=True, reglement_annule=True,
                               nb_candidats=1, mouvement_present=True)
    assert "RAPPROCHEMENT_REGLEMENT_ANNULE" in r["bloquants"]


def test_20_aucun_candidat_est_informatif():
    r = cand.evaluer_controles(source_etat_code=None, reglement_paye=True, reglement_annule=False,
                               nb_candidats=0, mouvement_present=None)
    assert "RAPPROCHEMENT_AUCUN_CANDIDAT" in r["informatifs"]
    assert "RAPPROCHEMENT_AUCUN_CANDIDAT" not in r["bloquants"]


def test_21_plusieurs_candidats_informatif():
    r = cand.evaluer_controles(source_etat_code=None, reglement_paye=True, reglement_annule=False,
                               nb_candidats=3, mouvement_present=None)
    assert "RAPPROCHEMENT_PLUSIEURS_CANDIDATS" in r["informatifs"]


def test_22_montant_different_informatif():
    r = cand.evaluer_controles(source_etat_code=None, reglement_paye=True, reglement_annule=False,
                               nb_candidats=1, mouvement_present=True, ecart_montant=10.0)
    assert "RAPPROCHEMENT_MONTANT_DIFFERENT" in r["informatifs"]


def test_23_mouvement_disparu_bloque():
    r = cand.evaluer_controles(source_etat_code=None, reglement_paye=True, reglement_annule=False,
                               nb_candidats=1, mouvement_present=False)
    assert "RAPPROCHEMENT_MOUVEMENT_DISPARU" in r["bloquants"]


def test_24_source_modifiee_bloque():
    r = cand.evaluer_controles(source_etat_code=None, reglement_paye=True, reglement_annule=False,
                               nb_candidats=1, mouvement_present=True, empreinte_snapshot="a",
                               empreinte_courante="b")
    assert "RAPPROCHEMENT_SOURCE_MODIFIEE" in r["bloquants"]


def test_25_sens_invalide_bloque():
    r = cand.evaluer_controles(source_etat_code=None, reglement_paye=True, reglement_annule=False,
                               nb_candidats=1, mouvement_present=True, sens_sortant=False)
    assert "RAPPROCHEMENT_SENS_INVALIDE" in r["bloquants"]


def test_26_mouvement_deja_utilise_bloque():
    r = cand.evaluer_controles(source_etat_code=None, reglement_paye=True, reglement_annule=False,
                               nb_candidats=1, mouvement_present=True, mouvement_deja_utilise=True)
    assert "RAPPROCHEMENT_MOUVEMENT_DEJA_UTILISE" in r["bloquants"]


def test_27_confirmable_si_tout_ok():
    r = cand.evaluer_controles(source_etat_code=None, reglement_paye=True, reglement_annule=False,
                               nb_candidats=1, mouvement_present=True, sens_sortant=True,
                               ecart_montant=0.0)
    assert r["confirmable"] is True and r["bloquants"] == []
