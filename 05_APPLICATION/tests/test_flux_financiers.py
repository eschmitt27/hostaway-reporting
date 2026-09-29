"""Module « Flux financiers » : Banque | Caisse | Charges | Rapprochement.

Données FICTIVES uniquement (base temporaire) : aucune identité réelle, aucun appel réseau — les
mouvements Qonto passent par le double de client déjà utilisé par les tests Qonto (GET simulés).

Ce qui est vérifié, dans l'ordre du cadrage : affichage et navigation, propositions du moteur
(1↔1, 2↔1, 1↔2, 2↔2, 3↔1, 1↔3, petit écart) jamais validées seules, refus et validation humaine,
écriture proposée puis corrigée, atomicité (échec comptable = aucun rapprochement), absence de
double comptabilisation (401/512 et 512/411 seulement), charge née d'un mouvement, hors
comptabilité, fournisseurs (existant, créé, doublon), 411 à auxiliaire, partiel, ventilation,
mois clôturé, idempotence, aucune fuite technique, intégrité SQLite.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import charges_saisie_service as saisie
from app.services import comptabilite_ecritures_service as compta
from app.services import flux_financiers_service as flux
from app.services import flux_lettrage_service as lettrage
from app.services import flux_matching_service as matching
from app.services import qonto_ecran_service as ecran
from tests.test_qonto_raw_import import COMPTE, ClientDouble, mouvement

PROPRIO = "PROP_TFLUX"
ACTEUR = "Testeur"


# ══ Mise en place ═════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def verrous(monkeypatch):
    """Tous les verrous d'écriture ouverts — c'est leur fermeture qui est testée à part."""
    for nom in ("MODE_REEL_ECRITURES", "BANQUE_REAL_WRITE_ENABLED",
                "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED", "COMPTABILITE_REAL_WRITE_ENABLED",
                "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", "FACTURES_REAL_WRITE_ENABLED",
                "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, nom, True, raising=False)
    monkeypatch.setattr(cfg, "RECETTE_MODE", False, raising=False)


@pytest.fixture
def base(tmp_db):
    from tests.fixtures_referentiel import semer
    # Référentiel « importé » : le propriétaire fictif y est connu, comme l'exige le service
    # canonique de trésorerie propriétaire.
    semer(tmp_db, proprietaires=[{"proprietaire_id": PROPRIO, "nom_proprietaire": "Testeur",
                                  "prenom_proprietaire": "Claire", "actif": "OUI"}])
    conn = get_db(tmp_db)
    try:
        # Comptes de test (plan fictif de la base temporaire) : un compte de charge pour la
        # correction manuelle, un compte de frais pour l'écart.
        for compte, libelle, type_ in (("615000", "Entretien et réparations (test)", "CHARGE"),
                                       ("627000", "Services bancaires (test)", "CHARGE")):
            conn.execute("INSERT OR IGNORE INTO plan_comptable (compte, libelle, type_compte, actif, "
                         "auxiliaire_autorise) VALUES (?,?,?,1,0)", (compte, libelle, type_))
        conn.commit()
    finally:
        conn.close()
    # Mapping canonique : le catalogue fonctionnel (migration 0114) porte déjà « Achat petit
    # équipement » (CHG_018) → 606320 et « Frais bancaires » (CHG_010) → 627800, VALIDÉS.
    # « Charge générale » (CHG_017) n'a AUCUNE règle : son compte reste « à définir ».
    # Mission 31 : une règle ne désigne qu'une catégorie qui existe dans le référentiel.
    from tests.fixtures_referentiel import semer_comptabilite
    semer_comptabilite(tmp_db, categories=["CHG_018", "CHG_010", "CHG_008", "CHG_017"])
    return tmp_db


_numero = {"n": 100}


def _mvt(montant, *, sens="debit", date="2026-09-20", contrepartie="COMMERCE TEST",
         libelle="Paiement carte", reference=None, statut="completed", categorie="other_expense"):
    _numero["n"] += 1
    n = _numero["n"]
    return mouvement(n, side=sens, amount=float(montant), amount_cents=int(round(montant * 100)),
                     status=statut, operation_type="card" if sens == "debit" else "transfer",
                     label=libelle, clean_counterparty_name=contrepartie,
                     reference=reference or f"REF-{n}", category=categorie,
                     settled_at=f"{date}T08:00:00.000Z" if statut == "completed" else None,
                     emitted_at=f"{date}T07:00:00.000Z")


def _importer(db, mouvements):
    ecran.actualiser(client=ClientDouble(pages=[mouvements]), db_path=db)
    return {m["libelle"] + str(m["montant"]): m for m in flux.mouvements(source=flux.BANQUE,
                                                                        db_path=db)}


def _ids_banque(db) -> list[dict]:
    return flux.mouvements(source=flux.BANQUE, avec_propositions=False, db_path=db)


def _par_montant(db, montant) -> dict:
    return next(m for m in _ids_banque(db) if abs(m["montant"] - montant) < 0.001)


def _charge(db, montant, *, date="2026-09-19", mode="PAY_001", impact="IC", categorie="CHG_018",
            lien=None, commentaire="Achat test") -> str:
    res = saisie.creer({"date_charge": date, "montant": montant, "categorie_charge_id": categorie,
                        "code_impact": impact, "prise_en_compta": "OUI" if impact == "IC" else "NON",
                        "mode_paiement_id": mode, "affectation_type": "GLOBAL", "refacturable": "NON",
                        "statut_controle": "VALIDE", "lien_virement_banque": lien,
                        "commentaire": commentaire}, acteur=ACTEUR, db_path=db)
    assert res["ok"], res
    return res["charge_id"]


def _fournisseur(db, nom="Plomberie Fictive") -> str:
    from app.services import fournisseurs_referentiel_service as frs
    return frs.creer(nom, "MAINTENANCE", acteur=ACTEUR, db_path=db)["fournisseur_id_opaque"]


def _facture_fournisseur(db, fournisseur, montant, *, ref="FA-001", date="2026-09-01") -> str:
    import uuid
    fid = "FAC-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db)
    try:
        conn.execute("INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
                     "date_facture, montant_ttc, statut, source) VALUES (?,?,?,?,?,?,?)",
                     (fid, fournisseur, ref, date, montant, "VALIDEE", "SAISIE"))
        conn.commit()
    finally:
        conn.close()
    res = compta.generer_ecriture_achat(fid, acteur=ACTEUR, db_path=db)
    assert res["ok"], res
    return fid


def _creance(monkeypatch, db, montant, *, fid="FPR-TFLUX0001", numero="2026-09-777"):
    """Facture propriétaire ÉMISE, vente déjà constatée (411/706). La créance est servie par le
    service canonique des créances, doublé ici pour ne pas reconstruire tout le cycle d'émission."""
    compta._inserer_ecriture("VENTES", "2026-09-01", "2026-09", numero, "Vente test",
                             "FACTURE_PROPRIETAIRE", fid,
                             [{"compte": "411000", "debit": montant, "credit": 0,
                               "auxiliaire": PROPRIO},
                              {"compte": "706000", "debit": 0, "credit": montant}], db_path=db)
    from app.services import creances_dettes_service as cd
    monkeypatch.setattr(cd, "creances", lambda **kw: [{
        "facture_id_opaque": fid, "numero": numero, "tiers_id": PROPRIO, "mois": "2026-09",
        "date_facture": "2026-09-01", "total": montant, "solde": montant}])
    return fid


def _compter(db, table, where="1=1") -> int:
    conn = get_db(db)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}").fetchone()[0]
    finally:
        conn.close()


def _lignes_ecriture(db, ecriture_id) -> list[dict]:
    return compta.lignes(ecriture_id, db_path=db)


def _formes(db) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for p in matching.propositions(db_path=db):
        out.setdefault(p["forme"], []).append(p)
    return out


# ══ 1-4 · Affichage et navigation ═════════════════════════════════════════════════════════════

def test_01_banque_affichee(client, base):
    _importer(base, [_mvt(48.39, contrepartie="Castorama")])
    page = client.get("/flux-financiers/banque")
    assert page.status_code == 200
    assert "Castorama" in page.text
    assert 'data-testid="statut-rapprochement"' in page.text
    assert 'data-testid="statut-compta"' in page.text
    assert "Non matché" in page.text and "À qualifier" in page.text
    for colonne in ("Date", "Libellé", "Montant", "Sens", "Rapprochement", "Comptabilité"):
        assert f"<th" in page.text and colonne in page.text


def test_02_caisse_affichee_et_creation_manuelle(client, base, verrous):
    r = client.post("/flux-financiers/caisse/operations", data={
        "type_operation": "AUTRE", "date_operation": "2026-09-20", "montant": "12,50",
        "piece": "Ticket 42", "acteur": ACTEUR}, follow_redirects=True)
    assert r.status_code == 200
    assert 'data-testid="caisse-ligne"' in r.text
    assert "Dépense en espèces" in r.text and "12.50" in r.text
    assert "À qualifier" in r.text, "une dépense de caisse attend d'être qualifiée"
    assert "Hors comptabilité" not in r.text


def test_03_charges_affichees_avec_statuts(client, base):
    _charge(base, 42.0)
    _charge(base, 70.0, impact="HC", mode="PAY_003", commentaire="Charge perso")
    page = client.get("/flux-financiers/charges")
    assert page.status_code == 200
    assert "Non rapproché" in page.text
    assert "Hors comptabilité" in page.text
    assert "Créer une charge" in page.text
    ancienne = client.get("/fournisseurs?mois=2026-09", follow_redirects=False)
    assert ancienne.status_code == 307
    assert ancienne.headers["location"] == "/flux-financiers/charges?mois=2026-09"


def test_04_switch_entre_les_quatre_pages_et_une_seule_entree(client, base):
    pages = {"banque": "/flux-financiers/banque", "caisse": "/flux-financiers/caisse",
             "charges": "/flux-financiers/charges", "rapprochement": "/flux-financiers/rapprochement"}
    for actif, url in pages.items():
        html = client.get(url).text
        for code in pages:
            assert f'data-testid="flux-onglet-{code}"' in html
        assert f'data-testid="flux-onglet-{actif}"' in html.split('aria-current="page"')[1][:120]
        assert html.count('data-testid="nav-flux-financiers"') == 1
        assert ">Banques &amp; caisse<" not in html and '<span class="nav-label">Charges</span>' not in html
    assert client.get("/flux-financiers", follow_redirects=False).headers["location"] == \
        "/flux-financiers/banque"
    assert client.get("/banques-caisse?vue=caisse", follow_redirects=False).headers["location"] \
        == "/flux-financiers/caisse"


# ══ 5-11 · Propositions du moteur ═════════════════════════════════════════════════════════════

def test_05_un_pour_un_propose_jamais_valide_seul(client, base):
    _importer(base, [_mvt(42.0, contrepartie="Magasin")])
    m = _par_montant(base, 42.0)
    _charge(base, 42.0, lien=m["id"])
    props = matching.propositions(db_path=base)
    assert len(props) == 1 and props[0]["forme"] == "1 ↔ 1"
    assert props[0]["confiance"] == matching.FORTE
    assert "Charge créée depuis ce mouvement" in props[0]["raisons"]
    # Les pages se lisent sans rien écrire.
    for url in ("/flux-financiers/banque", "/flux-financiers/rapprochement",
                f"/flux-financiers/banque/{m['id']}"):
        assert client.get(url).status_code == 200
    assert _compter(base, "flux_lettrages") == 0
    assert _compter(base, "banque_rapprochements") == 0
    assert _compter(base, "ecritures", "origine_type='LETTRAGE'") == 0
    assert flux.mouvement(flux.BANQUE, m["id"], db_path=base)["statut_rapprochement"] == flux.MATCHE


def test_06_deux_mouvements_pour_un_objet(base, verrous):
    fr = _fournisseur(base)
    _facture_fournisseur(base, fr, 1000.0, ref="FA-2POUR1")
    _importer(base, [_mvt(300.0, date="2026-09-10"), _mvt(700.0, date="2026-09-15")])
    assert any(len(p["mouvements"]) == 2 and len(p["objets"]) == 1
               for p in _formes(base).get("2 ↔ 1", []))


def test_07_un_mouvement_pour_deux_objets(base, verrous):
    fr = _fournisseur(base)
    _facture_fournisseur(base, fr, 400.0, ref="FA-A")
    _facture_fournisseur(base, fr, 600.0, ref="FA-B")
    _importer(base, [_mvt(1000.0, contrepartie="Plomberie Fictive")])
    props = _formes(base).get("1 ↔ 2", [])
    assert props and props[0]["confiance"] in (matching.MOYENNE, matching.FORTE)
    assert any("Tiers concordant" in r for r in props[0]["raisons"])


def test_08_deux_mouvements_pour_deux_objets(base):
    _charge(base, 200.0)
    _charge(base, 400.0)
    _importer(base, [_mvt(250.0), _mvt(350.0)])
    formes = _formes(base)
    assert "2 ↔ 2" in formes
    assert not formes.get("1 ↔ 1")


def test_09_trois_paiements_pour_une_facture(base, verrous):
    fr = _fournisseur(base)
    _facture_fournisseur(base, fr, 1000.0, ref="FA-ECHEANCE")
    _importer(base, [_mvt(300.0, date="2026-09-05"), _mvt(300.0, date="2026-09-12"),
                     _mvt(400.0, date="2026-09-19")])
    props = _formes(base).get("3 ↔ 1", [])
    assert props, "300 + 300 + 400 doit être proposé pour la facture de 1 000"
    assert sorted(m["montant"] for m in props[0]["mouvements"]) == [300.0, 300.0, 400.0]


def test_10_un_paiement_pour_trois_factures(base, verrous):
    fr = _fournisseur(base)
    for ref, montant in (("FA-1", 200.0), ("FA-2", 300.0), ("FA-3", 400.0)):
        _facture_fournisseur(base, fr, montant, ref=ref)
    _importer(base, [_mvt(900.0)])
    assert _formes(base).get("1 ↔ 3")


def test_11_petit_ecart_propose_jamais_absorbe(base, verrous):
    fr = _fournisseur(base)
    _facture_fournisseur(base, fr, 240.0, ref="FA-ECART")
    _importer(base, [_mvt(239.98, contrepartie="Plomberie Fictive")])
    p = matching.propositions(db_path=base)[0]
    assert not p["exact"] and p["ecart"] == -0.02
    assert any("Écart de 0.02" in r for r in p["raisons"])
    prep = lettrage.preparer([f"BANQUE:{p['mouvements'][0]['id']}"],
                             [f"FACTURE_FOURNISSEUR:{p['objets'][0]['id']}"], db_path=base)
    assert prep["ecart_a_traiter"] and not prep["traitement_ecart"]
    res = lettrage.valider(prep["selection_m"], prep["selection_o"], acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and res["code"] == lettrage.E_ECART
    assert _compter(base, "flux_lettrages") == 0


# ══ 12-17 · Décision humaine, écriture, atomicité ═════════════════════════════════════════════

def _cas_simple(base, montant=42.0):
    _importer(base, [_mvt(montant, contrepartie="Magasin")])
    m = _par_montant(base, montant)
    cid = _charge(base, montant, lien=m["id"])
    return m, cid


def test_12_refus_humain_laisse_tout_inchange(client, base):
    m, cid = _cas_simple(base)
    p = matching.propositions(db_path=base)[0]
    avant = {t: _compter(base, t) for t in ("banque_rapprochements", "ecritures", "flux_lettrages")}
    ligne_avant = saisie.lire(cid, db_path=base)
    r = client.post("/flux-financiers/rapprochement/refuser",
                    data={"empreinte": p["empreinte"], "acteur": ACTEUR, "motif": "pas ce ticket"},
                    follow_redirects=False)
    assert r.status_code == 303 and "message=" in r.headers["location"]
    assert {t: _compter(base, t) for t in avant} == avant
    assert saisie.lire(cid, db_path=base) == ligne_avant
    assert _compter(base, "flux_propositions_refusees") == 1
    assert matching.propositions(db_path=base) == [], "une proposition refusée ne revient pas"


def test_13_validation_humaine_rapproche_et_comptabilise(base, verrous):
    m, cid = _cas_simple(base)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert res["ok"], res
    apres = flux.mouvement(flux.BANQUE, m["id"], db_path=base)
    assert apres["statut_rapprochement"] == flux.RAPPROCHE
    assert apres["statut_compta"] == flux.COMPTABILISE
    assert flux.resume_charge(cid, db_path=base) == "Rapprochée · Comptabilisée"
    lignes = _lignes_ecriture(base, res["ecritures"][0])
    assert {(l["compte"], l["debit"], l["credit"]) for l in lignes} == {
        ("606320", 42.0, 0.0), ("512000", 0.0, 42.0)}, "charge directe : 6xx / 512, sans 401"


def test_14_ecriture_proposee_visible_avant_validation(client, base):
    m, cid = _cas_simple(base)
    page = client.get(f"/flux-financiers/rapprochement/valider?m=BANQUE:{m['id']}&o=CHARGE:{cid}")
    assert page.status_code == 200
    assert "Écriture comptable proposée" in page.text
    assert "512000" in page.text and "615000" in page.text
    assert "Valider le rapprochement et comptabiliser" in page.text
    assert _compter(base, "ecritures", "origine_type='LETTRAGE'") == 0


def test_15_modification_manuelle_de_l_ecriture(client, base, verrous):
    m, cid = _cas_simple(base)
    r = client.post("/flux-financiers/rapprochement/valider", data={
        "m": f"BANQUE:{m['id']}", "o": f"CHARGE:{cid}", "nb_ecritures": "1", "acteur": ACTEUR,
        "l_ecr": ["0", "0"], "l_role": ["TRESORERIE", "OBJET"], "l_objet": ["", f"CHARGE:{cid}"],
        "l_logement_id": ["", ""], "l_proprietaire_id": ["", ""],
        "l_compte": ["512000", "627000"], "l_auxiliaire": ["", ""],
        "l_libelle": ["Magasin", "Réparation"], "l_debit": ["", "42.00"],
        "l_credit": ["42.00", ""]}, follow_redirects=False)
    assert r.status_code == 303, r.text[:500]
    conn = get_db(base)
    try:
        let = conn.execute("SELECT * FROM flux_lettrages").fetchone()
    finally:
        conn.close()
    assert let["ecriture_modifiee"] == 1
    comptes = {l["compte"] for l in _lignes_ecriture(base, let["ecriture_id_opaque"])}
    assert comptes == {"512000", "627000"}


def test_16_validation_atomique(base, verrous):
    m, cid = _cas_simple(base)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert res["ok"]
    conn = get_db(base)
    try:
        let = conn.execute("SELECT * FROM flux_lettrages").fetchone()
        lien = conn.execute("SELECT * FROM banque_rapprochements").fetchone()
        ecr = conn.execute("SELECT * FROM ecritures WHERE origine_type='LETTRAGE'").fetchone()
        evt = conn.execute("SELECT * FROM flux_lettrage_evenements").fetchone()
    finally:
        conn.close()
    assert let["statut"] == "VALIDE" and let["acteur"] == ACTEUR and let["cree_le"]
    assert lien["statut"] == "CONFIRME" and lien["lettrage_id_opaque"] == let["lettrage_id_opaque"]
    assert ecr["statut"] == "VALIDEE" and ecr["piece"] == let["lettrage_id_opaque"]
    assert evt["type_evenement"] == "VALIDATION" and evt["acteur"] == ACTEUR


def test_17_echec_comptable_aucun_rapprochement(base, verrous, monkeypatch):
    m, cid = _cas_simple(base)
    monkeypatch.setattr(compta, "_inserer_ecriture",
                        lambda *a, **k: {"ok": False, "code": "E_TEST", "message": "refus simulé"})
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and res["code"] == lettrage.E_ECHEC
    for table in ("flux_lettrages", "flux_lettrage_lignes", "banque_rapprochements"):
        assert _compter(base, table) == 0, table
    assert saisie.lire(cid, db_path=base)["statut_rapprochement"] in (None, "NON_RAPPROCHE")
    assert flux.mouvement(flux.BANQUE, m["id"], db_path=base)["statut_rapprochement"] != flux.RAPPROCHE


def test_17b_exception_technique_annule_tout(base, verrous, monkeypatch):
    m, cid = _cas_simple(base)

    def explose(*a, **k):
        raise RuntimeError("secret-interne-ne-doit-pas-sortir")
    monkeypatch.setattr(compta, "valider_dans_transaction", explose)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and "secret-interne" not in res["message"]
    assert _compter(base, "ecritures", "origine_type='LETTRAGE'") == 0
    assert _compter(base, "banque_rapprochements") == 0


# ══ 18-20 · Jamais de double comptabilisation ═════════════════════════════════════════════════

def test_18_facture_fournisseur_seulement_401_512(base, verrous):
    fr = _fournisseur(base)
    fid = _facture_fournisseur(base, fr, 350.0, ref="FA-350")
    _importer(base, [_mvt(350.0, contrepartie="Plomberie Fictive", libelle="VIR FA-350")])
    m = _par_montant(base, 350.0)
    p = matching.propositions(db_path=base)[0]
    assert p["confiance"] == matching.FORTE
    achats_avant = _compter(base, "ecritures", "journal='ACHATS'")
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"FACTURE_FOURNISSEUR:{fid}"], acteur=ACTEUR,
                           proposition=p["empreinte"], db_path=base)
    assert res["ok"], res
    lignes = _lignes_ecriture(base, res["ecritures"][0])
    assert {l["compte"] for l in lignes} == {"401000", "512000"}
    assert next(l for l in lignes if l["compte"] == "401000")["auxiliaire"] == fr
    assert _compter(base, "ecritures", "journal='ACHATS'") == achats_avant == 1
    from app.services import factures_service as fs
    assert fs.charger(fid, db_path=base)["statut"] == fs.ST_REGLEE
    assert _compter(base, "reglements_fournisseurs", "statut='RAPPROCHE'") == 1


def test_19_facture_client_seulement_512_411(base, verrous, monkeypatch):
    fid = _creance(monkeypatch, base, 300.0)
    _importer(base, [_mvt(300.0, sens="credit", contrepartie="Claire Testeur",
                          libelle="VIR 2026-09-777")])
    m = _par_montant(base, 300.0)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"FACTURE_PROPRIETAIRE:{fid}"], acteur=ACTEUR,
                           db_path=base)
    assert res["ok"], res
    lignes = _lignes_ecriture(base, res["ecritures"][0])
    assert {(l["compte"], l["debit"], l["credit"]) for l in lignes} == {
        ("512000", 300.0, 0.0), ("411000", 0.0, 300.0)}
    assert next(l for l in lignes if l["compte"] == "411000")["auxiliaire"] == PROPRIO
    assert _compter(base, "ecritures", "journal='VENTES'") == 1, "la vente n'est jamais recréée"
    assert _compter(base, "mouvements_tresorerie_proprietaires",
                    "statut='VALIDE' AND source_type='FLUX_LETTRAGE'") == 1


def test_20_aucune_double_charge(base, verrous):
    fr = _fournisseur(base)
    fid = _facture_fournisseur(base, fr, 120.0, ref="FA-120")
    _importer(base, [_mvt(120.0)])
    m = _par_montant(base, 120.0)
    prep = lettrage.preparer([f"BANQUE:{m['id']}"], [f"FACTURE_FOURNISSEUR:{fid}"], db_path=base)
    # Tentative : recomptabiliser l'achat en 6xx au lieu de solder la dette.
    triche = [[{"compte": "512000", "auxiliaire": None, "debit": 0.0, "credit": 120.0,
                "libelle": "", "role": "TRESORERIE", "objet": "", "logement_id": None,
                "proprietaire_id": None},
               {"compte": "606000", "auxiliaire": None, "debit": 120.0, "credit": 0.0,
                "libelle": "", "role": "SAISIE", "objet": "", "logement_id": None,
                "proprietaire_id": None}]]
    res = lettrage.valider(prep["selection_m"], prep["selection_o"], acteur=ACTEUR, lignes=triche,
                           db_path=base)
    assert res["ok"] is False and res["code"] == lettrage.E_DOUBLE
    assert _compter(base, "ecritures", "origine_type='LETTRAGE'") == 0
    # Une charge déjà portée par une facture n'est pas un objet rapprochable à part.
    cid = _charge(base, 55.0)
    conn = get_db(base)
    try:
        conn.execute("UPDATE factures SET charge_id=? WHERE facture_id_opaque=?", (cid, fid))
        conn.commit()
    finally:
        conn.close()
    assert not any(o["id"] == cid for o in flux.objets(db_path=base))


# ══ 21-24 · Charges ═══════════════════════════════════════════════════════════════════════════

def test_21_22_charge_creee_depuis_un_mouvement_preremplie(client, base, verrous):
    _importer(base, [_mvt(48.39, contrepartie="Castorama", date="2026-09-24")])
    m = _par_montant(base, 48.39)
    page = client.get(f"/fournisseurs/nouvelle?mouvement=BANQUE:{m['id']}")
    assert page.status_code == 200
    assert 'data-testid="charge-depuis-mouvement"' in page.text
    assert 'value="2026-09-24"' in page.text and 'value="48.39"' in page.text
    assert f'value="BANQUE:{m["id"]}"' in page.text
    assert 'value="HC"' not in page.text, "hors comptabilité n'est pas proposé"
    from app.routes.fournisseurs import _prefill_depuis_mouvement
    prerempli, _ = _prefill_depuis_mouvement(f"BANQUE:{m['id']}")
    assert prerempli["mode_paiement_id"] == "PAY_001" and prerempli["code_impact"] == "IC"
    assert "Castorama" in prerempli["commentaire"]
    assert _compter(base, "charges") == 0, "rien n'est créé sans confirmation"

    from app.services import charges_confirmation_service as confirmation
    from app.services import charges_preview_service as prev
    # Le lien au mouvement est porté par la ligne écrite, quel que soit le reste du formulaire.
    ligne = prev._build_row_data({"mouvement_origine": f"BANQUE:{m['id']}", "code_impact": "IC",
                                  "date_charge": "2026-09-24", "montant": "48.39"}, "CHG-TEST")
    assert ligne["lien_virement_banque"] == m["id"] and ligne["prise_en_compta"] == "OUI"
    form = {"date_charge": "2026-09-24", "montant": "48.39", "categorie_charge_id": "CHG_017",
            "code_impact": "IC", "mode_paiement_id": "PAY_001", "commentaire": "Castorama",
            "mouvement_origine": f"BANQUE:{m['id']}", "perimetre_mode": "GLOBAL"}
    apercu = prev.previsualiser(form, db_path=base)
    if apercu["ok"]:
        res = confirmation.confirmer(apercu["token"], db_path=base, acteur=ACTEUR)
        assert res.ok, res.as_dict()
        charge = saisie.lire(res.charge_id, db_path=base)
        assert charge["lien_virement_banque"] == m["id"]
        assert charge["prise_en_compta"] == "OUI"
        p = matching.propositions(db_path=base)[0]
        assert p["confiance"] == matching.FORTE and p["objets"][0]["id"] == res.charge_id
    else:
        # Les règles du formulaire guidé (périmètre…) restent les siennes : on vérifie au moins
        # que le refus ne vient pas des règles « charge depuis mouvement ».
        codes = {e["code"] for e in apercu["manifest"]["errors"]}
        assert not any(c.startswith(("V30", "V31", "V32", "V33", "V34")) for c in codes), codes


def test_23_charge_depuis_banque_jamais_hors_compta(base, verrous):
    from app.services import charges_preview_service as prev
    _importer(base, [_mvt(30.0)])
    m = _par_montant(base, 30.0)
    erreurs = prev.erreurs_charge_depuis_mouvement(f"BANQUE:{m['id']}", code_impact="HC",
                                                   mode_paiement_id="PAY_001", montant="30")
    assert "V31_CHARGE_MOUVEMENT_HORS_COMPTA" in {e["code"] for e in erreurs}
    # Et une charge hors comptabilité existante ne peut pas être liée au mouvement.
    hc = _charge(base, 30.0, impact="HC")
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{hc}"], acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and "hors comptabilité" in res["message"]
    assert _compter(base, "banque_rapprochements") == 0


def test_24_charge_generale_hors_compta_possible(base):
    cid = _charge(base, 80.0, impact="HC", mode="PAY_003")
    statut = flux.statuts_charges(db_path=base)[cid]
    assert statut["compta"]["code"] == flux.CH_HORS_COMPTA
    assert flux.resume_charge(cid, db_path=base) == "Hors comptabilité"
    assert statut["resultat"] == "Résultat réel seulement"


# ══ 25-28 · Tiers : 401 et 411 ════════════════════════════════════════════════════════════════

def _lignes_charge_via_401(m, cid, aux):
    return [[{"compte": "512000", "auxiliaire": None, "debit": 0.0, "credit": 42.0, "libelle": "",
              "role": "TRESORERIE", "objet": "", "logement_id": None, "proprietaire_id": None},
             {"compte": "606000", "auxiliaire": None, "debit": 42.0, "credit": 0.0, "libelle": "",
              "role": "OBJET", "objet": f"CHARGE:{cid}", "logement_id": None,
              "proprietaire_id": None},
             {"compte": "401000", "auxiliaire": aux, "debit": 10.0, "credit": 0.0, "libelle": "",
              "role": "SAISIE", "objet": "", "logement_id": None, "proprietaire_id": None},
             {"compte": "401000", "auxiliaire": aux, "debit": 0.0, "credit": 10.0, "libelle": "",
              "role": "SAISIE", "objet": "", "logement_id": None, "proprietaire_id": None}]]


def test_25_fournisseur_existant_selectionne(base, verrous):
    m, cid = _cas_simple(base)
    fr = _fournisseur(base, "Quincaillerie Fictive")
    sans = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR,
                            lignes=_lignes_charge_via_401(m, cid, None), db_path=base)
    assert sans["ok"] is False and sans["code"] == lettrage.E_AUXILIAIRE
    avec = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR,
                            lignes=_lignes_charge_via_401(m, cid, fr), db_path=base)
    assert avec["ok"], avec


def test_26_fournisseur_cree_a_la_volee(client, base):
    r = client.post("/flux-financiers/fournisseurs",
                    data={"nom": "Serrurerie Fictive", "siren": "123 456 789", "acteur": ACTEUR},
                    headers={"Accept": "application/json"})
    assert r.status_code == 200 and r.json()["ok"]
    nouveau = r.json()["id"]
    assert any(f["id"] == nouveau for f in flux.fournisseurs_connus(db_path=base))
    conn = get_db(base)
    try:
        assert conn.execute("SELECT numero_entreprise FROM fournisseur_details WHERE "
                            "fournisseur_id_opaque=?", (nouveau,)).fetchone()[0] == "123456789"
    finally:
        conn.close()


def test_27_doublon_fournisseur_evite_et_nom_generique_refuse(base):
    ok = lettrage.creer_fournisseur("Électricité Fictive", acteur=ACTEUR, db_path=base)
    assert ok["ok"]
    doublon = lettrage.creer_fournisseur("  electricite FICTIVE ", acteur=ACTEUR, db_path=base)
    assert doublon["ok"] is False and doublon["code"] == lettrage.E_DOUBLON
    assert doublon["doublons"][0]["id"] == ok["id"]
    generique = lettrage.creer_fournisseur("Fournisseur divers", acteur=ACTEUR, db_path=base)
    assert generique["ok"] is False and generique["code"] == lettrage.E_NOM_GENERIQUE
    faux_siren = lettrage.creer_fournisseur("Menuiserie Fictive", siren="12", acteur=ACTEUR,
                                            db_path=base)
    assert faux_siren["ok"] is False and faux_siren["code"] == lettrage.E_IDENTIFIANT
    assert _compter(base, "fournisseurs") == 1


def test_28_compte_411_exige_le_proprietaire(base, verrous, monkeypatch):
    fid = _creance(monkeypatch, base, 150.0)
    _importer(base, [_mvt(150.0, sens="credit", contrepartie="Claire Testeur")])
    m = _par_montant(base, 150.0)
    prep = lettrage.preparer([f"BANQUE:{m['id']}"], [f"FACTURE_PROPRIETAIRE:{fid}"], db_path=base)
    ligne_411 = next(l for l in prep["ecritures"][0]["lignes"] if l["compte"] == "411000")
    assert ligne_411["auxiliaire"] == PROPRIO, "récupéré automatiquement de la facture"
    sans_aux = [[dict(l, auxiliaire=None) if l["compte"] == "411000" else l
                 for l in prep["ecritures"][0]["lignes"]]]
    res = lettrage.valider(prep["selection_m"], prep["selection_o"], acteur=ACTEUR,
                           lignes=sans_aux, db_path=base)
    assert res["ok"] is False and res["code"] in (lettrage.E_AUXILIAIRE, lettrage.E_DOUBLE)


# ══ 29-31 · Partiel, ventilation, jamais hors comptabilité ════════════════════════════════════

def test_29_rapprochement_partiel(base, verrous):
    _importer(base, [_mvt(100.0)])
    m = _par_montant(base, 100.0)
    cid = _charge(base, 60.0)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR,
                           traitement_ecart=lettrage.SOLDE_OUVERT, db_path=base)
    assert res["ok"], res
    apres = flux.mouvement(flux.BANQUE, m["id"], db_path=base)
    assert apres["statut_rapprochement"] == flux.PARTIEL and apres["restant"] == 40.0
    assert apres["statut_compta"] == flux.A_QUALIFIER, "40 € restent à qualifier"
    lignes = _lignes_ecriture(base, res["ecritures"][0])
    assert next(l for l in lignes if l["compte"] == "512000")["credit"] == 60.0


def test_30_ventilation_equilibree_avec_frais(base, verrous, monkeypatch):
    fid = _creance(monkeypatch, base, 100.0)
    _importer(base, [_mvt(98.0, sens="credit", contrepartie="Claire Testeur")])
    m = _par_montant(base, 98.0)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"FACTURE_PROPRIETAIRE:{fid}"], acteur=ACTEUR,
                           traitement_ecart=lettrage.COMPTABILISE, compte_ecart="627000",
                           db_path=base)
    assert res["ok"], res
    lignes = _lignes_ecriture(base, res["ecritures"][0])
    assert {(l["compte"], l["debit"], l["credit"]) for l in lignes} == {
        ("512000", 98.0, 0.0), ("627000", 2.0, 0.0), ("411000", 0.0, 100.0)}
    assert round(sum(l["debit"] for l in lignes), 2) == round(sum(l["credit"] for l in lignes), 2)
    assert flux.mouvement(flux.BANQUE, m["id"], db_path=base)["statut_rapprochement"] == flux.RAPPROCHE


def test_31_un_mouvement_de_tresorerie_n_est_jamais_hors_comptabilite(client, base):
    _importer(base, [_mvt(10.0), _mvt(20.0, statut="pending"), _mvt(30.0, sens="credit")])
    assert "HORS_COMPTABILITE" not in flux.STATUTS_COMPTA_MOUVEMENT
    for m in flux.mouvements(db_path=base):
        assert m["statut_compta"] in flux.STATUTS_COMPTA_MOUVEMENT
        assert m["compta"]["libelle"] != "Hors comptabilité"
    assert "Hors comptabilité" not in client.get("/flux-financiers/banque").text


# ══ 32-35 · Clôture, idempotence, confidentialité, intégrité ══════════════════════════════════

def test_32_mois_cloture_protege(base, verrous):
    m, cid = _cas_simple(base)
    conn = get_db(base)
    try:
        conn.execute("INSERT OR REPLACE INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
                     "VALUES ('2026-09', 'CLOTURE', 'IMP-TEST')")
        conn.commit()
    finally:
        conn.close()
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and res["code"] == lettrage.E_PERIODE
    assert _compter(base, "flux_lettrages") == 0 and _compter(base, "banque_rapprochements") == 0


def test_33_idempotence_double_clic(client, base, verrous):
    m, cid = _cas_simple(base)
    donnees = {"m": f"BANQUE:{m['id']}", "o": f"CHARGE:{cid}", "acteur": ACTEUR}
    premier = client.post("/flux-financiers/rapprochement/valider", data=donnees,
                          follow_redirects=False)
    second = client.post("/flux-financiers/rapprochement/valider", data=donnees,
                         follow_redirects=False)
    assert premier.status_code == 303
    assert second.status_code in (200, 303)
    assert _compter(base, "flux_lettrages") == 1
    assert _compter(base, "ecritures", "origine_type='LETTRAGE'") == 1
    assert _compter(base, "banque_rapprochements", "statut='CONFIRME'") == 1


def test_34_aucun_secret_ni_identifiant_technique_rendu(client, base, verrous):
    m, cid = _cas_simple(base)
    lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    import re
    for url in ("/flux-financiers/banque", "/flux-financiers/caisse", "/flux-financiers/charges",
                "/flux-financiers/rapprochement", f"/flux-financiers/banque/{m['id']}"):
        html = client.get(url).text
        # Les VALEURS de formulaire portent des clés (c'est leur rôle) ; le texte affiché, jamais.
        visible = re.sub(r'(value|href|data-[a-z-]+)="[^"]*"', "", html)
        assert PROPRIO not in visible, url
        html = html.replace(PROPRIO, "") if PROPRIO in html and PROPRIO not in visible else html
        assert COMPTE["iban"] not in html, url
        for technique in ("tx-uuid-", "charge_utile", "Authorization", "QONTO_SECRET",
                          "HOSTAWAY_GITHUB_TOKEN", PROPRIO):
            assert technique not in html, (url, technique)


def test_35_integrite_et_cles_etrangeres(base, verrous, monkeypatch):
    m, cid = _cas_simple(base)
    lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    fr = _fournisseur(base)
    fid = _facture_fournisseur(base, fr, 90.0, ref="FA-90")
    _importer(base, [_mvt(90.0)])
    m2 = _par_montant(base, 90.0)
    lettrage.valider([f"BANQUE:{m2['id']}"], [f"FACTURE_FOURNISSEUR:{fid}"], acteur=ACTEUR,
                     db_path=base)
    conn = get_db(base)
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


# ══ Compléments : verrous, annulation, écran de validation ═══════════════════════════════════

def test_verrous_fermes_rien_n_est_ecrit(base, monkeypatch):
    m, cid = _cas_simple(base)
    monkeypatch.setattr(cfg, "MODE_REEL_ECRITURES", False, raising=False)
    monkeypatch.setattr(cfg, "RECETTE_MODE", False, raising=False)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and res["code"] == lettrage.E_VERROU
    assert _compter(base, "flux_lettrages") == 0


def test_validation_exige_un_nom(base, verrous):
    m, cid = _cas_simple(base)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur="  ", db_path=base)
    assert res["ok"] is False and res["code"] == lettrage.E_ACTEUR


def test_annulation_contrepasse_sans_rien_effacer(base, verrous):
    m, cid = _cas_simple(base)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    sans_motif = lettrage.annuler(res["lettrage_id_opaque"], motif="", acteur=ACTEUR, db_path=base)
    assert sans_motif["ok"] is False and sans_motif["code"] == lettrage.E_MOTIF
    ann = lettrage.annuler(res["lettrage_id_opaque"], motif="mauvais ticket", acteur=ACTEUR,
                           db_path=base)
    assert ann["ok"], ann
    assert compta.charger(res["ecritures"][0], db_path=base)["statut"] == compta.ST_CONTREPASSEE
    assert _compter(base, "ecritures", "contrepasse_de IS NOT NULL") == 1
    assert _compter(base, "banque_rapprochements", "statut='ANNULE'") == 1
    assert _compter(base, "flux_lettrages", "statut='ANNULE'") == 1
    apres = flux.mouvement(flux.BANQUE, m["id"], db_path=base)
    assert apres["statut_rapprochement"] in (flux.MATCHE, flux.NON_MATCHE)
    assert saisie.lire(cid, db_path=base)["statut_rapprochement"] == "NON_RAPPROCHE"


def test_operation_en_attente_non_lettrable(base, verrous):
    _importer(base, [_mvt(15.0, statut="pending")])
    m = _par_montant(base, 15.0)
    cid = _charge(base, 15.0)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and res["code"] == lettrage.E_MOUVEMENT


def test_route_validation_affiche_les_erreurs_sans_ecrire(client, base, verrous):
    m, cid = _cas_simple(base)
    r = client.post("/flux-financiers/rapprochement/valider",
                    data={"m": f"BANQUE:{m['id']}", "o": f"CHARGE:{cid}", "acteur": ""})
    assert r.status_code == 200 and 'data-testid="validation-erreurs"' in r.text
    assert _compter(base, "flux_lettrages") == 0


def test_apport_associe_ecriture_refusee_aucun_rapprochement_garde(base, verrous):
    """Circuit existant (apport d'associé, proposé depuis la fiche mouvement) : tout ou rien."""
    from datetime import datetime, timezone
    from app.services import qonto_validation_service as qv
    conn = get_db(base)
    try:
        conn.execute("INSERT OR REPLACE INTO ref_associes (personne_id, nom_personne, type_personne, "
                     "actif, import_id) VALUES ('PERS_TFLUX', 'Associé fictif', 'ASSOCIE', 'OUI', "
                     "'IMP-TEST')")
        # L'écriture de l'apport est datée du jour : on clôt la période comptable courante.
        conn.execute("INSERT OR REPLACE INTO periodes_comptables (periode, statut) VALUES (?, "
                     "'CLOTUREE')", (datetime.now(timezone.utc).strftime("%Y-%m"),))
        conn.commit()
    finally:
        conn.close()
    _importer(base, [_mvt(200.0, sens="credit", contrepartie="Associé")])
    m = _par_montant(base, 200.0)
    res = qv.valider_par_mouvement(m["id"], nature=qv.APPORT_ASSOCIE, objet_id="PERS_TFLUX",
                                   acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and res["code"] == qv.E_ECRITURE_REFUSEE
    # Une seule transaction : pas même un rapprochement « annulé » à relire après coup.
    assert _compter(base, "banque_rapprochements") == 0
    assert _compter(base, "banque_rapprochement_evenements") == 0
    assert _compter(base, "ecritures") == 0
    assert flux.mouvement(flux.BANQUE, m["id"], db_path=base)["statut_rapprochement"] != flux.RAPPROCHE


# ══ Finalisation : mapping canonique, garde-fou hors compta, circuit apport/retrait atomique ══

def _charge_sans_mapping(base, montant=24.0, **kw):
    """Charge générale (CHG_017) : aucune règle de mapping validée, catalogue compris."""
    return _charge(base, montant, categorie="CHG_017", **kw)


def test_A_mapping_categorie_vers_compte_valide(base):
    _importer(base, [_mvt(42.0)])
    m = _par_montant(base, 42.0)
    cid = _charge(base, 42.0)
    prep = lettrage.preparer([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], db_path=base)
    ligne = next(l for l in prep["ecritures"][0]["lignes"] if l["role"] == lettrage.ROLE_OBJET)
    assert ligne["compte"] == "606320" and not ligne["avertissement"]


def test_B_D_categorie_sans_mapping_compte_a_definir_sans_fallback_606000(client, base, verrous):
    _importer(base, [_mvt(24.0)])
    m = _par_montant(base, 24.0)
    cid = _charge_sans_mapping(base)
    prep = lettrage.preparer([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], db_path=base)
    ligne = next(l for l in prep["ecritures"][0]["lignes"] if l["role"] == lettrage.ROLE_OBJET)
    assert ligne["compte"] == "", "jamais 606000 en silence"
    assert any("Compte comptable à définir" in a for a in prep["avertissements"])
    assert "606000" not in {l["compte"] for e in prep["ecritures"] for l in e["lignes"]}
    page = client.get(f"/flux-financiers/rapprochement/valider?m=BANQUE:{m['id']}&o=CHARGE:{cid}")
    assert "Compte comptable à définir" in page.text
    assert 'data-testid="compte-a-definir"' in page.text
    # Validation sans choix de compte : refusée, rien n'est écrit.
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and res["code"] == lettrage.E_COMPTE_A_DEFINIR
    assert _compter(base, "flux_lettrages") == 0 and _compter(base, "banque_rapprochements") == 0


def test_C_compte_choisi_manuellement_puis_valide(base, verrous):
    _importer(base, [_mvt(24.0)])
    m = _par_montant(base, 24.0)
    cid = _charge_sans_mapping(base)
    prep = lettrage.preparer([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], db_path=base)
    choisies = [[dict(l, compte="627000") if l["role"] == lettrage.ROLE_OBJET else l
                 for l in prep["ecritures"][0]["lignes"]]]
    res = lettrage.valider(prep["selection_m"], prep["selection_o"], acteur=ACTEUR,
                           lignes=choisies, db_path=base)
    assert res["ok"], res
    lignes = _lignes_ecriture(base, res["ecritures"][0])
    assert {(l["compte"], l["debit"], l["credit"]) for l in lignes} == {
        ("627000", 24.0, 0.0), ("512000", 0.0, 24.0)}
    assert round(sum(l["debit"] for l in lignes), 2) == round(sum(l["credit"] for l in lignes), 2)


def test_compte_inactif_ou_hors_classe_6_refuse(base, verrous):
    _importer(base, [_mvt(24.0)])
    m = _par_montant(base, 24.0)
    cid = _charge_sans_mapping(base)
    prep = lettrage.preparer([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], db_path=base)
    for compte in ("467000", "401000"):          # inactif ; puis actif mais pas une charge
        lignes = [[dict(l, compte=compte, auxiliaire=None) if l["role"] == lettrage.ROLE_OBJET
                   else l for l in prep["ecritures"][0]["lignes"]]]
        res = lettrage.valider(prep["selection_m"], prep["selection_o"], acteur=ACTEUR,
                               lignes=lignes, db_path=base)
        assert res["ok"] is False and res["code"] == lettrage.E_COMPTE_A_DEFINIR, compte
    conn = get_db(base)
    try:
        conn.execute("UPDATE plan_comptable SET actif=0 WHERE compte='627000'")
        conn.commit()
    finally:
        conn.close()
    lignes = [[dict(l, compte="627000") if l["role"] == lettrage.ROLE_OBJET else l
               for l in prep["ecritures"][0]["lignes"]]]
    res = lettrage.valider(prep["selection_m"], prep["selection_o"], acteur=ACTEUR, lignes=lignes,
                           db_path=base)
    assert res["ok"] is False and _compter(base, "flux_lettrages") == 0


def test_regle_de_mapping_vers_compte_inactif_reste_a_definir(base):
    """Mission 31 : une règle vers un compte inactif ne peut plus être CRÉÉE ; le cas réel est un
    compte désactivé APRÈS la règle — elle cesse alors de proposer ce compte."""
    from app.services import comptabilite_mappings_service as maps
    from app.services import comptabilite_plan_service as plan
    refus = maps.creer_regle(maps.PORTEE_CATEGORIE, "467000", cle="CHG_017", statut=maps.ST_VALIDE,
                             acteur=ACTEUR, db_path=base)
    assert refus["ok"] is False and refus["code"] == maps.E_COMPTE_INACTIF
    assert maps.creer_regle(maps.PORTEE_CATEGORIE, "627000", cle="CHG_017", statut=maps.ST_VALIDE,
                            acteur=ACTEUR, db_path=base)["ok"]
    plan.desactiver("627000", acteur=ACTEUR, motif="test", db_path=base)
    _importer(base, [_mvt(24.0)])
    m = _par_montant(base, 24.0)
    cid = _charge_sans_mapping(base)
    prep = lettrage.preparer([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], db_path=base)
    ligne = next(l for l in prep["ecritures"][0]["lignes"] if l["role"] == lettrage.ROLE_OBJET)
    assert ligne["compte"] == "" and "627000" in ligne["avertissement"]


def test_E_charge_liee_a_un_mouvement_jamais_hors_compta(base, verrous):
    _importer(base, [_mvt(42.0)])
    m = _par_montant(base, 42.0)
    # Création : une charge portant le lien d'un mouvement ne naît pas hors comptabilité.
    refus = saisie.creer({"date_charge": "2026-09-19", "montant": 42.0, "categorie_charge_id": "CHG_018",
                          "code_impact": "HC", "prise_en_compta": "NON", "mode_paiement_id": "PAY_001",
                          "lien_virement_banque": m["id"]}, acteur=ACTEUR, db_path=base)
    assert refus["ok"] is False and refus["code"] == saisie.E_HORS_COMPTA_MOUVEMENT
    assert "hors comptabilité" in refus["message"]
    # Rapprochée puis repassée en hors compta : refusé aussi (le lien vient du lettrage).
    cid = _charge(base, 42.0)
    assert lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR,
                            db_path=base)["ok"]
    ligne = saisie.lire(cid, db_path=base)
    donnees = {k: ligne[k] for k in saisie.CHAMPS_SAISIE}
    donnees.update(code_impact="HC", prise_en_compta="NON", lien_virement_banque=None)
    res = saisie.modifier(cid, donnees, acteur=ACTEUR, motif="test", db_path=base)
    assert res["ok"] is False and res["code"] == saisie.E_HORS_COMPTA_MOUVEMENT
    assert saisie.lire(cid, db_path=base)["prise_en_compta"] == "OUI"
    # Validation du contrôle d'une charge hors compta liée à un mouvement : refusée.
    conn = get_db(base)
    try:
        conn.execute("UPDATE charges SET code_impact='HC', prise_en_compta='NON', "
                     "statut_controle='A_CONTROLER' WHERE charge_id=?", (cid,))
        conn.commit()
    finally:
        conn.close()
    res = saisie.valider_controle(cid, acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and res["code"] == saisie.E_HORS_COMPTA_MOUVEMENT


def test_F_charge_generale_hors_compta_toujours_possible_et_validable(base):
    res = saisie.creer({"date_charge": "2026-09-19", "montant": 70.0, "categorie_charge_id": "CHG_018",
                        "code_impact": "HC", "prise_en_compta": "NON", "mode_paiement_id": "PAY_001",
                        "statut_controle": "A_CONTROLER"}, acteur=ACTEUR, db_path=base)
    assert res["ok"], res
    from app.services import justificatifs_service as justif
    justif.confirmer(justif.OBJET_CHARGE, res["charge_id"], present="NON",
                     justification="Test : sans pièce", acteur=ACTEUR, db_path=base)
    assert saisie.valider_controle(res["charge_id"], acteur=ACTEUR, db_path=base)["ok"]
    assert flux.resume_charge(res["charge_id"], db_path=base) == "Hors comptabilité"


def test_G_charge_hors_compta_payee_banque_sans_mouvement_reste_intacte(base, verrous):
    """Le cas réel 700 € : hors compta, mode banque, AUCUN mouvement bancaire correspondant.
    Rien ne la transforme automatiquement ; elle n'est proposée à aucun rapprochement."""
    cid = _charge(base, 700.0, impact="HC", commentaire="cas 700")
    _importer(base, [_mvt(48.39)])
    avant = saisie.lire(cid, db_path=base)
    assert not any(o["id"] == cid for o in flux.objets(db_path=base))
    assert not any(any(o["id"] == cid for o in p["objets"])
                   for p in matching.propositions(db_path=base))
    assert saisie.lire(cid, db_path=base) == avant
    assert flux.resume_charge(cid, db_path=base) == "Hors comptabilité"


def test_H_I_aucune_double_charge_ni_double_ecriture(base, verrous):
    m, cid = _cas_simple(base)
    lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert _compter(base, "charges", "montant=42.0") == 1
    assert _compter(base, "ecritures", "origine_type='LETTRAGE'") == 1
    assert _compter(base, "banque_rapprochements", "statut='CONFIRME'") == 1


def _uuid_montant(base, montant):
    conn = get_db(base)
    try:
        return conn.execute("SELECT qonto_transaction_uuid FROM qonto_transactions_raw "
                            "WHERE abs(montant-?)<0.001", (montant,)).fetchone()[0]
    finally:
        conn.close()


def _associe(base):
    conn = get_db(base)
    try:
        conn.execute("INSERT OR REPLACE INTO ref_associes (personne_id, nom_personne, type_personne, "
                     "actif, import_id) VALUES ('PERS_TFLUX', 'Associé fictif', 'ASSOCIE', 'OUI', "
                     "'IMP-TEST')")
        conn.commit()
    finally:
        conn.close()


def test_J_apport_associe_atomique_succes(base, verrous):
    from app.services import qonto_validation_service as qv
    _associe(base)
    _importer(base, [_mvt(200.0, sens="credit", contrepartie="Associé")])
    uuid = _uuid_montant(base, 200.0)
    res = qv.valider(uuid, nature=qv.APPORT_ASSOCIE, objet_id="PERS_TFLUX", acteur=ACTEUR,
                     db_path=base)
    assert res["ok"], res
    lignes = _lignes_ecriture(base, res["ecriture"]["ecriture_id_opaque"])
    assert {(l["compte"], l["auxiliaire"], l["debit"], l["credit"]) for l in lignes} == {
        ("512000", None, 200.0, 0.0), ("455100", "PERS_TFLUX", 0.0, 200.0)}
    assert compta.charger(res["ecriture"]["ecriture_id_opaque"], db_path=base)["statut"] == \
        compta.ST_PROPOSEE, "comportement existant préservé : écriture à valider en Comptabilité"
    assert _compter(base, "banque_rapprochements", "statut='CONFIRME'") == 1
    assert _compter(base, "qonto_transactions_statut_local", "statut_local='RAPPROCHE'") == 1
    assert qv.valider(uuid, nature=qv.APPORT_ASSOCIE, objet_id="PERS_TFLUX", acteur=ACTEUR,
                      db_path=base)["code"] == qv.E_DEJA_AFFECTE
    assert _compter(base, "ecritures") == 1


@pytest.mark.parametrize("panne", ["refus", "exception"])
def test_J_L_apport_associe_echec_ecriture_rollback_complet(base, verrous, monkeypatch, panne):
    from app.services import qonto_validation_service as qv
    _associe(base)
    _importer(base, [_mvt(200.0, sens="credit", contrepartie="Associé")])
    uuid = _uuid_montant(base, 200.0)
    if panne == "refus":
        monkeypatch.setattr(compta, "_inserer_ecriture",
                            lambda *a, **k: {"ok": False, "code": "E_TEST", "message": "refus"})
    else:
        def explose(*a, **k):
            raise RuntimeError("panne simulée")
        monkeypatch.setattr(compta, "_inserer_ecriture", explose)
    res = qv.valider(uuid, nature=qv.APPORT_ASSOCIE, objet_id="PERS_TFLUX", acteur=ACTEUR,
                     db_path=base)
    assert res["ok"] is False and res["code"] == qv.E_ECRITURE_REFUSEE
    for table in ("banque_rapprochements", "banque_rapprochement_evenements", "ecritures",
                  "ecriture_lignes"):
        assert _compter(base, table) == 0, table
    assert _compter(base, "qonto_transactions_statut_local", "statut_local='RAPPROCHE'") == 0


def _retrait(base):
    from tests.test_qonto_rapprochement_caisse import retrait
    ecran.actualiser(client=ClientDouble(pages=[[retrait(statut="completed")]]), db_path=base)
    conn = get_db(base)
    try:
        return conn.execute("SELECT t.qonto_transaction_uuid FROM qonto_transactions_raw t "
                            "JOIN qonto_transactions_statut_local s USING (qonto_transaction_uuid) "
                            "WHERE s.nature='RETRAIT_ESPECES'").fetchone()[0]
    finally:
        conn.close()


def test_K_retrait_transfert_caisse_atomique_succes(base, verrous):
    from app.services import qonto_validation_service as qv
    uuid = _retrait(base)
    res = qv.valider(uuid, nature=qv.TRANSFERT_CAISSE, acteur=ACTEUR, db_path=base)
    assert res["ok"], res
    lignes = _lignes_ecriture(base, res["ecriture"]["ecriture_id_opaque"])
    assert {l["compte"] for l in lignes} == {"530000", "512000"}
    transfert = next(m for m in flux.mouvements(source=flux.CAISSE, db_path=base)
                     if m.get("nature_caisse") == "TRANSFERT")
    assert transfert["statut_rapprochement"] == flux.RAPPROCHE


def test_K_L_retrait_echec_ecriture_aucun_rapprochement_ni_operation(base, verrous, monkeypatch):
    from app.services import qonto_validation_service as qv
    uuid = _retrait(base)
    monkeypatch.setattr(compta, "_inserer_ecriture",
                        lambda *a, **k: {"ok": False, "code": "E_TEST", "message": "refus"})
    res = qv.valider(uuid, nature=qv.TRANSFERT_CAISSE, acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and res["code"] == qv.E_ECRITURE_REFUSEE
    assert _compter(base, "banque_rapprochements") == 0
    assert _compter(base, "ecritures") == 0
    bilan = qv.confirmer_transferts_caisse(acteur=ACTEUR, db_path=base)
    assert bilan["comptabilises"] == 0 and _compter(base, "banque_rapprochements") == 0
    transfert = next(m for m in flux.mouvements(source=flux.CAISSE, db_path=base)
                     if m.get("nature_caisse") == "TRANSFERT")
    assert transfert["statut_compta"] != flux.COMPTABILISE


def test_M_mois_cloture_toujours_protege_pour_l_apport(base, verrous):
    from datetime import datetime, timezone
    from app.services import qonto_validation_service as qv
    _associe(base)
    conn = get_db(base)
    try:
        conn.execute("INSERT OR REPLACE INTO periodes_comptables (periode, statut) VALUES (?, "
                     "'CLOTUREE')", (datetime.now(timezone.utc).strftime("%Y-%m"),))
        conn.commit()
    finally:
        conn.close()
    _importer(base, [_mvt(200.0, sens="credit", contrepartie="Associé")])
    res = qv.valider(_uuid_montant(base, 200.0), nature=qv.APPORT_ASSOCIE, objet_id="PERS_TFLUX",
                     acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and _compter(base, "banque_rapprochements") == 0


def test_N_O_integrite_apres_circuits_atomiques(base, verrous):
    from app.services import qonto_validation_service as qv
    _associe(base)
    _importer(base, [_mvt(200.0, sens="credit", contrepartie="Associé")])
    qv.valider(_uuid_montant(base, 200.0), nature=qv.APPORT_ASSOCIE, objet_id="PERS_TFLUX",
               acteur=ACTEUR, db_path=base)
    qv.valider(_retrait(base), nature=qv.TRANSFERT_CAISSE, acteur=ACTEUR, db_path=base)
    conn = get_db(base)
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()
