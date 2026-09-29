"""Mission 36 — circuit Banque → Charges → Comptabilité → Facturation, de bout en bout.

Trois objets qui ne se confondent jamais : le MOUVEMENT (réalité de trésorerie, immuable), la
CHARGE (réalité économique, montant libre) et le RAPPROCHEMENT (le lien, plafonné des deux côtés).

Cas de recette réel rejoué ici sur données FICTIVES : GiFi, un acompte de 5 € puis le solde de
15,25 € → UNE charge de 20,25 €, deux écritures 606320 / 512 (5 € puis 15,25 €), jamais une
écriture de 20,25 € contre un mouvement de 5 €.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import charges_confirmation_service as confirmation
from app.services import charges_preview_service as prev
from app.services import charges_saisie_service as saisie
from app.services import comptabilite_ecritures_service as compta
from app.services import comptabilite_mappings_service as maps
from app.services import comptabilite_plan_service as plan
from app.services import flux_financiers_service as flux
from app.services import flux_lettrage_service as lettrage
from app.services import flux_matching_service as matching
from app.services import justificatifs_service as justif
from tests.test_flux_financiers import (ACTEUR, PROPRIO, _charge, _compter, _importer, _mvt,
                                        _par_montant, base, verrous)  # noqa: F401 — fixtures
from tests.test_lecture_seule_flux import _ecarts, _empreintes


# ══ Outils ═══════════════════════════════════════════════════════════════════════════════════════

@pytest.fixture(autouse=True)
def _referentiel_saisie(request):
    """Référentiels du formulaire « Nouvelle charge » (modes de paiement, statuts, mois ouverts)."""
    if "base" not in request.fixturenames:
        return
    db = request.getfixturevalue("base")
    conn = get_db(db)
    try:
        for table, ligne in (
                ("ref_modes_paiement", {"mode_paiement_id": "PAY_001", "mode_paiement": "VIREMENT",
                                        "actif": "OUI"}),
                ("ref_codes_impact", {"code_impact": "IC", "libelle": "Impact comptable",
                                      "actif": "OUI"}),
                ("ref_types_flux", {"type_flux_id": "TYPE_FLUX_020", "type_flux": "Depense",
                                    "code_impact_defaut": "IC", "actif": "OUI"}),
                ("ref_assoc_mode", {"assoc_mode_id": "AM_1", "mode_paiement_id": "PAY_001",
                                    "associe_id": "", "assoc_mode": "CONCIERGERIE", "actif": "OUI"}),
                ("ref_statuts", {"statut_id": "STC_1", "famille_statut": "statut_controle",
                                 "statut": "A_CONTROLER", "actif": "OUI"}),
                ("ref_types_affectation", {"affectation_id": "AFF_1", "type_affectation": "GLOBAL",
                                           "actif": "OUI"})):
            from tests.fixtures_referentiel import IMPORT_TEST
            ligne = dict(ligne, import_id=IMPORT_TEST)
            conn.execute(f"INSERT OR IGNORE INTO {table} ({', '.join(ligne)}) "
                         f"VALUES ({', '.join('?' * len(ligne))})", tuple(ligne.values()))
        for mois in ("2026-08", "2026-09"):
            conn.execute("INSERT OR IGNORE INTO ref_cloture_mensuelle (mois, statut_mois) "
                         "VALUES (?, 'OUVERT')", (mois,))
        conn.commit()
    finally:
        conn.close()


def _formulaire(m, montant, categorie="CHG_018", **extra):
    f = {"date_charge": m["date"], "montant": f"{montant:.2f}", "categorie_charge_id": categorie,
         "code_impact": "IC", "mode_paiement_id": "PAY_001", "commentaire": "Achat test",
         "mouvement_origine": f"BANQUE:{m['id']}", "perimetre_mode": "GLOBAL",
         "impact_menage": "NON", "avantage_associe": "NON", "refacturable": "NON"}
    f.update(extra)
    return f


def _charge_depuis(db, m, montant, *, categorie="CHG_018", reponse=None, **extra):
    """Saisie réelle : prévisualisation puis confirmation, comme l'écran."""
    apercu = prev.previsualiser(_formulaire(m, montant, categorie, **extra), db_path=db)
    assert apercu["ok"], apercu["manifest"]["errors"]
    res = confirmation.confirmer(apercu["token"], db_path=db, acteur=ACTEUR,
                                 justificatif=reponse or {"present": "NON",
                                                          "justification": "Ticket perdu (test)"})
    assert res.ok, res.as_dict()
    return res.charge_id


def _lignes(db, ecriture_id):
    return sorted((l["compte"], l["debit"], l["credit"], l["auxiliaire"])
                  for l in compta.lignes(ecriture_id, db_path=db))


def _rapproche(db, cle):
    conn = get_db(db)
    try:
        return round(conn.execute(
            "SELECT COALESCE(SUM(l.montant),0) FROM flux_lettrage_lignes l JOIN flux_lettrages t "
            "ON t.lettrage_id_opaque=l.lettrage_id_opaque WHERE t.statut='VALIDE' "
            "AND l.cote='OBJET' AND l.element_id=?", (cle,)).fetchone()[0], 2)
    finally:
        conn.close()


# ══ Plusieurs paiements, une charge ══════════════════════════════════════════════════════════════

def test_01_gifi_5_puis_15_25_une_seule_charge(base, verrous):
    _importer(base, [_mvt(5.0, contrepartie="GIFI", date="2026-09-12"),
                     _mvt(15.25, contrepartie="GIFI", date="2026-09-19")])
    m5, m15 = _par_montant(base, 5.0), _par_montant(base, 15.25)

    # Le montant de la charge est libre ; l'écart avec le mouvement se justifie.
    sans = prev.previsualiser(_formulaire(m5, 20.25), db_path=base)
    assert not sans["ok"]
    codes = {e["code"] for e in sans["manifest"]["errors"]}
    assert "V34_JUSTIFICATION_ECART_MONTANT" in codes
    assert "V34_MONTANT_SUPERIEUR_AU_MOUVEMENT" not in codes, "l'ancien plafond a disparu"
    cid = _charge_depuis(base, m5, 20.25, justification_ecart_montant=(
        "Acompte de 5 € puis paiement du solde de 15,25 € lors du retrait de la commande."))
    charge = saisie.lire(cid, db_path=base)
    assert charge["montant"] == 20.25 and charge["compte_comptable"] == "606320"
    assert "15,25" in charge["justification_ecart_montant"]

    # 1er rapprochement : plafonné aux 5 € du mouvement ; la charge reste ouverte, sans anomalie.
    r1 = lettrage.valider([f"BANQUE:{m5['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert r1["ok"], r1
    assert _lignes(base, r1["ecritures"][0]) == [("512000", 0.0, 5.0, None),
                                                 ("606320", 5.0, 0.0, None)]
    obj = next(o for o in flux.objets(db_path=base) if o["id"] == cid)
    assert obj["montant"] == 20.25 and obj["reste"] == 15.25

    # Depuis le mouvement de 15,25 €, la charge existante est proposée pour son reste.
    props = matching.propositions(mouvement_id=m15["id"], db_path=base)
    assert any(o["id"] == cid for p in props for o in p["objets"]), props
    assert next(o for o in flux.objets(db_path=base) if o["id"] == cid)["reste"] == 15.25
    r2 = lettrage.valider([f"BANQUE:{m15['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert r2["ok"], r2
    assert _lignes(base, r2["ecritures"][0]) == [("512000", 0.0, 15.25, None),
                                                 ("606320", 15.25, 0.0, None)]
    assert _rapproche(base, cid) == 20.25
    assert _compter(base, "charges") == 1, "UNE seule charge"
    assert saisie.lire(cid, db_path=base)["statut_rapprochement"] == "RAPPROCHE"
    conn = get_db(base)
    try:
        assert conn.execute("SELECT MAX(total_debit) FROM ecritures").fetchone()[0] == 15.25, \
            "aucune écriture de 20,25 € contre un mouvement de 5 €"
    finally:
        conn.close()


def test_02_deux_mouvements_une_charge_en_une_fois(base, verrous):
    _importer(base, [_mvt(5.0, date="2026-09-12"), _mvt(15.25, date="2026-09-19")])
    m5, m15 = _par_montant(base, 5.0), _par_montant(base, 15.25)
    cid = _charge(base, 20.25)
    res = lettrage.valider([f"BANQUE:{m5['id']}", f"BANQUE:{m15['id']}"], [f"CHARGE:{cid}"],
                           acteur=ACTEUR, db_path=base)
    assert res["ok"], res
    assert _rapproche(base, cid) == 20.25


def test_03_un_mouvement_deux_charges_et_mouvement_partiellement_explique(base, verrous):
    _importer(base, [_mvt(100.0)])
    m = _par_montant(base, 100.0)
    c1, c2 = _charge(base, 60.0), _charge(base, 25.0)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{c1}", f"CHARGE:{c2}"],
                           acteur=ACTEUR, db_path=base)
    assert res["ok"], res                       # 85 € expliqués, 15 € restent ouverts
    assert _rapproche(base, c1) == 60.0 and _rapproche(base, c2) == 25.0
    assert _par_montant(base, 100.0)["restant"] == 15.0
    assert _lignes(base, res["ecritures"][0])[-1] == ("606320", 85.0, 0.0, None) or \
        sum(l[1] for l in _lignes(base, res["ecritures"][0])) == 85.0


def test_04_plafonds_du_rapprochement(base, verrous):
    """Jamais plus que le mouvement, jamais plus que la charge."""
    _importer(base, [_mvt(30.0)])
    m = _par_montant(base, 30.0)
    cid = _charge(base, 10.0)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert res["ok"]
    assert _rapproche(base, cid) == 10.0 and _par_montant(base, 30.0)["restant"] == 20.0
    encore = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR,
                              db_path=base)
    assert not encore["ok"], "une charge soldée ne se rapproche plus"


# ══ Mouvement bancaire immuable ══════════════════════════════════════════════════════════════════

def test_05_montant_bancaire_immuable(base, verrous):
    from app.routes.banques import _form_to_decision
    assert not {"montant", "sens", "date", "compte"} & set(
        _form_to_decision({"montant": "999", "sens": "credit"})), \
        "la décision sur un mouvement ne porte ni montant, ni sens, ni date, ni compte"
    _importer(base, [_mvt(42.0)])
    m = _par_montant(base, 42.0)
    cid = _charge(base, 42.0)
    prep = lettrage.preparer([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], db_path=base)
    lignes = [dict(l) for l in prep["ecritures"][0]["lignes"]]
    for l in lignes:
        if l["role"] == lettrage.ROLE_TRESORERIE:
            l["credit"] = 40.0
        else:
            l["debit"] = 40.0
    erreurs = lettrage.verifier_ecritures(prep, [lignes], db_path=base)
    assert any("doit rester 42.00 €" in e["message"] for e in erreurs)


# ══ Justificatifs ════════════════════════════════════════════════════════════════════════════════

def test_06_justificatif_archive_verifie_dans_le_dossier(base, verrous):
    _importer(base, [_mvt(9.9, date="2026-09-05")])
    m = _par_montant(base, 9.9)
    ref = justif.prochaine_reference(justif.OBJET_CHARGE, m["date"], db_path=base)
    assert ref == "CHG-2026-09-001"
    dossier = justif.dossier(justif.OBJET_CHARGE, m["date"])
    assert dossier.as_posix().endswith("Charges/2026/09")
    apercu = prev.previsualiser(_formulaire(m, 9.9), db_path=base)
    # « Oui » sans fichier : refusé, rien n'est écrit, la prévisualisation reste confirmable.
    refus = confirmation.confirmer(apercu["token"], db_path=base, acteur=ACTEUR,
                                   justificatif={"present": "OUI"})
    assert not refus.ok and refus.code == justif.E_FICHIER
    assert _compter(base, "charges") == 0 and not confirmation.resultat_existe(apercu["token"])
    dossier.mkdir(parents=True, exist_ok=True)
    (dossier / f"{ref}__GIFI.pdf").write_bytes(b"%PDF-1.4 test")
    res = confirmation.confirmer(apercu["token"], db_path=base, acteur=ACTEUR,
                                 justificatif={"present": "OUI"})
    assert res.ok, res.as_dict()
    j = justif.charger(justif.OBJET_CHARGE, res.charge_id, db_path=base)
    assert j["reference"] == ref and j["statut"] == justif.ST_ARCHIVE
    assert j["fichier_constate"] == f"{ref}__GIFI.pdf" and j["confirme_par"] == ACTEUR
    assert [e["type_evenement"] for e in justif.historique(ref, db_path=base)] == [
        "ATTRIBUTION", "CONFIRMATION_ARCHIVE"]


def test_07_justificatif_absent_exige_une_justification(base, verrous):
    _importer(base, [_mvt(12.0)])
    m = _par_montant(base, 12.0)
    apercu = prev.previsualiser(_formulaire(m, 12.0), db_path=base)
    vide = confirmation.confirmer(apercu["token"], db_path=base, acteur=ACTEUR,
                                  justificatif={"present": "NON", "justification": "  "})
    assert not vide.ok and vide.code == justif.E_JUSTIFICATION
    sans_reponse = confirmation.confirmer(apercu["token"], db_path=base, acteur=ACTEUR,
                                          justificatif={})
    assert not sans_reponse.ok and sans_reponse.code == justif.E_REPONSE
    res = confirmation.confirmer(apercu["token"], db_path=base, acteur=ACTEUR, justificatif={
        "present": "NON", "justification": "Ticket perdu — demande de duplicata au fournisseur"})
    assert res.ok
    j = justif.charger(justif.OBJET_CHARGE, res.charge_id, db_path=base)
    assert j["statut"] == justif.ST_ABSENT_JUSTIFIE and "duplicata" in j["justification_absence"]
    # La base elle-même refuse une absence non justifiée.
    conn = get_db(base)
    try:
        with pytest.raises(Exception):
            conn.execute("UPDATE justificatifs SET justification_absence=NULL WHERE reference=?",
                         (j["reference"],))
    finally:
        conn.close()


def test_08_pas_de_validation_finale_sans_reponse_sur_le_justificatif(base, verrous):
    cid = saisie.creer({"date_charge": "2026-09-19", "montant": 7.0, "categorie_charge_id": "CHG_018",
                        "code_impact": "IC", "prise_en_compta": "OUI", "mode_paiement_id": "PAY_001",
                        "affectation_type": "GLOBAL", "refacturable": "NON",
                        "statut_controle": "A_CONTROLER"}, acteur=ACTEUR, db_path=base)["charge_id"]
    j = justif.charger(justif.OBJET_CHARGE, cid, db_path=base)
    assert j["statut"] == justif.ST_A_CONFIRMER and j["reference"].startswith("CHG-2026-09-")
    refus = saisie.valider_controle(cid, acteur=ACTEUR, db_path=base)
    assert not refus["ok"] and refus["code"] == saisie.E_JUSTIFICATIF_A_CONFIRMER
    justif.confirmer(justif.OBJET_CHARGE, cid, present="NON", justification="Paiement sans ticket",
                     acteur=ACTEUR, db_path=base)
    assert saisie.valider_controle(cid, acteur=ACTEUR, db_path=base)["ok"]


def test_09_references_distinctes_et_identifiant_technique_conserve(base, verrous):
    a, b = _charge(base, 1.0), _charge(base, 2.0)
    ra = justif.charger(justif.OBJET_CHARGE, a, db_path=base)["reference"]
    rb = justif.charger(justif.OBJET_CHARGE, b, db_path=base)["reference"]
    assert ra != rb and ra.startswith("CHG-") and a.startswith("CHG-") and a != ra


# ══ Catégories → comptes ═════════════════════════════════════════════════════════════════════════

CATALOGUE = {
    "CHG_001": "611100", "CHG_003": "611200", "CHG_004": "606310", "CHG_018": "606320",
    "CHG_028": "606400", "CHG_029": "606800", "CHG_008": "615200", "CHG_030": "615500",
    "CHG_031": "615600", "CHG_010": "627800", "CHG_032": "622200", "CHG_033": "622600",
    "CHG_034": "622700", "CHG_035": "623100", "CHG_036": "623400", "CHG_037": "624100",
    "CHG_009": "625100", "CHG_038": "625700", "CHG_039": "626000", "CHG_011": "616000",
    "CHG_040": "613200", "CHG_041": "614000", "CHG_042": "628100", "CHG_005": "651100",
    "CHG_006": "651100", "CHG_007": "651100",
}


@pytest.mark.parametrize("categorie,compte", sorted(CATALOGUE.items()))
def test_10_chaque_categorie_du_catalogue_propose_son_compte(tmp_db, categorie, compte):
    p = maps.comptes_proposes(categorie, db_path=tmp_db)
    assert p["statut"] == maps.PROPOSITION_UNIQUE and p["compte_defaut"] == compte
    assert maps.resoudre_compte(categorie_charge_id=categorie, db_path=tmp_db)["compte"] == compte


def test_11_impots_au_choix_et_autre_charge_en_imputation_libre(base, verrous):
    impots = maps.comptes_proposes("CHG_043", db_path=base)
    assert impots["statut"] == maps.PROPOSITION_CHOIX and impots["compte_defaut"] == ""
    assert {c["compte"] for c in impots["comptes"]} == {"635110", "635400"}
    assert maps.comptes_proposes("CHG_024", db_path=base)["statut"] == maps.PROPOSITION_A_DEFINIR
    # Autre charge : imputation libre possible, justification obligatoire.
    valeurs = {"categorie_charge_id": "CHG_024", "compte_comptable": "628100",
               "date_charge": "2026-09-10"}
    assert saisie.refus_compte(dict(valeurs), db_path=base)["code"] == saisie.E_COMPTE_CHARGE
    ok = dict(valeurs, justification_imputation="Adhésion ponctuelle à une association locale")
    assert saisie.refus_compte(ok, db_path=base) is None and ok["compte_origine"] == "LIBRE"
    # Un compte inexistant ou non-charge est refusé même justifié.
    assert saisie.refus_compte(dict(ok, compte_comptable="512000"), db_path=base) is not None


def test_12_frais_bancaires_imputation_automatique(base, verrous):
    _importer(base, [_mvt(2.40, contrepartie="Qonto", libelle="Frais retrait")])
    m = _par_montant(base, 2.40)
    cid = _charge_depuis(base, m, 2.40, categorie="CHG_010")
    assert saisie.lire(cid, db_path=base)["compte_comptable"] == "627800"
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert _lignes(base, res["ecritures"][0]) == [("512000", 0.0, 2.4, None),
                                                  ("627800", 2.4, 0.0, None)]


# ══ Auxiliaires ══════════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("compte,mode,type_", [
    ("606320", "NONE", ""), ("706100", "NONE", ""), ("709600", "NONE", ""),
    ("512000", "NONE", ""), ("530000", "NONE", ""),
    ("401000", "REQUIRED", "FOURNISSEUR"), ("411000", "REQUIRED", "CLIENT"),
    ("419100", "REQUIRED", "CLIENT"), ("455100", "REQUIRED", "ASSOCIE")])
def test_13_mode_auxiliaire_par_compte(tmp_db, compte, mode, type_):
    assert plan.mode_auxiliaire(compte, db_path=tmp_db) == (mode, type_)


def test_14_auxiliaire_efface_sur_compte_sans_tiers_et_exige_sur_401(base, verrous):
    lignes = [{"compte": "606320", "debit": 10, "credit": 0, "auxiliaire": "FRS-X"},
              {"compte": "512000", "debit": 0, "credit": 10, "auxiliaire": "FRS-X"}]
    res = compta._inserer_ecriture("BANQUE", "2026-09-10", "2026-09", "T1", "t", "TEST", "T1",
                                   lignes, db_path=base)
    assert res["ok"]
    assert {l["auxiliaire"] for l in compta.lignes(res["ecriture_id_opaque"], db_path=base)} == {None}
    refus = compta._inserer_ecriture(
        "ACHATS", "2026-09-10", "2026-09", "T2", "t", "TEST", "T2",
        [{"compte": "606320", "debit": 10, "credit": 0},
         {"compte": "401000", "debit": 0, "credit": 10}], db_path=base)
    assert not refus["ok"] and refus["code"] == compta.E_AUXILIAIRE


def test_15_changement_de_compte_401_vers_606320_vide_le_fournisseur(base, verrous):
    _importer(base, [_mvt(50.0)])
    m = _par_montant(base, 50.0)
    cid = _charge(base, 50.0)
    prep = lettrage.preparer([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], db_path=base)
    lignes = [dict(l) for l in prep["ecritures"][0]["lignes"]]
    objet = next(l for l in lignes if l["role"] == lettrage.ROLE_OBJET)
    objet["auxiliaire"] = "FRS-QUELCONQUE"          # resté d'un ancien choix 401
    assert lettrage.verifier_ecritures(prep, [lignes], db_path=base) == []
    assert objet["auxiliaire"] is None, "le tiers d'un compte sans tiers est effacé"
    # Inversement, 401 exige un fournisseur, et du bon type (un propriétaire n'en est pas un).
    objet.update(compte="401000", auxiliaire=PROPRIO)
    erreurs = lettrage.verifier_ecritures(prep, [lignes], db_path=base)
    assert any(e["code"] == lettrage.E_AUXILIAIRE for e in erreurs)
    objet["auxiliaire"] = None
    assert any("exige un fournisseur" in e["message"]
               for e in lettrage.verifier_ecritures(prep, [lignes], db_path=base))


# ══ Écritures éditables : contrepartie seulement, ventilation exacte ═════════════════════════════

def _ventiler(base, parts):
    _importer(base, [_mvt(100.0)])
    m = _par_montant(base, 100.0)
    cid = _charge(base, 100.0)
    prep = lettrage.preparer([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], db_path=base)
    tres = next(l for l in prep["ecritures"][0]["lignes"] if l["role"] == lettrage.ROLE_TRESORERIE)
    objet = next(l for l in prep["ecritures"][0]["lignes"] if l["role"] == lettrage.ROLE_OBJET)
    lignes = [dict(tres), dict(objet, debit=parts[0])]
    for compte, montant in parts[1:]:
        lignes.append({"compte": compte, "auxiliaire": None, "debit": montant, "credit": 0.0,
                       "libelle": "Ventilation", "role": lettrage.ROLE_SAISIE, "objet": "",
                       "logement_id": None, "proprietaire_id": None})
    return m, cid, prep, lignes


def test_16_ventilation_70_30_acceptee(base, verrous):
    m, cid, prep, lignes = _ventiler(base, [70.0, ("627800", 30.0)])
    assert lettrage.verifier_ecritures(prep, [lignes], db_path=base) == []
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR,
                           lignes=[lignes], db_path=base)
    assert res["ok"] and res["ecriture_modifiee"]
    assert _lignes(base, res["ecritures"][0]) == [("512000", 0.0, 100.0, None),
                                                  ("606320", 70.0, 0.0, None),
                                                  ("627800", 30.0, 0.0, None)]


@pytest.mark.parametrize("seconde", [29.0, 31.0])
def test_17_ventilation_99_ou_101_refusee(base, verrous, seconde):
    _, _, prep, lignes = _ventiler(base, [70.0, ("627800", seconde)])
    messages = [e["message"] for e in lettrage.verifier_ecritures(prep, [lignes], db_path=base)]
    assert ("La ventilation comptable doit correspondre exactement au montant du mouvement "
            "bancaire : 100,00 €.") in messages


# ══ Facture fournisseur : 6/401 puis 401/512, jamais deux fois la charge ═════════════════════════

def test_18_facture_fournisseur_puis_paiement_sans_double_charge(base, verrous):
    from tests.test_flux_financiers import _facture_fournisseur, _fournisseur
    fr = _fournisseur(base)
    fid = _facture_fournisseur(base, fr, 120.0)
    achat = compta.charger_par_origine("FACTURE_FOURNISSEUR", fid, db_path=base) or {}
    _importer(base, [_mvt(120.0, contrepartie="Plomberie Fictive")])
    m = _par_montant(base, 120.0)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"FACTURE_FOURNISSEUR:{fid}"], acteur=ACTEUR,
                           db_path=base)
    assert res["ok"], res
    assert _lignes(base, res["ecritures"][0]) == [("401000", 120.0, 0.0, fr),
                                                  ("512000", 0.0, 120.0, None)]
    conn = get_db(base)
    try:
        classe6 = conn.execute("SELECT COALESCE(SUM(debit-credit),0) FROM ecriture_lignes "
                               "WHERE compte LIKE '6%'").fetchone()[0]
    finally:
        conn.close()
    assert round(classe6, 2) == 120.0, "la charge n'est constatée qu'une fois (à l'achat)"
    assert achat or _compter(base, "ecritures", "journal='ACHATS'") == 1


def test_18b_facture_fournisseur_reference_et_piece_importee(base, verrous):
    """Saisie manuelle : référence FAF, justificatif à confirmer. Import d'un PDF : la pièce est
    déjà liée, elle est constatée sans question."""
    from app.services import factures_service as fs
    from tests.test_flux_financiers import _fournisseur
    fr = _fournisseur(base)
    saisie_ = fs.creer({"fournisseur_id_opaque": fr, "facture_ref": "FA-9", "date_facture":
                        "2026-09-03", "montant_ttc": 50}, acteur=ACTEUR, db_path=base)
    importee = fs.creer({"fournisseur_id_opaque": fr, "facture_ref": "FA-10", "date_facture":
                         "2026-09-04", "montant_ttc": 60, "source": "PDF",
                         "justificatif": "FA-10.pdf"}, acteur=ACTEUR, db_path=base)
    a = justif.charger(justif.OBJET_FACTURE_FOURNISSEUR, saisie_["facture_id_opaque"], db_path=base)
    b = justif.charger(justif.OBJET_FACTURE_FOURNISSEUR, importee["facture_id_opaque"], db_path=base)
    assert a["reference"] == "FAF-2026-09-001" and a["statut"] == justif.ST_A_CONFIRMER
    assert b["reference"] == "FAF-2026-09-002" and b["statut"] == justif.ST_ARCHIVE
    assert b["fichier_constate"] == "FA-10.pdf"


# ══ Facture propriétaire : une écriture par nature, acomptes en 4191 ═════════════════════════════

def _facture(db, lignes, *, mois="2026-08"):
    from app.services import factures_proprietaires_composition_service as compo
    from app.services import factures_proprietaires_service as fpr
    from tests.test_creances_regle_compense import DESTINATAIRE, EMETTEUR
    source = {"mois": mois, "proprietaire_id": PROPRIO, "logement_id": "LOG_T",
              "source_calcul": f"PREF-{mois}", "COMMISSION_CONCIERGERIE": lignes["gestion"],
              "MENAGE_FACTURE": lignes.get("menage", 0), "montant_du_conciergerie":
                  lignes["gestion"] + lignes.get("menage", 0)}
    fid = fpr.creer(source, acteur="t", db_path=db)["facture_id_opaque"]
    if lignes.get("reduction"):
        compo.ajouter_reduction(fid, libelle="Geste commercial", montant=lignes["reduction"],
                                acteur="t", db_path=db)
    for mid in lignes.get("acomptes", []):
        conn = get_db(db)
        try:
            conn.execute("UPDATE mouvements_tresorerie_proprietaires SET reference_metier=? "
                         "WHERE mouvement_opaque=?", (fid, mid))
            conn.commit()
        finally:
            conn.close()
    for i, montant in enumerate(lignes.get("reversements", [])):
        conn = get_db(db)
        try:
            conn.execute("INSERT INTO imputations_airbnb (imputation_airbnb_id, proprietaire_id, "
                         "logement_id, mois, document_id, montant_impute, date_imputation, statut) "
                         "VALUES (?,?,?,?,?,?,?,'VALIDE')",
                         (f"IMPA-T{i}-{fid[-6:]}", PROPRIO, "LOG_T", mois, fid, montant,
                          f"{mois}-20"))
            conn.commit()
        finally:
            conn.close()
    fpr.valider(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, acteur="t", db_path=db)
    emise = fpr.emettre(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE,
                        date_facture=f"{mois}-31", acteur="t", exiger_conformite=False,
                        db_path=db)
    return fid, emise


def _acompte_encaisse(db, montant, date="2026-08-10"):
    """Un acompte réellement reçu : mouvement propriétaire VALIDE, puis rapproché d'un crédit
    bancaire par Flux → écriture 512 / 419100 (jamais 706)."""
    from app.services import proprietaires_tresorerie_service as tres
    cree = tres.creer(PROPRIO, "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE", montant, date,
                      acteur=ACTEUR, db_path=db)
    tres.valider(cree["mouvement_opaque"], acteur=ACTEUR, db_path=db)
    _importer(db, [_mvt(montant, sens="credit", contrepartie="Claire Testeur", date=date,
                        libelle="VIR ACOMPTE")])
    m = _par_montant(db, montant)
    res = lettrage.valider([f"BANQUE:{m['id']}"],
                           [f"{flux.MOUVEMENT_PROPRIETAIRE}:{cree['mouvement_opaque']}"],
                           acteur=ACTEUR, db_path=db)
    assert res["ok"], res
    return cree["mouvement_opaque"], res


@pytest.fixture
def factures_ok(monkeypatch):
    for flag in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, flag, True, raising=False)


def test_19_facture_multi_lignes_une_ecriture_par_nature(base, verrous, factures_ok):
    fid, emise = _facture(base, {"gestion": 150.0, "menage": 50.0, "reduction": 10.0})
    res = compta.comptabiliser_facture_emise(emise, acteur=ACTEUR, db_path=base)
    assert res["vente"]["ok"], res
    assert _lignes(base, res["vente"]["ecriture_id_opaque"]) == [
        ("411000", 190.0, 0.0, PROPRIO), ("706100", 0.0, 150.0, None),
        ("706200", 0.0, 50.0, None), ("709600", 10.0, 0.0, None)]
    ecr = compta.charger(res["vente"]["ecriture_id_opaque"], db_path=base)
    assert ecr["total_debit"] == ecr["total_credit"] == 200.0
    assert res["imputation"].get("rien_a_imputer"), "sans acompte, rien à imputer"


def test_20_acompte_4191_puis_imputation_et_net_411(base, verrous, factures_ok):
    mid, enc = _acompte_encaisse(base, 40.0)
    assert _lignes(base, enc["ecritures"][0]) == [("419100", 0.0, 40.0, PROPRIO),
                                                  ("512000", 40.0, 0.0, None)]
    fid, emise = _facture(base, {"gestion": 150.0, "menage": 50.0, "reduction": 10.0,
                                 "acomptes": [mid]})
    res = compta.comptabiliser_facture_emise(emise, acteur=ACTEUR, db_path=base)
    assert res["vente"]["ok"] and res["imputation"]["ok"], res
    assert _lignes(base, res["imputation"]["ecriture_id_opaque"]) == [
        ("411000", 0.0, 40.0, PROPRIO), ("419100", 40.0, 0.0, PROPRIO)]
    for compte in ("706100", "706200", "706300", "706900", "706000"):
        assert all(l[0] != compte for l in _lignes(base, res["imputation"]["ecriture_id_opaque"]))
    # Solde client 411 : 190 facturés − 40 d'acompte = 150 (écritures proposées comprises).
    conn = get_db(base)
    try:
        solde = conn.execute("SELECT SUM(debit-credit) FROM ecriture_lignes WHERE compte='411000' "
                             "AND auxiliaire=?", (PROPRIO,)).fetchone()[0]
    finally:
        conn.close()
    assert round(solde, 2) == 150.0


def test_21_reversement_airbnb_famille_acompte(base, verrous, factures_ok):
    _acompte_encaisse(base, 100.0)       # 100 € détenus pour le client en 419100
    fid, emise = _facture(base, {"gestion": 200.0, "reversements": [60.0]})
    res = compta.comptabiliser_facture_emise(emise, acteur=ACTEUR, db_path=base)
    assert res["imputation"]["ok"], res
    lignes = compta.lignes(res["imputation"]["ecriture_id_opaque"], db_path=base)
    assert {(l["compte"], l["debit"], l["credit"]) for l in lignes} == {
        ("419100", 60.0, 0.0), ("411000", 0.0, 60.0)}
    assert any("Reversement Airbnb" in (l["libelle"] or "") for l in lignes)
    assert all(l["compte"] not in ("709600", "706100") or l["credit"] > 0 for l in lignes)


def test_22_reversement_sans_origine_comptable_detecte_jamais_invente(base, verrous, factures_ok):
    fid, emise = _facture(base, {"gestion": 200.0, "reversements": [60.0]})
    avant = _compter(base, "ecritures")
    res = compta.generer_ecriture_imputation_acomptes(emise, acteur=ACTEUR, db_path=base)
    assert not res["ok"] and res["code"] == compta.E_ORIGINE_ACOMPTE
    assert _compter(base, "ecritures") == avant, "aucune écriture de banque ni d'imputation inventée"


def test_23_extra_sinistre_va_en_706400(base, verrous, factures_ok):
    from app.services import factures_proprietaires_composition_service as compo
    from app.services import factures_proprietaires_service as fpr
    source = {"mois": "2026-08", "proprietaire_id": PROPRIO, "logement_id": "LOG_T",
              "source_calcul": "PREF-2026-08-X", "COMMISSION_CONCIERGERIE": 80.0,
              "montant_du_conciergerie": 80.0}
    fid = fpr.creer(source, acteur="t", db_path=base)["facture_id_opaque"]
    compo.ajouter_extra(fid, libelle="Gestion dégât des eaux", montant=45.0, nature="SINISTRE",
                        acteur="t", db_path=base)
    compo.ajouter_extra(fid, libelle="Préparation canapé", montant=15.0, acteur="t", db_path=base)
    prevue = compta.ecriture_vente_prevue(dict(fpr.lire(fid, db_path=base), numero_facture="X"),
                                          db_path=base)
    credits = {l["compte"]: l["credit"] for l in prevue["lignes"] if l["credit"]}
    assert credits == {"706100": 80.0, "706400": 45.0, "706500": 15.0}


def test_24_tva_jamais_supposee(base, verrous, factures_ok, monkeypatch):
    from app.services import factures_proprietaires_conformite_service as conformite
    fid, emise = _facture(base, {"gestion": 100.0})
    monkeypatch.setattr(conformite, "charger", lambda *a, **k: {"total_tva": 20.0})
    res = compta.ecriture_vente_prevue(emise, db_path=base)
    assert not res["ok"] and res["code"] == compta.E_TVA


# ══ Lecture seule et non-régression ══════════════════════════════════════════════════════════════

def test_25_nouveaux_ecrans_sans_ecriture(client, base, verrous):
    _importer(base, [_mvt(5.0, contrepartie="GIFI")])
    m = _par_montant(base, 5.0)
    cid = _charge(base, 5.0)
    avant = _empreintes(base)
    for url in (f"/fournisseurs/nouvelle?mouvement=BANQUE:{m['id']}", f"/fournisseurs/{cid}",
                f"/flux-financiers/rapprochement/valider?m=BANQUE:{m['id']}&o=CHARGE:{cid}",
                "/comptabilite/plan-comptable", "/comptabilite/plan-comptable/401000",
                "/comptabilite/mappings"):
        assert client.get(url).status_code == 200, url
    assert _ecarts(avant, _empreintes(base)) == []


def test_26_retrait_caisse_530_512_non_regresse(base, verrous):
    lignes = [{"compte": "530000", "debit": 20, "credit": 0},
              {"compte": "512000", "debit": 0, "credit": 20}]
    res = compta._inserer_ecriture("BANQUE", "2026-09-21", "2026-09", "R", "Retrait", "TEST", "R",
                                   lignes, db_path=base)
    assert res["ok"] and _lignes(base, res["ecriture_id_opaque"]) == [
        ("512000", 0.0, 20.0, None), ("530000", 20.0, 0.0, None)]


def test_27_parcours_http_confirmation_et_fiche(client, base, verrous):
    """L'écran de confirmation montre la référence et le dossier ; sans réponse, rien n'est écrit
    et la prévisualisation reste confirmable ; la fiche permet de répondre plus tard."""
    import html as _html
    _importer(base, [_mvt(12.0, contrepartie="FREE MOBILE", date="2026-09-21")])
    m = _par_montant(base, 12.0)
    r = client.post("/fournisseurs/nouvelle/previsualiser", data=_formulaire(m, 12.0, "CHG_039"),
                    follow_redirects=False)
    assert r.status_code == 303, _html.unescape(r.text)[:2000]
    reponse = client.get(r.headers["location"])
    apercu = _html.unescape(reponse.text)
    assert "CHG-2026-09-001" in apercu and justif.dossier_affiche(
        justif.dossier(justif.OBJET_CHARGE, "2026-09-21")) in apercu
    assert "626000" in apercu and "Le justificatif a-t-il bien été enregistré ?" in apercu
    token = r.headers["location"].rsplit("/", 1)[-1]
    refus = client.post(f"/fournisseurs/nouvelle/confirmer/{token}", data={},
                        follow_redirects=False)
    assert refus.status_code == 422 and _compter(base, "charges") == 0
    ok = client.post(f"/fournisseurs/nouvelle/confirmer/{token}",
                     data={"justificatif_present": "NON",
                           "justification_absence": "Facture en ligne à télécharger"},
                     follow_redirects=False)
    assert ok.status_code == 303 and _compter(base, "charges") == 1
    cid = get_db(base).execute("SELECT charge_id FROM charges").fetchone()[0]
    fiche = _html.unescape(client.get(f"/fournisseurs/{cid}").text)
    assert "CHG-2026-09-001" in fiche and "absence justifiée" in fiche and "626000" in fiche
    # Le duplicata arrive : on le range, on répond « oui » depuis la fiche.
    d = justif.dossier(justif.OBJET_CHARGE, "2026-09-21")
    (d / "CHG-2026-09-001__FREE.pdf").write_bytes(b"%PDF")
    client.post(f"/fournisseurs/{cid}/justificatif", data={"justificatif_present": "OUI"})
    assert justif.charger(justif.OBJET_CHARGE, cid, db_path=base)["statut"] == justif.ST_ARCHIVE
    # Écran de rapprochement : le mode de tiers de chaque compte est fourni au script.
    page = client.get(f"/flux-financiers/rapprochement/valider?m=BANQUE:{m['id']}&o=CHARGE:{cid}").text
    assert 'id="modes-auxiliaires"' in page and '"REQUIRED"' in page and '"FOURNISSEUR"' in page
