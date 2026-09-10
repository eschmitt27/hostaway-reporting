"""Composition d'une facture propriétaire : extras, réductions, charges rattachées, formule,
prévisualisation, PDF de marque et comptabilisation.

Aucun appel réseau, aucune vraie base : `db` isole `cfg.DB_PATH` dans `tmp_path`.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import factures_proprietaires_composition_service as compo
from app.services import factures_proprietaires_pdf as pdfsvc
from app.services import factures_proprietaires_service as svc

EMETTEUR = {"nom": "Chouette Patrimoine", "adresse": "1 rue de Test, 31000 Toulouse",
            "siren": "109 624 767", "siret": ""}
DESTINATAIRE = {"nom": "Proprietaire Fixture", "adresse": "2 rue de Test, 31000 Toulouse"}


@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    apply_migrations(chemin)
    # Un acompte passe par `proprietaires_tresorerie_service`, qui refuse un propriétaire absent du
    # référentiel (`V01_PROPRIETAIRE_INCONNU`). C'est la bonne règle : on l'alimente plutôt que de
    # la contourner, sinon le test prouverait un chemin que la production n'emprunte jamais.
    conn = get_db(chemin)
    try:
        # Le référentiel n'est « disponible » que s'il porte un import abouti
        # (`ref_setup_repo.est_disponible`) : un schéma vide n'est pas un référentiel.
        conn.execute("INSERT OR IGNORE INTO ref_setup_imports "
                     "(import_id, horodatage, chemin_source, empreinte_source, statut, "
                     " nb_feuilles, nb_lignes) "
                     "VALUES ('IMP-TEST','2026-06-01T00:00:00Z','fixture','0'*64,'IMPORTE',1,1)")
        for pid in ("PROP_FIXT_1", "PROP_0", "PROP_1", "PROP_2", "PROP_AUTRE"):
            conn.execute(
                "INSERT OR IGNORE INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
                "prenom_proprietaire, adresse_facturation, actif, import_id) "
                "VALUES (?,?,?,?,'OUI','IMP-TEST')",
                (pid, f"Nom {pid}", "Prenom", "2 rue de Test, 31000 Toulouse"))
        conn.commit()
    finally:
        conn.close()
    return chemin


@pytest.fixture()
def ecritures_actives(monkeypatch):
    for flag in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                 "COMPTABILITE_REAL_WRITE_ENABLED",
                 "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED",
                 "CHARGES_REAL_WRITE_ENABLED", "CHARGES_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, flag, True, raising=False)


def _source(**kw):
    base = {"mois": "2026-06", "proprietaire_id": "PROP_FIXT_1", "logement_id": "LOG_FIXT_1",
            "source_calcul": "PREF-2026-06-PROP_FIXT_1-LOG_FIXT_1-001",
            "COMMISSION_CONCIERGERIE": 300.0, "MENAGE_FACTURE": 150.0, "CHARGE_FIXE": 50.0,
            "montant_du_conciergerie": 500.0}
    base.update(kw)
    return base


@pytest.fixture()
def facture(db):
    return svc.creer(_source(), acteur="test", db_path=db)["facture_id_opaque"]


def _charge(db, **kw):
    from app.services import charges_saisie_service as charges
    donnees = {"date_charge": "2026-06-12", "montant": 42.90, "categorie_charge_id": "CHG_004",
               "code_impact": "IC", "refacturable": "OUI", "proprietaire_id": "PROP_FIXT_1",
               "logement_id": "LOG_FIXT_1", "commentaire": "Remplacement bouilloire"}
    donnees.update(kw)
    return charges.creer(donnees, acteur="test", db_path=db)["charge_id"]


# ── 1-9 : brouillon, période, formule de base ───────────────────────────────────────────────────

def test_brouillon_cree_avec_ses_postes(db, facture):
    d = compo.decomposition(facture, db_path=db)
    assert d["par_cle"]["commissions"] == 300.0
    assert d["par_cle"]["menages"] == 150.0
    assert d["par_cle"]["forfait"] == 50.0
    assert d["total_facture"] == 500.0
    assert d["montant_du"] == 500.0


def test_forfait_est_une_seule_ligne_logiciel_et_consommables(db, facture):
    """Le référentiel (`REC_001`) porte « Forfait client logiciel et consommables » d'un seul
    tenant : la facture ne doit pas inventer deux postes séparés."""
    poste = next(p for p in compo.decomposition(facture, db_path=db)["postes"]
                 if p["cle"] == "forfait")
    assert poste["nb"] == 1
    assert "CHARGE_FIXE" in [l["type_ligne"] for l in poste["lignes"]]
    assert not any(p["cle"] == "consommables"
                   for p in compo.decomposition(facture, db_path=db)["postes"])


def test_periode_expose_les_bornes_reelles_du_mois(db, facture):
    p = compo.periode("2026-06")
    assert (p["debut"], p["fin"]) == ("2026-06-01", "2026-06-30")
    assert compo.periode("2026-02")["fin"] == "2026-02-28"


def test_periode_illisible_ne_leve_pas():
    assert compo.periode("")["debut"] == ""


# ── 10-15 : extras ──────────────────────────────────────────────────────────────────────────────

def test_ajout_extra_augmente_le_total(db, facture):
    compo.ajouter_extra(facture, libelle="Serrurier", montant=60, acteur="t", db_path=db)
    d = compo.decomposition(facture, db_path=db)
    assert d["par_cle"]["extras"] == 60.0
    assert d["total_facture"] == 560.0


def test_extra_negatif_refuse(db, facture):
    with pytest.raises(svc.FactureProprietaireError, match="strictement positif"):
        compo.ajouter_extra(facture, libelle="X", montant=-5, acteur="t", db_path=db)


def test_retrait_extra_retablit_le_total(db, facture):
    r = compo.ajouter_extra(facture, libelle="Serrurier", montant=60, acteur="t", db_path=db)
    svc.supprimer_ligne(facture, r["ligne_id_opaque"], acteur="t", db_path=db)
    assert compo.decomposition(facture, db_path=db)["total_facture"] == 500.0


# ── 16-20 : réductions, et leur différence avec un acompte ──────────────────────────────────────

def test_reduction_diminue_le_total_facture(db, facture):
    compo.ajouter_reduction(facture, libelle="Remise", montant=25, acteur="t", db_path=db)
    d = compo.decomposition(facture, db_path=db)
    assert d["par_cle"]["reductions"] == -25.0
    assert d["total_reductions"] == 25.0
    assert d["total_facture"] == 475.0


def test_reduction_saisie_positive_stockee_negative(db, facture):
    compo.ajouter_reduction(facture, libelle="Remise", montant=25, acteur="t", db_path=db)
    ligne = next(l for l in svc.lire(facture, db_path=db)["lignes"]
                 if l["type_ligne"] == compo.TYPE_REDUCTION)
    assert ligne["montant"] == -25.0


def test_reduction_ne_peut_pas_rendre_la_facture_negative(db, facture):
    with pytest.raises(svc.FactureProprietaireError, match="depasse le montant facturable"):
        compo.ajouter_reduction(facture, libelle="Remise", montant=10_000, acteur="t", db_path=db)


def test_reduction_et_acompte_ne_sont_pas_la_meme_chose(db, facture, monkeypatch):
    """Invariant central : la réduction entre dans le total facturé, l'acompte non."""
    from app.services import factures_proprietaires_edition_service as edition
    compo.ajouter_reduction(facture, libelle="Remise", montant=25, acteur="t", db_path=db)
    edition.ajouter_acompte(facture, montant=100, date_mouvement="2026-06-05", acteur="t",
                            db_path=db)
    d = compo.decomposition(facture, db_path=db)
    assert d["total_facture"] == 475.0, "l'acompte ne doit PAS diminuer le total facture"
    assert d["total_acomptes"] == 100.0
    assert d["montant_du"] == 375.0


# ── 21 : la formule complète ────────────────────────────────────────────────────────────────────

def test_formule_totale_exacte(db, facture, ecritures_actives):
    from app.services import factures_proprietaires_edition_service as edition
    compo.rattacher_charge(facture, _charge(db), acteur="t", db_path=db)      # +42.90
    compo.ajouter_extra(facture, libelle="Extra", montant=60, acteur="t", db_path=db)   # +60
    compo.ajouter_reduction(facture, libelle="Remise", montant=25, acteur="t", db_path=db)  # -25
    edition.ajouter_acompte(facture, montant=100, date_mouvement="2026-06-05", acteur="t",
                            db_path=db)
    d = compo.decomposition(facture, db_path=db)
    attendu_sous_total = 300.0 + 150.0 + 50.0 + 42.90 + 60.0
    assert d["sous_total"] == pytest.approx(attendu_sous_total)
    assert d["total_facture"] == pytest.approx(attendu_sous_total - 25.0)
    assert d["montant_du"] == pytest.approx(attendu_sous_total - 25.0 - 100.0)


# ── 22-26 : charges refacturables rattachées ────────────────────────────────────────────────────

def test_charge_eligible_proposee_puis_retiree_de_la_liste(db, facture, ecritures_actives):
    cid = _charge(db)
    assert [c["charge_id"] for c in compo.charges_eligibles(facture, db_path=db)] == [cid]
    compo.rattacher_charge(facture, cid, acteur="t", db_path=db)
    assert compo.charges_eligibles(facture, db_path=db) == []


def test_rattachement_reference_la_charge_sans_la_recreer(db, facture, ecritures_actives):
    cid = _charge(db)
    r = compo.rattacher_charge(facture, cid, acteur="t", db_path=db)
    ligne = next(l for l in svc.lire(facture, db_path=db)["lignes"]
                 if l["ligne_id_opaque"] == r["ligne_id_opaque"])
    assert ligne["objet_source_type"] == compo.SOURCE_CHARGE
    assert ligne["objet_source_ref"] == cid
    assert ligne["montant"] == 42.90, "le montant vient de la charge, jamais d'une ressaisie"
    conn = get_db(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM charges WHERE charge_id=?", (cid,)).fetchone()[0] == 1
    finally:
        conn.close()


def test_charge_deja_facturee_refusee(db, facture, ecritures_actives):
    cid = _charge(db)
    compo.rattacher_charge(facture, cid, acteur="t", db_path=db)
    with pytest.raises(svc.FactureProprietaireError, match="deja facturee"):
        compo.rattacher_charge(facture, cid, acteur="t", db_path=db)


def test_charge_mauvais_proprietaire_refusee(db, facture, ecritures_actives):
    cid = _charge(db, proprietaire_id="PROP_AUTRE", logement_id="LOG_AUTRE")
    assert cid not in [c["charge_id"] for c in compo.charges_eligibles(facture, db_path=db)]
    with pytest.raises(svc.FactureProprietaireError, match="proprietaire"):
        compo.rattacher_charge(facture, cid, acteur="t", db_path=db)


def test_charge_non_refacturable_refusee(db, facture, ecritures_actives):
    cid = _charge(db, refacturable="NON")
    assert cid not in [c["charge_id"] for c in compo.charges_eligibles(facture, db_path=db)]
    with pytest.raises(svc.FactureProprietaireError, match="non refacturable"):
        compo.rattacher_charge(facture, cid, acteur="t", db_path=db)


def test_detachement_libere_la_charge_sans_l_annuler(db, facture, ecritures_actives):
    cid = _charge(db)
    r = compo.rattacher_charge(facture, cid, acteur="t", db_path=db)
    compo.detacher_charge(facture, r["ligne_id_opaque"], acteur="t", db_path=db)
    assert [c["charge_id"] for c in compo.charges_eligibles(facture, db_path=db)] == [cid]
    conn = get_db(db)
    try:
        statut = conn.execute("SELECT statut FROM charges WHERE charge_id=?", (cid,)).fetchone()[0]
    finally:
        conn.close()
    assert statut == "ACTIVE", "une charge preexistante n'est jamais annulee par le document"


def test_index_unique_interdit_structurellement_le_double_rattachement(db, facture,
                                                                       ecritures_actives):
    """Ceinture ET bretelles : même en contournant la garde applicative, SQLite refuse."""
    import sqlite3
    cid = _charge(db)
    compo.rattacher_charge(facture, cid, acteur="t", db_path=db)
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("INSERT INTO factures_proprietaires_lignes_charge "
                         "(ligne_id_opaque, facture_id_opaque, charge_id, code_impact) "
                         "VALUES ('FPRL-FAKE', ?, ?, 'IC')", (facture, cid))
            conn.commit()
    finally:
        conn.close()


# ── 27-30 : document, prévisualisation, PDF ─────────────────────────────────────────────────────

def test_document_brouillon_et_pdf_utilisent_la_meme_structure(db, facture):
    doc = compo.document(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    assert doc["fige"] is False
    assert {"lignes", "reservations", "decomposition", "montant_total"} <= set(doc)
    octets = pdfsvc.rendre(doc)
    assert octets[:4] == b"%PDF"


def test_pdf_groupes_alignes_sur_le_service():
    """Le PDF redéfinit la table des groupes pour rester sans dépendance applicative : ce test
    empêche les deux de diverger en silence."""
    assert [(c, t) for c, _, t in pdfsvc._GROUPES_PDF] == [(c, t) for c, _, t in compo.GROUPES]


def test_pdf_porte_le_logo_officiel():
    assert pdfsvc.LOGO.exists(), "le logo officiel doit etre present dans les assets"
    assert pdfsvc.LOGO.name.endswith(".png")


def test_pdf_affiche_le_siren_jamais_comme_siret(db, facture):
    doc = compo.document(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    pdf = _Sonde(doc)
    assert any("SIREN 109 624 767" in l for l in pdf.mentions)
    assert not any("SIRET" in l for l in pdf.mentions), \
        "aucun SIRET ne doit apparaitre : seul un SIREN est connu"


class _Sonde:
    """Lit les mentions de pied de page sans rendre le PDF entier."""
    def __init__(self, snapshot):
        objet = pdfsvc._Facture(snapshot)
        self.mentions = objet._mentions_pied()


def test_pdf_multi_pages_repete_les_entetes(db, facture):
    doc = compo.document(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    doc["reservations"] = [
        {"reservation_id": f"HA-{i}", "check_in": "2026-06-01", "check_out": "2026-06-03",
         "nights": 2, "plateforme": "Airbnb", "payout": 200.0,
         "assiette_commission": 180.0, "taux_commission": 0.19, "commission": 34.2}
        for i in range(40)]
    octets = pdfsvc.rendre(doc)
    assert octets.count(b"/Type /Page\n") >= 2, "40 sejours doivent produire plusieurs pages"


def test_pdf_deterministe(db, facture):
    doc = compo.document(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    assert pdfsvc.rendre(doc) == pdfsvc.rendre(doc)


def test_pdf_sans_pii_voyageur(db, facture):
    doc = compo.document(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    doc["reservations"] = [{"reservation_id": "HA-1", "guest_name": "Jean SECRET",
                            "check_in": "2026-06-01", "check_out": "2026-06-03", "nights": 2,
                            "plateforme": "Airbnb", "payout": 200.0}]
    assert b"SECRET" not in pdfsvc.rendre(doc), "aucun nom de voyageur ne doit atteindre le PDF"


def test_taux_affiche_en_pourcent():
    assert pdfsvc._taux(0.19).startswith("19,00")
    assert pdfsvc._taux(19).startswith("19,00")
    assert pdfsvc._taux(None) == ""


# ── 31-36 : émission, numérotation, comptabilité ────────────────────────────────────────────────

def _emettre(db, facture, tmp_path, ecritures=True):
    svc.valider(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, acteur="t", db_path=db)
    return svc.emettre(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE,
                       date_facture="2026-07-01",
                       generer_pdf=pdfsvc.fabrique(Path(tmp_path) / "docs"), acteur="t",
                       exiger_conformite=False, db_path=db)


def test_identite_acceptee_avec_siren_sans_siret(db, facture):
    svc.valider(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, acteur="t", db_path=db)
    assert svc.lire(facture, db_path=db)["statut"] == svc.ST_VALIDE


def test_identite_refusee_sans_aucun_identifiant(db, facture):
    sans = {**EMETTEUR, "siren": "", "siret": ""}
    with pytest.raises(svc.FactureProprietaireError, match="siret ou siren"):
        svc.valider(facture, emetteur=sans, destinataire=DESTINATAIRE, acteur="t", db_path=db)


def test_emission_fige_reservations_et_decomposition(db, facture, tmp_path):
    emise = _emettre(db, facture, tmp_path)
    import json
    snap = json.loads(svc.lire(facture, db_path=db)["snapshot_json"])
    assert "reservations" in snap and "decomposition" in snap
    assert snap["decomposition"]["montant_du"] == emise["montant_total"]


def test_document_emis_rend_le_snapshot_fige(db, facture, tmp_path):
    _emettre(db, facture, tmp_path)
    doc = compo.document(facture, db_path=db)
    assert doc["fige"] is True
    assert doc["numero_facture"]


def test_comptabilisation_idempotente(db, facture, tmp_path, ecritures_actives):
    from app.services import comptabilite_ecritures_service as compta
    emise = _emettre(db, facture, tmp_path)
    e1 = compta.generer_ecriture_vente_facture(emise, acteur="t", db_path=db)
    e2 = compta.generer_ecriture_vente_facture(emise, acteur="t", db_path=db)
    assert e1["ok"] and e2["ok"]
    assert e1["ecriture_id_opaque"] == e2["ecriture_id_opaque"]
    assert e2.get("deja_generee") is True
    conn = get_db(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM ecritures WHERE origine_id_opaque=?",
                            (facture,)).fetchone()[0] == 1
    finally:
        conn.close()


def test_ecriture_liee_a_la_facture_et_equilibree(db, facture, tmp_path, ecritures_actives):
    from app.services import comptabilite_ecritures_service as compta
    emise = _emettre(db, facture, tmp_path)
    e = compta.generer_ecriture_vente_facture(emise, acteur="t", db_path=db)
    conn = get_db(db)
    try:
        row = conn.execute("SELECT origine_type, total_debit, total_credit FROM ecritures "
                           "WHERE ecriture_id_opaque=?", (e["ecriture_id_opaque"],)).fetchone()
    finally:
        conn.close()
    assert row["origine_type"]
    assert row["total_debit"] == row["total_credit"] == emise["montant_total"]


def test_brouillon_ne_produit_aucune_ecriture(db, facture, ecritures_actives):
    from app.services import comptabilite_ecritures_service as compta
    r = compta.generer_ecriture_vente_facture(svc.lire(facture, db_path=db), acteur="t",
                                              db_path=db)
    assert r["ok"] is False, "un BROUILLON ne constate aucune vente"


def test_facture_emise_non_editable(db, facture, tmp_path):
    _emettre(db, facture, tmp_path)
    for action in (
            lambda: compo.ajouter_extra(facture, libelle="X", montant=10, acteur="t", db_path=db),
            lambda: compo.ajouter_reduction(facture, libelle="R", montant=10, acteur="t",
                                            db_path=db),
    ):
        with pytest.raises(svc.FactureProprietaireError):
            action()


def test_numero_unique_par_serie(db, tmp_path):
    ids = [svc.creer(_source(proprietaire_id=f"PROP_{i}"), acteur="t",
                     db_path=db)["facture_id_opaque"] for i in range(3)]
    numeros = [_emettre(db, fid, tmp_path)["numero_facture"] for fid in ids]
    assert len(set(numeros)) == 3, f"numeros dupliques : {numeros}"
