"""Conformité des factures propriétaires : numérotation légale, identités, TVA, mentions.

Toutes les valeurs sont fictives (« SAS DEMO CONCIERGERIE »). Aucune identité réelle, aucune
donnée d'entreprise réelle, aucun montant réel.

Le fil conducteur : ce module ne doit jamais compléter une donnée réglementaire absente. Un manque
bloque l'émission et se nomme explicitement.
"""
import pytest

import app.config as cfg
from app.db.connection import apply_migrations
from app.services import facturation_config_service as fconf
from app.services import factures_proprietaires_conformite_service as conformite
from app.services import factures_proprietaires_pdf as pdf
from app.services import factures_proprietaires_service as svc

EMETTEUR_LEGACY = {"nom": "SAS DEMO CONCIERGERIE", "adresse": "1 rue Demo", "siret": "00000000000000"}
DESTINATAIRE_LEGACY = {"nom": "Client Demo", "adresse": "2 rue Demo"}

# Configuration fictive complète — jamais des valeurs réelles.
CONFIG_COMPLETE = {
    "SOCIETE_NOM": "SAS DEMO CONCIERGERIE",
    "SOCIETE_FORME_JURIDIQUE": "SAS",
    "SOCIETE_CAPITAL": "1 000 EUR",
    "SOCIETE_SIREN": "000000000",
    "SOCIETE_SIRET": "00000000000000",
    "SOCIETE_RCS": "RCS DEMO 000 000 000",
    "SOCIETE_ADRESSE": "1 rue Demo, 00000 Villedemo",
    "SOCIETE_CONTACT": "contact@demo.invalid",
    "FACTURATION_REGIME_TVA": fconf.TVA_FRANCHISE,
    "FACTURATION_MENTION_FRANCHISE_TVA": "Mention de franchise (fixture de recette)",
    "FACTURATION_DELAI_PAIEMENT_JOURS": "30",
    "FACTURATION_CONDITIONS_ESCOMPTE": "Escompte pour paiement anticipe : neant",
}


@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    apply_migrations(chemin)
    return chemin


@pytest.fixture()
def config(monkeypatch):
    """Applique une configuration fictive ; `set(...)` permet d'en retirer/modifier une clé."""
    def appliquer(**surcharges):
        valeurs = dict(CONFIG_COMPLETE)
        valeurs.update(surcharges)
        for cle, val in valeurs.items():
            if val is None:
                monkeypatch.delenv(cle, raising=False)
                monkeypatch.setattr(cfg, cle, "", raising=False)
            else:
                monkeypatch.setenv(cle, val)
        return valeurs
    return appliquer


def source(**kw):
    base = {"mois": "2026-07", "proprietaire_id": "PROP_DEMO", "logement_id": "LOG_DEMO",
            "source_calcul": "PREF-DEMO", "COMMISSION_CONCIERGERIE": 300.0,
            "MENAGE_FACTURE": 150.0, "CHARGE_FIXE": 50.0, "montant_du_conciergerie": 500.0}
    base.update(kw)
    return base


def _client(type_client, **kw):
    base = {"type_client": type_client, "denomination": "Client Demo",
            "adresse": "2 rue Demo, 00000 Villedemo",
            "adresse_facturation": "2 rue Demo, 00000 Villedemo",
            "siren": "", "tva_intra": "", "numero_bon_commande": ""}
    base.update(kw)
    return base


@pytest.fixture()
def client_particulier(monkeypatch):
    monkeypatch.setattr(conformite, "client",
                        lambda pid, **kw: _client(fconf.CLIENT_PARTICULIER))


@pytest.fixture()
def client_professionnel(monkeypatch):
    monkeypatch.setattr(conformite, "client",
                        lambda pid, **kw: _client(fconf.CLIENT_PROFESSIONNEL,
                                                  denomination="SARL CLIENT DEMO",
                                                  siren="111111111"))


# ── Numérotation ────────────────────────────────────────────────────────────────────────────────

def test_format_numero_facture_et_avoir():
    assert fconf.serie("FACTURE", 2026) == "F-2026"
    assert fconf.serie("AVOIR", 2026) == "A-2026"
    assert fconf.formater_numero("F-2026", 1) == "F-2026-000001"
    assert fconf.formater_numero("A-2026", 1) == "A-2026-000001"


def _emettre(db, fid, date_facture="2026-08-01", repertoire=None, **kw):
    svc.valider(fid, emetteur=EMETTEUR_LEGACY, destinataire=DESTINATAIRE_LEGACY, db_path=db)
    gen = pdf.fabrique(repertoire) if repertoire else None
    return svc.emettre(fid, emetteur=EMETTEUR_LEGACY, destinataire=DESTINATAIRE_LEGACY,
                       date_facture=date_facture, generer_pdf=gen, db_path=db, **kw)


def test_series_facture_et_avoir_independantes(db, config, client_particulier):
    config()
    numeros = []
    for i in range(3):
        f = svc.creer(source(logement_id=f"LOG_{i}"), db_path=db)
        numeros.append(_emettre(db, f["facture_id_opaque"])["numero_facture"])
    assert numeros == ["F-2026-000001", "F-2026-000002", "F-2026-000003"]

    avoir = svc.creer_avoir(svc.lister(db_path=db)[0]["facture_id_opaque"], motif="x", db_path=db)
    num_avoir = _emettre(db, avoir["facture_id_opaque"])["numero_facture"]
    assert num_avoir == "A-2026-000001"        # série indépendante, repart à 1


def test_brouillon_abandonne_ne_consomme_pas_de_numero(db, config, client_particulier):
    """Un brouillon supprimé ne doit pas créer de trou : le numéro n'est pris qu'à l'émission."""
    config()
    f1 = svc.creer(source(logement_id="LOG_A"), db_path=db)
    n1 = _emettre(db, f1["facture_id_opaque"])["numero_facture"]

    abandonne = svc.creer(source(logement_id="LOG_ABANDON"), db_path=db)
    assert abandonne["numero_facture"] is None
    svc.annuler(abandonne["facture_id_opaque"], motif="abandon", db_path=db)

    f2 = svc.creer(source(logement_id="LOG_B"), db_path=db)
    n2 = _emettre(db, f2["facture_id_opaque"])["numero_facture"]
    assert (n1, n2) == ("F-2026-000001", "F-2026-000002")


def test_changement_annee_ouvre_une_nouvelle_serie(db, config, client_particulier):
    config()
    f1 = svc.creer(source(mois="2026-12", logement_id="LOG_X"), db_path=db)
    n2026 = _emettre(db, f1["facture_id_opaque"], date_facture="2026-12-31")["numero_facture"]
    f2 = svc.creer(source(mois="2027-01", logement_id="LOG_X"), db_path=db)
    n2027 = _emettre(db, f2["facture_id_opaque"], date_facture="2027-01-01")["numero_facture"]
    assert n2026 == "F-2026-000001"
    assert n2027 == "F-2027-000001"       # compteur propre à l'année, série lisible dans le numéro


def test_numero_fige_et_jamais_reecrit(db, config, client_particulier):
    config()
    f = svc.creer(source(), db_path=db)
    emise = _emettre(db, f["facture_id_opaque"])
    numero = emise["numero_facture"]
    snap = svc.contenu_emis(emise["facture_id_opaque"], db_path=db)
    assert snap["numero_facture"] == numero
    assert svc.lire(emise["facture_id_opaque"], db_path=db)["numero_facture"] == numero


# ── Contrôle de pré-émission ────────────────────────────────────────────────────────────────────

def test_configuration_complete_facture_prete(db, config, client_particulier):
    config()
    f = svc.creer(source(), db_path=db)
    res = conformite.verifier(f, date_facture="2026-08-01", db_path=db)
    assert res["statut"] == conformite.PRETE, res["manques"]


def test_identite_emetteur_incomplete_bloque(db, config, client_particulier):
    config(SOCIETE_SIREN=None)
    f = svc.creer(source(), db_path=db)
    res = conformite.verifier(f, date_facture="2026-08-01", db_path=db)
    assert res["statut"] == conformite.BLOQUEE
    assert any(m["code"] == conformite.C_IDENTITE_EMETTEUR for m in res["manques"])


def test_regime_tva_non_confirme_bloque(db, config, client_particulier):
    config(FACTURATION_REGIME_TVA=fconf.TVA_A_CONTROLER)
    f = svc.creer(source(), db_path=db)
    res = conformite.verifier(f, date_facture="2026-08-01", db_path=db)
    assert any(m["code"] == conformite.C_REGIME_TVA for m in res["manques"])


def test_franchise_sans_mention_bloque(db, config, client_particulier):
    """Le régime seul ne suffit pas : la mention réglementaire doit être configurée."""
    config(FACTURATION_MENTION_FRANCHISE_TVA=None)
    f = svc.creer(source(), db_path=db)
    res = conformite.verifier(f, date_facture="2026-08-01", db_path=db)
    assert any(m["code"] == conformite.C_MENTION_TVA for m in res["manques"])


def test_type_client_indetermine_bloque(db, config, monkeypatch):
    config()
    monkeypatch.setattr(conformite, "client", lambda pid, **kw: _client(fconf.CLIENT_A_CONTROLER))
    f = svc.creer(source(), db_path=db)
    res = conformite.verifier(f, date_facture="2026-08-01", db_path=db)
    assert any(m["code"] == conformite.C_TYPE_CLIENT for m in res["manques"])


def test_echeance_non_configuree_bloque(db, config, client_particulier):
    config(FACTURATION_DELAI_PAIEMENT_JOURS=None)
    f = svc.creer(source(), db_path=db)
    res = conformite.verifier(f, date_facture="2026-08-01", db_path=db)
    assert any(m["code"] == conformite.C_ECHEANCE for m in res["manques"])


# ── Particulier vs professionnel ────────────────────────────────────────────────────────────────

def test_particulier_sans_clause_b2b(db, config, client_particulier):
    """Un particulier n'exige ni pénalités ni indemnité de recouvrement, même non configurées."""
    config()
    f = svc.creer(source(), db_path=db)
    res = conformite.verifier(f, date_facture="2026-08-01", db_path=db)
    codes = [m["code"] for m in res["manques"]]
    assert conformite.C_PENALITES not in codes
    assert conformite.C_INDEMNITE not in codes
    assert res["statut"] == conformite.PRETE


def test_professionnel_exige_penalites_et_indemnite(db, config, client_professionnel):
    config()
    f = svc.creer(source(), db_path=db)
    res = conformite.verifier(f, date_facture="2026-08-01", db_path=db)
    codes = [m["code"] for m in res["manques"]]
    assert conformite.C_PENALITES in codes
    assert conformite.C_INDEMNITE in codes
    assert res["statut"] == conformite.BLOQUEE


def test_professionnel_complet_est_pret(db, config, client_professionnel):
    config(FACTURATION_TAUX_PENALITES_RETARD="Taux fixture",
           FACTURATION_INDEMNITE_RECOUVREMENT="Indemnite fixture")
    f = svc.creer(source(), db_path=db)
    res = conformite.verifier(f, date_facture="2026-08-01", db_path=db)
    assert res["statut"] == conformite.PRETE, res["manques"]
    assert res["conformite"]["client"]["siren"] == "111111111"


# ── TVA et totaux ───────────────────────────────────────────────────────────────────────────────

def test_franchise_ht_egale_ttc(db, config, client_particulier):
    config()
    f = svc.creer(source(), db_path=db)
    bloc = conformite.construire(f, date_facture="2026-08-01", db_path=db)
    assert bloc["regime_tva"] == fconf.TVA_FRANCHISE
    assert bloc["total_tva"] == 0.0
    assert bloc["total_ht"] == bloc["total_ttc"] == 500.0
    assert bloc["mention_tva"]


def test_assujetti_calcule_la_tva(db, config, client_particulier):
    config(FACTURATION_REGIME_TVA=fconf.TVA_ASSUJETTI, FACTURATION_TAUX_TVA="20")
    f = svc.creer(source(), db_path=db)
    bloc = conformite.construire(f, date_facture="2026-08-01", db_path=db)
    assert bloc["total_ht"] == 500.0
    assert bloc["total_tva"] == 100.0
    assert bloc["total_ttc"] == 600.0


# ── Période de prestation ───────────────────────────────────────────────────────────────────────

def test_periode_prestation_distincte_de_la_date_emission(db, config, client_particulier):
    config()
    f = svc.creer(source(mois="2026-07"), db_path=db)
    bloc = conformite.construire(f, date_facture="2026-08-01", db_path=db)
    assert (bloc["periode_debut"], bloc["periode_fin"]) == ("2026-07-01", "2026-07-31")
    assert bloc["date_echeance"] == "2026-08-31"       # 1er août + 30 jours


def test_periode_fevrier_bissextile():
    assert conformite.periode_prestation("2028-02") == ("2028-02-01", "2028-02-29")


# ── Émission ────────────────────────────────────────────────────────────────────────────────────

def test_emission_refusee_si_conformite_exigee_et_incomplete(db, config, client_professionnel):
    config()      # professionnel sans pénalités ni indemnité
    f = svc.creer(source(), db_path=db)
    svc.valider(f["facture_id_opaque"], emetteur=EMETTEUR_LEGACY, destinataire=DESTINATAIRE_LEGACY,
                db_path=db)
    with pytest.raises(svc.FactureProprietaireError, match=conformite.C_PENALITES):
        svc.emettre(f["facture_id_opaque"], emetteur=EMETTEUR_LEGACY,
                    destinataire=DESTINATAIRE_LEGACY, date_facture="2026-08-01",
                    db_path=db, exiger_conformite=True)
    assert svc.lire(f["facture_id_opaque"], db_path=db)["statut"] == svc.ST_VALIDE


def test_conformite_figee_a_emission(db, config, client_particulier):
    config()
    f = svc.creer(source(), db_path=db)
    emise = _emettre(db, f["facture_id_opaque"])

    enregistre = conformite.charger(emise["facture_id_opaque"], db_path=db)
    assert enregistre["emetteur_siren"] == "000000000"
    assert enregistre["type_client"] == fconf.CLIENT_PARTICULIER
    assert enregistre["regime_tva"] == fconf.TVA_FRANCHISE
    assert enregistre["nature_operation"] == fconf.NATURE_PRESTATION
    assert enregistre["adresse_livraison"] == fconf.ADRESSE_LIVRAISON_NA
    assert enregistre["periode_debut"] == "2026-07-01"
    assert enregistre["total_ht"] == enregistre["total_ttc"] == 500.0


def test_snapshot_contient_la_conformite(db, config, client_particulier):
    config()
    f = svc.creer(source(), db_path=db)
    emise = _emettre(db, f["facture_id_opaque"])
    snap = svc.contenu_emis(emise["facture_id_opaque"], db_path=db)
    assert snap["conformite"]["emetteur"]["siren"] == "000000000"
    assert snap["conformite"]["nature_operation"] == fconf.NATURE_PRESTATION
    assert snap["total_ttc"] == 500.0


def test_identite_client_figee_insensible_aux_changements(db, config, monkeypatch):
    """Modifier le référentiel après émission ne doit pas altérer une facture déjà émise."""
    config()
    monkeypatch.setattr(conformite, "client",
                        lambda pid, **kw: _client(fconf.CLIENT_PARTICULIER,
                                                  denomination="Nom initial"))
    f = svc.creer(source(), db_path=db)
    emise = _emettre(db, f["facture_id_opaque"])

    monkeypatch.setattr(conformite, "client",
                        lambda pid, **kw: _client(fconf.CLIENT_PARTICULIER,
                                                  denomination="Nom modifie apres coup"))
    snap = svc.contenu_emis(emise["facture_id_opaque"], db_path=db)
    assert snap["conformite"]["client"]["denomination"] == "Nom initial"
    assert conformite.charger(emise["facture_id_opaque"],
                              db_path=db)["client_denomination"] == "Nom initial"


# ── Facturation électronique (préparation seulement) ────────────────────────────────────────────

def test_champs_facturation_electronique_neutres(db, config, client_particulier):
    config()
    f = svc.creer(source(), db_path=db)
    emise = _emettre(db, f["facture_id_opaque"])
    enregistre = conformite.charger(emise["facture_id_opaque"], db_path=db)
    assert enregistre["electronic_invoice_status"] == "NON_APPLICABLE"
    assert enregistre["electronic_invoice_provider"] is None
    assert enregistre["electronic_invoice_sent_at"] is None


# ── PDF ─────────────────────────────────────────────────────────────────────────────────────────

def _texte_pdf(chemin):
    import fitz
    with fitz.open(str(chemin)) as d:
        return "\n".join(p.get_text() for p in d)


def test_pdf_particulier(db, config, client_particulier, tmp_path):
    config()
    f = svc.creer(source(), db_path=db)
    emise = _emettre(db, f["facture_id_opaque"], repertoire=tmp_path / "pdf")
    texte = _texte_pdf(tmp_path / "pdf" / "2026" / "07" / emise["document_nom"])

    assert "FACTURE" in texte
    assert "F-2026-000001" in texte
    assert "SAS DEMO CONCIERGERIE" in texte
    assert "SIREN 000000000" in texte
    assert "Periode des prestations : du 01/07/2026 au 31/07/2026" in texte
    assert "TOTAL HT" in texte and "TOTAL TTC" in texte
    assert "Echeance de paiement : 31/08/2026" in texte
    assert "Mention de franchise" in texte
    # Aucune clause professionnelle sur une facture adressée à un particulier.
    assert "Penalites de retard" not in texte
    assert "Indemnite forfaitaire" not in texte


def test_pdf_professionnel(db, config, client_professionnel, tmp_path):
    config(FACTURATION_TAUX_PENALITES_RETARD="Taux fixture",
           FACTURATION_INDEMNITE_RECOUVREMENT="Indemnite fixture")
    f = svc.creer(source(), db_path=db)
    emise = _emettre(db, f["facture_id_opaque"], repertoire=tmp_path / "pdf")
    texte = _texte_pdf(tmp_path / "pdf" / "2026" / "07" / emise["document_nom"])

    assert "SARL CLIENT DEMO" in texte
    assert "SIREN 111111111" in texte
    assert "Penalites de retard : Taux fixture" in texte
    assert "Indemnite forfaitaire de recouvrement : Indemnite fixture" in texte
    assert "PRESTATION_DE_SERVICES" in texte


def test_pdf_avoir_reference_la_facture(db, config, client_particulier, tmp_path):
    config()
    f = svc.creer(source(), db_path=db)
    emise = _emettre(db, f["facture_id_opaque"], repertoire=tmp_path / "pdf")
    avoir = svc.creer_avoir(emise["facture_id_opaque"], motif="correction", db_path=db)
    a_emis = _emettre(db, avoir["facture_id_opaque"], repertoire=tmp_path / "pdf")
    texte = _texte_pdf(tmp_path / "pdf" / "2026" / "07" / a_emis["document_nom"])

    assert "AVOIR" in texte
    assert a_emis["numero_facture"].startswith("A-2026-")
    assert emise["facture_id_opaque"] in texte      # lien vers la facture d'origine


def test_pdf_reste_deterministe(db, config, client_particulier, tmp_path):
    import hashlib
    config()
    f = svc.creer(source(), db_path=db)
    emise = _emettre(db, f["facture_id_opaque"], repertoire=tmp_path / "pdf")
    snap = svc.contenu_emis(emise["facture_id_opaque"], db_path=db)
    a = hashlib.sha256(pdf.rendre(snap)).hexdigest()
    b = hashlib.sha256(pdf.rendre(snap)).hexdigest()
    assert a == b == emise["document_hash"]
