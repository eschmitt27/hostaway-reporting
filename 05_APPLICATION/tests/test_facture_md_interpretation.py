"""Le MD structuré prend la main sur le parseur PDF — contrat FACTURE_FOURNISSEUR_MD_V1.

RÈGLE CENTRALE : les lignes d'une facture viennent d'UNE source, jamais d'un mélange. Un MD valide
posé à côté du PDF fait foi ; retiré, invalide ou écrit pour une autre version du PDF, il est
écarté et le parseur reprend. Une facture VALIDÉE, elle, ne change jamais de lecture.

Les cas A→K de la commande sont couverts ici, chacun nommé.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import sys
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import facture_interpretation_service as interpretation
from app.services import facture_lignes_menage_service as flm
from app.services import facture_md_service as md_svc
from app.services import factures_service as fact
from app.services import menages_pdf_import_service as pdf_import
from app.services import referentiel_logements_export_service as ref_export

pytest.importorskip("fitz")
_TRAVAIL = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))

AOUT = cfg.MENAGES_PDF_DIR / "08-26-Aissata.pdf"
IMRANE = cfg.MENAGES_PDF_DIR / "02-26-Imrane.pdf"
reels = pytest.mark.skipif(not (AOUT.exists() and IMRANE.exists()),
                           reason="PDF réels absents d'un checkout propre")


@pytest.fixture(autouse=True)
def _ecriture_activee(monkeypatch):
    for drapeau in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                    "ECRITURE_OPERATIONNELLE_ENABLED"):
        monkeypatch.setattr(cfg, drapeau, True, raising=False)


@pytest.fixture()
def base(tmp_path):
    db = tmp_path / "app.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        for tid, libelle, compte in (("TLM_001", "MENAGE_STANDARD", "OUI"),
                                     ("TLM_002", "REMISE_EN_ETAT", "OUI"),
                                     ("TLM_006", "AUTRE", "NON")):
            conn.execute(
                "INSERT OR IGNORE INTO ref_types_lignes_menage (type_ligne_menage_id, "
                "type_ligne_menage, compte_comme_menage, repartissable_sur_menages, "
                "impact_cout_menage, actif, import_id) VALUES (?,?,?,'NON','OUI','OUI','IMP-T')",
                (tid, libelle, compte))
        for lid, nom in (("LOG_0012", "Studio - Côte Pavée"), ("LOG_0007", "T2 - 09")):
            conn.execute(
                "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, "
                "adresse, ville, actif, statut_parc, import_id) VALUES (?,?,?,?,?,?,?,?)",
                (lid, nom, nom, "1 rue Test", "TOULOUSE", "OUI", "GERE", "IMP-T"))
        conn.commit()
    finally:
        conn.close()
    return db


@pytest.fixture()
def dossier(tmp_path):
    d = tmp_path / "depot"
    d.mkdir()
    return d


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _md(pdf: Path, *, lignes=None, pdf_sha=None, reference="2026-41", total=2790.00,
        referentiel_version=None, entete=True) -> str:
    """Un MD conforme au contrat, paramétrable pour éprouver chaque règle."""
    contenu = {
        "schema": "FACTURE_FOURNISSEUR_MD_V1",
        "source": {"pdf_filename": pdf.name, "pdf_sha256": pdf_sha or _sha(pdf),
                   "referentiel_version": referentiel_version},
        "facture": {"fournisseur": "Aissata", "reference_fournisseur": reference,
                    "date_facture": "2026-08-31", "mois_concerne": "2026-08", "devise": "EUR",
                    "total_ttc": total, "total_document": total},
        "lignes": lignes if lignes is not None else [
            {"numero": 1, "page": 1, "libelle_source": "ménage studio côte pavée",
             "nature": "MENAGE", "dates_prestation": ["2026-08-06"], "quantite_source": 8,
             "prix_unitaire_source": 29.00, "montant_source": 232.00, "montant_calcule": 232.00,
             "logements": [{"logement_id": "LOG_0012", "confiance": "CERTAIN"}], "anomalies": []},
            {"numero": 2, "page": 1, "libelle_source": "solde de juillet", "nature": "A_CLASSER",
             "dates_prestation": [], "quantite_source": 1, "prix_unitaire_source": total - 232.00,
             "montant_source": total - 232.00, "logements": [], "anomalies": []},
        ],
    }
    tete = "# FACTURE_FOURNISSEUR_MD_V1\n\n" if entete else "# AUTRE CHOSE\n\n"
    return tete + "```json\n" + json.dumps(contenu, ensure_ascii=False, indent=2) + "\n```\n"


def _lignes_actives(db, opaque):
    return [l for l in flm.lignes(opaque, db_path=db)
            if str(l.get("statut_ligne") or "ACTIVE") == "ACTIVE"]


def _facture(db):
    return fact.lister(db_path=db)[0]


# ── A — PDF seul ────────────────────────────────────────────────────────────────────────────────

@reels
def test_A_pdf_seul_source_pdf(base, dossier):
    shutil.copy2(AOUT, dossier / AOUT.name)
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)

    opaque = _facture(base)["facture_id_opaque"]
    etat = interpretation.etat(opaque, db_path=base)
    assert etat["source_interpretation"] == interpretation.SOURCE_PDF
    assert etat["md_etat"] == md_svc.ETAT_ABSENT
    assert len(_lignes_actives(base, opaque)) == 15, "les 15 lignes du parseur"


# ── B — PDF + MD valide dès le premier import ───────────────────────────────────────────────────

@reels
def test_B_md_valide_des_le_premier_import(base, dossier):
    shutil.copy2(AOUT, dossier / AOUT.name)
    (dossier / "08-26-Aissata.md").write_text(_md(dossier / AOUT.name), encoding="utf-8")
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)

    opaque = _facture(base)["facture_id_opaque"]
    etat = interpretation.etat(opaque, db_path=base)
    assert etat["source_interpretation"] == interpretation.SOURCE_MD
    assert etat["md_nom_fichier"] == "08-26-Aissata.md"
    lignes = _lignes_actives(base, opaque)
    assert len(lignes) == 2, "les lignes viennent du MD, pas du parseur"
    assert all(l["source"] == interpretation.SOURCE_LIGNE_MD for l in lignes)
    assert {l["logement_id"] for l in lignes} == {"LOG_0012", None}


# ── C — MD ajouté APRÈS le parsing PDF ──────────────────────────────────────────────────────────

@reels
def test_C_md_ajoute_apres_coup_remplace_les_lignes_pdf(base, dossier):
    shutil.copy2(AOUT, dossier / AOUT.name)
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    opaque = _facture(base)["facture_id_opaque"]
    assert len(_lignes_actives(base, opaque)) == 15

    (dossier / "08-26-Aissata.md").write_text(_md(dossier / AOUT.name), encoding="utf-8")
    res = pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)

    assert [c["source"] for c in res["interpretations_changees"]] == [interpretation.SOURCE_MD]
    etat = interpretation.etat(opaque, db_path=base)
    assert etat["source_interpretation"] == interpretation.SOURCE_MD
    assert [v["version"] for v in etat["versions"]] == [1, 2]
    assert [v["active"] for v in etat["versions"]] == [0, 1]

    actives = _lignes_actives(base, opaque)
    assert len(actives) == 2 and all(l["source"] == interpretation.SOURCE_LIGNE_MD for l in actives)
    anciennes = [l for l in flm.lignes(opaque, db_path=base)
                 if l["statut_ligne"] == interpretation.STATUT_LIGNE_REMPLACEE]
    assert len(anciennes) == 15, "les lignes PDF restent lisibles, mais ne comptent plus"
    # Aucun doublon : la somme canonique ne compte que l'interprétation active.
    assert flm.somme_lignes_effectives(opaque, db_path=base) == pytest.approx(2790.0)
    assert fact.lister(db_path=base)[0]["ecart_lignes"] == pytest.approx(0.0)


@reels
def test_C_bis_le_rapprochement_menages_suit_le_changement_de_source(base, dossier):
    """Ménages ne lit jamais le MD : il lit les lignes canoniques, qui viennent de changer."""
    shutil.copy2(AOUT, dossier / AOUT.name)
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    opaque = _facture(base)["facture_id_opaque"]
    # Le lecteur Ménages ne voit que les lignes RATTACHÉES à un logement. Ici le parseur n'en a
    # rattaché aucune : les libellés du document ne désignent aucun logement de ce référentiel.
    assert flm.lignes_externes_pour_reader(db_path=base) == []

    (dossier / "08-26-Aissata.md").write_text(_md(dossier / AOUT.name), encoding="utf-8")
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)

    # Le MD, lui, NOMME le logement : le rapprochement change — sans que Ménages ait lu le MD,
    # il ne lit que les lignes canoniques de la facture.
    apres = flm.lignes_externes_pour_reader(db_path=base)
    assert [l["montant_ligne_ttc"] for l in apres] == [232.0]
    assert all(l["logement_id"] == "LOG_0012" for l in apres)


# ── D — MD retiré, facture à contrôler ──────────────────────────────────────────────────────────

@reels
def test_D_md_retire_retour_au_parseur_pdf(base, dossier):
    shutil.copy2(AOUT, dossier / AOUT.name)
    md = dossier / "08-26-Aissata.md"
    md.write_text(_md(dossier / AOUT.name), encoding="utf-8")
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    opaque = _facture(base)["facture_id_opaque"]
    assert interpretation.etat(opaque, db_path=base)["source_interpretation"] == "MD"

    md.unlink()
    res = pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)

    assert [c["source"] for c in res["interpretations_changees"]] == [interpretation.SOURCE_PDF]
    etat = interpretation.etat(opaque, db_path=base)
    assert etat["source_interpretation"] == interpretation.SOURCE_PDF
    assert etat["md_etat"] == md_svc.ETAT_ABSENT
    actives = _lignes_actives(base, opaque)
    assert len(actives) == 15 and all(l["source"] == flm.SOURCE_PDF for l in actives)
    assert flm.somme_lignes_effectives(opaque, db_path=base) == pytest.approx(2790.0)


# ── E / F / G — MD écarté : invalide, obsolète, logement inconnu ────────────────────────────────

@reels
@pytest.mark.parametrize("fabrique,etat_attendu", [
    (lambda pdf: "# FACTURE_FOURNISSEUR_MD_V1\n\n```json\n{ pas du json\n```\n", md_svc.ETAT_INVALIDE),
    (lambda pdf: _md(pdf, entete=False), md_svc.ETAT_INVALIDE),
    (lambda pdf: _md(pdf, lignes=[{"libelle_source": "x", "nature": "TONDRE_LA_PELOUSE",
                                   "montant_source": 10.0}]), md_svc.ETAT_INVALIDE),
    (lambda pdf: _md(pdf, pdf_sha="0" * 64), md_svc.ETAT_OBSOLETE),
], ids=["json_illisible", "entete_absente", "nature_hors_contrat", "hash_pdf_different"])
def test_EF_md_ecarte_le_parseur_garde_la_main(base, dossier, fabrique, etat_attendu):
    shutil.copy2(AOUT, dossier / AOUT.name)
    (dossier / "08-26-Aissata.md").write_text(fabrique(dossier / AOUT.name), encoding="utf-8")
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)

    opaque = _facture(base)["facture_id_opaque"]
    etat = interpretation.etat(opaque, db_path=base)
    assert etat["md_etat"] == etat_attendu
    assert etat["source_interpretation"] == interpretation.SOURCE_PDF, "jamais activé en silence"
    assert len(_lignes_actives(base, opaque)) == 15


@reels
def test_G_logement_inconnu_ligne_non_affectee_et_facture_a_controler(base, dossier):
    shutil.copy2(AOUT, dossier / AOUT.name)
    lignes = [{"numero": 1, "page": 1, "libelle_source": "ménage", "nature": "MENAGE",
               "quantite_source": 1, "prix_unitaire_source": 2790.00, "montant_source": 2790.00,
               "logements": [{"logement_id": "LOG_9999", "confiance": "CERTAIN"}],
               "anomalies": []}]
    (dossier / "08-26-Aissata.md").write_text(_md(dossier / AOUT.name, lignes=lignes),
                                              encoding="utf-8")
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)

    opaque = _facture(base)["facture_id_opaque"]
    analyse = md_svc.analyser(dossier / AOUT.name, db_path=base)
    assert analyse["etat"] == md_svc.ETAT_VALIDE
    assert analyse["logements_inconnus"] == ["LOG_9999"]
    ligne = _lignes_actives(base, opaque)[0]
    assert ligne["logement_id"] is None, "aucun logement n'est créé depuis un MD"
    assert "LOGEMENT_INCONNU" in (ligne["commentaire"] or "")
    assert fact.charger(opaque, db_path=base)["statut"] == fact.ST_A_CONTROLER


# ── H — facture validée : figée ─────────────────────────────────────────────────────────────────

@reels
def test_H_facture_validee_ignore_un_md_ajoute(base, dossier):
    shutil.copy2(AOUT, dossier / AOUT.name)
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    opaque = _facture(base)["facture_id_opaque"]
    conn = get_db(base)
    try:
        conn.execute("UPDATE factures SET statut = ? WHERE facture_id_opaque = ?",
                     (fact.ST_VALIDEE, opaque))
        conn.commit()
    finally:
        conn.close()

    (dossier / "08-26-Aissata.md").write_text(_md(dossier / AOUT.name), encoding="utf-8")
    res = pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)

    assert res["interpretations_changees"] == []
    etat = interpretation.etat(opaque, db_path=base)
    assert etat["source_interpretation"] == interpretation.SOURCE_PDF
    assert etat["figee"] is True
    assert len(_lignes_actives(base, opaque)) == 15, "lignes inchangées"
    # L'application refuse aussi l'application directe, pas seulement via le rechargement.
    refus = interpretation.appliquer(opaque, source=interpretation.SOURCE_MD, lignes=[],
                                     db_path=base)
    assert refus["ok"] is False and refus["code"] == interpretation.E_FACTURE_FIGEE


# ── I — Imrane : le MD transcrit, il ne corrige pas ─────────────────────────────────────────────

@reels
def test_I_incoherence_du_document_transcrite_sans_correction(base, dossier):
    shutil.copy2(IMRANE, dossier / IMRANE.name)
    lignes = [
        {"numero": 1, "libelle_source": "Nettoyage T1/ 1lits", "nature": "MENAGE",
         "quantite_source": 5, "prix_unitaire_source": 29.00, "montant_source": 145.00,
         "montant_calcule": 145.00, "logements": [], "anomalies": []},
        {"numero": 2, "libelle_source": "Nettoyage T1/2 lits", "nature": "MENAGE",
         "quantite_source": 2, "prix_unitaire_source": 30.00, "montant_source": 30.00,
         "montant_calcule": 60.00, "logements": [],
         "anomalies": ["INCOHERENCE_ARITHMETIQUE_DOCUMENT"]},
    ]
    (dossier / "02-26-Imrane.md").write_text(
        _md(dossier / IMRANE.name, lignes=lignes, reference="2025-016", total=205.00),
        encoding="utf-8")
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)

    opaque = _facture(base)["facture_id_opaque"]
    montants = sorted(l["montant_ttc"] for l in _lignes_actives(base, opaque))
    assert montants == [30.0, 145.0], "le montant IMPRIMÉ reste la donnée source"
    ligne = next(l for l in _lignes_actives(base, opaque) if l["montant_ttc"] == 30.0)
    assert "INCOHERENCE_ARITHMETIQUE_DOCUMENT" in (ligne["commentaire"] or "")
    # L'application recalcule elle-même : 175 € de lignes contre 205 € au document.
    assert flm.somme_lignes_effectives(opaque, db_path=base) == pytest.approx(175.0)
    assert flm.controler_total(opaque, db_path=base)["ecart"] == pytest.approx(30.0)
    assert fact.charger(opaque, db_path=base)["statut"] == fact.ST_A_CONTROLER


# ── J — Aissata août : 15 lignes, 2 790 €, écart 0 ──────────────────────────────────────────────

@reels
def test_J_md_complet_aout_quinze_lignes_2790_ecart_nul(base, dossier):
    shutil.copy2(AOUT, dossier / AOUT.name)
    montants = [232.0, 495.0, 145.0, 220.0, 55.0, 110.0, 552.0, 550.0, 29.0, 29.0, 49.0, 150.0,
                39.0, 85.0, 50.0]
    natures = ["MENAGE"] * 10 + ["REMISE_EN_ETAT", "REMISE_EN_ETAT", "MENAGE",
                                 "AUTRE_PRESTATION", "A_CLASSER"]
    lignes = [{"numero": i, "page": 1 if i <= 12 else 2,
               "libelle_source": f"ligne {i} du document", "nature": n,
               "quantite_source": 1, "prix_unitaire_source": m, "montant_source": m,
               "montant_calcule": m,
               "logements": [{"logement_id": "LOG_0012", "confiance": "CERTAIN"}]
               if n in ("MENAGE", "REMISE_EN_ETAT") else [], "anomalies": []}
              for i, (m, n) in enumerate(zip(montants, natures), start=1)]
    (dossier / "08-26-Aissata.md").write_text(_md(dossier / AOUT.name, lignes=lignes),
                                              encoding="utf-8")
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)

    opaque = _facture(base)["facture_id_opaque"]
    actives = _lignes_actives(base, opaque)
    assert len(actives) == 15
    assert sum(l["montant_ttc"] for l in actives) == pytest.approx(2790.0)
    facture = fact.lister(db_path=base)[0]
    assert facture["montant_ttc"] == pytest.approx(2790.0)
    assert facture["montant_lignes_ttc"] == pytest.approx(2790.0)
    assert facture["ecart_lignes"] == pytest.approx(0.0)
    menages = flm.lignes_externes_pour_reader(db_path=base)
    assert len(menages) == 13,         "Ménages lit les lignes canoniques du MD : les 13 rattachées à un logement"
    assert sum(l["montant_ligne_ttc"] for l in menages) == pytest.approx(2790.0 - 85.0 - 50.0)


# ── K — export du référentiel logements ─────────────────────────────────────────────────────────

def test_K_export_referentiel_stable_et_versionne(base):
    premier = ref_export.construire(db_path=base)
    second = ref_export.construire(db_path=base)
    assert premier["schema"] == "REFERENTIEL_LOGEMENTS_V1"
    assert premier["referentiel_version"] == second["referentiel_version"], \
        "mêmes données, même version — l'empreinte ne dépend pas de l'instant"
    assert {l["logement_id"] for l in premier["logements"]} == {"LOG_0012", "LOG_0007"}
    assert all("telephone" not in l and "email" not in l for l in premier["logements"])

    conn = get_db(base)
    try:
        conn.execute("INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, "
                     "adresse, ville, actif, statut_parc, import_id) VALUES "
                     "('LOG_0099','Nouveau','Nouveau','2 rue Neuve','TOULOUSE','OUI','GERE','T')")
        conn.commit()
    finally:
        conn.close()
    assert ref_export.version(db_path=base) != premier["referentiel_version"], \
        "référentiel modifié, version différente"


def test_K_bis_l_export_se_telecharge_depuis_l_ecran(client, tmp_db):
    page = client.get("/factures").text
    assert 'data-testid="export-referentiel-logements"' in page
    reponse = client.get("/factures/referentiel-logements.json")
    assert reponse.status_code == 200
    assert "referentiel_logements.json" in reponse.headers["content-disposition"]
    charge = json.loads(reponse.text)
    assert charge["schema"] == "REFERENTIEL_LOGEMENTS_V1" and "referentiel_version" in charge


# ── MD orphelin et référentiel ancien ───────────────────────────────────────────────────────────

def test_md_sans_pdf_est_orphelin(base, dossier):
    (dossier / "facture-fantome.md").write_text("# FACTURE_FOURNISSEUR_MD_V1\n\n```json\n{}\n```\n",
                                                encoding="utf-8")
    res = pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    assert res["md_orphelins"] == ["facture-fantome.md"]
    assert fact.lister(db_path=base) == [], "un MD seul ne crée jamais de facture"


@reels
def test_referentiel_ancien_est_signale_sans_bloquer(base, dossier):
    shutil.copy2(AOUT, dossier / AOUT.name)
    (dossier / "08-26-Aissata.md").write_text(
        _md(dossier / AOUT.name, referentiel_version="a" * 64), encoding="utf-8")
    analyse = md_svc.analyser(dossier / AOUT.name, db_path=base)
    assert analyse["etat"] == md_svc.ETAT_VALIDE
    assert md_svc.A_REFERENTIEL_ANCIEN in analyse["anomalies"]


# ── Archivage préparé mais inactif ──────────────────────────────────────────────────────────────

def test_archivage_des_pieces_prepare_mais_desactive():
    from app.services import pieces_archivage_service as archivage

    assert archivage.actif() is False
    etat = archivage.etat()
    assert etat["actif"] is False and etat["destination_creee"] is False
    res = archivage.archiver_a_la_validation("FAC-TEST")
    assert res["ok"] is False and res["code"] == archivage.E_DESACTIVE
    assert not Path(cfg.ARCHIVAGE_PIECES_DIR).exists(), "rien n'est créé tant que c'est désactivé"


def test_le_contrat_livre_avec_l_application_valide_son_propre_exemple():
    exemple = md_svc.CHEMIN_SCHEMA.with_name("FACTURE_FOURNISSEUR_MD_V1.example.md")
    donnees, erreurs = md_svc.lire(exemple)
    assert erreurs == [] and donnees is not None
    assert md_svc.valider_contrat(donnees) == []


@reels
def test_un_rechargement_sans_changement_ne_cree_aucune_version(base, dossier):
    """Idempotence de la lecture : si ni le PDF ni le MD n'ont bougé, rien n'est réinterprété."""
    shutil.copy2(AOUT, dossier / AOUT.name)
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    opaque = _facture(base)["facture_id_opaque"]
    versions_initiales = interpretation.etat(opaque, db_path=base)["versions"]

    for _ in range(3):
        res = pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
        assert res["interpretations_changees"] == []
    assert interpretation.etat(opaque, db_path=base)["versions"] == versions_initiales

    # Même chose une fois le MD en place : un dossier stable ne produit plus de version.
    (dossier / "08-26-Aissata.md").write_text(_md(dossier / AOUT.name), encoding="utf-8")
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    apres_bascule = interpretation.etat(opaque, db_path=base)["versions"]
    assert len(apres_bascule) == len(versions_initiales) + 1

    for _ in range(3):
        res = pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
        assert res["interpretations_changees"] == []
    etat = interpretation.etat(opaque, db_path=base)
    assert etat["versions"] == apres_bascule
    assert len(_lignes_actives(base, opaque)) == 2, "aucune ligne dupliquée par les rechargements"


@reels
def test_une_facture_importee_porte_toujours_sa_source(base, dossier):
    shutil.copy2(AOUT, dossier / AOUT.name)
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    conn = get_db(base)
    try:
        sources = [r[0] for r in conn.execute("SELECT source_interpretation FROM factures")]
    finally:
        conn.close()
    assert sources == [interpretation.SOURCE_PDF], "jamais NULL après un import"
