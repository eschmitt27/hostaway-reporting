"""Recette utilisateur n°3 — points finaux fermés à la réconciliation §§0-109.

Chaque test verrouille un écart constaté entre le prompt de recette et le code au checkpoint final :

  §18/§95  le nom `MM-YY-Prestataire[_suffixe].pdf` est lu comme une INDICATION et confronté au
           contenu ; une contradiction est enregistrée, affichée, et signalée au catalogue ;
  §94/§95  un PDF ajouté après un premier passage est importé au passage suivant ; deux factures du
           même prestataire le même mois, distinguées par leur suffixe, coexistent ;
  §34/§35  plus de contrôle « facture validée sans charge » : la facture EST la dépense ;
  §42/§43  PDF propriétaire : plus de tableau détaillé des acomptes, ordre reversement → acompte ;
  §31/§34  liste des factures fournisseurs : VALIDÉE en vert, plus de colonne « Charge » brute ;
  §85      coût standard ménage : le type de logement se CHOISIT ;
  §9/§14   écran Ménages : ni compteurs PDF répétés, ni outil de développeur.

Aucune donnée réelle : bases temporaires, extractions construites, PDF factices.
"""
from __future__ import annotations

import io
import re
import sys
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db

_TRAVAIL = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))
pdfex = pytest.importorskip("lib_menages_externes_pdf")

from app.services import facture_menage_pdf_service as pdf_svc  # noqa: E402
from app.services import factures_controles_service as controles  # noqa: E402
from app.services import factures_service as fact  # noqa: E402
from app.services import menages_pdf_import_service as import_svc  # noqa: E402


def _fac(nom, *, date="2026-08-31", prestataire="Aïssata Diallo", mois_lignes=()):
    fac = pdfex.FactureExtraite(nom, "AISSATA", nom_prestataire=prestataire,
                                numero_facture="2026-40", date_facture=date,
                                periode_facture=(date or "")[:7] or None)
    for m in mois_lignes:
        fac.lignes.append(pdfex.LigneFacture("T3 Bardou", 1, 130.0, 130.0,
                                             date_menage=f"{m}-15"))
    return fac


# ── §18 — la convention de nom, lue comme une indication ────────────────────────────────────────

@pytest.mark.parametrize("nom, attendu", [
    ("08-26-Aissata.pdf", {"mois": "2026-08", "prestataire": "Aissata", "suffixe": ""}),
    ("08-26-Aissata_2026-40.pdf", {"mois": "2026-08", "prestataire": "Aissata",
                                   "suffixe": "2026-40"}),
])
def test_nom_canonique_indique_periode_prestataire_et_suffixe(nom, attendu):
    ind = pdfex.indication_nom_fichier(nom)
    assert ind["conforme"] is True
    assert {k: ind[k] for k in attendu} == attendu


@pytest.mark.parametrize("nom", ["2026-08-Aissata.pdf", "08-2026-Aissata.pdf",
                                 "Facture juillet Aissata.pdf", "13-26-Aissata.pdf"])
def test_nom_hors_convention_n_indique_rien(nom):
    """Année sur 4 chiffres, ordre inversé, nom libre, mois impossible : aucune indication."""
    assert pdfex.indication_nom_fichier(nom) == {"conforme": False}


def test_periode_du_nom_contredite_par_le_document():
    fac = _fac("07-26-Aissata.pdf", date="2026-08-31", mois_lignes=("2026-08",))
    pdfex.controler_nom_fichier(fac)
    assert "NOM_FICHIER_PERIODE_CONTRADICTOIRE:2026-07" in fac.anomalies


def test_facture_de_fin_de_mois_emise_le_mois_suivant_ne_contredit_rien():
    """Ménages d'août facturés le 1er septembre : le nom 08-26 est juste."""
    fac = _fac("08-26-Aissata.pdf", date="2026-09-01", mois_lignes=("2026-08",))
    pdfex.controler_nom_fichier(fac)
    assert fac.anomalies == []


def test_prestataire_du_nom_contredit_par_le_document():
    fac = _fac("08-26-Mounir.pdf", prestataire="Aïssata Diallo")
    pdfex.controler_nom_fichier(fac)
    assert "NOM_FICHIER_PRESTATAIRE_CONTRADICTOIRE:Mounir" in fac.anomalies


def test_comparaison_du_prestataire_insensible_aux_accents_et_a_la_casse():
    fac = _fac("08-26-aissata.pdf", prestataire="AÏSSATA Diallo")
    pdfex.controler_nom_fichier(fac)
    assert fac.anomalies == []


def test_sans_prestataire_lu_rien_n_est_affirme():
    fac = _fac("08-26-Mounir.pdf", prestataire=None)
    pdfex.controler_nom_fichier(fac)
    assert not any("PRESTATAIRE" in a for a in fac.anomalies)


def test_nom_hors_convention_ne_produit_aucune_anomalie():
    fac = _fac("Facture juillet Mounir.pdf", date="2026-08-31")
    pdfex.controler_nom_fichier(fac)
    assert fac.anomalies == []


# ── §18 — la contradiction est enregistrée, affichée, signalée ──────────────────────────────────

@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return p


def _facture(db, ref="2026-40"):
    res = fact.creer({"fournisseur_id_opaque": "FRS-TEST01", "facture_ref": ref,
                      "date_facture": "2026-08-31", "montant_ttc": 120.0},
                     acteur="recette", db_path=db)
    assert res["ok"], res
    return res["facture_id_opaque"]


def _diagnostic(db, facture_id, anomalies):
    conn = get_db(db)
    try:
        conn.execute("INSERT INTO facture_pdf_diagnostics (nom_fichier, statut_extraction, "
                     "anomalies, facture_id_opaque) VALUES (?,?,?,?)",
                     ("07-26-Mounir.pdf", "OK", anomalies, facture_id))
        conn.commit()
    finally:
        conn.close()


def _codes_de(db, facture_ref):
    return [a["code"] for a in controles.controler(db_path=db)["anomalies"]
            if a["identifiant"] == facture_ref]


def test_contradiction_lisible_depuis_le_dernier_diagnostic(db):
    fid = _facture(db)
    _diagnostic(db, fid, "NOM_FICHIER_PERIODE_CONTRADICTOIRE:2026-07,RECONCILIATION_ECART_1.5")
    contradictions = pdf_svc.contradictions_nom_fichier(fid, db_path=db)
    assert [c["code"] for c in contradictions] == ["NOM_FICHIER_PERIODE_CONTRADICTOIRE"]
    assert "2026-07" in contradictions[0]["libelle"]


def test_contradiction_signalee_au_catalogue_tant_que_la_facture_est_a_controler(db):
    fid = _facture(db)
    _diagnostic(db, fid, "NOM_FICHIER_PRESTATAIRE_CONTRADICTOIRE:Mounir")
    assert controles.F_NOM_FICHIER_CONTRADICTOIRE in _codes_de(db, "2026-40")

    assert fact.changer_statut(fid, fact.ST_VALIDEE, acteur="recette", db_path=db)["ok"]
    assert controles.F_NOM_FICHIER_CONTRADICTOIRE not in _codes_de(db, "2026-40")


def test_une_facture_validee_sans_charge_n_est_plus_une_anomalie(db):
    """§34/§35 — « Rattacher une charge » n'existe plus : la facture fournisseur EST la dépense.
    Exiger une charge rattachée produisait un contrôle CRITIQUE que plus rien ne permet de lever."""
    fid = _facture(db)
    assert fact.changer_statut(fid, fact.ST_VALIDEE, acteur="recette", db_path=db)["ok"]
    assert controles.F_VALIDEE_SANS_CHARGE not in _codes_de(db, "2026-40")


# ── §94/§95 — le dossier est relu à chaque passage ──────────────────────────────────────────────

def _faux_import(db):
    compteur = {"n": 0}

    def importer(path, *, acteur="", db_path=None):
        compteur["n"] += 1
        fid = f"FAC-FAUX-{compteur['n']}"
        conn = get_db(db)
        try:
            conn.execute("INSERT INTO facture_pdf_diagnostics (nom_fichier, statut_extraction, "
                         "facture_id_opaque) VALUES (?,?,?)", (Path(path).name, "OK", fid))
            conn.commit()
        finally:
            conn.close()
        return {"ok": True, "facture_id_opaque": fid, "mois_impacte": "2026-08"}
    return importer


def test_un_pdf_ajoute_apres_un_premier_passage_est_importe_au_suivant(tmp_path, db, monkeypatch):
    dossier = tmp_path / "MenagesExternes"
    dossier.mkdir()
    (dossier / "08-26-Aissata.pdf").write_bytes(b"%PDF-1.4 premiere facture")
    monkeypatch.setattr(import_svc.pdf_import, "importer", _faux_import(db))

    premier = import_svc.importer_nouveaux(dossier=dossier, db_path=db)
    assert (premier["nb_detectes"], premier["nb_importees"]) == (1, 1)

    (dossier / "08-26-Mounir.pdf").write_bytes(b"%PDF-1.4 deposee apres le premier passage")
    second = import_svc.importer_nouveaux(dossier=dossier, db_path=db)
    assert (second["nb_detectes"], second["nb_importees"], second["nb_deja_importees"]) == (2, 1, 1)


def test_deux_factures_du_meme_prestataire_le_meme_mois_coexistent(tmp_path, db, monkeypatch):
    dossier = tmp_path / "MenagesExternes"
    dossier.mkdir()
    (dossier / "08-26-Aissata_2026-40.pdf").write_bytes(b"%PDF-1.4 facture 40")
    (dossier / "08-26-Aissata_2026-41.pdf").write_bytes(b"%PDF-1.4 facture 41")
    monkeypatch.setattr(import_svc.pdf_import, "importer", _faux_import(db))

    res = import_svc.importer_nouveaux(dossier=dossier, db_path=db)
    assert (res["nb_detectes"], res["nb_importees"]) == (2, 2)
    assert import_svc.importer_nouveaux(dossier=dossier, db_path=db)["nb_importees"] == 0


# ── §42/§43 — PDF propriétaire ──────────────────────────────────────────────────────────────────

def test_pdf_proprietaire_sans_tableau_des_acomptes_et_dans_l_ordre_definitif():
    pymupdf = pytest.importorskip("pymupdf")
    from app.services import factures_proprietaires_pdf as pdfsvc
    import test_facture_proprietaire_document as ref

    decomposition = dict(ref._snapshot()["decomposition"],
                         montant_du=399.88, net=399.88, sens_net="A_PAYER",
                         total_acomptes=12.0, total_reversements_airbnb=54.0,
                         acomptes=[{"date_mouvement": "2026-08-20", "mode_reglement": "Virement",
                                    "reference_metier": "VIR ACOMPTE", "montant": 12.0,
                                    "mouvement_opaque": "MTP-E00AEA98D2EF"}])
    octets = pdfsvc.rendre(ref._snapshot(decomposition=decomposition))
    doc = pymupdf.open(stream=io.BytesIO(octets), filetype="pdf")
    texte = "\n".join(p.get_text() for p in doc)

    assert "Acomptes déjà versés" not in texte and "Référence du paiement" not in texte
    assert "MTP-" not in texte
    positions = [texte.find(s) for s in ("TOTAL FACTURE", "Reversement Airbnb du mois",
                                         "Acompte(s) déjà versé(s)", "NET À PAYER")]
    assert -1 not in positions and positions == sorted(positions), positions


# ── §31/§34 — liste des factures fournisseurs ───────────────────────────────────────────────────

def test_liste_des_factures_validee_en_vert_et_sans_colonne_charge():
    source = (Path(cfg.APP_ROOT) / "app" / "templates" / "factures_list.html").read_text(
        encoding="utf-8")
    assert "'valide' if f.statut in ['REGLEE','VALIDEE','PARTIELLEMENT_REGLEE']" in source
    assert "<th>Charge</th>" not in source and "f.charge_id" not in source


# ── §85 — coût standard ménage : le type se choisit ─────────────────────────────────────────────

def test_changer_le_cout_menage_propose_les_types_de_logement(client, tmp_db):
    import fixtures_referentiel as fxr
    fxr.semer_parc_standard(tmp_db)
    texte = client.get("/administration/referentiels/ref_couts_standards_menage").text
    formulaire = texte[texte.find("changer-cout"):]
    assert re.search(r'<select[^>]*name="type_logement_id"', formulaire)
    assert not re.search(r'<input[^>]*name="type_logement_id"', formulaire)


# ── §9/§14 — écran Ménages ──────────────────────────────────────────────────────────────────────

def test_ecran_menages_sans_outil_de_developpeur_ni_compteurs_repetes(client, tmp_db):
    texte = client.get("/menages").text
    for interdit in ("Diagnostic du pipeline", "Désactivé à ce lot", "Factures extraites (MASTER)",
                     "PDF en doublon", "PDF non supportés", 'href="/menages/diagnostic"'):
        assert interdit not in texte, interdit
    assert "/menages/diagnostic" in client.get("/observabilite/runs").text
