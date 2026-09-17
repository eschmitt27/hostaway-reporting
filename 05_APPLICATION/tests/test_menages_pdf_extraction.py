"""Extraction PDF des factures de ménage externes (Lot6c) — tests sur COPIES.

Couvre les 24 points de la recette : extraction Aissata/Mounir, detection fournisseur,
numero/date/montant, lignes/quantite, normalisation des libelles, dedoublonnage
(PDF duplique, meme facture sous autre nom), formats non supporte/vide/corrompu, champ
absent, reconciliation (coherent / ecart), integration lot6c + chaine, et intangibilite
du reel (PDF, MASTER, app.db).

Aucun PDF reel n'est modifie (copies uniquement). fitz (PyMuPDF) requis.
"""
import hashlib
import shutil
import sys
from pathlib import Path

import pytest

import app.config as cfg

fitz = pytest.importorskip("fitz")

# Module moteur d'extraction (hors app/, sous 02_TRAVAIL du worktree courant).
_TRAVAIL = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))
pdfex = pytest.importorskip("lib_menages_externes_pdf")

REAL_PDF_DIR = cfg.MENAGES_PDF_DIR
# Les PDF réels sont indexés sur la CONVENTION de nommage (MM-YY-Prestataire), et non plus sur un
# nom littéral : les fichiers ont été renommés une fois, et les tests qui en dépendaient se sont
# alors mis à se skipper en silence — l'extraction réelle n'était plus couverte du tout.
AISSATA = REAL_PDF_DIR / "05-26-Aissata.pdf"
MOUNIR = REAL_PDF_DIR / "05-26-Mounir.pdf"
pdf_reels = pytest.mark.skipif(not (AISSATA.exists() and MOUNIR.exists()),
                               reason="PDF réels absents d'un checkout propre")


# ── Fixtures : copies des PDF réels + PDF fabriqués ──────────────────────────

@pytest.fixture
def aissata(tmp_path):
    dst = tmp_path / AISSATA.name
    shutil.copy2(AISSATA, dst)
    return dst


@pytest.fixture
def mounir(tmp_path):
    dst = tmp_path / MOUNIR.name
    shutil.copy2(MOUNIR, dst)
    return dst


@pytest.fixture
def pdf_non_supporte(tmp_path):
    p = tmp_path / "facture_inconnue.pdf"
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), "Facture ACME Corp\nPrestation diverse\nTotal 100 EUR")
    doc.save(p)
    doc.close()
    return p


@pytest.fixture
def pdf_vide(tmp_path):
    p = tmp_path / "vide.pdf"
    doc = fitz.open()
    doc.new_page()
    doc.save(p)
    doc.close()
    return p


@pytest.fixture
def pdf_corrompu(tmp_path):
    p = tmp_path / "corrompu.pdf"
    p.write_bytes(b"%PDF-1.4\nCECI N'EST PAS UN PDF VALIDE\x00\xff")
    return p


# ── 1-8 : extraction Aissata & Mounir ────────────────────────────────────────

@pdf_reels
def test_01_aissata_extrait(aissata):
    fac = pdfex.extraire_pdf(aissata)
    assert fac.statut_extraction == "OK"
    assert len(fac.lignes) == 8


@pdf_reels
def test_02_mounir_extrait(mounir):
    fac = pdfex.extraire_pdf(mounir)
    assert fac.statut_extraction == "OK"
    assert len(fac.lignes) == 4


@pdf_reels
def test_03_fournisseur_detecte(aissata, mounir):
    assert pdfex.extraire_pdf(aissata).format_detecte == "AISSATA"
    assert pdfex.extraire_pdf(mounir).format_detecte == "MOUNIR"
    assert pdfex.extraire_pdf(aissata).prestataire_id == "INT_0004"
    assert pdfex.extraire_pdf(mounir).prestataire_id == "INT_0003"


@pdf_reels
def test_04_numero_facture(aissata, mounir):
    assert pdfex.extraire_pdf(aissata).numero_facture == "2026-37"
    assert pdfex.extraire_pdf(mounir).numero_facture == "0003"


@pdf_reels
def test_05_date_facture(aissata, mounir):
    assert pdfex.extraire_pdf(aissata).date_facture == "2026-05-31"
    assert pdfex.extraire_pdf(mounir).date_facture == "2026-05-31"


@pdf_reels
def test_06_montant_total(aissata, mounir):
    assert pdfex.extraire_pdf(aissata).montant_total_facture == 1439.0
    assert pdfex.extraire_pdf(mounir).montant_total_facture == 942.0


@pdf_reels
def test_07_lignes_libelle_logement(aissata):
    """L'extracteur rend le LIBELLÉ du document, pas un identifiant de notre parc (§23).

    Contrat modifié en recette utilisateur n°3 : décider quel logement un libellé désigne dépend du
    référentiel, qui vit ; l'extraction, elle, doit rester une lecture hors-ligne du document. Le
    rapprochement est vérifié à sa place, dans `test_logement_matching`.
    """
    fac = pdfex.extraire_pdf(aissata)
    libelles = {pdfex.normaliser_libelle(l.logement_source) for l in fac.lignes}
    # Libellés tels que les porte la facture réelle de mai : le prénom entre parenthèses fait
    # partie de la désignation du logement et aide le rapprochement, seule l'adresse-entre-
    # parenthèses est retirée. La nature de la prestation, elle, n'est plus dans le libellé.
    assert {"studio 76 (dureuil)", "t3 4 rue engalieres"} <= libelles
    assert all(l.logement_id is None for l in fac.lignes), \
        "l'extracteur ne doit plus rapprocher de logement lui-même"


@pdf_reels
def test_08_quantite(mounir):
    fac = pdfex.extraire_pdf(mounir)
    q = {pdfex.normaliser_libelle(l.logement_source): l.quantite for l in fac.lignes}
    assert q["t.4-90 blagnac (cedrine)"] == 6 and q["t.3 sept deniers (francois)"] == 10
    # Un appartement sans ménage du mois est facturé 0 : c'est une information du document, pas une
    # ligne à faire disparaître.
    assert q["t.2-65 (gabriel)"] == 0 and q["studio puits vert (caroline)"] == 1


# ── 9-10 : l'extraction ne rapproche plus, elle normalise ────────────────────

def test_09_libelle_normalise_stable():
    """La normalisation reste ici : c'est une propriété du TEXTE, pas du référentiel."""
    assert pdfex.normaliser_libelle("studio 76 (Dureuil)") == \
        pdfex.normaliser_libelle("Studio  76  (DUREUIL)")


def test_10_extracteur_sans_table_de_correspondance():
    """Garde : plus aucun identifiant de logement codé en dur dans l'extracteur.

    C'est la régression à empêcher — réintroduire seize libellés recopiés à la main condamnerait à
    nouveau le rapprochement à ignorer tout logement entré depuis.
    """
    assert not hasattr(pdfex, "MAPPING_LOGEMENTS")
    assert not hasattr(pdfex, "mapper_logement")
    source = Path(pdfex.__file__).read_text(encoding="utf-8")
    code = "\n".join(l for l in source.splitlines() if not l.lstrip().startswith("#"))
    assert "LOG_00" not in code


# ── 11-12 : dedoublonnage ────────────────────────────────────────────────────

@pdf_reels
def test_11_pdf_duplique_meme_empreinte(aissata, tmp_path):
    copie = tmp_path / "copie.pdf"
    shutil.copy2(aissata, copie)
    assert pdfex.empreinte_facture(pdfex.extraire_pdf(aissata)) == \
           pdfex.empreinte_facture(pdfex.extraire_pdf(copie))


@pdf_reels
def test_12_meme_facture_autre_nom(aissata, tmp_path):
    autre = tmp_path / "renomme_2026.pdf"
    shutil.copy2(aissata, autre)
    f1, f2 = pdfex.extraire_pdf(aissata), pdfex.extraire_pdf(autre)
    # noms differents mais meme SHA256, meme numero, meme empreinte -> doublon detectable
    assert f1.sha256_pdf == f2.sha256_pdf
    assert pdfex.empreinte_facture(f1) == pdfex.empreinte_facture(f2)


# ── 13-15 : formats non supporte / vide / corrompu ───────────────────────────

def test_13_pdf_non_supporte(pdf_non_supporte):
    fac = pdfex.extraire_pdf(pdf_non_supporte)
    assert fac.statut_extraction == "NON_SUPPORTE"
    assert fac.lignes == []


def test_14_pdf_vide(pdf_vide):
    fac = pdfex.extraire_pdf(pdf_vide)
    assert fac.statut_extraction == "VIDE"
    assert "OCR" in " ".join(fac.anomalies) or "TEXTE" in " ".join(fac.anomalies)


def test_15_pdf_corrompu(pdf_corrompu):
    fac = pdfex.extraire_pdf(pdf_corrompu)
    assert fac.statut_extraction in ("CORROMPU", "VIDE")


# ── 16 : champ obligatoire absent (aucune invention) ─────────────────────────

def test_16_champ_absent_aucune_invention(pdf_non_supporte):
    fac = pdfex.extraire_pdf(pdf_non_supporte)
    assert fac.numero_facture is None
    assert fac.montant_total_facture is None
    assert fac.date_facture is None


# ── 17-18 : reconciliation ───────────────────────────────────────────────────

@pdf_reels
def test_17_total_lignes_coherent(aissata, mounir):
    for pdf in (aissata, mounir):
        fac = pdfex.extraire_pdf(pdf)
        somme = sum(l.montant_ligne or 0 for l in fac.lignes)
        assert abs(somme - fac.montant_total_facture) <= 1.0
        assert fac.ecart_reconciliation == 0.0


@pdf_reels
def test_18_ecart_total_signale(aissata, monkeypatch):
    """Un total facture fausse (via patch) doit lever l'anomalie de reconciliation."""
    orig = pdfex._controle_reconciliation

    def faux(fac):
        fac.montant_total_facture = 9999.0
        orig(fac)
    monkeypatch.setattr(pdfex, "_controle_reconciliation", faux)
    fac = pdfex.extraire_pdf(aissata)
    assert any("RECONCILIATION_ECART" in a for a in fac.anomalies)


# ── 19-24 : integration lot6c + chaine + intangibilite du reel ───────────────

@pdf_reels
def test_19_lot6c_genere_master_sur_copies(tmp_path):
    """lot6c en mode PDF sur copies produit un MASTER lisible ; le reel ne bouge pas."""
    engine = Path(cfg.MENAGES_ENGINE_PYTHON)
    if not engine.exists():
        pytest.skip("Interpréteur moteur absent")
    import subprocess
    ws = tmp_path / "ws"
    for rel in ["01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm",
                "02_TRAVAIL/Lot1_Hostaway/MASTER_FACT_HA_CleaningTasks_Discovery.xlsx"]:
        d = ws / rel
        d.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(Path(cfg.PROJECT_ROOT) / rel, d)
    pdfd = ws / "01_SOURCES_BRUTES/MenagesExternes/Factures_PDF"
    pdfd.mkdir(parents=True, exist_ok=True)
    shutil.copy2(AISSATA, pdfd / AISSATA.name)
    shutil.copy2(MOUNIR, pdfd / MOUNIR.name)
    sha_pdf_avant = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (AISSATA, MOUNIR)}
    for s in ["lot6c_menages_externes.py", "lib_parc.py", "lib_ref_history.py",
              "lib_menages_externes_pdf.py", "lib_db_moteur.py"]:
        shutil.copy2(_TRAVAIL / s, ws / "02_TRAVAIL" / s)
    r = subprocess.run([str(engine), str(ws / "02_TRAVAIL/lot6c_menages_externes.py")],
                       cwd=str(ws / "02_TRAVAIL"), capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-500:]
    assert "PDF_AUTOMATIQUE" in r.stdout
    out = ws / "02_TRAVAIL/Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx"
    assert out.exists()
    import openpyxl
    wb = openpyxl.load_workbook(out, read_only=True)
    assert "DIAGNOSTIC_PDF" in wb.sheetnames
    wb.close()
    # 22 : aucun PDF réel modifié
    sha_pdf_apres = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (AISSATA, MOUNIR)}
    assert sha_pdf_apres == sha_pdf_avant


@pdf_reels
def test_20_master_reel_intact_apres_extraction(tmp_path):
    """23-24 : le MASTER Lot6c réel et la vraie app.db ne sont pas touchés par une extraction."""
    reel = cfg.MASTER_MENAGES_EXTERNES
    if not Path(reel).exists():
        pytest.skip("MASTER réel absent")
    sha_master_avant = hashlib.sha256(Path(reel).read_bytes()).hexdigest()
    db = Path(cfg.DB_PATH)
    sha_db_avant = hashlib.sha256(db.read_bytes()).hexdigest() if db.exists() else None
    # extraction en copie
    for pdf in (AISSATA, MOUNIR):
        pdfex.extraire_pdf(pdf)
    assert hashlib.sha256(Path(reel).read_bytes()).hexdigest() == sha_master_avant
    if sha_db_avant is not None:
        assert hashlib.sha256(db.read_bytes()).hexdigest() == sha_db_avant


def test_21_ecran_reflete_mode(monkeypatch):
    """21 : l'écran/service reflète le mode et le diagnostic (0037/0040, SQLite — `externes()`/
    `diagnostic_pdf()`/`mode_extraction_externes()` n'ont plus de repli Excel, stub direct comme
    `test_menages_chaine.py::test_pdf_info_prestataires_depuis_master`)."""
    from app.readers import menages_reader as reader
    from app.services import menages_service as svc

    lignes = [{"nom_prestataire": "Aissata", "mois": "2026-05",
              "nom_fichier_source": "Facture mai Aissata.pdf"}]
    monkeypatch.setattr(reader, "externes", lambda: reader.SourceMenages(
        reader.EtatSource("externes", "Externes", "facture_lignes_menage",
                          "facture_lignes_menage", reader.ETAT_OK, len(lignes)),
        lignes))
    diag = [{"nom_fichier": "Facture mai Aissata.pdf", "format_detecte": "AISSATA",
            "statut_extraction": "OK", "numero_facture": "2026-37", "montant_total": 1439,
            "nb_lignes": 8, "ecart_reconciliation": 0, "doublon_de": "", "anomalies": ""}]
    monkeypatch.setattr(reader, "diagnostic_pdf", lambda: reader.SourceMenages(
        reader.EtatSource("diag", "Diag", "facture_pdf_diagnostics", "facture_pdf_diagnostics",
                          reader.ETAT_OK, len(diag)),
        diag))
    monkeypatch.setattr(reader, "mode_extraction_externes", lambda: "PDF_AUTOMATIQUE")

    info = svc.load_pdf_externes_info()
    assert info["mode_extraction"] == "PDF_AUTOMATIQUE"
    assert info["extraction_automatique"] is True
    assert info["nb_pdf_reconnus"] == 1
    assert info["diagnostic"][0]["numero_facture"] == "2026-37"
