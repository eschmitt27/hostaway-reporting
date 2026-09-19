"""Deux vérités du document que le logiciel ne doit ni maquiller ni écraser.

1. 02-26-Imrane : la pièce imprime « 2 × 30 € = 30 € » alors que 2 × 30 = 60 et que son total
   (205 €) compte 60. Le montant IMPRIMÉ reste 30 € ; 60 € n'est qu'une suggestion. La facture
   reste À CONTRÔLER et ne se valide qu'après un geste humain.
2. « 2026-37 » : le même prestataire a émis deux factures différentes sous ce numéro (30 avril,
   15 € ; 31 mai, 1 439 €). Les deux coexistent, aucune n'annule ni ne modifie l'autre, et chacune
   dit à l'humain que l'autre existe. Une VRAIE nouvelle version (même période, contenu différent)
   suit toujours le remplacement canonique.

Tout se joue sur des bases temporaires. Les PDF réels sont lus, jamais modifiés.
"""
from __future__ import annotations

import hashlib
import sqlite3
import sys
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import facture_lignes_menage_service as flm
from app.services import facture_menage_pdf_service as imp
from app.services import factures_controles_service as ctrl
from app.services import factures_service as fact

fitz = pytest.importorskip("fitz")
_TRAVAIL = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))
pdfex = pytest.importorskip("lib_menages_externes_pdf")

DOSSIER = cfg.MENAGES_PDF_DIR
IMRANE = DOSSIER / "02-26-Imrane.pdf"
AVRIL_37 = DOSSIER / "04-26-Aissata_2.pdf"
MAI_37 = DOSSIER / "05-26-Aissata.pdf"


def _present(*pdfs):
    return pytest.mark.skipif(not all(p.exists() for p in pdfs),
                              reason="PDF réels absents d'un checkout propre")


@pytest.fixture(autouse=True)
def _ecriture_activee(monkeypatch):
    for drapeau in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                    "ECRITURE_OPERATIONNELLE_ENABLED", "COMPTABILITE_REAL_WRITE_ENABLED",
                    "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, drapeau, True, raising=False)


@pytest.fixture()
def base(tmp_path):
    db = tmp_path / "app.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        for tid, libelle, compte in (
                ("TLM_001", "MENAGE_STANDARD", "OUI"), ("TLM_002", "REMISE_EN_ETAT", "OUI"),
                ("TLM_003", "FRAIS_DEPLACEMENT", "NON"), ("TLM_004", "LINGE", "NON"),
                ("TLM_005", "ACHAT_PRODUIT", "NON"), ("TLM_006", "AUTRE", "NON")):
            conn.execute(
                "INSERT OR IGNORE INTO ref_types_lignes_menage (type_ligne_menage_id, "
                "type_ligne_menage, compte_comme_menage, repartissable_sur_menages, "
                "impact_cout_menage, actif, import_id) VALUES (?,?,?,'NON','OUI','OUI','IMP-T')",
                (tid, libelle, compte))
        conn.commit()
    finally:
        conn.close()
    return db


def _factures(db, ref):
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM factures WHERE facture_ref = ? ORDER BY id", (ref,))]
    finally:
        conn.close()


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


# ── 1. Document arithmétiquement faux : le montant imprimé n'est jamais écrasé ─────────────────

@_present(IMRANE)
def test_imrane_extraction_conserve_le_montant_imprime():
    fac = pdfex.extraire_pdf(IMRANE)
    ligne = fac.lignes[1]
    assert (ligne.quantite, ligne.prix_unitaire, ligne.montant_ligne) == (2, 30.0, 30.0)
    assert ligne.montant_calcule == 60.0 and ligne.ecart_arithmetique == 30.0
    assert "LIGNE_NB_PU_TOTAL_INCOHERENT" in ligne.code_anomalie
    # Fidélité de l'extraction ET incohérence du document, dites séparément.
    assert fac.montant_total_facture == 205.0
    assert fac.somme_lignes == 175.0 and fac.ecart_reconciliation == -30.0
    assert fac.somme_theorique == 205.0 and fac.correction_suggeree == 30.0
    assert any(a.startswith("INCOHERENCE_ARITHMETIQUE_DOCUMENT:ligne=2") for a in fac.anomalies_document)
    assert fac.anomalies_parseur == [], "le parseur a lu fidèlement : l'erreur est celle du document"


@_present(IMRANE)
def test_imrane_reste_a_controler_et_ne_se_valide_qu_apres_un_geste_humain(base):
    r = imp.importer(IMRANE, acteur="test", db_path=base)
    assert r["ok"] is True, r
    opaque = r["facture_id_opaque"]
    lignes = flm.lignes(opaque, db_path=base)
    assert sorted(l["montant_ttc"] for l in lignes) == [30.0, 145.0], "montant source jamais écrasé"
    ligne = next(l for l in lignes if l["montant_ttc"] == 30.0)
    assert (ligne["quantite"], ligne["prix_unitaire"]) == (2, 30.0)

    assert fact.charger(opaque, db_path=base)["statut"] == fact.ST_A_CONTROLER
    codes = [a["code"] for a in imp.anomalies_document(opaque, db_path=base)]
    assert imp.A_INCOHERENCE_ARITHMETIQUE in codes
    assert flm.controler_total(opaque, db_path=base)["ecart"] == pytest.approx(30.0), \
        "total document 205 € − montants imprimés 175 €"

    refus = fact.changer_statut(opaque, fact.ST_VALIDEE, acteur="test", db_path=base)
    assert refus["ok"] is False and refus["code"] == fact.E_ECART_LIGNES_TOTAL
    assert fact.consequences_constatees(opaque, db_path=base)["ecritures"] == [], \
        "aucune écriture ACHATS, aucune dette fournisseur"

    # Le geste humain existant (ligne corrective) lève le blocage — rien d'automatique.
    ajout = flm.ajouter_ligne_manquante(opaque, motif="2 × 30 € = 60 € : écart du document",
                                        montant_ttc=30.0, description="correction ligne 2",
                                        categorie=flm.CAT_MENAGE_STANDARD, db_path=base)
    assert ajout["ok"] is True, ajout
    assert flm.controler_total(opaque, db_path=base)["ecart"] == pytest.approx(0.0)
    # Le blocage arithmétique (V11) est levé. Ce qui reste est une AUTRE tâche humaine : ces lignes
    # « Nettoyage T2 » ne désignent aucun logement (V13) — rien n'est affecté d'office.
    suite = fact.changer_statut(opaque, fact.ST_VALIDEE, acteur="test", db_path=base)
    assert suite["ok"] is False and suite["code"] != fact.E_ECART_LIGNES_TOTAL
    assert fact.consequences_constatees(opaque, db_path=base)["ecritures"] == []
    assert any(l["montant_ttc"] == 30.0 and l["quantite"] == 2
               for l in flm.lignes(opaque, db_path=base)), "la ligne imprimée reste telle quelle"


# ── 2. Numéro réutilisé : deux factures distinctes, aucune n'annule l'autre ─────────────────────

@_present(AVRIL_37, MAI_37)
@pytest.mark.parametrize("ordre", [(AVRIL_37, MAI_37), (MAI_37, AVRIL_37)],
                         ids=["avril_puis_mai", "mai_puis_avril"])
def test_deux_factures_2026_37_coexistent(base, ordre):
    premier, second = ordre
    r1 = imp.importer(premier, acteur="test", db_path=base)
    r2 = imp.importer(second, acteur="test", db_path=base)
    assert r1["ok"] is True and r2["ok"] is True, (r1, r2)
    assert r2.get("remplacement_de") is None, "un numéro réutilisé n'est pas une nouvelle version"
    assert r2["numero_reutilise_de"] == r1["facture_id_opaque"]
    assert _sha(premier) != _sha(second)

    factures = _factures(base, "2026-37")
    assert len(factures) == 2
    assert {f["facture_id_opaque"] for f in factures} == {r1["facture_id_opaque"], r2["facture_id_opaque"]}
    assert all(f["statut"] == fact.ST_A_CONTROLER for f in factures), "aucune annulée"
    assert sorted(f["montant_ttc"] for f in factures) == [15.0, 1439.0]
    assert sorted(f["date_facture"] for f in factures) == ["2026-04-30", "2026-05-31"]
    # La première n'a pas été touchée : même version, même montant qu'à sa création.
    premiere = next(f for f in factures if f["facture_id_opaque"] == r1["facture_id_opaque"])
    assert premiere["version"] == 1

    for f, autre in ((factures[0], factures[1]), (factures[1], factures[0])):
        anomalies = imp.anomalies_document(f["facture_id_opaque"], db_path=base)
        reutilise = [a for a in anomalies if a["code"] == imp.A_NUMERO_FACTURE_REUTILISE]
        assert [a["facture_id_opaque"] for a in reutilise] == [autre["facture_id_opaque"]]
        assert autre["date_facture"] in reutilise[0]["libelle"]
        assert str(autre["montant_ttc"]) in reutilise[0]["libelle"]
        assert fact.consequences_constatees(f["facture_id_opaque"], db_path=base)["ecritures"] == []
        assert flm.controler_total(f["facture_id_opaque"], db_path=base)["ecart"] == pytest.approx(0.0)

    # Le catalogue des contrôles signale, sans exiger d'annuler l'une des deux.
    anomalies = ctrl.controler(db_path=base)["anomalies"]
    assert not [a for a in anomalies if a["code"] == ctrl.F_DOUBLON_CERTAIN]
    assert len([a for a in anomalies if a["code"] == ctrl.F_ANOMALIE_DOCUMENT]) == 2

    # Réimporter les deux documents ne crée rien de plus : ce sont alors de vrais doublons.
    assert imp.importer(premier, acteur="test", db_path=base)["code"] == fact.E_DOUBLON_CERTAIN
    assert imp.importer(second, acteur="test", db_path=base)["code"] == fact.E_DOUBLON_CERTAIN
    assert len(_factures(base, "2026-37")) == 2


@_present(AVRIL_37, MAI_37)
def test_les_deux_factures_se_valident_separement(base):
    r1 = imp.importer(AVRIL_37, acteur="test", db_path=base)
    r2 = imp.importer(MAI_37, acteur="test", db_path=base)
    assert fact.changer_statut(r1["facture_id_opaque"], fact.ST_VALIDEE, acteur="t",
                               db_path=base)["ok"] is True
    assert fact.charger(r2["facture_id_opaque"], db_path=base)["statut"] == fact.ST_A_CONTROLER
    assert fact.consequences_constatees(r2["facture_id_opaque"], db_path=base)["ecritures"] == []


@_present(AVRIL_37, MAI_37)
def test_ecran_de_la_facture_montre_l_autre_document(client, tmp_db, monkeypatch):
    r1 = imp.importer(AVRIL_37, acteur="test", db_path=tmp_db)
    r2 = imp.importer(MAI_37, acteur="test", db_path=tmp_db)
    page = client.get(f"/factures/{r2['facture_id_opaque']}").text
    assert 'data-testid="anomalies-document"' in page
    assert r1["facture_id_opaque"] in page and "2026-04-30" in page


def test_la_saisie_manuelle_refuse_toujours_un_numero_deja_connu(base):
    """L'assouplissement ne vaut que pour l'import d'un document réel, jamais pour une saisie."""
    form = {"fournisseur_id_opaque": "FRS-T", "facture_ref": "R-1", "montant_ttc": 10.0}
    assert fact.creer({**form, "date_facture": "2026-04-30"}, db_path=base)["ok"] is True
    refus = fact.creer({**form, "date_facture": "2026-05-31"}, db_path=base)
    assert refus["ok"] is False
    assert fact.E_DOUBLON_CERTAIN in [e["code"] for e in refus["erreurs"]]


def test_le_schema_interdit_toujours_le_doublon_du_meme_mois(base):
    conn = get_db(base)
    try:
        conn.execute("INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
                     "date_facture, montant_ttc, statut) VALUES ('FAC-A','F','X','2026-05-02',1,'A_CONTROLER')")
        conn.execute("INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
                     "date_facture, montant_ttc, statut) VALUES ('FAC-B','F','X','2026-06-02',1,'A_CONTROLER')")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, "
                         "facture_ref, date_facture, montant_ttc, statut) "
                         "VALUES ('FAC-C','F','X','2026-05-30',1,'A_CONTROLER')")
    finally:
        conn.close()


# ── 3. Une vraie nouvelle version suit toujours le remplacement canonique ───────────────────────

def _pdf_facture(path: Path, total: str, lignes: list[tuple[str, str]]) -> Path:
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    for x, y, t in [(60, 60, "Rends-moi un service"), (380, 100, "Facture n°2099-50"),
                    (60, 140, "Date de la facture : 31 mai 2026"),
                    (60, 200, "Description"), (440, 200, "Montant HT")]:
        page.insert_text((x, y), t, fontsize=10)
    y = 240
    for i, (lib, prix) in enumerate(lignes, 1):
        page.insert_text((20, y), f"{i}. {lib}", fontsize=10)
        page.insert_text((440, y), prix, fontsize=10)
        y += 30
    page.insert_text((40, 500), "Total TTC", fontsize=10)
    page.insert_text((40, 514), total, fontsize=10)
    doc.save(path)
    doc.close()
    return path


def test_une_vraie_nouvelle_version_remplace_toujours_la_precedente(base, tmp_path):
    """Même fournisseur, même numéro, MÊME période, contenu corrigé, hash différent : V1 est
    annulée (tracée), V2 devient la seule facture active — le workflow canonique, intact."""
    v1 = _pdf_facture(tmp_path / "v1.pdf", "55,00", [("service de nettoyage T3 4 rue A", "55,00")])
    v2 = _pdf_facture(tmp_path / "v2.pdf", "84,00", [("service de nettoyage T3 4 rue A", "55,00"),
                                                     ("service de nettoyage studio 6 rue B", "29,00")])
    assert _sha(v1) != _sha(v2)
    r1 = imp.importer(v1, acteur="test", db_path=base)
    r2 = imp.importer(v2, acteur="test", db_path=base)
    assert r1["ok"] and r2["ok"], (r1, r2)
    assert r2["remplacement_de"] == r1["facture_id_opaque"]
    assert r2["numero_reutilise_de"] is None
    statuts = {f["facture_id_opaque"]: f["statut"] for f in _factures(base, "2099-50")}
    assert statuts == {r1["facture_id_opaque"]: fact.ST_ANNULEE,
                       r2["facture_id_opaque"]: fact.ST_A_CONTROLER}
