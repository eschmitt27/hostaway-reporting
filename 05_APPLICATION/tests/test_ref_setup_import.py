"""Import REF_Setup.xlsm → SQLite (migration 0029).

Toutes les fixtures sont SYNTHÉTIQUES et construites depuis le catalogue : aucune donnée réelle,
aucun nom de propriétaire, aucun identifiant du parc. Le classeur réel n'est lu que par les tests
marqués `reel_requis`, qui se contentent de vérifier des invariants de structure.
"""
from datetime import datetime
from pathlib import Path

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import ref_setup_catalogue as cat
from app.services import ref_setup_import_service as imp

openpyxl = pytest.importorskip("openpyxl")

REEL = Path(__import__("app.config", fromlist=["x"]).REF_SETUP)
reel_requis = pytest.mark.skipif(not REEL.exists(), reason="REF_Setup.xlsm absent de cet environnement")


# ── Fixtures synthétiques ───────────────────────────────────────────────────────────────────────

# Valeurs par onglet : chaque entrée est {colonne: valeur}. Les colonnes non citées restent vides.
# Grain volontairement minimal — on teste le mécanisme d'import, pas le contenu du référentiel.
LIGNES_SYNTHETIQUES: dict[str, list[dict[str, str]]] = {
    "REF_Logements": [
        {"logement_id": "LOG_9001", "nom_court": "Fixture A", "type_logement_id": "TYPE_901"},
        {"logement_id": "LOG_9002", "nom_court": "Fixture B", "type_logement_id": "TYPE_901"},
    ],
    "REF_Proprietaires": [
        {"proprietaire_id": "PROP_9001", "nom_proprietaire": "DEMO UN"},
        {"proprietaire_id": "PROP_9002", "nom_proprietaire": "DEMO DEUX"},
    ],
    "REF_Types_Logements": [{"type_logement_id": "TYPE_901", "type_logement": "Fixture"}],
    "REF_Gestion_Logements_Hist": [
        {"gestion_id": "GST_9001", "logement_id": "LOG_9001", "proprietaire_id": "PROP_9001",
         "date_debut": "2025-01-01", "statut_gestion": "ACTIF"},
    ],
    "REF_Taux_Commission": [
        {"taux_commission_id": "TX_9001", "proprietaire_id": "PROP_9001",
         "taux_commission": "0.15", "date_debut": "2025-01-01", "date_fin": "2025-12-31"},
        {"taux_commission_id": "TX_9002", "proprietaire_id": "PROP_9001",
         "taux_commission": "0.18", "date_debut": "2026-01-01"},
    ],
}


def _ecrire_classeur(chemin: Path, lignes: dict[str, list[dict[str, str]]] | None = None,
                     *, omettre: str = "", colonnes_override: dict[str, list[str]] | None = None):
    """Construit un classeur complet depuis le catalogue. Seul le contenu cité est renseigné."""
    lignes = LIGNES_SYNTHETIQUES if lignes is None else lignes
    colonnes_override = colonnes_override or {}
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for f in cat.FEUILLES:
        if f.onglet == omettre:
            continue
        ws = wb.create_sheet(f.onglet)
        colonnes = colonnes_override.get(f.onglet, list(f.colonnes))
        ws.append(colonnes)
        for ligne in lignes.get(f.onglet, []):
            ws.append([ligne.get(c, "") for c in colonnes])
    wb.save(chemin)
    return chemin


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "ref.db"
    apply_migrations(p)
    return p


@pytest.fixture
def classeur(tmp_path):
    return _ecrire_classeur(tmp_path / "REF_Setup_fixture.xlsx")


# ── Schéma ──────────────────────────────────────────────────────────────────────────────────────

def test_migration_cree_les_28_tables_du_catalogue(db):
    conn = get_db(db)
    try:
        noms = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()
    assert len(cat.FEUILLES) == 28
    manquantes = [t for t in cat.toutes_les_tables() if t not in noms]
    assert manquantes == [], f"Tables du catalogue absentes : {manquantes}"
    assert cat.TABLE_IMPORTS in noms and cat.TABLE_IMPORT_FEUILLES in noms


def test_chaque_table_porte_exactement_les_colonnes_du_catalogue(db):
    """Le catalogue est un contrat : une dérive silencieuse entre lui et la migration rendrait
    l'import faux sans le faire échouer."""
    conn = get_db(db)
    try:
        for f in cat.FEUILLES:
            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({f.table})")]
            assert cols == list(f.colonnes) + ["import_id"], f.table
    finally:
        conn.close()


# ── Prévisualisation ────────────────────────────────────────────────────────────────────────────

def test_previsualiser_n_ecrit_rien(db, classeur):
    avant = imp.previsualiser(chemin=classeur, db_path=db)
    assert avant["ok"] is True
    conn = get_db(db)
    try:
        total = sum(conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    for t in cat.toutes_les_tables())
        assert total == 0
        assert conn.execute(f"SELECT COUNT(*) FROM {cat.TABLE_IMPORTS}").fetchone()[0] == 0
    finally:
        conn.close()


def test_previsualiser_annonce_ce_qui_sera_ecrit(db, classeur):
    p = imp.previsualiser(chemin=classeur, db_path=db)
    par_onglet = {f["onglet"]: f for f in p["feuilles"]}
    assert par_onglet["REF_Logements"]["nb_lignes"] == 2
    assert par_onglet["REF_Taux_Commission"]["nb_lignes"] == 2
    assert par_onglet["REF_Logements"]["nb_lignes_actuelles"] == 0
    assert p["nb_feuilles"] == 28


def test_source_absente_refusee(db, tmp_path):
    r = imp.previsualiser(chemin=tmp_path / "inexistant.xlsx", db_path=db)
    assert r["ok"] is False
    assert r["code"] == imp.E_SOURCE_ABSENTE


# ── Contrôles bloquants ─────────────────────────────────────────────────────────────────────────

def test_onglet_manquant_bloque(db, tmp_path):
    c = _ecrire_classeur(tmp_path / "sans_associes.xlsx", omettre="REF_Associes")
    r = imp.previsualiser(chemin=c, db_path=db)
    assert r["ok"] is False and r["code"] == imp.E_ONGLET_MANQUANT
    assert "REF_Associes" in r["detail"]


def test_colonne_renommee_bloque(db, tmp_path):
    """Une colonne renommée dans Excel doit ARRÊTER l'import, pas se perdre en silence."""
    cols = list(cat.PAR_ONGLET["REF_Proprietaires"].colonnes)
    cols[1] = "nom_du_proprietaire"
    c = _ecrire_classeur(tmp_path / "renomme.xlsx",
                         colonnes_override={"REF_Proprietaires": cols})
    r = imp.previsualiser(chemin=c, db_path=db)
    assert r["ok"] is False and r["code"] == imp.E_COLONNES_INATTENDUES
    assert "nom_proprietaire" in r["detail"] and "nom_du_proprietaire" in r["detail"]


def test_cle_vide_bloque(db, tmp_path):
    lignes = {**LIGNES_SYNTHETIQUES,
              "REF_Proprietaires": [{"proprietaire_id": "", "nom_proprietaire": "SANS CLE"}]}
    c = _ecrire_classeur(tmp_path / "cle_vide.xlsx", lignes)
    r = imp.previsualiser(chemin=c, db_path=db)
    assert r["ok"] is False and r["code"] == imp.E_CLE_VIDE


def test_cle_dupliquee_bloque(db, tmp_path):
    lignes = {**LIGNES_SYNTHETIQUES, "REF_Proprietaires": [
        {"proprietaire_id": "PROP_9001", "nom_proprietaire": "A"},
        {"proprietaire_id": "PROP_9001", "nom_proprietaire": "B"},
    ]}
    c = _ecrire_classeur(tmp_path / "doublon.xlsx", lignes)
    r = imp.previsualiser(chemin=c, db_path=db)
    assert r["ok"] is False and r["code"] == imp.E_CLE_DUPLIQUEE
    assert "PROP_9001" in r["detail"]


def test_refus_n_ecrit_aucune_ligne(db, tmp_path):
    """Fail-closed : un import refusé laisse les tables EXACTEMENT comme il les a trouvées."""
    imp.importer(chemin=_ecrire_classeur(tmp_path / "bon.xlsx"), db_path=db)
    conn = get_db(db)
    avant = conn.execute("SELECT COUNT(*) FROM ref_proprietaires").fetchone()[0]
    conn.close()

    lignes = {**LIGNES_SYNTHETIQUES, "REF_Proprietaires": [
        {"proprietaire_id": "PROP_9001"}, {"proprietaire_id": "PROP_9001"}]}
    r = imp.importer(chemin=_ecrire_classeur(tmp_path / "ko.xlsx", lignes), db_path=db)
    assert r["ok"] is False

    conn = get_db(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM ref_proprietaires").fetchone()[0] == avant
        # La tentative refusée reste tracée : un échec silencieux serait pire qu'un échec.
        refus = conn.execute(
            f"SELECT statut, code_refus FROM {cat.TABLE_IMPORTS} WHERE statut='REFUSE'").fetchall()
        assert len(refus) == 1 and refus[0][1] == imp.E_CLE_DUPLIQUEE
    finally:
        conn.close()


# ── Avertissements non bloquants ────────────────────────────────────────────────────────────────

def test_relation_orpheline_signalee_sans_bloquer(db, tmp_path):
    lignes = {**LIGNES_SYNTHETIQUES, "REF_Gestion_Logements_Hist": [
        {"gestion_id": "GST_9001", "logement_id": "LOG_INCONNU", "proprietaire_id": "PROP_9001",
         "date_debut": "2025-01-01", "statut_gestion": "ACTIF"}]}
    r = imp.previsualiser(chemin=_ecrire_classeur(tmp_path / "orphelin.xlsx", lignes), db_path=db)
    assert r["ok"] is True, "une relation orpheline ne doit pas empêcher de voir les données"
    codes = [a["code"] for a in r["avertissements"]]
    assert imp.A_RELATION_ORPHELINE in codes
    assert any("LOG_INCONNU" in a["message"] for a in r["avertissements"])


def test_historique_chevauchant_signale(db, tmp_path):
    """Deux taux du même propriétaire qui se recouvrent : signalé, jamais arbitré."""
    lignes = {**LIGNES_SYNTHETIQUES, "REF_Taux_Commission": [
        {"taux_commission_id": "TX_9001", "proprietaire_id": "PROP_9001",
         "taux_commission": "0.15", "date_debut": "2025-01-01", "date_fin": "2025-12-31"},
        {"taux_commission_id": "TX_9002", "proprietaire_id": "PROP_9001",
         "taux_commission": "0.18", "date_debut": "2025-06-01", "date_fin": "2026-12-31"},
    ]}
    r = imp.previsualiser(chemin=_ecrire_classeur(tmp_path / "chevauche.xlsx", lignes), db_path=db)
    assert r["ok"] is True
    assert imp.A_HISTORIQUE_CHEVAUCHANT in [a["code"] for a in r["avertissements"]]


def test_periodes_successives_ne_sont_pas_un_chevauchement(db, tmp_path):
    """Cas réel du référentiel : une période close suivie d'une période ouverte le lendemain."""
    r = imp.previsualiser(chemin=_ecrire_classeur(tmp_path / "ok.xlsx"), db_path=db)
    assert imp.A_HISTORIQUE_CHEVAUCHANT not in [a["code"] for a in r["avertissements"]]


def test_onglet_hors_catalogue_signale(db, tmp_path):
    c = _ecrire_classeur(tmp_path / "extra.xlsx")
    wb = openpyxl.load_workbook(c)
    wb.create_sheet("REF_Nouveaute_Inconnue").append(["a", "b"])
    wb.save(c)
    r = imp.previsualiser(chemin=c, db_path=db)
    assert r["ok"] is True
    assert any(a["code"] == imp.A_ONGLET_HORS_CATALOGUE and "REF_Nouveaute_Inconnue" in a["cible"]
               for a in r["avertissements"])


# ── Import ──────────────────────────────────────────────────────────────────────────────────────

def test_import_ecrit_les_lignes_et_trace_l_origine(db, classeur):
    r = imp.importer(chemin=classeur, db_path=db)
    assert r["ok"] is True and r["nb_feuilles"] == 28
    conn = get_db(db)
    try:
        rows = conn.execute(
            "SELECT logement_id, nom_court, import_id FROM ref_logements ORDER BY logement_id"
        ).fetchall()
        assert [x[0] for x in rows] == ["LOG_9001", "LOG_9002"]
        assert [x[1] for x in rows] == ["Fixture A", "Fixture B"]
        assert {x[2] for x in rows} == {r["import_id"]}
        assert conn.execute(
            f"SELECT COUNT(*) FROM {cat.TABLE_IMPORT_FEUILLES} WHERE import_id=?",
            (r["import_id"],)).fetchone()[0] == 28
    finally:
        conn.close()


def test_import_idempotent(db, classeur):
    """Même classeur deux fois : mêmes lignes, mêmes empreintes de contenu."""
    a = imp.importer(chemin=classeur, db_path=db)
    b = imp.importer(chemin=classeur, db_path=db)
    assert a["import_id"] != b["import_id"]
    conn = get_db(db)
    try:
        def empreintes(iid):
            return conn.execute(
                f"SELECT onglet, empreinte_contenu FROM {cat.TABLE_IMPORT_FEUILLES} "
                "WHERE import_id=? ORDER BY onglet", (iid,)).fetchall()
        assert empreintes(a["import_id"]) == empreintes(b["import_id"])
        assert conn.execute("SELECT COUNT(*) FROM ref_logements").fetchone()[0] == 2
    finally:
        conn.close()


def test_import_remplace_integralement(db, tmp_path):
    """Le classeur est la source, pas un complément : une ligne supprimée dans Excel disparaît."""
    imp.importer(chemin=_ecrire_classeur(tmp_path / "deux.xlsx"), db_path=db)
    lignes = {**LIGNES_SYNTHETIQUES,
              "REF_Logements": [{"logement_id": "LOG_9002", "nom_court": "Fixture B",
                                 "type_logement_id": "TYPE_901"}]}
    imp.importer(chemin=_ecrire_classeur(tmp_path / "un.xlsx", lignes), db_path=db)
    conn = get_db(db)
    try:
        assert [r[0] for r in conn.execute(
            "SELECT logement_id FROM ref_logements")] == ["LOG_9002"]
    finally:
        conn.close()


def test_dernier_import_et_historique(db, classeur, tmp_path):
    assert imp.dernier_import(db_path=db) is None
    imp.importer(chemin=classeur, db_path=db)
    lignes = {**LIGNES_SYNTHETIQUES, "REF_Proprietaires": [
        {"proprietaire_id": "X"}, {"proprietaire_id": "X"}]}
    imp.importer(chemin=_ecrire_classeur(tmp_path / "ko.xlsx", lignes), db_path=db)

    dernier = imp.dernier_import(db_path=db)
    assert dernier is not None and dernier["statut"] == "IMPORTE", (
        "un refus postérieur ne doit jamais devenir « le dernier import »")
    histo = imp.historique_imports(db_path=db)
    assert {h["statut"] for h in histo} == {"IMPORTE", "REFUSE"}


# ── Normalisation déterministe ──────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("brut, attendu", [
    (None, ""),
    ("  espace  ", "espace"),
    (35, "35"),
    (35.0, "35"),          # saisi comme entier : jamais « 35.0 »
    (0.15, "0.15"),
    (True, "OUI"),
    (False, "NON"),
    (datetime(2025, 1, 1), "2025-01-01"),                    # minuit = date pure
    (datetime(2025, 1, 1, 14, 30), "2025-01-01 14:30:00"),   # heure réelle conservée
])
def test_normalisation_valeurs(brut, attendu):
    assert imp._texte(brut) == attendu


def test_empreinte_insensible_a_l_ordre_des_lignes():
    """Réorganiser des lignes dans Excel ne change pas le référentiel."""
    cols = ("a", "b")
    x = [{"a": "1", "b": "x"}, {"a": "2", "b": "y"}]
    assert imp._empreinte(x, cols) == imp._empreinte(list(reversed(x)), cols)


def test_empreinte_change_si_une_valeur_change():
    cols = ("a", "b")
    assert imp._empreinte([{"a": "1", "b": "x"}], cols) != \
           imp._empreinte([{"a": "1", "b": "z"}], cols)


# ── Classeur réel : invariants de structure uniquement ──────────────────────────────────────────

@reel_requis
def test_classeur_reel_conforme_au_catalogue(db):
    """Le catalogue a été généré depuis ce classeur : il doit continuer à lui correspondre."""
    r = imp.previsualiser(db_path=db)
    assert r["ok"] is True, f"{r.get('code')} — {r.get('detail')}"
    assert r["nb_feuilles"] == 28
    assert r["nb_lignes"] > 0
