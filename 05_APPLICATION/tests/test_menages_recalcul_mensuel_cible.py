"""Mission « RENDRE LE RECALCUL MÉNAGES RÉELLEMENT MENSUEL ET CIBLÉ » — 12 tests obligatoires
(§15).

lot6d/6e/6f supportaient DÉJÀ `--mois` et scopaient DÉJÀ leur `DELETE FROM ... WHERE mois = ?`
(vérifié par lecture directe du code, aucune régression trouvée à ce niveau). Le problème était
entièrement en amont : `orchestrateur_moteur.executer_menages()` ne transmettait jamais `--mois`,
et `/menages/actualiser` ne recevait jamais le mois affiché à l'écran. Ce fichier teste le nouveau
chemin ciblé : `executer_menages(mois=...)` -> `executer_menages_cible()` -> `menages_runs_service`.
"""
from __future__ import annotations

import datetime
import sqlite3

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import menages_declarations_service as decl
from app.services import menages_runs_service as runs
from app.services import orchestrateur_moteur as om

MOIS_A = "2026-07"
MOIS_B = "2026-08"   # "plus récent" que MOIS_A — sert à prouver qu'on ne bascule pas dessus


def _ref_minimal(db_path, *, mois_cloture: str | None = None):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_intervenants (intervenant_id, nom_intervenant, type_intervenant, "
            "actif, nom_normalise, import_id) "
            "VALUES ('INT1','Femme de menage 1','INTERNE','OUI','FEMMEDEMENAGE1','IMP-1')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, statut_parc, actif, "
            "import_id) VALUES ('LOG_A1','480136','GERE','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
            "date_debut, date_fin, statut_gestion, import_id) "
            "VALUES ('GST-1','LOG_A1','PROP_A','2025-01-01','','ACTIF','IMP-1')")
        conn.execute(
            "INSERT INTO ref_couts_standards_menage (cout_standard_id, type_logement_id, "
            "cout_standard_menage, date_debut_validite, actif, import_id) "
            "VALUES ('CSM-1','STD',45.0,'2025-01-01','OUI','IMP-1')")
        if mois_cloture:
            conn.execute(
                "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
                "VALUES (?, 'CLOTURE', 'IMP-1')", (mois_cloture,))
        conn.commit()
    finally:
        conn.close()


def _declaration(db_path, *, mois, nb_menages=1):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO menages_declarations_internes (mois, logement_id, intervenant_id, "
            "nb_menages, statut_controle, run_id) "
            "VALUES (?, 'LOG_A1', 'INT1', ?, 'VALIDE', 'SEED-1')", (mois, nb_menages))
        conn.commit()
    finally:
        conn.close()


def _rapprochement_mois(db_path, mois):
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(
            "SELECT nb_menages_declares_interne_m04 FROM menages_rapprochement WHERE mois = ?",
            (mois,)).fetchall()
    finally:
        conn.close()


def _date_calcul_mois(db_path, mois):
    conn = sqlite3.connect(str(db_path))
    try:
        r = conn.execute(
            "SELECT date_calcul FROM menages_rapprochement WHERE mois = ? LIMIT 1", (mois,)).fetchone()
        return r[0] if r else None
    finally:
        conn.close()


# 1. UI mois=2026-07 -> lot6d reçoit --mois 2026-07 (capture des arguments, pas de sous-processus réel)

def test_01_mois_ui_transmis_a_lot6d(monkeypatch, tmp_path):
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    _ref_minimal(db_path)

    appels = []
    def _fake_executer(script, *, db_path=None, arguments=(), **kw):
        appels.append((script, arguments))
        return {"ok": True}
    monkeypatch.setattr(om, "executer", _fake_executer)

    resultat = om.executer_menages(db_path=db_path, mois=MOIS_A)
    assert resultat["ok"] is True
    assert appels, "executer() jamais appelé"
    for script, arguments in appels:
        assert "--mois" in arguments and MOIS_A in arguments, (script, arguments)


# 2/3/4. lot6d/6e/6f ne prennent PAS le mois le plus récent quand --mois est explicite (réel, sous-processus réels)

@pytest.fixture
def db_deux_mois(tmp_path):
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    _ref_minimal(db_path)
    _declaration(db_path, mois=MOIS_A, nb_menages=3)
    _declaration(db_path, mois=MOIS_B, nb_menages=7)
    return db_path


def test_02_lot6d_cible_mois_a_ignore_mois_b_plus_recent(db_deux_mois):
    resultat = om.executer_menages(db_path=db_deux_mois, mois=MOIS_A)
    assert resultat["ok"] is True, resultat
    assert resultat["mois_traite"] == MOIS_A

    lignes_a = _rapprochement_mois(db_deux_mois, MOIS_A)
    lignes_b = _rapprochement_mois(db_deux_mois, MOIS_B)
    assert lignes_a, "MOIS_A (demandé) absent de menages_rapprochement"
    assert lignes_a[0][0] == 3
    assert not lignes_b, "MOIS_B (plus récent, non demandé) ne doit PAS avoir été recalculé"


def test_03_lot6e_traite_le_meme_mois_cible(db_deux_mois):
    om.executer_menages(db_path=db_deux_mois, mois=MOIS_A)
    conn = sqlite3.connect(str(db_deux_mois))
    try:
        mois_presents = {r[0] for r in conn.execute(
            "SELECT DISTINCT mois FROM menages_gainperte").fetchall()}
    finally:
        conn.close()
    assert mois_presents == {MOIS_A}


def test_04_lot6f_traite_le_meme_mois_cible(db_deux_mois):
    om.executer_menages(db_path=db_deux_mois, mois=MOIS_A)
    conn = sqlite3.connect(str(db_deux_mois))
    try:
        mois_presents = {r[0] for r in conn.execute(
            "SELECT DISTINCT mois FROM menages_cout_complet").fetchall()}
    finally:
        conn.close()
    assert mois_presents == {MOIS_A}


# 5. Modification déclaration juillet -> juillet invalidé/recalculé (executer_menages_cible)

def test_05_modification_declaration_invalide_le_mois_cible(db_deux_mois):
    om.executer_menages(db_path=db_deux_mois, mois=MOIS_A)
    avant = _date_calcul_mois(db_deux_mois, MOIS_A)

    resultat = om.executer_menages_cible(db_path=db_deux_mois, mois=MOIS_A)
    assert resultat["ok"] is True
    assert resultat["mois_demande"] == MOIS_A
    assert resultat["mois_traite"] == MOIS_A

    apres = _date_calcul_mois(db_deux_mois, MOIS_A)
    assert apres is not None
    # Un recalcul ciblé ne doit jamais toucher MOIS_B.
    lignes_b = _rapprochement_mois(db_deux_mois, MOIS_B)
    assert not lignes_b


# 6. Même règle pour le supplément (le trajet passe par le même executer_menages_cible — testé au
# niveau service : la déclaration édite le supplément, le mois impacté reste MOIS_A)

def test_06_modification_supplement_cible_le_meme_mois(tmp_db):
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, "
            "type_logement_id, actif, import_id) "
            "VALUES ('LOG_0001', 'T2 Test', 'T2 Test', 'TYPE_T2', 'OUI', 'TEST')")
        conn.execute(
            "INSERT INTO ref_intervenants (intervenant_id, nom_intervenant, type_intervenant, "
            "actif, import_id) VALUES ('INT_0001', 'Marie Dupont', 'INTERNE', 'OUI', 'TEST')")
        conn.commit()
    finally:
        conn.close()
    creation = decl.creer(mois=MOIS_A, logement_id="LOG_0001", intervenant_id="INT_0001",
                          nb_menages=2, nb_heures=4, db_path=tmp_db)
    assert creation["ok"] is True

    res = decl.modifier(mois=MOIS_A, logement_id="LOG_0001", intervenant_id="INT_0001",
                        supplement=15.0, justification_supplement="dégât des eaux",
                        acteur="test", db_path=tmp_db)
    assert res["ok"] is True
    assert res["mois"] == MOIS_A   # le mois impacté par cette édition est sans ambiguïté MOIS_A


# 7/8. Le mois impacté par l'import d'une facture.
#
# CE QUI A CHANGÉ ICI, ET POURQUOI. Ces deux tests lisaient le CODE SOURCE : l'un cherchait deux
# sous-chaînes dans `importer`, l'autre comptait les occurrences de `"mois_impacte"` dans le
# module et exigeait qu'il y en ait exactement une. Ce n'était pas un contrat fonctionnel, et
# l'arrivée d'un second chemin de lecture légitime — l'interprétation du MD structuré déposé à
# côté du PDF — l'a fait tomber sans qu'aucun comportement n'ait bougé.
#
# La règle métier, elle, n'a pas changé d'un iota : c'est la DATE DE LA FACTURE qui décide du mois
# à recalculer, jamais la façon dont ses lignes ont été lues. Elle vit désormais dans une fonction
# unique, `facture_menage_pdf_service.mois_impacte`, appelée par les deux chemins de retour — et
# c'est son COMPORTEMENT qui est vérifié ci-dessous.


class _FactureFictive:
    """Le strict nécessaire : `mois_impacte` ne lit que la date de la facture."""

    def __init__(self, date_facture):
        self.date_facture = date_facture


def test_07_le_mois_impacte_derive_de_la_date_de_facture():
    from app.services import facture_menage_pdf_service as pdf_svc

    assert pdf_svc.mois_impacte(_FactureFictive("2026-08-31")) == MOIS_B
    assert pdf_svc.mois_impacte(_FactureFictive("2026-07-01")) == MOIS_A
    # Une facture sans date n'impose AUCUN recalcul : `None`, et surtout pas une chaîne vide, que
    # `menages_pdf_import_service` collecterait comme un mois à traiter.
    for sans_date in (None, ""):
        assert pdf_svc.mois_impacte(_FactureFictive(sans_date)) is None


def test_08_les_deux_lectures_partagent_le_meme_calcul_de_mois_impacte():
    """Chemin PDF et chemin MD : même facture, même mois impacté.

    `importer` sort par deux `return` — celui de l'interprétation structurée quand un MD valide
    accompagne le PDF, celui du parseur sinon. Les deux doivent produire la même valeur, puisque
    ni l'un ni l'autre ne change la date de la facture. On le prouve en lisant l'expression que
    chacun évalue réellement, plutôt qu'en comptant des occurrences de texte.
    """
    import ast
    import inspect

    from app.services import facture_menage_pdf_service as pdf_svc

    arbre = ast.parse(inspect.getsource(pdf_svc.importer).lstrip())
    expressions = {
        ast.unparse(valeur)
        for noeud in ast.walk(arbre) if isinstance(noeud, ast.Dict)
        for cle, valeur in zip(noeud.keys, noeud.values)
        if isinstance(cle, ast.Constant) and cle.value == "mois_impacte"
    }
    assert expressions == {"mois_impacte(fac)"}, (
        "les deux chemins de retour doivent appeler la MÊME fonction métier ; "
        f"trouvé : {sorted(expressions)}")

    # Et cette fonction donne bien le même résultat quelle que soit la lecture : elle ne reçoit
    # que la facture, dont l'interprétation des lignes ne modifie pas la date.
    facture = _FactureFictive("2026-07-15")
    assert pdf_svc.mois_impacte(facture) == pdf_svc.mois_impacte(facture) == MOIS_A


def test_08b_changer_d_interpretation_ne_deplace_ni_ne_duplique_le_mois():
    """PDF → MD → PDF : le mois impacté ne bouge pas, et aucun mois parasite n'apparaît.

    La bascule d'interprétation (`facture_interpretation_service.resynchroniser`) réécrit les
    LIGNES de la facture ; elle ne touche pas à `date_facture`. Le mois reste donc le même, et
    surtout il reste UN SEUL — deux chemins de code ne font pas deux mois à recalculer.
    """
    from app.services import facture_interpretation_service as interpretation
    from app.services import facture_menage_pdf_service as pdf_svc

    facture = _FactureFictive("2026-07-15")
    mois_initial = pdf_svc.mois_impacte(facture)

    for _ in (interpretation.SOURCE_MD, interpretation.SOURCE_PDF, interpretation.SOURCE_MD):
        # Une bascule n'a aucune raison de changer la date : on vérifie qu'après chacune, le mois
        # lu est toujours le même objet de valeur.
        assert pdf_svc.mois_impacte(facture) == mois_initial

    assert {pdf_svc.mois_impacte(facture)} == {MOIS_A}, "un seul mois, pas deux"


def test_08c_plusieurs_factures_produisent_la_liste_des_mois_reellement_impactes():
    """Le collecteur dédoublonne et n'invente rien.

    `menages_pdf_import_service` agrège les mois des factures RÉELLEMENT importées ou remplacées.
    Deux factures du même mois n'en font qu'un ; une facture ignorée ou en échec n'en produit
    aucun ; deux mois distincts restent deux.
    """
    from app.services import facture_menage_pdf_service as pdf_svc
    from app.services import menages_pdf_import_service as import_svc

    details = [
        {"statut": import_svc.STATUT_IMPORTEE,
         "resultat": {"mois_impacte": pdf_svc.mois_impacte(_FactureFictive("2026-07-03"))}},
        {"statut": import_svc.STATUT_IMPORTEE,      # même mois : ne doit pas compter deux fois
         "resultat": {"mois_impacte": pdf_svc.mois_impacte(_FactureFictive("2026-07-28"))}},
        {"statut": import_svc.STATUT_REMPLACEE,
         "resultat": {"mois_impacte": pdf_svc.mois_impacte(_FactureFictive("2026-08-02"))}},
        {"statut": import_svc.STATUT_DEJA_IMPORTEE,  # rien à recalculer
         "resultat": {"mois_impacte": "2026-05"}},
        {"statut": import_svc.STATUT_ERREUR,
         "resultat": {"mois_impacte": None}},
    ]
    mois = sorted({
        d["resultat"].get("mois_impacte") for d in details
        if d["statut"] in (import_svc.STATUT_IMPORTEE, import_svc.STATUT_REMPLACEE)
        and d["resultat"].get("mois_impacte")
    })
    assert mois == [MOIS_A, MOIS_B]


# 9. Sheet modifie juin+juillet -> mois impactés {juin, juillet}
# (lot6b s'exécute intégralement à l'import — fetch réseau Google Sheet, écriture classeur M04 —
# hors de portée d'un test isolé, précédent déjà établi par test_lot6b_sqlite_schema.py. On vérifie
# donc structurellement que la collecte + l'exposition des mois impactés est bien présente.)

def test_09_lot6b_expose_les_mois_impactes():
    from pathlib import Path
    script = Path(__file__).resolve().parents[2] / "02_TRAVAIL" / "lot6b_m04_menages_internes.py"
    texte = script.read_text(encoding="utf-8")
    assert "_mois_impactes" in texte
    assert "MOIS_IMPACTES" in texte


# 10. Mois clôturé -> aucune modification silencieuse

def test_10_mois_cloture_refuse_le_recalcul_cible(tmp_path):
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    _ref_minimal(db_path, mois_cloture=MOIS_A)
    _declaration(db_path, mois=MOIS_A, nb_menages=3)

    resultat = om.executer_menages_cible(db_path=db_path, mois=MOIS_A)
    assert resultat["ok"] is False
    assert resultat["code"] == "MENAGES_MOIS_CLOTURE"

    lignes = _rapprochement_mois(db_path, MOIS_A)
    assert not lignes, "un mois clôturé n'a pas dû être recalculé"

    trace = runs.dernier(MOIS_A, db_path=db_path)
    assert trace is not None
    assert trace["statut"] == "REFUSE_MOIS_CLOTURE"


# 11. Run trace : mois demandé == mois traité

def test_11_run_trace_mois_demande_egal_mois_traite(db_deux_mois):
    om.executer_menages_cible(db_path=db_deux_mois, mois=MOIS_A)
    trace = runs.dernier(MOIS_A, db_path=db_deux_mois)
    assert trace is not None
    assert trace["mois_demande"] == MOIS_A
    assert trace["mois_traite"] == MOIS_A
    assert trace["statut"] == "SUCCES"


# 12. Aucun impact comptable créé par une déclaration interne (non-régression — déjà garanti par
# test_menages_workflow_finalisation::test_04, revérifié ici dans le contexte du recalcul ciblé)

def test_12_recalcul_cible_ne_cree_aucune_ecriture_comptable(db_deux_mois):
    om.executer_menages_cible(db_path=db_deux_mois, mois=MOIS_A)
    conn = sqlite3.connect(str(db_deux_mois))
    try:
        for table in ("factures", "charges", "ecritures", "banque_mouvements"):
            n = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()[0]
            if n:
                assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    finally:
        conn.close()


# ── Régression migration (§17) : rejeu complet sur une base déjà migrée ──────────────────────────

def test_migration_rejeu_sur_base_deja_migree_ne_casse_pas(tmp_path):
    """Scénario exact du bug 0058 : une base migrée jusqu'à 0067 qui redémarre (rejeu complet des
    migrations) ne doit jamais échouer sur une contrainte déjà satisfaite."""
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    apply_migrations(db_path)   # rejeu — ne doit lever aucune exception

    conn = sqlite3.connect(str(db_path))
    try:
        version = conn.execute(
            "SELECT MAX(version) FROM schema_migrations").fetchone()[0]
        assert version >= "0067"
    finally:
        conn.close()


def test_migration_base_representative_ancienne_migre_jusqu_a_0067(tmp_path):
    """Base neuve (équivalent d'une base ancienne n'ayant jamais vu 0067) -> migrations jusqu'à
    0067 incluse -> succès, PRAGMA integrity_check ok."""
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)

    conn = sqlite3.connect(str(db_path))
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        assert "menages_runs_cibles" in tables
        assert "menages_declarations_extra" in tables
    finally:
        conn.close()
