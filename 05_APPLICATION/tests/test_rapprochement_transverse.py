"""APP-3F — Cas transverses : redémarrage, concurrence, intégrité bancaire, migrations."""
import sqlite3

import pytest

from app.db.connection import MIGRATIONS_DIR, apply_migrations, get_db
from app.services import rapprochement_reglements_service as rap


def test_redemarrage_persistance(tmp_db):
    """Un rapprochement confirmé persiste après relecture (redémarrage simulé)."""
    r = rap.creer_ou_charger("REG-redem", db_path=tmp_db)
    r = rap.enregistrer_proposition(r, "MVT-a", criteres=["montant_exact"], score=1,
                                    mouvement_empreinte="e", ecart_montant=0.0, ecart_jours=0,
                                    version_attendue=r["version"], db_path=tmp_db)
    r = rap.passer_a_controler(r, version_attendue=r["version"], db_path=tmp_db)
    r = rap.confirmer(r, reglement_paye=True, mouvement_present=True, sens_sortant=True,
                      version_attendue=r["version"], db_path=tmp_db)
    relu = rap.charger(r["rapprochement_id_opaque"], db_path=tmp_db)   # relecture = redémarrage
    assert relu["statut"] == rap.ST_RAPPROCHE


def test_concurrence_version_obsolete(tmp_db):
    r = rap.creer_ou_charger("REG-conc", db_path=tmp_db)
    rap.signaler_anomalie(r, "a", version_attendue=r["version"], db_path=tmp_db)   # incrémente version
    with pytest.raises(rap.RapprochementRefuse):
        rap.rouvrir(r, "b", version_attendue=r["version"], db_path=tmp_db)   # r porte l'ancienne version


def test_rollback_sur_conflit_unicite(tmp_db):
    """Un INSERT en doublon actif échoue et ne laisse aucune ligne fantôme."""
    rap.creer_ou_charger("REG-uni", db_path=tmp_db)
    conn = get_db(tmp_db)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO rapprochements_reglements (rapprochement_id_opaque, releve_id_opaque) "
                     "VALUES (?,?)", ("RAP-dupX", "REG-uni"))
        conn.commit()
    conn.rollback()
    n = conn.execute("SELECT COUNT(*) FROM rapprochements_reglements WHERE releve_id_opaque='REG-uni'").fetchone()[0]
    conn.close()
    assert n == 1


def test_integrite_bancaire_aucun_writer(tmp_db):
    """Aucun module APP-3F n'importe ni n'appelle le writer bancaire réel."""
    import inspect
    from app.readers import rapprochement_bancaire_reader as contrat
    from app.services import rapprochement_candidats_service as cand
    for mod in (rap, cand, contrat):
        src = inspect.getsource(mod)
        assert "banques_controle_writer" not in src
        assert "enregistrer_sur_copie" not in src


def test_migration_0014_partielle_puis_complete(tmp_path):
    """Base arrêtée à 0013 puis complétée : 0014 crée ses tables."""
    db = tmp_path / "p.db"
    conn = get_db(db)
    for m in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if m.name <= "0013_fournisseur_rattachements.sql":
            conn.executescript(m.read_text(encoding="utf-8"))
    conn.commit(); conn.close()
    tables_avant = {r[0] for r in get_db(db).execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "rapprochements_reglements" not in tables_avant
    apply_migrations(db)
    tables_apres = {r[0] for r in get_db(db).execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert {"rapprochements_reglements", "rapprochement_evenements"} <= tables_apres


def test_migration_0014_idempotente_et_sans_doublon(tmp_path):
    db = tmp_path / "i.db"
    apply_migrations(db); apply_migrations(db)
    conn = get_db(db)
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    idx = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'").fetchall()]
    n = conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
    conn.close()
    assert len(tables) == len(set(tables)) and len(idx) == len(set(idx))
    assert n == len(list(MIGRATIONS_DIR.glob("*.sql")))   # compte découvert par glob


def test_flags_write_false_marqueur():
    import app.config as cfg
    assert cfg.BANQUE_REAL_WRITE_ENABLED is False
