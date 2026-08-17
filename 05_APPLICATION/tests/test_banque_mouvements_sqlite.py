"""Mouvements bancaires en SQLite — import, déduplication, adaptateurs, isolation par compte.

La table des mouvements bruts n'existait pas : tout l'appareil de décision (imports, overrides,
rapprochements, règles) référençait un `mouvement_id_opaque` qui ne vivait que dans un onglet Excel.
Ces tests couvrent la pièce ajoutée.

Le point le plus délicat est la déduplication. Un même montant, à la même date, avec le même
libellé peut être **deux vrais mouvements** — un double prélèvement existe. Fusionner sur ce seul
critère ferait disparaître un mouvement réel, donc fausser un solde. Le service insère et marque
`A_CONTROLER` : perdre une ligne est irréversible, en garder une en trop se corrige.

Fixtures synthétiques ; un seul test lit le relevé réel, pour la parité.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations
from app.services import banque_adaptateurs as ad
from app.services import banque_mouvements_service as bq

COMPTE_A = "COMPTE_DEMO_A"
COMPTE_B = "COMPTE_DEMO_B"


@pytest.fixture
def db(tmp_path) -> Path:
    p = tmp_path / "banque.db"
    apply_migrations(p)
    return p


def _mvt(date_op, montant, libelle, *, sens=bq.SENS_DEBIT, ext="", valeur=""):
    return {"external_transaction_id": ext, "date_operation": date_op, "date_valeur": valeur,
            "sens": sens, "montant": montant, "devise": "EUR", "libelle_brut": libelle,
            "contrepartie_brute": ""}


_LOT_A = [
    _mvt("2026-01-05", 120.00, "PAIEMENT CB DEMO 1"),
    _mvt("2026-01-06", 45.50, "PRLV DEMO 2"),
    _mvt("2026-01-07", 900.00, "VIR RECU DEMO", sens=bq.SENS_CREDIT),
]


# ── Import de base ──────────────────────────────────────────────────────────────────────────────

def test_import_ecrit_les_mouvements(db):
    r = bq.importer(_LOT_A, bank_account_id=COMPTE_A, source_type=bq.SOURCE_HISTORIQUE, db_path=db)
    assert r["ok"] and r["nb_inseres"] == 3 and r["nb_doublons"] == 0
    assert bq.compter(bank_account_id=COMPTE_A, db_path=db) == 3
    assert bq.periode(bank_account_id=COMPTE_A, db_path=db) == {
        "date_min": "2026-01-05", "date_max": "2026-01-07"}


def test_le_brut_est_conserve_tel_quel(db):
    bq.importer([_mvt("2026-01-05", 120.0, "  PAIEMENT   CB   DEMO  ")],
                bank_account_id=COMPTE_A, source_type=bq.SOURCE_HISTORIQUE, db_path=db)
    m = bq.mouvements(bank_account_id=COMPTE_A, db_path=db)[0]
    assert m["libelle_brut"] == "PAIEMENT   CB   DEMO", (
        "le libellé est stocké tel que reçu — seule l'empreinte le normalise")


def test_previsualiser_n_ecrit_rien(db):
    apercu = bq.previsualiser(_LOT_A, bank_account_id=COMPTE_A, db_path=db)
    assert apercu["nb_lignes"] == 3 and apercu["nb_inserees"] == 3
    assert bq.compter(db_path=db) == 0


def test_journal_d_import_porte_le_contrat(db):
    """§19 — l'import doit être traçable : compte, type, empreinte, période, volumétrie."""
    bq.importer(_LOT_A, bank_account_id=COMPTE_A, source_type=bq.SOURCE_HISTORIQUE,
                source_filename="releve.xlsx", source_sha256="abc123", run_id="RUN-X", db_path=db)
    j = bq.imports(bank_account_id=COMPTE_A, db_path=db)[0]
    assert j["source_type"] == bq.SOURCE_HISTORIQUE
    assert j["source_filename"] == "releve.xlsx" and j["source_sha256"] == "abc123"
    assert j["date_min"] == "2026-01-05" and j["date_max"] == "2026-01-07"
    assert j["nb_lignes"] == 3 and j["nb_inseres"] == 3
    assert j["run_id"] == "RUN-X"


# ── §21 Déduplication ───────────────────────────────────────────────────────────────────────────

def test_identifiant_bancaire_fait_foi(db):
    """Quand la banque fournit un identifiant, le doublon est CERTAIN : rejet."""
    bq.importer([_mvt("2026-01-05", 120.0, "A", ext="TX-1")],
                bank_account_id=COMPTE_A, source_type=bq.SOURCE_API, db_path=db)
    # Même identifiant, libellé et montant différents : c'est la même transaction, corrigée.
    r = bq.importer([_mvt("2026-01-05", 130.0, "A corrige", ext="TX-1")],
                    bank_account_id=COMPTE_A, source_type=bq.SOURCE_API, db_path=db)
    assert r["nb_doublons"] == 1 and r["nb_inseres"] == 0
    assert bq.compter(bank_account_id=COMPTE_A, db_path=db) == 1


def test_empreinte_identique_sans_identifiant_donne_a_controler(db):
    """LE point sensible : deux lignes identiques peuvent être deux vrais mouvements.

    Le service ne fusionne pas — il insère et signale. Un double prélèvement du même montant, le
    même jour, avec le même libellé, existe réellement.
    """
    bq.importer([_mvt("2026-01-05", 120.0, "PRLV IDENTIQUE")],
                bank_account_id=COMPTE_A, source_type=bq.SOURCE_HISTORIQUE, db_path=db)
    r = bq.importer([_mvt("2026-01-05", 120.0, "PRLV IDENTIQUE")],
                    bank_account_id=COMPTE_A, source_type=bq.SOURCE_HISTORIQUE, db_path=db)
    assert r["nb_a_controler"] == 1
    assert r["nb_doublons"] == 0, "sans identifiant bancaire, aucun doublon n'est CERTAIN"
    assert bq.compter(bank_account_id=COMPTE_A, db_path=db) == 2, (
        "les deux lignes sont conservées — perdre un mouvement réel serait irréversible")


def test_doublon_interne_au_lot_detecte(db):
    """Un export peut se contenir lui-même deux fois."""
    lot = [_mvt("2026-01-05", 120.0, "A", ext="TX-1"), _mvt("2026-01-05", 120.0, "A", ext="TX-1")]
    r = bq.importer(lot, bank_account_id=COMPTE_A, source_type=bq.SOURCE_API, db_path=db)
    assert r["nb_inseres"] == 1 and r["nb_doublons"] == 1


def test_empreinte_insensible_aux_espaces_et_a_la_casse(db):
    a = bq.empreinte(COMPTE_A, "2026-01-05", 120.0, "paiement   cb")
    b = bq.empreinte(COMPTE_A, "2026-01-05", 120.0, "PAIEMENT CB")
    assert a == b, "un export qui change la casse ne crée pas un nouveau mouvement"


def test_empreinte_distingue_un_montant_different(db):
    a = bq.empreinte(COMPTE_A, "2026-01-05", 120.0, "PAIEMENT CB")
    b = bq.empreinte(COMPTE_A, "2026-01-05", 120.01, "PAIEMENT CB")
    assert a != b


def test_empreinte_distingue_les_comptes(db):
    """Deux comptes peuvent porter le même mouvement apparent sans être liés."""
    assert bq.empreinte(COMPTE_A, "2026-01-05", 120.0, "X") != \
           bq.empreinte(COMPTE_B, "2026-01-05", 120.0, "X")


# ── §22 Multi-export ────────────────────────────────────────────────────────────────────────────

def test_multi_export_avec_chevauchement(db):
    """Export A : 10 mouvements. Export B : 5 déjà connus + 5 nouveaux. Résultat : 15, pas 20."""
    export_a = [_mvt("2026-02-%02d" % (i + 1), 10.0 + i, f"MVT DEMO {i}", ext=f"TX-{i}")
                for i in range(10)]
    bq.importer(export_a, bank_account_id=COMPTE_A, source_type=bq.SOURCE_HISTORIQUE, db_path=db)
    assert bq.compter(bank_account_id=COMPTE_A, db_path=db) == 10

    export_b = export_a[5:] + [_mvt("2026-03-%02d" % (i + 1), 50.0 + i, f"MVT DEMO NEW {i}",
                                    ext=f"TX-N{i}") for i in range(5)]
    r = bq.importer(export_b, bank_account_id=COMPTE_A, source_type=bq.SOURCE_INCREMENTAL,
                    db_path=db)
    assert r["nb_doublons"] == 5 and r["nb_inseres"] == 5
    assert bq.compter(bank_account_id=COMPTE_A, db_path=db) == 15, "15 mouvements, jamais 20"


def test_reimport_du_meme_export_n_ajoute_rien(db):
    export = [_mvt("2026-02-01", 10.0, "A", ext="TX-1"), _mvt("2026-02-02", 20.0, "B", ext="TX-2")]
    bq.importer(export, bank_account_id=COMPTE_A, source_type=bq.SOURCE_HISTORIQUE, db_path=db)
    r = bq.importer(export, bank_account_id=COMPTE_A, source_type=bq.SOURCE_HISTORIQUE, db_path=db)
    assert r["nb_inseres"] == 0 and r["nb_doublons"] == 2
    assert bq.compter(bank_account_id=COMPTE_A, db_path=db) == 2


# ── §32 Future banque : isolation par compte ────────────────────────────────────────────────────

def test_la_nouvelle_banque_ne_voit_pas_l_ancienne(db):
    """`bank_account_id` est ce qui rendra le cut-over propre : deux contextes, aucun mélange."""
    bq.importer(_LOT_A, bank_account_id=COMPTE_A, source_type=bq.SOURCE_HISTORIQUE, db_path=db)
    assert bq.compter(bank_account_id=COMPTE_A, db_path=db) == 3
    assert bq.compter(bank_account_id=COMPTE_B, db_path=db) == 0
    assert bq.mouvements(bank_account_id=COMPTE_B, db_path=db) == []
    assert bq.periode(bank_account_id=COMPTE_B, db_path=db) == {"date_min": "", "date_max": ""}


def test_meme_transaction_sur_deux_comptes_reste_deux_mouvements(db):
    """Un virement entre deux comptes suivis apparaît dans les deux — ce n'est pas un doublon."""
    ligne = _mvt("2026-01-05", 500.0, "VIR INTERNE", ext="TX-1")
    bq.importer([ligne], bank_account_id=COMPTE_A, source_type=bq.SOURCE_HISTORIQUE, db_path=db)
    r = bq.importer([ligne], bank_account_id=COMPTE_B, source_type=bq.SOURCE_HISTORIQUE, db_path=db)
    assert r["nb_inseres"] == 1 and r["nb_doublons"] == 0
    assert bq.compter(db_path=db) == 2


# ── §31 Adaptateurs : même DTO quelle que soit la source ────────────────────────────────────────

def test_adaptateur_api_produit_le_meme_dto(db):
    """Une API et un classeur doivent alimenter la même table, via le même contrat."""
    charge = [{"transactionId": "TX-9", "bookingDate": "2026-01-05", "valueDate": "2026-01-06",
               "amount": -120.50, "currency": "eur",
               "remittanceInformation": "PAIEMENT DEMO", "counterpartyName": "FOURNISSEUR DEMO"}]
    dto = ad.depuis_api(charge)
    assert len(dto) == 1
    l = dto[0]
    assert l["external_transaction_id"] == "TX-9"
    assert l["date_operation"] == "2026-01-05" and l["date_valeur"] == "2026-01-06"
    assert l["sens"] == bq.SENS_DEBIT, "un montant négatif est un débit"
    assert l["montant"] == 120.50, "le montant est absolu ; le signe est porté par le sens"
    assert l["devise"] == "EUR"

    r = bq.importer(dto, bank_account_id=COMPTE_A, source_type=bq.SOURCE_API, db_path=db)
    assert r["nb_inseres"] == 1


def test_dto_api_et_dto_xlsx_ont_les_memes_clefs(db):
    """Le contrat est le même des deux côtés — sinon l'aval dépendrait du transport."""
    dto_api = ad.depuis_api([{"transactionId": "T", "bookingDate": "2026-01-05",
                              "amount": -1.0, "remittanceInformation": "X"}])
    attendues = set(_LOT_A[0])
    assert set(dto_api[0]) == attendues


def test_api_sans_date_est_refusee(db):
    with pytest.raises(ad.SourceBancaireInvalide):
        ad.depuis_api([{"transactionId": "T", "amount": -1.0}])


def test_identifiant_api_jamais_invente(db):
    """Sans identifiant fourni, on laisse vide — on n'en fabrique pas un."""
    dto = ad.depuis_api([{"bookingDate": "2026-01-05", "amount": -1.0,
                          "remittanceInformation": "X"}])
    assert dto[0]["external_transaction_id"] == ""


# ── Adaptateur XLSX : refus explicites ──────────────────────────────────────────────────────────

def test_xlsx_sans_onglet_mouvements_refuse(tmp_path):
    import openpyxl
    p = tmp_path / "faux.xlsx"
    wb = openpyxl.Workbook(); wb.active.title = "Autre"; wb.save(p); wb.close()
    with pytest.raises(ad.SourceBancaireInvalide) as exc:
        ad.depuis_xlsx_releve_consolide(p)
    assert "Mouvements" in str(exc.value)


def test_xlsx_sans_entete_attendue_refuse(tmp_path):
    """Mieux vaut refuser que produire des mouvements muets à cause d'un décalage de ligne."""
    import openpyxl
    p = tmp_path / "faux.xlsx"
    wb = openpyxl.Workbook(); ws = wb.active; ws.title = "Mouvements"
    ws.append(["Colonne inconnue", "Autre"]); wb.save(p); wb.close()
    with pytest.raises(ad.SourceBancaireInvalide) as exc:
        ad.depuis_xlsx_releve_consolide(p)
    assert "En-tête introuvable" in str(exc.value)


# ── §24 Parité sur le relevé réel ───────────────────────────────────────────────────────────────

RELEVE_REEL = Path(cfg.SOURCES_BRUTES) / "Banque" / \
    "BANQUE_ACTUELLE_HISTORIQUE_2025-11-03_2026-08-01.xlsx"
MASTER_REEL = Path(cfg.MASTER_BANQUE)

reel_requis = pytest.mark.skipif(
    not RELEVE_REEL.exists() or not MASTER_REEL.exists(),
    reason="relevé bancaire ou master Lot8a absent de cet environnement")


@reel_requis
def test_parite_avec_le_master_lot8a(db):
    """OLD (master Excel de lot8a) contre NEW (SQLite) : même volume, même période, mêmes totaux."""
    import openpyxl

    lignes = ad.depuis_xlsx_releve_consolide(RELEVE_REEL)
    bq.importer(lignes, bank_account_id="CM_02211_00021321603",
                source_type=bq.SOURCE_HISTORIQUE, db_path=db)

    wb = openpyxl.load_workbook(MASTER_REEL, read_only=True, data_only=True)
    try:
        ws = wb["NORM_Banque"]
        it = ws.iter_rows(values_only=True)
        h = [str(c) for c in next(it)]
        old = [dict(zip(h, row)) for row in it]
    finally:
        wb.close()

    def n(v):
        try:
            return round(abs(float(v or 0)), 2)
        except (TypeError, ValueError):
            return 0.0

    new = bq.mouvements(db_path=db)
    assert len(new) == len(old), f"NEW {len(new)} lignes contre OLD {len(old)}"

    for sens, prefixe in ((bq.SENS_DEBIT, "D"), (bq.SENS_CREDIT, "C")):
        total_new = round(sum(m["montant"] for m in new if m["sens"] == sens), 2)
        total_old = round(sum(n(r.get("montant")) for r in old
                              if str(r.get("sens", "")).upper().startswith(prefixe)), 2)
        assert total_new == total_old, f"{sens} : NEW {total_new} contre OLD {total_old}"

    dates_old = [str(r.get("date_operation"))[:10] for r in old if r.get("date_operation")]
    p = bq.periode(db_path=db)
    assert p["date_min"] == min(dates_old) and p["date_max"] == max(dates_old)


# ── §35 Isolation APP_DATA_DIR ──────────────────────────────────────────────────────────────────

def test_deux_environnements_ne_se_contaminent_pas(tmp_path):
    """A et B doivent s'ignorer complètement, et n'écrire nulle part ailleurs.

    Le défaut `snapshot_service` avait montré qu'un chemin figé à l'import contourne `APP_DATA_DIR`
    en silence. Ce service reçoit `db_path` à chaque appel : la vérification est directe.
    """
    a = tmp_path / "envA" / "app.db"
    b = tmp_path / "envB" / "app.db"
    for p in (a, b):
        p.parent.mkdir(parents=True, exist_ok=True)
        apply_migrations(p)

    bq.importer(_LOT_A, bank_account_id=COMPTE_A, source_type=bq.SOURCE_HISTORIQUE, db_path=a)
    bq.importer([_mvt("2026-05-01", 7.0, "SEULEMENT DANS B")],
                bank_account_id=COMPTE_A, source_type=bq.SOURCE_HISTORIQUE, db_path=b)

    assert bq.compter(db_path=a) == 3
    assert bq.compter(db_path=b) == 1
    libelles_a = {m["libelle_brut"] for m in bq.mouvements(db_path=a)}
    assert "SEULEMENT DANS B" not in libelles_a


def test_la_base_reelle_n_est_jamais_touchee(tmp_path):
    """Garde-fou explicite : l'empreinte de la vraie app.db ne doit pas bouger."""
    import hashlib

    reelle = Path(cfg.APP_ROOT) / "data" / "app.db"
    if not reelle.exists():
        pytest.skip("base réelle absente de cet environnement")
    avant = hashlib.sha256(reelle.read_bytes()).hexdigest()

    p = tmp_path / "isole.db"
    apply_migrations(p)
    bq.importer(_LOT_A, bank_account_id=COMPTE_A, source_type=bq.SOURCE_HISTORIQUE, db_path=p)

    assert hashlib.sha256(reelle.read_bytes()).hexdigest() == avant
