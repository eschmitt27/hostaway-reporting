"""Impact Lot9 : les mouvements produits par l'import applicatif face au filtre RÉEL du Lot 9.

Le filtre est celui de `lot9_construire_flux.py` (ligne 126 :
`bnq_valide = [r for r in bnq_all if r.get('type_flux_id') == 'TYPE_FLUX_016' and
r.get('statut_controle') == 'VALIDE']`), reproduit ici verbatim — pas de pandas requis pour cette
ligne, donc exécutable dans cet environnement contrairement au script complet.

Ce que ça prouve :
- un mouvement fraîchement importé n'est JAMAIS repris par Lot9 tant qu'il n'a pas été classé puis
  validé — pas d'impact financier silencieux d'un import brut ;
- seuls les frais bancaires (`TYPE_FLUX_016`) VALIDE entrent dans Lot9 ; un payout plateforme ou un
  reversement propriétaire n'y entrent jamais par ce chemin, même validés. Ce sont d'autres modules
  (Hostaway / hors Hostaway, règlements propriétaire) qui portent ces flux — jamais la banque, faute
  de quoi le même euro serait compté deux fois.

CE QUI A CHANGÉ AVEC LA MIGRATION
Le brut ne porte plus de statut. L'import écrivait auparavant `EN_ATTENTE_CLASSIFICATION` dans une
colonne de `NORM_Banque` ; désormais un mouvement non classé n'a simplement PAS de classification, et
son `statut_controle` est vide. Le résultat vis-à-vis de Lot 9 est identique — il n'entre pas — mais
l'absence est exprimée par une absence, non par un statut d'attente écrit dans la source.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import banque_classification_service as cls
from app.services import banque_vues_service as vues
from app.services import banques_import_service as svc

CSV_MIXTE = (
    "Date operation;Libelle;Debit;Credit\n"
    "05/06/2026;VIR HOSTAWAY PAYOUT;;850,00\n"      # encaissement réservation/payout -> jamais BNQ
    "10/06/2026;VIR PROPRIETAIRE PROP A;400,00;\n"   # reversement propriétaire -> jamais BNQ
    "15/06/2026;FRAIS TENUE DE COMPTE;8,90;\n"       # frais bancaires -> seul cas BNQ
).encode("utf-8")

# Type de flux attribué à chaque libellé une fois classé. Reprend le partage réel : seuls les frais
# bancaires relèvent de la banque ; le reste est porté par un autre module.
TYPES_APRES_CLASSIFICATION = {
    "FRAIS TENUE DE COMPTE": "TYPE_FLUX_016",
    "VIR HOSTAWAY PAYOUT": "TYPE_FLUX_017",       # jamais BNQ : porté par Hostaway
    "VIR PROPRIETAIRE PROP A": "TYPE_FLUX_007",   # reversement propriétaire : jamais BNQ
}

COLONNES_BRUT = ("mouvement_id_opaque", "montant", "sens", "libelle_brut", "fingerprint")


def _bnq_valide(rows: list[dict]) -> list[dict]:
    """Filtre copié verbatim depuis lot9_construire_flux.py:126 — jamais réécrit ici."""
    return [r for r in rows if r.get("type_flux_id") == "TYPE_FLUX_016"
            and r.get("statut_controle") == "VALIDE"]


@pytest.fixture
def ref(tmp_db, tmp_path, monkeypatch):
    """Base isolée + flags d'écriture. Aucun classeur : l'import écrit en base."""
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_path / "CLASSEUR_ABSENT.xlsx")
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", tmp_path.resolve())
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return tmp_db


def _lignes(db_path=None) -> list[dict]:
    """Mouvements tels que le reste de l'application les voit."""
    return vues.mouvements_normalises(db_path=db_path)


def _brut(db) -> list[tuple]:
    conn = get_db(db)
    try:
        return conn.execute(
            f"SELECT {', '.join(COLONNES_BRUT)} FROM banque_mouvements "
            "ORDER BY mouvement_id_opaque").fetchall()
    finally:
        conn.close()


def _importer(db) -> None:
    prev = svc.previsualiser(CSV_MIXTE, "releve.csv", "CM_TEST")
    assert prev["ok"], prev
    res = svc.confirmer(prev["token"], acteur="recette", db_path=db)
    assert res["ok"], res


def _classer_et_valider(db) -> None:
    """Écrit la classification que lot8b produirait, puis la validation humaine.

    En SQL direct plutôt que par `cls.classer()` : celui-ci exige un référentiel de règles, et ce test
    ne porte pas sur les règles mais sur ce que Lot 9 retient de leur résultat.
    """
    conn = get_db(db)
    try:
        for mid, libelle in conn.execute(
                "SELECT mouvement_id_opaque, libelle_brut FROM banque_mouvements").fetchall():
            conn.execute(
                "INSERT INTO banque_classifications (mouvement_id_opaque, classification_run_id, "
                "regle_id, categorie, tiers_detecte, type_flux_id, code_impact, source_economique, "
                "statut_controle, statut_classification, niveau_risque, rapprochement_requis) "
                "VALUES (?, 'CLS-LOT9-TEST', 'R_TEST', '', '', ?, '', ?, 'VALIDE', ?, 'FAIBLE', '')",
                (mid, TYPES_APRES_CLASSIFICATION[libelle], cls.SOURCE_REGLE, cls.CLASS_CLASSE))
        conn.commit()
    finally:
        conn.close()


def test_import_brut_nentre_jamais_dans_lot9(ref):
    """Un mouvement importé mais non classé est exclu du filtre Lot9, quel que soit son type."""
    _importer(ref)

    rows = _lignes(ref)
    assert len(rows) == 3
    assert all(not r["statut_controle"] for r in rows), "le brut ne porte aucun statut de contrôle"
    assert all(not r["type_flux_id"] for r in rows), "le brut ne porte aucun type de flux"
    assert _bnq_valide(rows) == []


def test_seuls_les_frais_bancaires_valides_entrent_dans_lot9(ref):
    """Une fois classés et validés, seul TYPE_FLUX_016 est repris par Lot9."""
    _importer(ref)
    _classer_et_valider(ref)

    rows = _lignes(ref)
    assert len(rows) == 3
    bnq = _bnq_valide(rows)
    assert len(bnq) == 1
    assert bnq[0]["libelle"] == "FRAIS TENUE DE COMPTE"
    assert bnq[0]["montant"] == 8.90

    # Le payout et le reversement propriétaire, même VALIDE, n'entrent jamais dans Lot9 par ce
    # chemin : aucun double comptage avec les flux Hostaway / hors Hostaway ou les règlements.
    libelles_bnq = {r["libelle"] for r in bnq}
    assert "VIR HOSTAWAY PAYOUT" not in libelles_bnq
    assert "VIR PROPRIETAIRE PROP A" not in libelles_bnq


def test_le_brut_reste_intact_apres_classification(ref):
    """Classer n'altère pas ce que la banque a envoyé — c'est ce qui rend deux exécutions comparables."""
    _importer(ref)
    avant = _brut(ref)
    _classer_et_valider(ref)
    assert _brut(ref) == avant
