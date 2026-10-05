"""Aide de test — marquer les modules d'un mois comme clôturés.

La clôture du MOIS exige que tous ses modules soient clôturés (clôture par modules). Les tests qui
exercent l'archivage lui-même (archive économique, protections après clôture, réversibilité…) n'ont
pas à rejouer la clôture module par module : ils posent l'état « tous les modules clôturés » ici,
directement, puis vérifient ce qui les intéresse. Les tests de la clôture par modules, eux, passent
par le service (`tests/test_cloture_modules_parcours.py`).
"""
from __future__ import annotations

from app.db.connection import get_db


def clore_modules(db_path, mois: str, *, acteur: str = "TEST", sauf: tuple[str, ...] = ()) -> None:
    """Tous les modules du mois passent à CLOS (sauf ceux de `sauf`). La clôture du mois doit exister.

    La Comptabilité se verrouille par sa période comptable : elle est clôturée aussi (l'état posé est
    celui que la clôture réelle d'un module laisse). Une écriture ne peut donc plus naître sur ce mois :
    un test qui doit en créer une le fait AVANT d'appeler cette aide."""
    from app.services import cloture_modules_service as cm
    from app.services import clotures_service as cs

    c = cs.charger_par_mois(mois, db_path)
    assert c is not None, f"aucune clôture ouverte pour {mois}"
    conn = get_db(db_path)
    try:
        for m in cm.MODULES:
            if m.cle in sauf:
                continue
            conn.execute(
                "INSERT INTO cloture_modules (mois, module, cloture_id_opaque, statut, date_cloture, "
                "acteur_cloture, nb_clotures) VALUES (?,?,?,'CLOS',?,?,1) "
                "ON CONFLICT(mois, module) DO UPDATE SET statut='CLOS'",
                (mois, m.cle, c["cloture_id_opaque"], "2026-01-01 00:00:00", acteur))
            if m.cle == cm.COMPTABILITE:
                conn.execute(
                    "INSERT INTO periodes_comptables (periode, statut) VALUES (?, 'CLOTUREE') "
                    "ON CONFLICT(periode) DO UPDATE SET statut='CLOTUREE'", (mois,))
        conn.commit()
    finally:
        conn.close()
