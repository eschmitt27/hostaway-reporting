"""Reprise de solde de l'ancienne structure — Marilyne (Maryline UZON, PROP_0011), 700 €.

Même mécanisme que Didier (PROP_0001, 300 €, 2026-10-03) : `credits_clients_service.
creer_reprise_solde`, aucune logique nouvelle. Base temporaire, données fictives ; Didier est
présent avec sa propre reprise pour prouver qu'il n'est pas touché.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import compte_proprietaire_service as cpt
from app.services import creances_dettes_service as creances
from app.services import creances_reglement_service as reglement
from app.services import credits_clients_service as credits
from app.services import factures_proprietaires_service as svc

EMETTEUR = {"nom": "Conciergerie T", "adresse": "1 rue T", "siret": "00000000000000"}
DEST = {"nom": "Maryline T", "adresse": "2 rue T"}
DIDIER, MARILYNE = "PROP_0001", "PROP_0011"
DATE_REPRISE = "2026-09-01"          # même date d'origine que la reprise de Didier

@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    monkeypatch.setattr(cfg, "BACKUPS_DIR", tmp_path / "backups", raising=False)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True, raising=False)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True, raising=False)
    apply_migrations(chemin)
    conn = get_db(chemin)
    conn.execute("INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
                 "empreinte_source, statut, nb_feuilles, nb_lignes) "
                 "VALUES ('IMP-T','2026-09-01T00:00:00','x','x','IMPORTE',1,1)")
    for pid, prenom in ((DIDIER, "Didier"), (MARILYNE, "Maryline")):
        conn.execute("INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
                     "prenom_proprietaire, actif, import_id) VALUES (?, 'UZON', ?, 'OUI', 'IMP-T')",
                     (pid, prenom))
    conn.commit()
    conn.close()
    assert credits.creer_reprise_solde(DIDIER, 300, DATE_REPRISE, acteur="t", db_path=chemin)["ok"]
    return chemin


def _reprise_marilyne(db):
    r = credits.creer_reprise_solde(MARILYNE, 700, DATE_REPRISE, acteur="t", db_path=db)
    assert r["ok"], r
    return r


def _solde(db, compte, auxiliaire=None, pieces=None):
    sql = ("SELECT COALESCE(SUM(l.debit),0) - COALESCE(SUM(l.credit),0) FROM ecriture_lignes l "
           "JOIN ecritures e ON e.ecriture_id_opaque = l.ecriture_id_opaque "
           "WHERE e.statut <> 'ANNULEE' AND l.compte = ?")
    args = [compte]
    if auxiliaire:
        sql += " AND l.auxiliaire = ?"
        args.append(auxiliaire)
    if pieces:
        sql += f" AND e.ecriture_id_opaque IN ({','.join('?' * len(pieces))})"
        args += pieces
    conn = get_db(db)
    try:
        return round(conn.execute(sql, args).fetchone()[0], 2)
    finally:
        conn.close()


def _etat_didier(db):
    pos = cpt.position(DIDIER, db_path=db)
    return (pos["credit_disponible"], pos["creance_restante"], _solde(db, "419700", DIDIER),
            [(c["credit_id_opaque"], c["reste"]) for c in credits.lister(proprietaire_id=DIDIER,
                                                                        db_path=db)])


def test_credit_et_ecritures_marilyne(db):
    r = _reprise_marilyne(db)
    # 1. crédit disponible 700 €, même origine et même libellé que Didier
    c = credits.lister(proprietaire_id=MARILYNE, db_path=db)
    assert len(c) == 1 and c[0]["reste"] == 700 and c[0]["origine"] == credits.ORIGINE_REPRISE_SOLDE
    assert c[0]["reference"] == "Solde créditeur repris de l'ancienne structure"
    ecr = r["ecritures"]
    assert len(ecr) == 2 and all(compta.charger(e, db)["statut"] == compta.ST_VALIDEE for e in ecr)
    # 2. 654000 débité de 700 € ; 3. 419700 crédité de 700 € (auxiliaire Marilyne)
    assert _solde(db, "654000", pieces=ecr) == 700
    assert _solde(db, "419700", MARILYNE) == -700
    assert _solde(db, "419100", MARILYNE) == 0
    # 4. 467100 : aucun solde résiduel lié à l'opération (ni au total)
    assert _solde(db, "467100", pieces=ecr) == 0
    assert _solde(db, "467100") == 0
    # pas de créance artificielle : aucune ligne 411 pour Marilyne
    assert _solde(db, "411000", MARILYNE) == 0
    # 9. base temporaire uniquement (la garde de conftest refuse toute ouverture de la vraie base)
    assert Path(cfg.DB_PATH) == db


def test_meme_credit_compte_proprietaire_et_creances(db):
    _reprise_marilyne(db)
    # 5. compte propriétaire / client et Créances & Dettes lisent la même position
    pos = cpt.position(MARILYNE, db_path=db)
    assert pos["credit_disponible"] == 700 and pos["creance_restante"] == 0
    assert pos["etat_compte"] == cpt.POS_CREDITEUR
    ligne = next(p for p in reglement.positions(inclure_soldes=True, db_path=db)
                 if p["proprietaire_id"] == MARILYNE)
    assert ligne["credit_disponible"] == 700 and ligne["restant_du"] == 0
    assert creances.synthese(db_path=db)["creances"]["total"] == 0


def test_imputation_sur_facture_comme_didier(db):
    _reprise_marilyne(db)
    didier = _etat_didier(db)
    # 6. facture de 250 € émise : le crédit s'impute à l'émission, reliquat conservé
    fid = svc.creer({"mois": "2026-09", "proprietaire_id": MARILYNE, "logement_id": "LOG_M",
                     "source_calcul": "PREF-M", "COMMISSION_CONCIERGERIE": 250,
                     "montant_du_conciergerie": 250}, db_path=db)["facture_id_opaque"]
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DEST, db_path=db)
    f = svc.emettre(fid, emetteur=EMETTEUR, destinataire=DEST, date_facture="2026-10-04", db_path=db)
    compta.comptabiliser_facture_emise(f, acteur="t", db_path=db)
    assert svc.lire(fid, db_path=db)["montant_total"] == 250
    pos = cpt.position(MARILYNE, db_path=db)
    assert pos["credit_disponible"] == 450 and pos["creance_restante"] == 0
    assert _solde(db, "411000", MARILYNE) == 0
    assert _solde(db, "419700", MARILYNE) == -450
    # 7. aucun double comptage : rejouer la comptabilisation et la reprise ne change rien
    compta.comptabiliser_facture_emise(svc.lire(fid, db_path=db), acteur="t", db_path=db)
    assert credits.creer_reprise_solde(MARILYNE, 700, DATE_REPRISE, acteur="t",
                                       db_path=db)["code"] == credits.E_REPRISE_EXISTANTE
    assert cpt.position(MARILYNE, db_path=db)["credit_disponible"] == 450
    assert _solde(db, "419700", MARILYNE) == -450
    assert _solde(db, "654000") == 1000                  # 300 Didier + 700 Marilyne, une fois
    # 8. Didier inchangé
    assert _etat_didier(db) == didier == (300, 0, -300, didier[3])
