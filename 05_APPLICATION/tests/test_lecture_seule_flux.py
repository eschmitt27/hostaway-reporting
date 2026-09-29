"""Mission 35 — une consultation n'écrit rien : GET Flux ≠ recalcul persistant FIFO.

L'incident qui motive ces tests (Mission 34) : un audit « en lecture » a appelé
`flux.mouvements()` avec les propositions de rapprochement ; la chaîne
propositions → objets rapprochables → créances propriétaires → `recalculer_tous` a régénéré
`proprietaire_allocations` et ajouté des lignes à `proprietaire_recalculs` dans la base réelle.

Ce qui est garanti ici, par empreinte de TOUTES les tables de la base (pas seulement de celles
qu'on soupçonne) :
    · chaque écran de consultation — Flux, mouvement, propositions, prévisualisation, clôture,
      créances, comptes propriétaires, exports — laisse la base identique ;
    · les fonctions de lecture appelées par l'audit de la Mission 34 aussi ;
    · deux consultations successives donnent la même page et la même base ;
    · les écritures métier qui changent une entrée du FIFO, elles, persistent toujours les
      allocations et le journal (émission de facture, validation / annulation d'un mouvement,
      lettrage Flux et son annulation).

Données FICTIVES uniquement (base temporaire).
"""
from __future__ import annotations

import hashlib
import sqlite3
import uuid

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import clotures_service as cs
from app.services import cloture_flux_service as cf
from app.services import compte_proprietaire_service as cpt
from app.services import comptabilite_ecritures_service as compta
from app.services import creances_dettes_service as cd
from app.services import flux_financiers_service as flux
from app.services import flux_lettrage_service as lettrage
from app.services import flux_matching_service as matching
from app.services import proprietaires_tresorerie_service as tres
from app.services import qonto_suggestions_service as suggestions
from tests.test_flux_financiers import (ACTEUR, PROPRIO, _charge, _importer, _mvt, _par_montant,
                                        base, verrous)  # noqa: F401 — fixtures

# Tables dont une écriture cachée serait un défaut métier. L'empreinte couvre TOUTE la base ;
# cette liste vérifie seulement que les noms réels existent (un renommage ne doit pas rendre le
# test aveugle).
SURVEILLEES = ("proprietaire_allocations", "proprietaire_recalculs", "banque_rapprochements",
               "ecritures", "ecriture_lignes", "charges", "flux_lettrages",
               "qonto_transactions_statut_local", "mouvements_tresorerie_proprietaires",
               "factures_proprietaires")


def _empreintes(db) -> dict[str, str]:
    """Empreinte de chaque table, lue en lecture seule (la mesure elle-même n'écrit pas)."""
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        out = {}
        for (t,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' "
                                 "AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall():
            h = hashlib.sha256()
            for ligne in sorted(repr(tuple(r)) for r in conn.execute(f'SELECT * FROM "{t}"')):
                h.update(ligne.encode())
            out[t] = h.hexdigest()
        return out
    finally:
        conn.close()


def _ecarts(avant: dict, apres: dict) -> list[str]:
    return sorted(t for t in set(avant) | set(apres) if avant.get(t) != apres.get(t))


def _compter(db, table, where="1=1") -> int:
    conn = get_db(db)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}").fetchone()[0]
    finally:
        conn.close()


def _facture_emise(db, montant, date_emission, numero):
    """Facture propriétaire ÉMISE, vente 411/706 constatée — une créance réelle du FIFO."""
    fid = "FPR-" + uuid.uuid4().hex[:8].upper()
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO factures_proprietaires (facture_id_opaque, numero_facture, type_document, "
            "proprietaire_id, logement_id, mois, montant_total, statut, date_emission, "
            "date_facture) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (fid, numero, "FACTURE", PROPRIO, "LOG_TFLUX", date_emission[:7], montant, "EMIS",
             date_emission, date_emission))
        conn.commit()
    finally:
        conn.close()
    compta._inserer_ecriture("VENTES", date_emission, date_emission[:7], numero, "Vente test",
                             "FACTURE_PROPRIETAIRE", fid,
                             [{"compte": "411000", "debit": montant, "credit": 0,
                               "auxiliaire": PROPRIO},
                              {"compte": "706000", "debit": 0, "credit": montant}], db_path=db)
    return fid


def _mouvement_valide(db, montant, date_mouvement):
    """Source FIFO insérée DIRECTEMENT (hors workflow) : la persistance est donc en retard."""
    mid = "MTP-" + uuid.uuid4().hex[:8].upper()
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO mouvements_tresorerie_proprietaires (mouvement_opaque, proprietaire_id, "
            "date_mouvement, montant, sens, nature, statut) VALUES (?,?,?,?,?,?,?)",
            (mid, PROPRIO, date_mouvement, montant, "PROPRIETAIRE_VERS_SOCIETE",
             "ACOMPTE_PROPRIETAIRE", "VALIDE"))
        conn.commit()
    finally:
        conn.close()
    return mid


@pytest.fixture
def scene(base, verrous):
    """Un propriétaire avec deux créances et un acompte (FIFO réel), une allocation déjà
    persistée, un virement reçu qui correspond à la seconde facture, une dépense et sa charge :
    de quoi produire des propositions de rapprochement des deux sens."""
    f1 = _facture_emise(base, 500.0, "2026-08-31", "2026-08-901")
    f2 = _facture_emise(base, 300.0, "2026-09-05", "2026-09-902")
    _mouvement_valide(base, 200.0, "2026-09-02")
    cpt.recalculer(PROPRIO, db_path=base)     # état persisté de départ, comme en base réelle
    mvts = _importer(base, [_mvt(300.0, sens="credit", contrepartie="Claire Testeur",
                                 libelle="VIR 2026-09-902"),
                            _mvt(42.0, contrepartie="MAGASIN TEST", libelle="Achat test")])
    _charge(base, 42.0)
    credit = _par_montant(base, 300.0)
    debit = _par_montant(base, 42.0)
    assert mvts and credit and debit
    return {"db": base, "f1": f1, "f2": f2, "credit": credit, "debit": debit}


# ══ Le détecteur est sensible ═══════════════════════════════════════════════════════════════════

def test_00_temoin_l_empreinte_voit_un_recalcul_persistant(scene):
    """Sans ce témoin, un test « rien n'a changé » pourrait passer faute de voir : le recalcul
    persistant (ce que faisait l'affichage avant la Mission 35) change bien l'empreinte."""
    db = scene["db"]
    avant = _empreintes(db)
    assert set(SURVEILLEES) <= set(avant), "une table surveillée a changé de nom"
    cpt.recalculer_tous(db_path=db)
    assert {"proprietaire_allocations", "proprietaire_recalculs"} <= set(
        _ecarts(avant, _empreintes(db)))


# ══ GET = zéro écriture ══════════════════════════════════════════════════════════════════════════

def _urls(s) -> list[str]:
    c, d = s["credit"]["id"], s["debit"]["id"]
    return [
        "/flux-financiers",
        "/flux-financiers/banque",
        f"/flux-financiers/banque/{c}",
        f"/flux-financiers/banque/{d}",
        "/flux-financiers/caisse",
        "/flux-financiers/charges",
        "/flux-financiers/rapprochement",
        f"/flux-financiers/rapprochement/valider?m=BANQUE:{c}&o=FACTURE_PROPRIETAIRE:{s['f2']}",
        "/banques-caisse",
        "/banques-caisse/a-rapprocher",
        f"/banques-caisse/qonto/{c}/traiter",
        "/banques-caisse/export.csv",
        "/creances",
        "/dettes",
        "/echeancier",
        "/comptes-proprietaires",
        f"/comptes-proprietaires/{PROPRIO}",
        "/clotures",
        "/clotures/mois",
        "/clotures/mois/2026-09",
        "/clotures/mois/2026-08",
        "/controles-cloture/mois/2026-09",
        "/pilotage-mensuel",
        "/exports",
    ]


def test_01_chaque_ecran_de_consultation_laisse_la_base_identique(client, scene):
    db = scene["db"]
    fautifs = []
    for url in _urls(scene):
        avant = _empreintes(db)
        r = client.get(url)
        assert r.status_code == 200, f"{url} → {r.status_code}"
        ecarts = _ecarts(avant, _empreintes(db))
        if ecarts:
            fautifs.append(f"{url} a écrit dans {ecarts}")
    assert not fautifs, "\n".join(fautifs)


def test_02_les_propositions_s_affichent_sans_rien_ecrire(client, scene):
    """Le cas précis de l'incident : des propositions RÉELLEMENT calculées (pas une page vide
    qui n'aurait rien eu à écrire), sur des créances dont le FIFO est non trivial."""
    db = scene["db"]
    avant = _empreintes(db)
    props = matching.propositions(db_path=db)
    cles = {f"{o['type']}:{o['id']}" for p in props for o in p["objets"]}
    assert f"FACTURE_PROPRIETAIRE:{scene['f2']}" in cles, props
    page = client.get("/flux-financiers/rapprochement").text
    assert "2026-09-902" in page
    assert _ecarts(avant, _empreintes(db)) == []


def test_03_chemin_de_l_audit_mission_34_sans_ecriture(scene):
    """Les appels exacts de l'audit qui a écrit en base réelle, plus les lectures voisines."""
    db = scene["db"]
    avant = _empreintes(db)
    mvts = flux.mouvements(db_path=db)                        # propositions calculées (défaut)
    flux.mouvements(source=flux.BANQUE, db_path=db)
    flux.objets(inclure_non_rapprochables=True, db_path=db)
    cd.creances(db_path=db)
    cd.synthese(db_path=db)
    cd.echeancier(db_path=db)
    cd.par_tiers(db_path=db)
    cpt.position(PROPRIO, db_path=db)
    cpt.imputations_detail(scene["f1"], db_path=db)
    cs.calcul_progression("2026-09", db_path=db)
    cf.analyser("2026-09", db_path=db)
    suggestions.suggerer({"sens": "credit", "montant": 300.0, "libelle": "VIR 2026-09-902",
                          "contrepartie": "Claire Testeur", "date": "2026-09-20"}, db_path=db)
    credit = next(m for m in mvts if m["id"] == scene["credit"]["id"])
    assert credit["statut_rapprochement"] == flux.MATCHE, "les propositions n'ont pas été calculées"
    assert _ecarts(avant, _empreintes(db)) == []


def test_04_deux_consultations_identiques_meme_page_meme_base(client, scene):
    db = scene["db"]
    avant = _empreintes(db)
    recalculs = _compter(db, "proprietaire_recalculs")
    ids = _compter(db, "proprietaire_allocations")
    pages = [(client.get("/flux-financiers/rapprochement").text,
              client.get(f"/flux-financiers/banque/{scene['credit']['id']}").text,
              client.get(f"/comptes-proprietaires/{PROPRIO}").text) for _ in range(2)]
    assert pages[0] == pages[1]
    assert _ecarts(avant, _empreintes(db)) == []
    assert _compter(db, "proprietaire_recalculs") == recalculs, "aucune ligne de journal"
    assert _compter(db, "proprietaire_allocations") == ids


def test_05_l_affichage_reste_juste_quand_la_persistance_est_en_retard(client, scene):
    """Une source ajoutée HORS workflow (reprise, correction directe) : l'écran la compte
    aussitôt, sans rien enregistrer ; il signale seulement que l'enregistrement est en retard.
    C'est le bouton existant « Recalculer » — une action — qui réenregistre."""
    db = scene["db"]
    assert cpt.position(PROPRIO, db_path=db)["persistance"]["a_jour"]
    assert cpt.position(PROPRIO, db_path=db)["creance_restante"] == 600.0     # 800 − 200
    _mouvement_valide(db, 100.0, "2026-09-10")
    avant = _empreintes(db)
    p = cpt.position(PROPRIO, db_path=db)
    assert p["creance_restante"] == 500.0, "l'acompte ajouté est compté sans recalcul persistant"
    assert not p["persistance"]["a_jour"]
    ligne = next(l for l in cd.creances(db_path=db) if l["facture_id_opaque"] == scene["f1"])
    assert ligne["regle"] == 300.0 and ligne["solde"] == 200.0
    assert "Les allocations enregistrées" in client.get(f"/comptes-proprietaires/{PROPRIO}").text
    assert _ecarts(avant, _empreintes(db)) == []

    client.post(f"/comptes-proprietaires/{PROPRIO}/recalculer")
    assert cpt.position(PROPRIO, db_path=db)["persistance"]["a_jour"]
    assert "Les allocations enregistrées" not in client.get(
        f"/comptes-proprietaires/{PROPRIO}").text


# ══ Les écritures métier persistent toujours ═════════════════════════════════════════════════════

def _dernier_declencheur(db) -> str:
    conn = get_db(db)
    try:
        return conn.execute("SELECT declencheur FROM proprietaire_recalculs "
                            "ORDER BY horodatage DESC, id DESC LIMIT 1").fetchone()[0]
    finally:
        conn.close()


def _alloue_sur(db, fid) -> float:
    conn = get_db(db)
    try:
        return round(conn.execute("SELECT COALESCE(SUM(montant_alloue),0) FROM "
                                  "proprietaire_allocations WHERE facture_id_opaque=?",
                                  (fid,)).fetchone()[0], 2)
    finally:
        conn.close()


def test_06_valider_puis_annuler_un_mouvement_persiste_le_fifo(client, scene):
    db = scene["db"]
    n = _compter(db, "proprietaire_recalculs")
    assert _alloue_sur(db, scene["f1"]) == 200.0
    cree = tres.creer(PROPRIO, "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE", 250.0,
                      "2026-09-15", acteur=ACTEUR, db_path=db)
    assert _compter(db, "proprietaire_recalculs") == n, "un brouillon n'est pas une source"
    assert tres.valider(cree["mouvement_opaque"], acteur=ACTEUR, db_path=db)["ok"]
    assert _compter(db, "proprietaire_recalculs") == n + 1
    assert _dernier_declencheur(db) == cpt.DECL_VALIDATION_MOUVEMENT
    assert _alloue_sur(db, scene["f1"]) == 450.0, "allocations persistées à jour"
    assert cpt.position(PROPRIO, db_path=db)["persistance"]["a_jour"]

    # Et l'affichage qui suit n'écrit plus rien.
    avant = _empreintes(db)
    client.get("/flux-financiers/rapprochement")
    client.get(f"/comptes-proprietaires/{PROPRIO}")
    assert _ecarts(avant, _empreintes(db)) == []

    assert tres.annuler(cree["mouvement_opaque"], commentaire="test", acteur=ACTEUR,
                        db_path=db)["ok"]
    assert _compter(db, "proprietaire_recalculs") == n + 2
    assert _dernier_declencheur(db) == cpt.DECL_ANNULATION_MOUVEMENT
    assert _alloue_sur(db, scene["f1"]) == 200.0


def test_07_lettrage_flux_d_une_facture_persiste_le_fifo_et_son_annulation_aussi(client, scene):
    """Le workflow réel de Flux : rapprocher le virement de 300 € de la facture 2026-09-902
    (créance servie par le vrai service, sans double) encaisse, comptabilise et persiste."""
    db = scene["db"]
    n = _compter(db, "proprietaire_recalculs")
    res = lettrage.valider([f"BANQUE:{scene['credit']['id']}"],
                           [f"FACTURE_PROPRIETAIRE:{scene['f2']}"], acteur=ACTEUR, db_path=db)
    assert res["ok"], res
    assert _compter(db, "proprietaire_recalculs") == n + 1
    assert _dernier_declencheur(db) == cpt.DECL_VALIDATION_MOUVEMENT
    # FIFO : l'encaissement solde d'abord la plus ancienne (300 restants sur 2026-08-901).
    assert _alloue_sur(db, scene["f1"]) == 500.0
    assert cpt.position(PROPRIO, db_path=db)["persistance"]["a_jour"]

    avant = _empreintes(db)
    for url in _urls(scene):
        client.get(url)
    assert _ecarts(avant, _empreintes(db)) == [], "après un workflow, l'affichage n'écrit rien"

    assert lettrage.annuler(res["lettrage_id_opaque"], motif="test", acteur=ACTEUR,
                            db_path=db)["ok"]
    assert _compter(db, "proprietaire_recalculs") == n + 2
    assert _dernier_declencheur(db) == cpt.DECL_ANNULATION_MOUVEMENT
    assert _alloue_sur(db, scene["f1"]) == 200.0


def test_08_emettre_une_facture_persiste_le_fifo(tmp_path, monkeypatch):
    """Le parcours réel d'émission (création → validation → émission) persiste le FIFO."""
    from app.services import factures_proprietaires_service as fpr
    from tests.test_creances_regle_compense import DESTINATAIRE, EMETTEUR

    db = tmp_path / "emission.db"
    monkeypatch.setattr(cfg, "DB_PATH", db, raising=False)
    for flag in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, flag, True, raising=False)
    apply_migrations(db)
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO mouvements_tresorerie_proprietaires (mouvement_opaque, proprietaire_id, "
            "date_mouvement, montant, sens, nature, statut) VALUES "
            "('MTP-EMIS01', 'PROP_T', '2026-08-01', 100, 'PROPRIETAIRE_VERS_SOCIETE', "
            "'ACOMPTE_PROPRIETAIRE', 'VALIDE')")
        conn.commit()
    finally:
        conn.close()
    f = fpr.creer({"mois": "2026-08", "proprietaire_id": "PROP_T", "logement_id": "LOG_T",
                   "source_calcul": "PREF-2026-08", "COMMISSION_CONCIERGERIE": 250.0,
                   "montant_du_conciergerie": 250.0}, acteur="t", db_path=db)
    fid = f["facture_id_opaque"]
    fpr.valider(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, acteur="t", db_path=db)
    assert _compter(db, "proprietaire_recalculs") == 0, "une facture validée n'est pas une créance"
    fpr.emettre(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, date_facture="2026-08-31",
                acteur="t", exiger_conformite=False, db_path=db)
    assert _compter(db, "proprietaire_recalculs") == 1
    assert _dernier_declencheur(db) == cpt.DECL_EMISSION_FACTURE
    assert _alloue_sur(db, fid) == 100.0, "le crédit existant solde la facture émise"

    avant = _empreintes(db)
    cd.creances(db_path=db)
    cpt.position("PROP_T", db_path=db)
    assert _ecarts(avant, _empreintes(db)) == []


def test_09_aucun_ecran_ne_produit_plus_de_recalcul_auto(client, scene):
    """« AUTO » était le déclencheur de l'affichage ; il ne doit plus apparaître."""
    db = scene["db"]
    for url in _urls(scene):
        client.get(url)
    assert _compter(db, "proprietaire_recalculs", "declencheur='AUTO'") == 0
