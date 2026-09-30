"""Mission « Résultats lisibles » — filtres, cohérence KPI / graphique / tableau, état vide, lecture seule.

Présentation uniquement : ces tests vérifient que les écrans /resultats et /resultats/pilotage
transmettent les filtres, affichent le MÊME périmètre partout et n'écrivent rien. Les montants
attendus sont ceux semés tels quels (aucun recalcul métier n'est testé ici).
Données FICTIVES uniquement (base temporaire).
"""
from __future__ import annotations

import hashlib
import json
import sqlite3

import pytest
from bs4 import BeautifulSoup

import fixtures_lot10 as fx
from app.db.connection import get_db
from app.readers import proprietaires_reglements_reader as reader
from app.services import resultats_pilotage_service as pilot
from app.template_env import _euros

MOIS = ("2026-04", "2026-05", "2026-06")
LOGEMENTS = (("LOG_RL1", "PROP_RL1", "Villa Garonne"), ("LOG_RL2", "PROP_RL2", "Studio Capitole"))


@pytest.fixture
def base(tmp_db):
    conn = get_db(tmp_db)
    try:
        for lid, pid, nom in LOGEMENTS:
            conn.execute("INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
                         "prenom_proprietaire, import_id) VALUES (?, ?, 'Test', 'IMP-RL')",
                         (pid, f"Proprio{pid[-1]}"))
            conn.execute("INSERT INTO ref_logements (logement_id, hostaway_listing_id, statut_parc, "
                         "actif, import_id, nom_court, nom_logement_officiel) "
                         "VALUES (?, ?, 'GERE', 'OUI', 'IMP-RL', ?, ?)", (lid, lid, nom, nom))
        conn.commit()
    finally:
        conn.close()
    net, com, res = [], [], []
    for i, m in enumerate(MOIS):
        for j, (lid, pid, _) in enumerate(LOGEMENTS):
            base_v = 1000.0 * (i + 1) + 100.0 * j
            net.append({"mois": m, "logement_id": lid, "proprietaire_id": pid,
                        "total_payout_mois": base_v, "montant_du_conciergerie": round(base_v * 0.3, 2),
                        "total_commission_mois": round(base_v * 0.2, 2), "total_menage_mois": 50.0,
                        "net_proprietaire_apres_charge_mois": round(base_v * 0.7, 2),
                        "nb_reservations": 2})
            com.append({"reservation_calc_id": f"R-{m}-{lid}", "logement_id": lid,
                        "proprietaire_id": pid, "mois": m,
                        "channel_type": "AIRBNBOFFICIAL" if j == 0 else "BOOKINGCOM",
                        "payout_calcule": base_v, "commission_conciergerie": round(base_v * 0.2, 2),
                        "menage_retenu": 50.0})
            res.append({"mois": m, "logement_id": lid, "proprietaire_id": pid, "vision": "REEL",
                        "total_produits": 500.0, "total_charges": 200.0, "resultat": 300.0 + j,
                        "nb_flux": 3, "commentaire": ""})
    fx.seeder(tmp_db, net_reglement=net, commissions=com, resultats=res)
    reader.vider_cache()
    yield tmp_db
    reader.vider_cache()


def _page(client, url):
    r = client.get(url)
    assert r.status_code == 200, url
    return BeautifulSoup(r.text, "html.parser")


def _selection(page, nom):
    opt = page.select_one(f'select[name="{nom}"] option[selected]')
    return opt["value"] if opt else ""


def _serie(page):
    return json.loads(page.select_one("script[data-rs-donnees]").string)


# ── 1. Filtres principaux transmis (et restitués sans identifiant technique) ─────────────────────

@pytest.mark.parametrize("chemin", ["/resultats", "/resultats/pilotage"])
def test_filtres_principaux_transmis_et_restitues(client, base, chemin):
    page = _page(client, f"{chemin}?du=2026-04&au=2026-05&proprietaire_id=PROP_RL1"
                         f"&logement_id=LOG_RL1&canal=AIRBNBOFFICIAL")
    assert (_selection(page, "du"), _selection(page, "au")) == ("2026-04", "2026-05")
    assert _selection(page, "proprietaire_id") == "PROP_RL1"
    assert _selection(page, "logement_id") == "LOG_RL1"
    assert _selection(page, "canal") == "AIRBNBOFFICIAL"
    actifs = page.select_one(".rs-actifs").get_text(" ", strip=True)
    assert "Avril → Mai 2026" in actifs and "Villa Garonne" in actifs and "Airbnb" in actifs
    assert "LOG_RL1" not in actifs and "PROP_RL1" not in actifs
    # Chaque filtre posé se retire d'un lien, sans perdre les autres.
    retrait = page.select_one('.rs-chip__retrait[aria-label*="logement"]')["href"]
    assert "logement_id" not in retrait and "proprietaire_id=PROP_RL1" in retrait and "du=2026-04" in retrait


# ── 2. Période + logement : KPI, graphique et tableau sur le même périmètre ──────────────────────

def test_periode_et_logement_meme_perimetre_partout(client, base):
    page = _page(client, "/resultats/pilotage?du=2026-05&au=2026-06&logement_id=LOG_RL2")
    # Sommes semées : mai (2100) + juin (3100) pour LOG_RL2 uniquement.
    attendu = pilot.vue(du="2026-05", au="2026-06", logement_id="LOG_RL2", db_path=base)["kpi"]
    assert attendu["total_payout"] == 5200.0
    kpis = page.select_one(".rs-kpis").get_text(" ", strip=True)
    assert _euros(attendu["ca_conciergerie"]) in kpis and _euros(attendu["total_payout"]) in kpis
    lignes = page.select("#par-logement tbody tr")
    assert [l.select_one("th").get_text(strip=True) for l in lignes] == ["Studio Capitole"]
    assert [p["mois"] for p in _serie(page)["points"]] == ["2026-05", "2026-06"]


# ── 3. État vide ─────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("chemin", ["/resultats", "/resultats/pilotage"])
def test_etat_vide_explicite_sans_graphique_casse(client, base, chemin):
    page = _page(client, f"{chemin}?du=2024-01&au=2024-03")
    texte = page.get_text(" ", strip=True)
    assert "Aucun résultat pour cette période." in texte
    assert "Modifiez les filtres" in texte
    assert page.select_one("canvas") is None and page.select_one(".rs-kpis") is None
    assert page.select_one(f'a.btn[href="{chemin}"]') is not None      # Réinitialiser les filtres
    for bruit in ("None", "nan", "NaN", "null"):
        assert f">{bruit}<" not in str(page)


# ── 4. GET = lecture seule ───────────────────────────────────────────────────────────────────────

def _empreinte(db) -> str:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        h = hashlib.sha256()
        for (t,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' "
                                 "AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall():
            for ligne in sorted(repr(tuple(r)) for r in conn.execute(f'SELECT * FROM "{t}"')):
                h.update(ligne.encode())
        return h.hexdigest()
    finally:
        conn.close()


def test_resultats_et_filtres_n_ecrivent_rien(client, base):
    avant = _empreinte(base)
    for url in ("/resultats", "/resultats?du=&au=", "/resultats?mois=2026-05&vision=COMPTABLE",
                "/resultats?du=2026-04&au=2026-06&proprietaire_id=PROP_RL1&canal=BOOKINGCOM",
                "/resultats/pilotage", "/resultats/pilotage?du=2026-06&au=2026-06&logement_id=LOG_RL1",
                "/resultats/pilotage?canal=AIRBNBOFFICIAL", "/resultats?du=2024-01&au=2024-01"):
        assert client.get(url).status_code == 200, url
    assert _empreinte(base) == avant


# ── 5. Données du graphique cohérentes avec le filtre ────────────────────────────────────────────

def test_graphique_coherent_avec_le_filtre(client, base):
    # Plage : la courbe couvre exactement la plage, et ses points somment aux KPI du périmètre.
    page = _page(client, "/resultats?du=2026-04&au=2026-06&proprietaire_id=PROP_RL1")
    points = _serie(page)["points"]
    assert [p["mois"] for p in points] == list(MOIS)
    kpi = pilot.vue(du="2026-04", au="2026-06", proprietaire_id="PROP_RL1", db_path=base)["kpi"]
    assert round(sum(p["ca_conciergerie"] for p in points), 2) == kpi["ca_conciergerie"]
    assert round(sum(p["total_payout"] for p in points), 2) == kpi["total_payout"]
    # Le tableau « Voir les données » reprend les mêmes chiffres que la courbe.
    cellules = [td.get_text(strip=True) for td in page.select(".rs-graphique details tbody td")]
    assert _euros(points[0]["total_payout"]) in cellules

    # Mois unique : 12 mois de contexte, mois choisi mis en évidence et annoncé à l'écran.
    page = _page(client, "/resultats?mois=2026-05")
    donnees = _serie(page)
    assert donnees["surligne"] == "2026-05" and donnees["points"][-1]["mois"] == "2026-05"
    assert page.select_one(".rs-note-perimetre") is not None

    # Série lue en une fois = mêmes sommes que `vue` mois par mois (aucun écart de calcul).
    for canal in ("", "BOOKINGCOM"):
        serie = pilot.serie_mensuelle(canal=canal, db_path=base)["points"]
        for p in serie:
            k = pilot.vue(mois=p["mois"], canal=canal, db_path=base)["kpi"]
            assert (p["total_payout"], p["ca_conciergerie"], p["commission"]) == \
                   (k["total_payout"], k["ca_conciergerie"], k["commission"])


def test_format_monetaire_francais():
    assert _euros(1245.5) == "1 245,50 €"
    assert _euros(-245.5) == "−245,50 €"
    assert _euros(0) == "0,00 €" and _euros(-0.001) == "0,00 €"
    assert _euros(None) == "—" and _euros(float("nan")) == "—"
