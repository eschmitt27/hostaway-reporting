"""Ingestion Hostaway par le dépôt publié — une seule chaîne, deux dates, aucune règle importée.

CE QUE CE FICHIER PROUVE
  1. le lecteur de dépôt présente les faits dans le TYPE de l'API (un identifiant est un entier) ;
  2. il refuse d'affirmer ce qu'il ne sait pas (tâches de ménage, activité d'une annonce) ;
  3. il écarte les champs financiers supprimés côté plateforme ;
  4. il n'importe JAMAIS les décisions de gestion publiées à côté (coût de ménage, commission) ;
  5. l'extraction porte la version exacte de la source et la date à laquelle elle a été produite ;
  6. la synchronisation est idempotente : réimporter le même état ne recrée rien ;
  7. « produites le » et « synchronisées le » restent deux dates distinctes ;
  8. un run laissé ouvert par un processus mort ne bloque plus le bouton pour toujours.

Aucun test ne touche au réseau ni au dépôt réel : un dépôt Git jetable est construit sur place.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import app.config as cfg

RESERVATIONS = "\t".join((
    "reservationId", "listingMapId", "listingName", "channelName", "channelId",
    "reservationCode", "guestName", "guestFirstName", "guestLastName", "arrivalDate",
    "departureDate", "nights", "numberOfGuests", "status", "paymentStatus", "totalPrice",
    "currency", "reservationDate", "insertedOn", "updatedOn", "latestActivityOn")) + "\n" + "\n".join((
    "\t".join(("9001", "481998", "Studio - 76", "airbnbOfficial", "2018", "HMABC", "Ana",
               "Ana", "R", "2026-03-01", "2026-03-04", "3", "2", "new", "Paid", "300.0",
               "EUR", "2026-02-01 10:00:00", "2026-02-01 10:01:00", "2026-02-02 09:00:00",
               "2026-02-02 09:00:00")),
    "\t".join(("9002", "485104", "T2 - 65", "bookingcom", "2005", "BK-2", "Bo", "Bo", "C",
               "2026-03-10", "2026-03-12", "2", "1", "new", "Paid", "200.0", "EUR",
               "2026-02-05 10:00:00", "2026-02-05 10:01:00", "2026-02-06 09:00:00",
               "2026-02-06 09:00:00")),
    "\t".join(("9003", "485104", "T2 - 65", "airbnbOfficial", "2018", "HMZZZ", "Cy", "Cy", "D",
               "2026-04-01", "2026-04-02", "1", "1", "inquiry", "Unknown", "0.0", "EUR",
               "2026-03-01 10:00:00", "2026-03-01 10:01:00", "2026-03-01 10:01:00",
               "2026-03-01 10:01:00")),
)) + "\n"

FINANCE = "\t".join((
    "reservationId", "financeFieldId", "type", "name", "title", "value", "total",
    "isIncludedInTotalPrice", "isOverriddenByUser", "isMandatory", "isDeleted")) + "\n" + "\n".join((
    "\t".join(("9001", "1", "totals", "airbnbPayoutSum", "Payout", "270.0", "270.0", "0", "0", "0", "0")),
    "\t".join(("9001", "2", "fee", "cleaningFee", "Cleaning", "40.0", "40.0", "1", "0", "1", "0")),
    # Champ SUPPRIMÉ côté plateforme : doit être écarté, jamais recompté.
    "\t".join(("9001", "3", "totals", "airbnbPayoutSum", "Payout", "999.0", "999.0", "0", "0", "0", "1")),
    "\t".join(("9002", "4", "totals", "totalPriceFromChannel", "Total", "200.0", "200.0", "0", "0", "0", "0")),
    "\t".join(("9002", "5", "tax", "cityTax", "City", "10.0", "10.0", "1", "0", "1", "0")),
    "\t".join(("9002", "6", "commissions", "hostChannelFee", "Fee", "30.0", "30.0", "1", "0", "1", "0")),
)) + "\n"

# Le pipeline publie AUSSI ces deux fichiers. Ils portent des DÉCISIONS DE GESTION (coût de ménage,
# taux de commission) et un payout déjà calculé. Ils sont volontairement présents dans le dépôt
# jetable pour qu'un test puisse prouver qu'ils ne sont pas lus.
CONSTANTES = "listingMapId;listingName;CoutMenage;TauxCommission\n481998;Studio - 76;999;0.99\n"
RAPPORT = "reservationId\tTotalPayout\n9001\t111.11\n"


@pytest.fixture()
def depot(tmp_path):
    """Un dépôt Git jetable, avec une branche portant les fichiers publiés."""
    racine = tmp_path / "depot_source"
    racine.mkdir()

    def git(*a, cwd=racine):
        subprocess.run(["git", *a], cwd=str(cwd), check=True, capture_output=True)

    git("init", "-b", "main")
    git("config", "user.email", "pipeline@example.invalid")
    git("config", "user.name", "pipeline")
    (racine / "reservations_hostaway.tsv").write_text(RESERVATIONS, encoding="utf-8-sig")
    (racine / "finance_fields_hostaway.tsv").write_text(FINANCE, encoding="utf-8-sig")
    (racine / "listing_constants.csv").write_text(CONSTANTES, encoding="utf-8-sig")
    (racine / "hostaway_reporting_final.tsv").write_text(RAPPORT, encoding="utf-8-sig")
    git("add", "-A")
    git("commit", "-m", "Automated data refresh")

    # Le « poste » : un clone, dont `origin` est le dépôt de données — exactement la topologie
    # réelle, où le dépôt du projet est aussi celui qui porte les données publiées.
    poste = tmp_path / "poste"
    subprocess.run(["git", "clone", str(racine), str(poste)], check=True, capture_output=True)
    return {"source": racine, "poste": poste, "git": git}


@pytest.fixture()
def lib(depot, monkeypatch):
    import sys

    chemin = str(Path(cfg.PROJECT_ROOT) / "02_TRAVAIL")
    if chemin not in sys.path:
        sys.path.insert(0, chemin)
    import lib_hostaway_depot as module

    return module


class _Log:
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


# ── Ce que le dépôt publie ──────────────────────────────────────────────────────────────────────

def test_etat_rend_le_commit_et_sa_date_de_production(depot, lib):
    etat = lib.etat(depot["poste"])
    assert etat["disponible"] is True
    assert len(etat["commit"]) == 40
    # La date rendue est celle du commit du pipeline : le moment où les données ont été PRODUITES.
    assert etat["source_horodatage"].endswith("Z") or "+" in etat["source_horodatage"]


def test_un_depot_sans_les_fichiers_attendus_est_indisponible_pas_vide(depot, lib):
    subprocess.run(["git", "rm", "reservations_hostaway.tsv"], cwd=str(depot["source"]),
                   check=True, capture_output=True)
    depot["git"]("commit", "-m", "retrait")
    etat = lib.etat(depot["poste"])
    assert etat["disponible"] is False
    assert "reservations_hostaway.tsv" in etat["erreur"]


# ── Les faits, dans le type de l'API ────────────────────────────────────────────────────────────

def test_les_identifiants_sont_rendus_en_entier_comme_l_api(depot, lib):
    """Le défaut qui a fait classer 1 589 réservations « listing orphelin ».

    Un fichier délimité n'a qu'un type : le texte. Mais le moteur compare `listingMapId` aux
    identifiants du référentiel, qui sont des entiers — et `"481998" in {481998}` est faux.
    """
    source = lib.SourceDepotGitHub(depot["poste"], _Log())
    page = source.get_reservations_page("2026-01-01", 0)
    assert page[0]["listingMapId"] == 481998
    assert isinstance(page[0]["listingMapId"], int)
    assert page[0]["id"] == 9001


def test_les_champs_financiers_supprimes_sont_ecartes(depot, lib):
    source = lib.SourceDepotGitHub(depot["poste"], _Log())
    page = source.get_reservations_page("2026-01-01", 0)
    payouts = [f["value"] for f in page[0]["financeField"] if f["name"] == "airbnbPayoutSum"]
    assert payouts == [270.0], "le champ isDeleted=1 ne doit pas être ressuscité"


def test_le_comptage_ne_filtre_pas_sur_une_semantique_de_date_non_verifiee(depot, lib):
    source = lib.SourceDepotGitHub(depot["poste"], _Log())
    assert source.count_reservations("2030-01-01") == 3


# ── Ce que le dépôt REFUSE d'affirmer ───────────────────────────────────────────────────────────

def test_les_taches_de_menage_ne_sont_pas_rendues_vides_mais_refusees(depot, lib):
    """« Non fourni par cette source » n'est pas « aucune tâche ».

    Rendre une liste vide ferait disparaître tous les ménages du mois sans la moindre alerte.
    """
    source = lib.SourceDepotGitHub(depot["poste"], _Log())
    with pytest.raises(lib.DonneeNonFournieParCetteSource):
        source.get_tasks("2026-01-01")


def test_une_annonce_deduite_ne_pretend_pas_etre_active(depot, lib):
    source = lib.SourceDepotGitHub(depot["poste"], _Log())
    annonces = source.get_listings()
    assert {a["listingMapId"] for a in annonces} == {481998, 485104}
    for a in annonces:
        assert a["city"] is None
        assert a["specialStatus"] is None
        assert a["_deduit_des_reservations"] is True


def test_une_reservation_absente_du_depot_leve_au_lieu_de_rendre_un_objet_vide(depot, lib):
    source = lib.SourceDepotGitHub(depot["poste"], _Log())
    with pytest.raises(lib.DonneeNonFournieParCetteSource):
        source.get_reservation_detail("404404")


# ── Aucune règle de gestion n'entre par ce chemin ───────────────────────────────────────────────

def test_le_cout_de_menage_et_la_commission_publies_ne_sont_jamais_lus(depot, lib):
    """Le dépôt publie `listing_constants.csv` (CoutMenage, TauxCommission) et un `TotalPayout`
    déjà calculé. Ce sont des DÉCISIONS, pas des faits : elles vivent dans les référentiels datés
    de l'application. Les importer créerait une seconde vérité, muette et plus récente en
    apparence."""
    assert lib.FICHIER_CONSTANTES not in (lib.FICHIER_RESERVATIONS, lib.FICHIER_FINANCE_FIELDS)
    source = lib.SourceDepotGitHub(depot["poste"], _Log())
    source.count_reservations("2026-01-01")
    page = source.get_reservations_page("2026-01-01", 0)
    for reservation in page:
        assert "CoutMenage" not in reservation
        assert "TauxCommission" not in reservation
        assert "TotalPayout" not in reservation


# ── Provenance et idempotence, côté base ────────────────────────────────────────────────────────

def test_l_extraction_porte_la_version_de_la_source_et_sa_date(tmp_db):
    from app.services import hostaway_raw_service as raw

    eid = raw.ouvrir(mode=raw.MODE_DEPOT_GITHUB, db_path=tmp_db, source_ref="abc123",
                     source_horodatage="2026-09-12T14:35:42Z")
    raw.cloturer(eid, statut=raw.ST_SUCCES, db_path=tmp_db)
    etat = raw.fraicheur(db_path=tmp_db)
    assert etat["source_ref"] == "abc123"
    assert etat["source_horodatage"] == "2026-09-12T14:35:42Z"


def test_deux_dates_distinctes_produite_et_synchronisee(tmp_db):
    """La date de production et la date d'import ne doivent jamais se confondre : entre les deux,
    il peut s'écouler des heures pendant lesquelles la base n'est PAS à jour."""
    from app.services import hostaway_raw_service as raw

    eid = raw.ouvrir(mode=raw.MODE_DEPOT_GITHUB, db_path=tmp_db, source_ref="abc123",
                     source_horodatage="2026-09-12T06:00:00Z")
    raw.cloturer(eid, statut=raw.ST_SUCCES, db_path=tmp_db)
    etat = raw.fraicheur(db_path=tmp_db)
    assert etat["source_horodatage"] == "2026-09-12T06:00:00Z"
    assert etat["importe_le"] != etat["source_horodatage"]


def test_une_version_deja_importee_est_reconnue(tmp_db):
    from app.services import hostaway_raw_service as raw

    assert raw.extraction_de_source("zzz", db_path=tmp_db) is None
    eid = raw.ouvrir(mode=raw.MODE_DEPOT_GITHUB, db_path=tmp_db, source_ref="zzz")
    raw.cloturer(eid, statut=raw.ST_SUCCES, db_path=tmp_db)
    deja = raw.extraction_de_source("zzz", db_path=tmp_db)
    assert deja is not None and deja["extraction_id"] == eid


def test_une_extraction_echouee_ne_compte_pas_comme_deja_importee(tmp_db):
    """Sinon un import raté empêcherait pour toujours de réessayer cette version."""
    from app.services import hostaway_raw_service as raw

    eid = raw.ouvrir(mode=raw.MODE_DEPOT_GITHUB, db_path=tmp_db, source_ref="ko")
    raw.cloturer(eid, statut=raw.ST_ECHEC, db_path=tmp_db)
    assert raw.extraction_de_source("ko", db_path=tmp_db) is None


# ── Un processus mort ne bloque plus le bouton ──────────────────────────────────────────────────

def test_un_run_reste_ouvert_est_requalifie_interrompu(tmp_db):
    """Le défaut constaté : un run du 2026-09-10 resté EN_COURS rendait l'actualisation Hostaway
    définitivement impossible, en répondant « une actualisation est déjà en cours »."""
    from app.db.connection import get_db
    from app.services import hostaway_actualisation_service as ha

    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO moteur_runs (run_id, lot, started_at, statut, declencheur) "
            "VALUES (?,?,?,?,?)",
            ("RUN-fantome", "lot1_hostaway_extract", "2020-01-01T00:00:00", ha.ST_EN_COURS,
             "MANUEL"))
        conn.commit()
    finally:
        conn.close()

    assert ha.marquer_runs_interrompus(db_path=tmp_db) == ["RUN-fantome"]
    assert ha.actualisation_en_cours(db_path=tmp_db) is None


def test_un_run_recent_reste_considere_en_cours(tmp_db):
    """La requalification ne doit pas tuer une extraction qui tourne vraiment."""
    from datetime import datetime, timezone

    from app.db.connection import get_db
    from app.services import hostaway_actualisation_service as ha

    maintenant = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO moteur_runs (run_id, lot, started_at, statut, declencheur) "
            "VALUES (?,?,?,?,?)",
            ("RUN-vivant", "lot1_hostaway_extract", maintenant, ha.ST_EN_COURS, "MANUEL"))
        conn.commit()
    finally:
        conn.close()

    assert ha.marquer_runs_interrompus(db_path=tmp_db) == []
    en_cours = ha.actualisation_en_cours(db_path=tmp_db)
    assert en_cours is not None and en_cours["run_id"] == "RUN-vivant"


# ── Le chemin canonique n'exige aucun secret local ──────────────────────────────────────────────

def test_le_chemin_canonique_passe_par_le_depot_sans_identifiants_locaux():
    from app.services import hostaway_actualisation_service as ha

    assert "--source" in ha.ARGUMENTS_DEPOT
    assert "DEPOT_GITHUB" in ha.ARGUMENTS_DEPOT


def test_une_seconde_synchronisation_du_meme_etat_n_importe_rien(tmp_db, monkeypatch):
    from app.services import hostaway_depot_service as depot_svc
    from app.services import hostaway_raw_service as raw

    monkeypatch.setattr(depot_svc, "etat_publie",
                        lambda **k: {"disponible": True, "commit": "deadbeef",
                                     "commit_court": "deadbeef", "source_horodatage": "2026-09-12T14:35:42Z"})
    eid = raw.ouvrir(mode=raw.MODE_DEPOT_GITHUB, db_path=tmp_db, source_ref="deadbeef")
    raw.cloturer(eid, statut=raw.ST_SUCCES, db_path=tmp_db)

    resultat = depot_svc.synchroniser(db_path=tmp_db)
    assert resultat["ok"] is True
    assert resultat["importe"] is False
    assert resultat["code"] == depot_svc.E_DEJA_SYNCHRONISE


def test_une_version_plus_recente_est_signalee_en_retard(tmp_db, monkeypatch):
    """« À jour » se juge sur l'IDENTITÉ de la version importée, jamais sur son ancienneté."""
    from app.services import hostaway_depot_service as depot_svc
    from app.services import hostaway_raw_service as raw

    monkeypatch.setattr(depot_svc, "etat_publie",
                        lambda **k: {"disponible": True, "commit": "nouveau",
                                     "commit_court": "nouveau",
                                     "source_horodatage": "2026-09-12T14:35:42Z"})
    eid = raw.ouvrir(mode=raw.MODE_DEPOT_GITHUB, db_path=tmp_db, source_ref="ancien",
                     source_horodatage="2026-09-12T06:00:00Z")
    raw.cloturer(eid, statut=raw.ST_SUCCES, db_path=tmp_db)

    etat = depot_svc.fraicheur(rafraichir=False, db_path=tmp_db)
    assert etat["etat"] == "RETARD"
    assert etat["a_jour"] is False


def test_un_depot_illisible_ne_permet_pas_d_affirmer_a_jour(tmp_db, monkeypatch):
    from app.services import hostaway_depot_service as depot_svc
    from app.services import hostaway_raw_service as raw

    monkeypatch.setattr(depot_svc, "etat_publie",
                        lambda **k: {"disponible": False, "code": depot_svc.E_DEPOT_INDISPONIBLE})
    eid = raw.ouvrir(mode=raw.MODE_DEPOT_GITHUB, db_path=tmp_db, source_ref="x")
    raw.cloturer(eid, statut=raw.ST_SUCCES, db_path=tmp_db)

    etat = depot_svc.fraicheur(rafraichir=False, db_path=tmp_db)
    assert etat["etat"] == "INDETERMINE"
    assert etat["a_jour"] is False
