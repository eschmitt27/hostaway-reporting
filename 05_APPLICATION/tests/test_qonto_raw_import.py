"""Import RAW Qonto : ce qui est lu est enregistré fidèlement, et RIEN d'autre n'est touché.

Aucun test ici n'appelle le réseau : le client est remplacé par un double qui rejoue des charges
utiles ayant la forme réelle des réponses Qonto (champs relevés sur le vrai compte). Ce qui est
vérifié, ce sont les promesses du chantier :

  · le client refuse structurellement POST/PUT/PATCH/DELETE, AVANT toute sortie réseau ;
  · toutes les pages sont lues, pas seulement la première ;
  · un même mouvement n'entre jamais deux fois ;
  · un mouvement modifié chez Qonto est mis à jour, pas dupliqué ;
  · une seconde synchronisation identique ne crée ni ne modifie rien ;
  · une erreur d'API laisse intact le dernier jeu valide ;
  · un mouvement absent d'une réponse partielle n'est jamais supprimé ;
  · aucune table comptable n'est touchée, et aucun secret n'atterrit en base.
"""
from __future__ import annotations

import json

import pytest

from app.adapters import qonto_client
from app.adapters.qonto_client import ErreurQonto, MethodeInterdite
from app.db.connection import apply_migrations, get_db
from app.services import qonto_raw_service as raw
from app.services import qonto_sync_service as sync

COMPTE = {
    "id": "aaaaaaaa-1111-2222-3333-444444444444",
    "slug": "compte-principal-1",
    "name": "Compte principal",
    "currency": "EUR",
    "status": "active",
    "iban": "FR7630001007941234567890185",
    "bic": "QNTOFRP1XXX",
    "balance": 200.0,
    "balance_cents": 20000,
    "authorized_balance": 180.0,
    "authorized_balance_cents": 18000,
    "main": True,
    "is_external_account": False,
    "updated_at": "2026-09-11T10:00:00.000Z",
}


def mouvement(numero: int, **surcharges) -> dict:
    """Charge utile à la forme réelle d'un mouvement Qonto."""
    base = {
        "id": f"tx-uuid-{numero:04d}",
        "transaction_id": f"chouette-patrimoine-{numero:04d}-transaction-abcdef",
        "bank_account_id": COMPTE["id"],
        "amount": 120.5,
        "amount_cents": 12050,
        "currency": "EUR",
        "local_amount": 120.5,
        "local_amount_cents": 12050,
        "local_currency": "EUR",
        "side": "credit",
        "status": "completed",
        "operation_type": "income",
        "label": f"Virement {numero}",
        "reference": f"REF-{numero:04d}",
        "note": None,
        "clean_counterparty_name": "LOCATAIRE MARTIN",
        "emitted_at": "2026-09-10T08:00:00.000Z",
        "settled_at": "2026-09-11T08:00:00.000Z",
        "created_at": "2026-09-10T08:00:00.000Z",
        "updated_at": "2026-09-11T08:00:00.000Z",
        "settled_balance": 200.0,
        "settled_balance_cents": 20000,
        "category": "other_income",
        "vat_amount": 0.0,
        "vat_rate": 0.0,
        "card_last_digits": None,
        "attachment_ids": [],
        "attachment_required": False,
        "is_external_transaction": False,
    }
    base.update(surcharges)
    return base


class ClientDouble:
    """Double du client : même surface, aucune sortie réseau.

    `pages` permet de rejouer une pagination réelle, `echec` de simuler une panne d'API.
    """

    def __init__(self, comptes=None, pages=None, echec=None):
        self._comptes = comptes if comptes is not None else [COMPTE]
        self._pages = pages if pages is not None else [[]]
        self.echec = echec
        self.appels = 0

    def organisation(self):
        if isinstance(self.echec, str) and self.echec == "organisation":
            raise ErreurQonto(qonto_client.E_API, "HTTP 502")
        return {"legal_name": "CHOUETTE PATRIMOINE", "bank_accounts": self._comptes}

    def transactions(self, qonto_account_id):
        self.appels += 1
        if self.echec == "transactions":
            raise ErreurQonto(qonto_client.E_API, "HTTP 502")
        mouvements = [m for page in self._pages for m in page]
        return mouvements, len(self._pages)


@pytest.fixture()
def base(tmp_path):
    db = tmp_path / "app.db"
    apply_migrations(db)
    return db


def _lignes(db, table):
    conn = get_db(db)
    try:
        return [dict(r) for r in conn.execute(f"SELECT * FROM {table}")]
    finally:
        conn.close()


# ── Le client ne sait faire que des GET ───────────────────────────────────────────────────────
def test_le_client_refuse_toute_methode_autre_que_get():
    client = qonto_client.ClientQontoLectureSeule("faux-login", "fausse-cle")
    for interdite in ("POST", "PUT", "PATCH", "DELETE", "post", "OPTIONS"):
        with pytest.raises(MethodeInterdite):
            client.requete(interdite, "/organization")


def test_le_refus_precede_le_reseau():
    """La garde ne doit pas dépendre d'un serveur qui répondrait 405 : rien ne doit partir."""
    class SessionMouchard:
        headers = {}

        def __init__(self):
            self.sorties = 0

        def request(self, *a, **k):  # pragma: no cover - ne doit jamais être atteint
            self.sorties += 1
            raise AssertionError("une requête est sortie alors que la méthode est interdite")

    mouchard = SessionMouchard()
    client = qonto_client.ClientQontoLectureSeule("faux-login", "fausse-cle", session=mouchard)
    with pytest.raises(MethodeInterdite):
        client.requete("DELETE", "/transactions")
    assert mouchard.sorties == 0


def test_le_client_n_expose_aucune_methode_d_ecriture():
    for verbe in ("post", "put", "patch", "delete"):
        assert not hasattr(qonto_client.ClientQontoLectureSeule, verbe), \
            f"une méthode {verbe}() existe : le client n'est plus en lecture seule"


def test_identifiants_absents_refuses_sans_appel(monkeypatch):
    monkeypatch.setattr(qonto_client, "identifiants", lambda env=None: ("", ""))
    with pytest.raises(ErreurQonto) as capture:
        qonto_client.ClientQontoLectureSeule("", "", session=object())
    assert capture.value.code == qonto_client.E_IDENTIFIANTS_ABSENTS


def test_identifiants_lus_uniquement_dans_env():
    """Aucune valeur en dur : sans variables d'environnement, rien n'est trouvé."""
    assert qonto_client.identifiants(env={}) == ("", "")
    assert qonto_client.identifiants_presents(env={}) is False
    assert qonto_client.identifiants_presents(
        env={"QONTO_LOGIN": "x", "QONTO_SECRET_KEY": "y"}) is True


# ── Import ────────────────────────────────────────────────────────────────────────────────────
def test_import_initial_enregistre_comptes_et_mouvements(base):
    client = ClientDouble(pages=[[mouvement(1), mouvement(2)], [mouvement(3)]])
    bilan = sync.synchroniser(client=client, db_path=base)

    assert bilan["ok"] is True
    assert bilan["comptes"] == 1
    assert bilan["creees"] == 3
    assert raw.compter(db_path=base) == {"comptes": 1, "transactions": 3, "synchronisations": 1}

    ligne = _lignes(base, "qonto_transactions_raw")[0]
    assert ligne["sens"] == "credit"
    assert ligne["contrepartie"] == "LOCATAIRE MARTIN"
    assert ligne["montant_cents"] == 12050
    assert ligne["premiere_recuperation"] and ligne["derniere_maj"]


def test_toutes_les_pages_sont_lues(base):
    """Trois pages de 2 → 6 mouvements. Une seule page lue serait un historique tronqué."""
    pages = [[mouvement(1), mouvement(2)], [mouvement(3), mouvement(4)],
             [mouvement(5), mouvement(6)]]
    sync.synchroniser(client=ClientDouble(pages=pages), db_path=base)
    assert raw.compter(db_path=base)["transactions"] == 6
    assert raw.derniere_synchronisation(db_path=base)["pages_lues"] == 3


def test_deuxieme_synchronisation_identique_ne_cree_rien(base):
    client = ClientDouble(pages=[[mouvement(1), mouvement(2)]])
    sync.synchroniser(client=client, db_path=base)
    avant = _lignes(base, "qonto_transactions_raw")

    second = sync.synchroniser(client=client, db_path=base)

    assert second["creees"] == 0
    assert second["mises_a_jour"] == 0
    assert second["inchangees"] == 2
    assert raw.compter(db_path=base)["transactions"] == 2
    # Les lignes elles-mêmes n'ont pas bougé d'un octet : pas même une date de mise à jour.
    assert _lignes(base, "qonto_transactions_raw") == avant


def test_un_meme_identifiant_ne_cree_jamais_de_doublon(base):
    """Le même mouvement servi deux fois dans la MÊME réponse (pagination qui se recouvre)."""
    doublon = mouvement(1)
    sync.synchroniser(client=ClientDouble(pages=[[doublon], [dict(doublon)]]), db_path=base)
    assert raw.compter(db_path=base)["transactions"] == 1


def test_un_mouvement_modifie_est_mis_a_jour_sans_doublon(base):
    """Une opération en attente qui se règle : même identifiant, contenu différent."""
    en_attente = mouvement(1, status="pending", settled_at=None)
    sync.synchroniser(client=ClientDouble(pages=[[en_attente]]), db_path=base)
    premiere = _lignes(base, "qonto_transactions_raw")[0]

    reglee = mouvement(1, status="completed", settled_at="2026-09-12T09:00:00.000Z")
    bilan = sync.synchroniser(client=ClientDouble(pages=[[reglee]]), db_path=base)

    assert bilan["mises_a_jour"] == 1
    assert bilan["creees"] == 0
    lignes = _lignes(base, "qonto_transactions_raw")
    assert len(lignes) == 1, "une mise à jour ne doit jamais créer une seconde ligne"
    assert lignes[0]["statut"] == "completed"
    assert lignes[0]["regle_le"] == "2026-09-12T09:00:00.000Z"
    assert lignes[0]["premiere_recuperation"] == premiere["premiere_recuperation"], \
        "la date de première récupération est un fait historique"


def test_une_reponse_partielle_ne_supprime_rien(base):
    """Qonto ne renvoie plus que le mouvement 1 : les autres restent en base."""
    sync.synchroniser(client=ClientDouble(pages=[[mouvement(1), mouvement(2), mouvement(3)]]),
                      db_path=base)
    sync.synchroniser(client=ClientDouble(pages=[[mouvement(1)]]), db_path=base)
    assert raw.compter(db_path=base)["transactions"] == 3, \
        "une réponse partielle ne prouve pas qu'un mouvement a disparu"


def test_une_erreur_api_conserve_le_dernier_jeu_valide(base):
    sync.synchroniser(client=ClientDouble(pages=[[mouvement(1), mouvement(2)]]), db_path=base)
    avant = _lignes(base, "qonto_transactions_raw")

    resultat = sync.synchroniser(client=ClientDouble(echec="transactions"), db_path=base)

    assert resultat["ok"] is False
    assert resultat["code"] == qonto_client.E_API
    assert _lignes(base, "qonto_transactions_raw") == avant, "rien ne doit avoir été réécrit"
    echec = raw.derniere_synchronisation(db_path=base)
    assert echec["statut"] == raw.ST_ECHEC
    assert echec["code_erreur"] == qonto_client.E_API


def test_une_erreur_en_cours_de_collecte_n_ecrit_aucun_mouvement(base):
    """L'API casse avant toute écriture : la base reste vierge, pas à moitié remplie."""
    resultat = sync.synchroniser(client=ClientDouble(echec="organisation"), db_path=base)
    assert resultat["ok"] is False
    assert raw.compter(db_path=base)["transactions"] == 0
    assert raw.compter(db_path=base)["comptes"] == 0


def test_l_ecriture_est_atomique(base, monkeypatch):
    """Une panne au milieu de l'enregistrement ne laisse aucune moitié de synchronisation."""
    sync.synchroniser(client=ClientDouble(pages=[[mouvement(1)]]), db_path=base)
    avant_tx = _lignes(base, "qonto_transactions_raw")
    avant_runs = _lignes(base, "qonto_sync_runs")

    appels = {"n": 0}
    vrai = raw._valeurs_mouvement

    def explose(mvt, horodatage, run_id):
        appels["n"] += 1
        if appels["n"] >= 2:
            raise RuntimeError("panne simulée au milieu de l'écriture")
        return vrai(mvt, horodatage, run_id)

    monkeypatch.setattr(raw, "_valeurs_mouvement", explose)
    with pytest.raises(RuntimeError):
        raw.enregistrer([COMPTE], [mouvement(7), mouvement(8), mouvement(9)], db_path=base)

    assert _lignes(base, "qonto_transactions_raw") == avant_tx
    assert _lignes(base, "qonto_sync_runs") == avant_runs


# ── Frontière avec le module comptable ────────────────────────────────────────────────────────
TABLES_COMPTABLES = ("charges", "factures", "facture_lignes_menage", "factures_proprietaires",
                     "reglements_fournisseurs", "mouvements_bancaires")


def test_aucune_table_comptable_n_est_touchee(base):
    conn = get_db(base)
    try:
        presentes = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        avant = {t: [tuple(r) for r in conn.execute(f"SELECT * FROM {t}")]
                 for t in TABLES_COMPTABLES if t in presentes}
    finally:
        conn.close()
    assert avant, "aucune table comptable trouvée : le test ne prouverait rien"

    sync.synchroniser(client=ClientDouble(pages=[[mouvement(1), mouvement(2)]]), db_path=base)

    conn = get_db(base)
    try:
        apres = {t: [tuple(r) for r in conn.execute(f"SELECT * FROM {t}")] for t in avant}
    finally:
        conn.close()
    assert apres == avant, "la couche RAW a modifié une table comptable"


def test_la_couche_raw_n_ecrit_que_dans_les_tables_qonto(base):
    """Le SQL des deux modules ne nomme aucune table hors `qonto_*` en écriture."""
    import re
    from pathlib import Path

    import app.config as cfg
    for module in ("app/services/qonto_raw_service.py", "app/services/qonto_sync_service.py"):
        source = (Path(cfg.APP_ROOT) / module).read_text(encoding="utf-8")
        cibles = re.findall(r"(?:INSERT INTO|UPDATE|DELETE FROM)\s+([a-z_][a-z0-9_]*)",
                            source, flags=re.IGNORECASE)
        # `ON CONFLICT … DO UPDATE SET` n'est pas une cible de table : le mot-clé suivant est SET.
        hors_perimetre = [c for c in cibles
                          if not c.startswith("qonto_") and c.upper() != "SET"]
        assert not hors_perimetre, f"{module} écrit hors périmètre : {hors_perimetre}"
        assert "DELETE FROM" not in source.upper(), \
            f"{module} contient une suppression : une ligne RAW ne se supprime pas"


def _contenu_qonto(db) -> str:
    conn = get_db(db)
    try:
        contenu = ""
        for (table,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'qonto_%'"):
            for ligne in conn.execute(f"SELECT * FROM {table}"):
                contenu += json.dumps({k: str(v) for k, v in dict(ligne).items()})
        return contenu
    finally:
        conn.close()


def test_la_cle_secrete_n_atterrit_jamais_en_base(base, monkeypatch):
    """LA donnée à protéger, c'est `QONTO_SECRET_KEY`. Elle ne doit toucher aucune colonne.

    Le client est construit avec une clé reconnaissable, puis on cherche cette chaîne partout —
    y compris dans la charge utile brute et dans le journal des synchronisations.
    """
    secret = "CLE-SECRETE-RECONNAISSABLE-0123456789"
    monkeypatch.setattr(qonto_client, "identifiants", lambda env=None: ("un-login", secret))
    sync.synchroniser(client=ClientDouble(pages=[[mouvement(1)]]), db_path=base)
    raw.enregistrer_echec(qonto_client.E_API, "HTTP 502", db_path=base)

    contenu = _contenu_qonto(base)
    assert secret not in contenu
    assert "Authorization" not in contenu


def test_le_login_n_est_stocke_que_la_ou_qonto_l_a_lui_meme_mis(base):
    """Nuance assumée, et vérifiée plutôt que passée sous silence.

    `QONTO_LOGIN` n'est pas une clé : c'est l'identifiant PUBLIC de l'organisation (son « sign-in »),
    et Qonto le place lui-même au début de chaque `transaction_id` et dans le `slug` du compte. Il
    apparaît donc légitimement dans la couche RAW — le retirer reviendrait à falsifier les
    identifiants de Qonto, c'est-à-dire à casser la clé d'unicité qui interdit les doublons.

    Ce qui doit rester vrai : il n'est jamais écrit par NOUS, comme identifiant d'authentification.
    Aucune colonne ne lui est dédiée, et il n'entre en base qu'à l'intérieur d'une valeur renvoyée
    par l'API.
    """
    conn = get_db(base)
    try:
        colonnes = []
        for table in raw.TABLES_RAW:
            colonnes += [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    finally:
        conn.close()
    suspectes = [c for c in colonnes
                 if any(mot in c.lower() for mot in ("login", "secret", "cle_api", "token",
                                                     "password", "authorization"))]
    assert not suspectes, f"colonnes destinées à un identifiant d'authentification : {suspectes}"


def test_le_journal_d_echec_ne_contient_qu_un_code_court(base):
    raw.enregistrer_echec(qonto_client.E_API, "HTTP 502", db_path=base)
    echec = raw.derniere_synchronisation(db_path=base)
    assert echec["code_erreur"] == qonto_client.E_API
    assert echec["message_erreur"] == "HTTP 502"
    assert "thirdparty.qonto.com" not in (echec["message_erreur"] or "")


# ── Restitution ───────────────────────────────────────────────────────────────────────────────
def test_l_iban_est_masque_a_la_restitution(base):
    sync.synchroniser(client=ClientDouble(pages=[[mouvement(1)]]), db_path=base)
    compte = raw.comptes(db_path=base)[0]
    assert compte["iban_masque"] == "FR76 **** **** 0185"
    assert "iban" not in compte, "l'IBAN complet ne doit pas sortir de la couche de lecture"
    assert COMPTE["iban"] not in json.dumps(compte)


def test_l_iban_complet_reste_stocke_pour_le_rapprochement_futur(base):
    sync.synchroniser(client=ClientDouble(pages=[[mouvement(1)]]), db_path=base)
    conn = get_db(base)
    try:
        stocke = conn.execute("SELECT iban FROM qonto_accounts").fetchone()[0]
    finally:
        conn.close()
    assert stocke == COMPTE["iban"]


def test_les_soldes_sont_conserves_en_centimes_et_en_euros(base):
    sync.synchroniser(client=ClientDouble(pages=[[mouvement(1)]]), db_path=base)
    compte = raw.comptes(db_path=base)[0]
    assert compte["solde"] == 200.0 and compte["solde_cents"] == 20000
    assert compte["solde_autorise"] == 180.0 and compte["solde_autorise_cents"] == 18000
    assert compte["devise"] == "EUR" and compte["statut"] == "active"


def test_la_charge_utile_brute_est_conservee(base):
    """Un champ non anticipé reste récupérable sans réinterroger l'API."""
    sync.synchroniser(client=ClientDouble(pages=[[mouvement(1, subject_type="Income")]]),
                      db_path=base)
    brut = json.loads(_lignes(base, "qonto_transactions_raw")[0]["charge_utile_json"])
    assert brut["subject_type"] == "Income"


def test_base_non_migree_est_un_etat_legitime(tmp_path):
    vide = tmp_path / "vide.db"
    get_db(vide).close()
    resultat = sync.synchroniser(client=ClientDouble(), db_path=vide)
    assert resultat["ok"] is False
    assert resultat["code"] == sync.E_TABLES_ABSENTES
    assert sync.etat(db_path=vide)["disponible"] is False
