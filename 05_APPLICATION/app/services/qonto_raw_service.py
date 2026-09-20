"""Couche RAW Qonto — écriture en base de ce que l'API a répondu, sans interprétation.

    GET /v2/organization + GET /v2/transactions → qonto_accounts, qonto_transactions_raw

CE MODULE N'ÉCRIT QUE DANS LES TABLES `qonto_*`. Aucune charge, aucun mouvement bancaire métier,
aucun règlement, aucune facture, aucune créance n'est touché — ni créé, ni modifié. Le
rapprochement viendra plus tard et LIRA cette couche ; il n'est pas ici.

Trois règles gouvernent l'écriture :
  1. Le même mouvement Qonto ne peut jamais exister deux fois : `id` et `transaction_id` sont tous
     deux uniques en base.
  2. Si Qonto modifie un mouvement (une opération en attente qui se règle, un libellé corrigé),
     la ligne RAW est mise à jour — reconnue par l'empreinte de la charge utile, pas par une
     comparaison champ à champ qui laisserait filer ce qu'on n'a pas anticipé.
  3. Une ligne n'est JAMAIS supprimée parce qu'elle manque dans une réponse. Une réponse partielle
     ne prouve pas une disparition.

L'écriture est atomique : soit la synchronisation entière est enregistrée, soit rien ne l'est.

SECRETS — UNE NUANCE À NE PAS MASQUER. `QONTO_SECRET_KEY` n'entre jamais en base, nulle part : il
n'existe aucune colonne pour cela et un test le cherche dans toutes les tables. `QONTO_LOGIN`, en
revanche, n'est pas une clé : c'est l'identifiant PUBLIC de l'organisation (son « sign-in »), et
Qonto le place lui-même en tête de chaque `transaction_id` et dans le `slug` du compte. Il apparaît
donc à l'intérieur de valeurs renvoyées par l'API. Le retirer reviendrait à réécrire les
identifiants de Qonto — donc à casser la clé d'unicité qui interdit les doublons. Il n'est écrit
nulle part par nous, et aucune colonne ne lui est dédiée.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone

from app.db.connection import get_db

ST_EN_COURS = "EN_COURS"
ST_SUCCES = "SUCCES"
ST_ECHEC = "ECHEC"

# Seules ces tables sont écrites par la couche RAW. La liste est vérifiée par un test : elle sert
# de frontière explicite avec le module comptable.
TABLES_RAW = ("qonto_accounts", "qonto_transactions_raw", "qonto_sync_runs")


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _empreinte(charge: dict) -> str:
    """SHA-256 d'une charge utile canonique : dit si Qonto a changé quelque chose, quoi que ce soit."""
    canonique = json.dumps(charge, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonique.encode("utf-8")).hexdigest()


def tables_presentes(*, db_path=None) -> bool:
    """Les tables Qonto n'existent qu'à partir de la migration 0095 : absence = base non migrée,
    état légitime et pas une erreur."""
    conn = get_db(db_path)
    try:
        noms = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'qonto_%'")}
        return set(TABLES_RAW).issubset(noms)
    finally:
        conn.close()


def masquer_iban(iban) -> str:
    """`FR7630001007941234567890185` → `FR76 **** **** 0185`.

    L'IBAN complet est stocké (c'est la clé d'appariement du futur rapprochement) mais ne doit
    jamais s'afficher entier. Tout écran passe par ici.
    """
    if not iban:
        return "—"
    propre = str(iban).replace(" ", "")
    if len(propre) < 8:
        return "****"
    return f"{propre[:4]} **** **** {propre[-4:]}"


# ── lecture d'un compte tel que Qonto le décrit ───────────────────────────────────────────────
def _valeurs_compte(compte: dict, horodatage: str) -> tuple:
    return (
        compte.get("id"),
        compte.get("slug"),
        compte.get("name"),
        compte.get("currency"),
        compte.get("status"),
        compte.get("iban"),
        compte.get("bic"),
        compte.get("balance"),
        compte.get("balance_cents"),
        compte.get("authorized_balance"),
        compte.get("authorized_balance_cents"),
        1 if compte.get("main") else 0,
        1 if compte.get("is_external_account") else 0,
        compte.get("updated_at"),
        horodatage,
    )


def _valeurs_mouvement(mouvement: dict, horodatage: str, run_id: str) -> tuple:
    justificatifs = mouvement.get("attachment_ids") or []
    return (
        mouvement.get("id"),
        mouvement.get("transaction_id"),
        mouvement.get("bank_account_id"),
        mouvement.get("amount"),
        mouvement.get("amount_cents"),
        mouvement.get("currency"),
        mouvement.get("local_amount"),
        mouvement.get("local_amount_cents"),
        mouvement.get("local_currency"),
        mouvement.get("side"),
        mouvement.get("status"),
        mouvement.get("operation_type"),
        mouvement.get("label"),
        mouvement.get("reference"),
        mouvement.get("note"),
        mouvement.get("clean_counterparty_name"),
        mouvement.get("emitted_at"),
        mouvement.get("settled_at"),
        mouvement.get("created_at"),
        mouvement.get("updated_at"),
        mouvement.get("settled_balance"),
        mouvement.get("settled_balance_cents"),
        mouvement.get("category"),
        # Catégorie de flux posée par le TITULAIRE dans Qonto : information humaine, pas
        # une déduction — le moteur de suggestions s'en sert comme d'une preuve.
        ((mouvement.get("cashflow_category") or {}).get("name")
         if isinstance(mouvement.get("cashflow_category"), dict) else None),
        mouvement.get("vat_amount"),
        mouvement.get("vat_rate"),
        mouvement.get("card_last_digits"),
        len(justificatifs) if isinstance(justificatifs, list) else 0,
        1 if mouvement.get("attachment_required") else 0,
        1 if mouvement.get("is_external_transaction") else 0,
        json.dumps(mouvement, sort_keys=True, ensure_ascii=False),
        _empreinte(mouvement),
        horodatage,
        run_id,
    )


_COLONNES_MOUVEMENT = """
    qonto_transaction_uuid, transaction_id, qonto_account_id,
    montant, montant_cents, devise, montant_local, montant_local_cents, devise_locale,
    sens, statut, type_operation,
    libelle, reference, note, contrepartie,
    emis_le, regle_le, cree_le, maj_le,
    solde_apres, solde_apres_cents, categorie, categorie_flux, tva_montant, tva_taux,
    carte_4_derniers,
    justificatifs_nb, justificatif_requis, operation_externe,
    charge_utile_json, empreinte
"""


def enregistrer(comptes: list[dict], mouvements: list[dict], *, pages_lues: int = 0,
                db_path=None) -> dict:
    """Écrit une synchronisation complète, en UNE transaction. Retourne le bilan chiffré.

    Rien n'est écrit tant que tout n'a pas été collecté : l'appelant fournit ici des données déjà
    complètes. Si l'écriture échoue à mi-chemin, le `ROLLBACK` laisse la base exactement dans son
    état précédent — le dernier jeu valide est conservé.
    """
    horodatage = _maintenant()
    run_id = f"QSYNC-{uuid.uuid4().hex[:12].upper()}"

    # Un même mouvement peut apparaître deux fois dans UNE collecte : deux pages qui se recouvrent
    # parce qu'une écriture est tombée pendant la pagination. Le dédoublonnage est fait ici, à
    # l'entrée de l'écriture, et pas seulement dans le client : la promesse « jamais de doublon »
    # doit tenir quelle que soit la qualité de ce qu'on nous passe. La dernière version l'emporte.
    uniques: dict[str, dict] = {}
    for mouvement in mouvements:
        identifiant = mouvement.get("id")
        if identifiant:
            uniques[identifiant] = mouvement

    bilan = {"sync_run_id": run_id, "comptes": 0, "vues": len(uniques),
             "creees": 0, "mises_a_jour": 0, "inchangees": 0, "pages_lues": pages_lues}

    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")

        for compte in comptes:
            if not compte.get("id"):
                continue
            conn.execute(
                "INSERT INTO qonto_accounts (qonto_account_id, slug, nom, devise, statut, iban, "
                "bic, solde, solde_cents, solde_autorise, solde_autorise_cents, compte_principal, "
                "compte_externe, maj_qonto, premiere_recuperation, derniere_synchronisation) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(qonto_account_id) DO UPDATE SET "
                "  slug=excluded.slug, nom=excluded.nom, devise=excluded.devise, "
                "  statut=excluded.statut, iban=excluded.iban, bic=excluded.bic, "
                "  solde=excluded.solde, solde_cents=excluded.solde_cents, "
                "  solde_autorise=excluded.solde_autorise, "
                "  solde_autorise_cents=excluded.solde_autorise_cents, "
                "  compte_principal=excluded.compte_principal, "
                "  compte_externe=excluded.compte_externe, maj_qonto=excluded.maj_qonto, "
                "  derniere_synchronisation=excluded.derniere_synchronisation",
                _valeurs_compte(compte, horodatage) + (horodatage,))
            bilan["comptes"] += 1

        # Empreintes déjà connues : décide création / mise à jour / rien, sans relire les lignes.
        connues = {r[0]: r[1] for r in conn.execute(
            "SELECT qonto_transaction_uuid, empreinte FROM qonto_transactions_raw")}

        for identifiant, mouvement in uniques.items():
            empreinte = _empreinte(mouvement)
            if identifiant not in connues:
                conn.execute(
                    f"INSERT INTO qonto_transactions_raw ({_COLONNES_MOUVEMENT}, "
                    "premiere_recuperation, derniere_maj, dernier_sync_run_id) "
                    "VALUES (" + ",".join(["?"] * 32) + ",?,?,?)",
                    _valeurs_mouvement(mouvement, horodatage, run_id)[:32]
                    + (horodatage, horodatage, run_id))
                bilan["creees"] += 1
            elif connues[identifiant] != empreinte:
                # Qonto a modifié ce mouvement : on réécrit tout sauf la date de PREMIÈRE
                # récupération, qui est un fait historique.
                valeurs = _valeurs_mouvement(mouvement, horodatage, run_id)
                conn.execute(
                    "UPDATE qonto_transactions_raw SET "
                    "  transaction_id=?, qonto_account_id=?, montant=?, montant_cents=?, devise=?, "
                    "  montant_local=?, montant_local_cents=?, devise_locale=?, sens=?, statut=?, "
                    "  type_operation=?, libelle=?, reference=?, note=?, contrepartie=?, "
                    "  emis_le=?, regle_le=?, cree_le=?, maj_le=?, solde_apres=?, "
                    "  solde_apres_cents=?, categorie=?, categorie_flux=?, tva_montant=?, tva_taux=?, "
                    "  carte_4_derniers=?, justificatifs_nb=?, justificatif_requis=?, "
                    "  operation_externe=?, charge_utile_json=?, empreinte=?, derniere_maj=?, "
                    "  dernier_sync_run_id=? "
                    "WHERE qonto_transaction_uuid=?",
                    valeurs[1:32] + (horodatage, run_id, identifiant))
                bilan["mises_a_jour"] += 1
            else:
                # Rien n'a bougé : la ligne n'est même pas touchée. C'est ce qui rend une seconde
                # synchronisation vérifiable — aucune date ne bouge sans raison.
                bilan["inchangees"] += 1

        conn.execute(
            "INSERT INTO qonto_sync_runs (sync_run_id, demarre_le, termine_le, statut, "
            "comptes_vus, pages_lues, transactions_vues, transactions_creees, "
            "transactions_mises_a_jour, transactions_inchangees) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (run_id, horodatage, _maintenant(), ST_SUCCES, bilan["comptes"], pages_lues,
             bilan["vues"], bilan["creees"], bilan["mises_a_jour"], bilan["inchangees"]))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return bilan


def enregistrer_echec(code: str, detail: str = "", *, db_path=None) -> str:
    """Trace une synchronisation ratée SANS toucher aux données déjà importées.

    `detail` est un code technique court (« HTTP 502 », « ConnectionError »), jamais une URL
    complète ni un en-tête : rien qui puisse contenir un identifiant.
    """
    run_id = f"QSYNC-{uuid.uuid4().hex[:12].upper()}"
    horodatage = _maintenant()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO qonto_sync_runs (sync_run_id, demarre_le, termine_le, statut, "
            "code_erreur, message_erreur) VALUES (?,?,?,?,?,?)",
            (run_id, horodatage, horodatage, ST_ECHEC, code, (detail or "")[:200]))
        conn.commit()
    finally:
        conn.close()
    return run_id


# ── lectures ──────────────────────────────────────────────────────────────────────────────────
def comptes(*, db_path=None) -> list[dict]:
    """Comptes importés, IBAN déjà masqué : cette fonction alimente des écrans."""
    conn = get_db(db_path)
    try:
        lignes = []
        for r in conn.execute("SELECT * FROM qonto_accounts ORDER BY compte_principal DESC, nom"):
            compte = dict(r)
            compte["iban_masque"] = masquer_iban(compte.pop("iban", None))
            lignes.append(compte)
        return lignes
    finally:
        conn.close()


def transactions(*, qonto_account_id: str | None = None, limite: int | None = None,
                 db_path=None) -> list[dict]:
    conn = get_db(db_path)
    try:
        sql = "SELECT * FROM qonto_transactions_raw"
        params: list = []
        if qonto_account_id:
            sql += " WHERE qonto_account_id = ?"
            params.append(qonto_account_id)
        sql += " ORDER BY COALESCE(regle_le, emis_le, cree_le) DESC"
        if limite:
            sql += " LIMIT ?"
            params.append(int(limite))
        return [dict(r) for r in conn.execute(sql, params)]
    finally:
        conn.close()


def compter(*, db_path=None) -> dict:
    conn = get_db(db_path)
    try:
        return {
            "comptes": conn.execute("SELECT COUNT(*) FROM qonto_accounts").fetchone()[0],
            "transactions": conn.execute(
                "SELECT COUNT(*) FROM qonto_transactions_raw").fetchone()[0],
            "synchronisations": conn.execute("SELECT COUNT(*) FROM qonto_sync_runs").fetchone()[0],
        }
    finally:
        conn.close()


def derniere_synchronisation(*, db_path=None) -> dict:
    conn = get_db(db_path)
    try:
        ligne = conn.execute(
            "SELECT * FROM qonto_sync_runs ORDER BY demarre_le DESC, rowid DESC LIMIT 1").fetchone()
        return dict(ligne) if ligne else {}
    finally:
        conn.close()
