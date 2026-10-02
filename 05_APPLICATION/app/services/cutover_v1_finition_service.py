"""Finition du cutover V1 — plus aucun reliquat de comptabilité antérieure au 1er septembre 2026.

DÉCISIONS (2026-10-03, ordre explicite de l'utilisateur — `00_CADRAGE/CUTOVER_V1_2026-09.md`)
  · D-V1-FIN-1 — la comptabilité PROPRIÉTAIRE antérieure à la V1 n'est pas conservée, pas même
    sous forme d'historique consultable : relevés, soldes, restes à payer, règlements et données
    calculées correspondantes sont supprimés dans TOUS les runs (actif, inactifs, anciens) et ne
    peuvent plus être recalculés (Lot10 : motif d'exclusion `ANTERIEUR_V1`).
  · D-V1-FIN-2 — une facture FOURNISSEUR antérieure à la V1 ne subsiste que si elle est une
    dépendance nécessaire d'une charge déjà rapprochée avec la banque ; elle ne crée alors aucune
    dette. Une facture sans cette dépendance est purgée, même si sa pièce PDF est réelle : la pièce
    reste dans le dossier source, l'import la reconnaît et ne la reprend plus (`ANTERIEURE_V1`).

CE QUE CE MODULE NE FAIT PAS
  Il ne refait pas le cutover (`cutover_v1_service`, déjà appliqué, refuse tout second passage) et
  ne touche à aucune source : banque, Hostaway, réservations, archives, référentiels, fiche société,
  charges rapprochées et leurs rapprochements sont prouvés intacts, par empreinte, AVANT le COMMIT.

  Il ne tranche pas non plus un cas que la décision ne couvre pas. Une facture fournisseur
  antérieure qui serait référencée hors de ses propres tables (charge, règlement, rapprochement,
  lettrage, écriture, justificatif) est une ANOMALIE : rien n'est supprimé, la décision revient à
  l'utilisateur. Au 2026-10-03, aucune des 15 factures n'est dans ce cas.

MÉTHODE — celle du cutover
  `simuler` : lecture seule (`mode=ro`) ; deux passages rendent la même empreinte.
  `executer` : UNE transaction `BEGIN IMMEDIATE` ; plan recalculé dans la transaction ; anomalie =
  aucun effacement ; enfants avant parents ; invariants A → U vérifiés AVANT le COMMIT ; ROLLBACK
  sinon. Idempotent : un second passage ne trouve rien à supprimer.
  `verifier` : après coup, sans écriture.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Callable

from app.services import cutover_v1_service as cut
from app.services import perimetre_v1_service as v1

E_CUTOVER_ABSENT = "FINITION_SANS_CUTOVER"
E_CONFIRMATION = "FINITION_CONFIRMATION_REQUISE"
E_ANOMALIES = "FINITION_ANOMALIES"
E_VERIFICATION = "FINITION_VERIFICATION_ECHOUEE"

# ── Comptabilité propriétaire calculée (Lot10, Lot12, Lot11) ────────────────────────────────────
#: Sorties du moteur Lot10 portant un mois : toutes relèvent de la comptabilité propriétaire ou des
#: résultats qui la composent (D-V1-FIN-1). Tous runs confondus.
LOT10_PAR_MOIS = ("lot10_commissions", "lot10_net_exploitation", "lot10_net_reglement",
                  "lot10_net_vue_mois", "lot10_resultats", "lot10_run_mois_provenance")
LOT10_ANOMALIES = "lot10_commissions_a_controler"
LOT10_PROVENANCE = "lot10_run_mois_provenance"
#: Trace qu'un run Lot10 écrit pour CHAQUE mois antérieur qu'il exclut (motif `ANTERIEUR_V1`) : elle
#: dit qu'aucune donnée n'a été produite. Ce n'est pas de la comptabilité, c'est la preuve de son
#: absence — elle n'est ni purgée ni comptée comme un reliquat.
MODE_TRACE_EXCLUSION_V1 = "EXCLU_PERIMETRE_ANTERIEUR_V1"
CODE_ANOMALIE_CANAPE = "GUEST_COUNT_MANQUANT_PREPARATION_CANAPE"
LOT12_ENTETES = "lot12_prefactures_entete"
LOT12_PAR_FACTURE = ("lot12_prefactures_lignes", "lot12_prefactures_id_legacy")
LOT12_PAR_MOIS = ("lot12_controle_mensuel", "lot12_dashboard_facturation", "lot12_a_controler")
LOT11_DASHBOARD = "controles_lot11_dashboard_mois"
#: Un constat Lot11 ne relève de la comptabilité propriétaire que s'il porte sur une sortie Lot10 ou
#: Lot12 : les contrôles des SOURCES (réservations, banque, ménages) restent, ils ne disent rien d'une
#: comptabilité.
PREFIXES_SOURCES_COMPTABLES = ("lot10_", "lot12_")
#: Modules de `controles_suivi` qui ne suivent que des contrôles de comptabilité propriétaire.
MODULES_SUIVI_COMPTABLES = ("COMMISSIONS", "EXPLOITATION", "REGLEMENT", "RESULTATS", "FACTURATION")

# ── Factures fournisseurs ─────────────────────────────────────────────────────────────────────────
#: Tables propres d'une facture fournisseur — la liste de `factures_service._TABLES_FILLES`, dans son
#: ordre (feuilles → racine), plus les interprétations versionnées. Hors de ces tables, une
#: référence à la facture est une dépendance réelle qu'il faut examiner.
TABLES_PROPRES_FACTURE = ("facture_lignes_menage_detail", "facture_lignes_menage_pdf",
                          "facture_lignes_menage", "facture_lignes", "facture_ventilation_parts",
                          "facture_ventilations", "facture_classification", "facture_pdf_diagnostics",
                          "facture_evenements", "facture_interpretations", "factures")
VERDICT_ANTERIEURE_V1 = "ANTERIEURE_V1"
MARQUE_FINITION = "CUTOVER_V1_FINITION"

# ── Ce qui doit rester identique, octet pour octet ───────────────────────────────────────────────
CHAINES_CHARGES = ("charges", "charge_evenements", "charges_perimetre_analytique",
                   "charges_perimetre_menage", "charges_refacturation_positions",
                   "charges_refacturation_evenements", "banque_rapprochements",
                   "banque_rapprochement_evenements", "flux_lettrages", "flux_lettrage_lignes",
                   "flux_lettrage_evenements", "ecritures", "ecriture_lignes",
                   "ecriture_evenements", "justificatifs", "justificatif_evenements")
TABLES_PROTEGEES = tuple(dict.fromkeys(
    cut.BANQUE + cut.HOSTAWAY + cut.RESERVATIONS + cut.ARCHIVES_RESERVATIONS + cut.REFERENTIELS
    + cut.MENAGES_SOURCES + CHAINES_CHARGES
    + ("parametres_societe_facturation", "reservations_resolues", "reservations_calculees",
       "reservations_datasets", "menages_pdf_fichiers_hash", "proprietaires_releves",
       "factures_proprietaires", "mouvements_tresorerie_proprietaires")))


@dataclass
class Plan:
    debut: str
    mois_v1: str
    #: (table, rowid) à supprimer, par table — l'identité physique de chaque ligne visée.
    lignes: dict[str, list[int]] = field(default_factory=dict)
    #: Avant / à supprimer, par table, pour le rapport.
    comptes: dict[str, dict[str, Any]] = field(default_factory=dict)
    factures: list[dict[str, Any]] = field(default_factory=list)
    verdicts: list[dict[str, Any]] = field(default_factory=list)
    anomalies: list[str] = field(default_factory=list)
    infos: dict[str, Any] = field(default_factory=dict)


def _q(conn, sql: str, args: tuple = ()) -> list[sqlite3.Row]:
    return list(conn.execute(sql, args))


def _rowids(conn, table: str, ou: str, args: tuple = ()) -> list[int]:
    if not cut._existe(conn, table):
        return []
    return [r[0] for r in conn.execute(f'SELECT rowid FROM "{table}" WHERE {ou}', args)]


def _noter(p: Plan, table: str, rowids: list[int], *, detail: dict[str, Any] | None = None) -> None:
    existants = p.lignes.setdefault(table, [])
    vus = set(existants)
    existants.extend(r for r in rowids if r not in vus)
    p.comptes[table] = {"avant": None, "a_supprimer": len(existants), **(detail or {})}


def _cle_resa(v: Any) -> str:
    try:
        return str(int(float(v)))
    except (TypeError, ValueError):
        return ""


def _mois_des_reservations(conn) -> dict[str, str]:
    """Réservation Hostaway → mois d'arrivée (D097 : le mois d'une réservation est celui de son
    arrivée). Dataset RÉSOLU actif d'abord, puis la date d'arrivée Hostaway pour les réservations
    exclues du calcul (annulées, séjours propriétaire…), qui n'y figurent pas toutes."""
    mois: dict[str, str] = {}
    if cut._existe(conn, "hostaway_reservations"):
        for r in conn.execute("SELECT reservation_id, check_in_date FROM hostaway_reservations "
                              "ORDER BY id"):
            cle, m = _cle_resa(r[0]), str(r[1] or "")[:7]
            if cle and len(m) == 7:
                mois[cle] = m
    if cut._existe(conn, "reservations_datasets") and cut._existe(conn, "reservations_resolues"):
        ds = conn.execute("SELECT dataset_id FROM reservations_datasets WHERE etape='RESOLUES' "
                          "AND actif=1 ORDER BY rowid DESC LIMIT 1").fetchone()
        if ds:
            for r in conn.execute("SELECT reservation_id_hostaway, date_arrivee FROM "
                                  "reservations_resolues WHERE dataset_id = ?", (ds[0],)):
                cle, m = _cle_resa(r[0]), str(r[1] or "")[:7]
                if cle and len(m) == 7:
                    mois[cle] = m
    return mois


# ── Plan ──────────────────────────────────────────────────────────────────────────────────────────

def _plan_lot10(conn, p: Plan) -> None:
    mois_v1 = p.mois_v1
    for t in LOT10_PAR_MOIS:
        ou, args = "substr(mois,1,7) < ? AND mois IS NOT NULL AND mois <> ''", (mois_v1,)
        if t == LOT10_PROVENANCE:
            ou, args = ou + " AND COALESCE(mode_traitement, '') <> ?", (mois_v1, MODE_TRACE_EXCLUSION_V1)
        ids = _rowids(conn, t, ou, args)
        par_run = {r[0]: r[1] for r in conn.execute(
            f'SELECT run_id, COUNT(*) FROM "{t}" WHERE {ou} GROUP BY run_id', args)}             if cut._existe(conn, t) else {}
        _noter(p, t, ids, detail={"par_run": par_run})

    # Anomalies de commission : pas de mois stocké. Attribuées par la réservation, ou — pour les
    # anomalies « canapé », sans identifiant de réservation — par les lignes de commission du MÊME
    # run qui les ont produites (`controle_preparation_canape = 'A_CONTROLER'`).
    if not cut._existe(conn, LOT10_ANOMALIES):
        _noter(p, LOT10_ANOMALIES, [])
        return
    mois_resa = _mois_des_reservations(conn)
    canape: dict[str, list[str]] = {}
    for r in conn.execute("SELECT run_id, mois FROM lot10_commissions "
                          "WHERE controle_preparation_canape = 'A_CONTROLER'"):
        canape.setdefault(r[0], []).append(str(r[1] or "")[:7])
    a_supprimer, non_attribuables = [], 0
    for r in conn.execute(f"SELECT rowid, run_id, reservation_id, code_anomalie_lot10 "
                          f"FROM {LOT10_ANOMALIES}"):
        rowid, run, resa, code = r[0], r[1], r[2], r[3]
        if _cle_resa(resa):
            m = mois_resa.get(_cle_resa(resa), "")
            if m and m < mois_v1:
                a_supprimer.append(rowid)
            elif not m:
                non_attribuables += 1
        elif code == CODE_ANOMALIE_CANAPE:
            mois_run = canape.get(run, [])
            if mois_run and all(m and m < mois_v1 for m in mois_run):
                a_supprimer.append(rowid)
            elif mois_run and any(m and m < mois_v1 for m in mois_run):
                p.anomalies.append(f"{LOT10_ANOMALIES} (run {run}) : anomalies canapé à cheval sur "
                                   "la V1, attribution impossible ligne à ligne")
            else:
                non_attribuables += 1 if not mois_run else 0
        else:
            non_attribuables += 1
    _noter(p, LOT10_ANOMALIES, a_supprimer,
           detail={"non_attribuables_conservees": non_attribuables})


def _plan_lot12(conn, p: Plan) -> None:
    mois_v1 = p.mois_v1
    entetes = _q(conn, f"SELECT rowid, run_id, facture_id FROM {LOT12_ENTETES} "
                       "WHERE substr(mois,1,7) < ?", (mois_v1,)) \
        if cut._existe(conn, LOT12_ENTETES) else []
    _noter(p, LOT12_ENTETES, [r[0] for r in entetes])
    cles = {(r[1], r[2]) for r in entetes}
    for t in LOT12_PAR_FACTURE:
        ids = [r[0] for r in conn.execute(f'SELECT rowid, run_id, facture_id FROM "{t}"')
               if (r[1], r[2]) in cles] if cut._existe(conn, t) else []
        _noter(p, t, ids)
    for t in LOT12_PAR_MOIS:
        if t != "lot12_a_controler":
            _noter(p, t, _rowids(conn, t, "substr(mois,1,7) < ? AND mois <> ''", (mois_v1,)))
    _plan_lot12_a_controler(conn, p)


def _plan_lot12_a_controler(conn, p: Plan) -> None:
    """Les contrôles de facturation Lot12 sont, pour l'essentiel, la COPIE des anomalies de
    commission du Lot10 actif au moment du run (`impact_facturation = EXCLUE_DE_FACTURE`), sans mois
    stocké. Même attribution que pour le Lot10 : le mois de la réservation ; pour les anomalies
    « canapé », sans réservation, les lignes de commission du run Lot10 qui les a produites (le
    dernier run Lot10 calculé avant ce run Lot12)."""
    t = "lot12_a_controler"
    if not cut._existe(conn, t):
        _noter(p, t, [])
        return
    mois_v1 = p.mois_v1
    mois_resa = _mois_des_reservations(conn)
    runs10 = [(r[0], r[1]) for r in conn.execute(
        "SELECT run_id, date_calcul FROM lot10_runs ORDER BY date_calcul")]         if cut._existe(conn, "lot10_runs") else []
    dates12 = {r[0]: r[1] for r in conn.execute("SELECT run_id, date_calcul FROM lot12_runs")}         if cut._existe(conn, "lot12_runs") else {}
    canape10: dict[str, list[str]] = {}
    for r in conn.execute("SELECT run_id, mois FROM lot10_commissions "
                          "WHERE controle_preparation_canape = 'A_CONTROLER'"):
        canape10.setdefault(r[0], []).append(str(r[1] or "")[:7])

    def _run10(run12: str) -> str:
        d12 = str(dates12.get(run12) or "")
        precedents = [rid for rid, d in runs10 if str(d or "") <= d12]
        return precedents[-1] if precedents else ""

    a_supprimer, non_attribuables = [], 0
    for r in conn.execute(f"SELECT rowid, run_id, mois, reservation, code_anomalie FROM {t}"):
        rowid, run12, mois, resa, code = r[0], r[1], str(r[2] or "")[:7], r[3], r[4]
        if mois:
            if mois < mois_v1:
                a_supprimer.append(rowid)
            continue
        if _cle_resa(resa):
            m = mois_resa.get(_cle_resa(resa), "")
            if m and m < mois_v1:
                a_supprimer.append(rowid)
            elif not m:
                non_attribuables += 1
        elif code == CODE_ANOMALIE_CANAPE:
            mois_run = canape10.get(_run10(run12), [])
            if mois_run and all(m and m < mois_v1 for m in mois_run):
                a_supprimer.append(rowid)
            elif mois_run and any(m and m < mois_v1 for m in mois_run):
                p.anomalies.append(f"{t} (run {run12}) : anomalies canapé à cheval sur la V1")
            elif not mois_run:
                non_attribuables += 1
        else:
            non_attribuables += 1
    _noter(p, t, a_supprimer, detail={"non_attribuables_conservees": non_attribuables})


def _plan_lot11(conn, p: Plan) -> None:
    mois_v1 = p.mois_v1
    pks = set()
    if cut._existe(conn, "controles_lot11_constats_champs") and cut._existe(
            conn, "controles_lot11_constats"):
        anciens = {r[0] for r in conn.execute(
            "SELECT ctrl_pk FROM controles_lot11_constats_champs WHERE substr(mois,1,7) < ? "
            "AND mois <> ''", (mois_v1,))}
        for r in conn.execute("SELECT ctrl_pk, source_table FROM controles_lot11_constats"):
            if r[0] in anciens and str(r[1] or "").startswith(PREFIXES_SOURCES_COMPTABLES):
                pks.add(r[0])
        _noter(p, "controles_lot11_constats_champs",
               [r[0] for r in conn.execute(
                   "SELECT rowid, ctrl_pk FROM controles_lot11_constats_champs") if r[1] in pks])
        _noter(p, "controles_lot11_constats",
               [r[0] for r in conn.execute(
                   "SELECT rowid, ctrl_pk FROM controles_lot11_constats") if r[1] in pks])
    _noter(p, LOT11_DASHBOARD, _rowids(conn, LOT11_DASHBOARD,
                                       "mois <> 'TRANSVERSE' AND substr(mois,1,7) < ?", (mois_v1,)))

    # Décisions humaines rattachées à ces contrôles : sans objet une fois la comptabilité supprimée.
    suivis = _q(conn, "SELECT rowid, controle_id_opaque, ctrl_pk_moteur, module FROM "
                      "controles_suivi WHERE substr(mois,1,7) < ? AND mois <> ''", (mois_v1,)) \
        if cut._existe(conn, "controles_suivi") else []
    retenus = [s for s in suivis if s[2] in pks or str(s[3] or "") in MODULES_SUIVI_COMPTABLES]
    _noter(p, "controles_suivi", [s[0] for s in retenus])
    opaques = {s[1] for s in retenus}
    _noter(p, "controles_suivi_historique",
           [r[0] for r in conn.execute("SELECT rowid, controle_id_opaque FROM "
                                       "controles_suivi_historique") if r[1] in opaques]
           if cut._existe(conn, "controles_suivi_historique") else [])
    p.infos["controles_suivi"] = [dict(s) for s in retenus]


def _references_hors_tables_propres(conn, ids: set[str]) -> dict[str, list[str]]:
    """Toute colonne, hors des tables propres d'une facture, qui contient l'identifiant de l'une
    d'elles — la preuve qu'elle n'est (ou n'est pas) une dépendance d'autre chose."""
    trouvees: dict[str, list[str]] = {}
    if not ids:
        return trouvees
    marques = ",".join("?" * len(ids))
    for t in cut._tables(conn):
        if t in TABLES_PROPRES_FACTURE or t == "menages_pdf_fichiers_hash":
            continue
        for col in cut._colonnes(conn, t):
            try:
                rows = conn.execute(f'SELECT "{col}" FROM "{t}" WHERE "{col}" IN ({marques})',
                                    tuple(ids)).fetchall()
            except sqlite3.Error:
                continue
            for r in rows:
                trouvees.setdefault(str(r[0]), []).append(f"{t}.{col}")
    return trouvees


def _plan_factures_fournisseurs(conn, p: Plan) -> None:
    if not cut._existe(conn, "factures"):
        return
    factures = [dict(r) for r in conn.execute(
        "SELECT * FROM factures WHERE date_facture IS NOT NULL AND TRIM(date_facture) <> '' "
        "AND substr(date_facture,1,10) < ? ORDER BY date_facture, id", (p.debut,))]
    ids = {f["facture_id_opaque"] for f in factures}
    refs = _references_hors_tables_propres(conn, ids)
    libelles = {r[0]: r[1] for r in conn.execute(
        "SELECT intervenant_id, COALESCE(nom_intervenant, intervenant_id) FROM ref_intervenants")} \
        if cut._existe(conn, "ref_intervenants") and "nom_intervenant" in cut._colonnes(
            conn, "ref_intervenants") else {}
    for f in factures:
        fid = f["facture_id_opaque"]
        logements = sorted({str(r[0]) for r in conn.execute(
            "SELECT DISTINCT logement_id FROM facture_lignes_menage WHERE facture_id_opaque = ? "
            "AND logement_id IS NOT NULL AND logement_id <> ''", (fid,))})
        dependances = refs.get(fid, [])
        lien_charge = f.get("charge_id")
        fichier = conn.execute("SELECT nom_fichier FROM facture_pdf_diagnostics WHERE "
                               "facture_id_opaque = ? ORDER BY id DESC LIMIT 1", (fid,)).fetchone()
        decision = cut.PURGE if not dependances and not lien_charge else "ANOMALIE"
        if decision == "ANOMALIE":
            p.anomalies.append(f"facture fournisseur {fid} ({f.get('facture_ref')}) référencée "
                               f"hors de ses tables : {dependances or ['charge_id']} — décision "
                               "utilisateur requise (D-V1-FIN-2)")
        p.factures.append({
            "facture_id_opaque": fid, "reference": f.get("facture_ref_source") or f.get("facture_ref"),
            "date_facture": f.get("date_facture"),
            "fournisseur_id": f.get("fournisseur_id_opaque"),
            "fournisseur": libelles.get(f.get("fournisseur_id_opaque"), f.get("fournisseur_id_opaque")),
            "montant_ttc": f.get("montant_ttc"), "statut": f.get("statut"),
            "fichier_source": fichier[0] if fichier else f.get("justificatif"),
            "logements": logements, "charge_liee": lien_charge, "reglement": None,
            "mouvement_bancaire": None, "rapprochement_valide": False,
            "dependances_hors_tables_propres": dependances,
            "decision": decision,
            "justification": ("Aucune charge, aucun règlement, aucun rapprochement, aucun lettrage, "
                              "aucune écriture : seules ses tables propres la référencent. Elle ne "
                              "soutient aucune dépense rapprochée — D-V1-FIN-2, cas B.")
            if decision == cut.PURGE else "Dépendance à examiner.",
        })
    purgees = {f["facture_id_opaque"] for f in p.factures if f["decision"] == cut.PURGE}
    lignes_menage = {r[0] for r in conn.execute(
        "SELECT ligne_id_opaque FROM facture_lignes_menage WHERE facture_id_opaque IN "
        f"({','.join('?' * len(purgees))})", tuple(purgees))} if purgees else set()
    ventilations = {r[0] for r in conn.execute(
        "SELECT ventilation_id_opaque FROM facture_ventilations WHERE facture_id_opaque IN "
        f"({','.join('?' * len(purgees))})", tuple(purgees))} if purgees else set()
    for t in TABLES_PROPRES_FACTURE:
        if not cut._existe(conn, t):
            continue
        cols = cut._colonnes(conn, t)
        if "facture_id_opaque" in cols:
            ids_t = [r[0] for r in conn.execute(f'SELECT rowid, facture_id_opaque FROM "{t}"')
                     if r[1] in purgees]
        elif "ligne_id_opaque" in cols:
            ids_t = [r[0] for r in conn.execute(f'SELECT rowid, ligne_id_opaque FROM "{t}"')
                     if r[1] in lignes_menage]
        elif "ventilation_id_opaque" in cols:
            ids_t = [r[0] for r in conn.execute(f'SELECT rowid, ventilation_id_opaque FROM "{t}"')
                     if r[1] in ventilations]
        else:
            ids_t = []
        _noter(p, t, ids_t)
    # Justificatifs : la base en interdit la suppression. Une facture purgée qui en porterait un
    # est donc un cas non tranché, jamais un effacement forcé.
    if purgees and cut._existe(conn, "justificatifs") and "objet_id" in cut._colonnes(
            conn, "justificatifs"):
        n = conn.execute("SELECT COUNT(*) FROM justificatifs WHERE objet_id IN "
                         f"({','.join('?' * len(purgees))})", tuple(purgees)).fetchone()[0]
        if n:
            p.anomalies.append(f"{n} justificatif(s) rattaché(s) à une facture à purger")
    # Verdict « antérieur à la V1 » pour chaque pièce : l'import la reconnaîtra sans la reprendre.
    for fid in sorted(purgees):
        d = conn.execute("SELECT * FROM facture_pdf_diagnostics WHERE facture_id_opaque = ? "
                         "ORDER BY id DESC LIMIT 1", (fid,)).fetchone()
        if d is not None:
            d = dict(d)
            p.verdicts.append({
                "nom_fichier": d["nom_fichier"], "format_detecte": d.get("format_detecte"),
                "numero_facture": d.get("numero_facture"), "montant_total": d.get("montant_total"),
                "somme_lignes": d.get("somme_lignes"),
                "ecart_reconciliation": d.get("ecart_reconciliation"),
                "nb_lignes": d.get("nb_lignes"), "sha256_pdf": d.get("sha256_pdf"),
                "facture_purgee": fid})
    actives = [f for f in p.factures if f["statut"] in ("A_CONTROLER", "VALIDEE",
                                                         "PARTIELLEMENT_REGLEE", "LITIGE")]
    p.infos["dettes_actives_avant"] = {"nb": len(actives),
                                       "montant": round(sum(f["montant_ttc"] or 0 for f in actives), 2)}


def _rapprochements_charges(conn) -> dict[str, Any]:
    """La preuve 8 rapprochements → 7 charges : une ligne par charge, montants rapprochés."""
    mouvements = cut._mouvements_bancaires(conn)
    liens, raps, _ = cut._liens_charges(conn, mouvements)
    charges = {r["charge_id"]: dict(r) for r in conn.execute("SELECT * FROM charges")}
    detail = {}
    for r in conn.execute("SELECT * FROM banque_rapprochements"):
        if r["rapprochement_id_opaque"] not in raps:
            continue
        d = detail.setdefault(r["objet_id"], {"charge": r["objet_id"], "mouvements": [],
                                              "montants": [], "statuts": set()})
        d["mouvements"].append(r["mouvement_id_opaque"])
        d["montants"].append(round(r["montant_rapproche"] or 0, 2))
        d["statuts"].add(r["statut"])
    lignes = []
    for cid in sorted(charges):
        d = detail.get(cid, {"mouvements": [], "montants": [], "statuts": set()})
        lignes.append({"charge": cid, "montant_charge": round(charges[cid]["montant"] or 0, 2),
                       "mouvements": d["mouvements"], "montant_rapproche": round(sum(d["montants"]), 2),
                       "nb_rapprochements": len(d["mouvements"]),
                       "statut": "/".join(sorted(d["statuts"])) or "AUCUN"})
    orphelins = [cid for cid in detail if cid not in charges]
    return {"lignes": lignes, "nb_charges": len(charges), "nb_rapprochements": len(raps),
            "total_charges": round(sum(l["montant_charge"] for l in lignes), 2),
            "total_rapproche": round(sum(l["montant_rapproche"] for l in lignes), 2),
            "charges_sans_rapprochement": [l["charge"] for l in lignes if not l["nb_rapprochements"]],
            "rapprochements_orphelins": orphelins,
            "ecarts": [l["charge"] for l in lignes
                       if abs(l["montant_charge"] - l["montant_rapproche"]) > 0.005]}


def _plan(conn) -> Plan:
    debut = v1.debut_depuis_connexion(conn)
    if not debut:
        p = Plan(debut="", mois_v1="")
        p.anomalies.append("cutover V1 non appliqué : `V1_ACCOUNTING_START_DATE` absent")
        return p
    p = Plan(debut=debut, mois_v1=debut[:7])
    _plan_lot10(conn, p)
    _plan_lot12(conn, p)
    _plan_lot11(conn, p)
    _plan_factures_fournisseurs(conn, p)
    p.anomalies = list(dict.fromkeys(p.anomalies))
    for t, c in p.comptes.items():
        c["avant"] = cut._compte(conn, t)
        c["apres_attendu"] = c["avant"] - c["a_supprimer"]
    p.infos["rapprochements_charges"] = _rapprochements_charges(conn)
    return p


def _canonique(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=lambda o: sorted(o)
                      if isinstance(o, set) else str(o))


def _rapport(p: Plan) -> dict[str, Any]:
    comptables = {t: c for t, c in p.comptes.items() if t not in TABLES_PROPRES_FACTURE}
    fournisseurs = {t: c for t, c in p.comptes.items() if t in TABLES_PROPRES_FACTURE}
    rapport = {
        "debut_v1": p.debut,
        "comptabilite_proprietaire": comptables,
        "factures_fournisseurs": p.factures,
        "factures_fournisseurs_resume": {
            "analysees": len(p.factures),
            "keep_by_dependency": sum(1 for f in p.factures if f["decision"] == cut.KEEP_BY_DEPENDENCY),
            "purge": sum(1 for f in p.factures if f["decision"] == cut.PURGE),
            "anomalie": sum(1 for f in p.factures if f["decision"] == "ANOMALIE"),
            "montant_ttc": round(sum(f["montant_ttc"] or 0 for f in p.factures), 2)},
        "factures_fournisseurs_tables": fournisseurs,
        "verdicts_anterieure_v1_a_poser": len(p.verdicts),
        "dettes_actives_avant": p.infos.get("dettes_actives_avant", {"nb": 0, "montant": 0}),
        "dettes_actives_apres_attendu": 0,
        "controles_suivi": p.infos.get("controles_suivi", []),
        "rapprochements_charges": p.infos.get("rapprochements_charges"),
        "lignes_a_supprimer": sum(c["a_supprimer"] for c in p.comptes.values()),
        "impact": {"charges": 0, "banque": 0, "hostaway": 0, "reservations": 0,
                   "referentiels": 0, "fiche_societe": 0},
        "anomalies": p.anomalies,
    }
    rapport["empreinte_rapport"] = hashlib.sha256(_canonique(rapport).encode("utf-8")).hexdigest()[:24]
    return rapport


def simuler(*, db_path=None) -> dict[str, Any]:
    """CUTOVER V1 — FINITION, SIMULATION. Lecture seule : la base est ouverte en `mode=ro`."""
    conn = cut._connexion(db_path, lecture_seule=True)
    try:
        return {"mode": "SIMULATION", **_rapport(_plan(conn))}
    finally:
        conn.close()


# ── Exécution ─────────────────────────────────────────────────────────────────────────────────────

def _purger(conn, p: Plan) -> dict[str, int]:
    journal: dict[str, int] = {}
    ordre = (list(LOT10_PAR_MOIS) + [LOT10_ANOMALIES] + list(LOT12_PAR_FACTURE) + [LOT12_ENTETES]
             + list(LOT12_PAR_MOIS) + ["controles_lot11_constats_champs", "controles_lot11_constats",
                                       LOT11_DASHBOARD, "controles_suivi_historique",
                                       "controles_suivi"] + list(TABLES_PROPRES_FACTURE))
    for t in ordre:
        ids = p.lignes.get(t) or []
        n = 0
        for i in range(0, len(ids), 500):
            lot = ids[i:i + 500]
            n += conn.execute(f'DELETE FROM "{t}" WHERE rowid IN ({",".join("?" * len(lot))})',
                              tuple(lot)).rowcount
        journal[t] = n
    for v in p.verdicts:
        conn.execute(
            "INSERT INTO facture_pdf_diagnostics (nom_fichier, format_detecte, statut_extraction, "
            "numero_facture, montant_total, somme_lignes, ecart_reconciliation, nb_lignes, "
            "anomalies, mode_extraction, facture_id_opaque, sha256_pdf) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,NULL,?)",
            (v["nom_fichier"], v["format_detecte"], VERDICT_ANTERIEURE_V1, v["numero_facture"],
             v["montant_total"], v["somme_lignes"], v["ecart_reconciliation"], v["nb_lignes"],
             f"{VERDICT_ANTERIEURE_V1},{MARQUE_FINITION}", "PDF_AUTOMATIQUE", v["sha256_pdf"]))
    journal["facture_pdf_diagnostics (verdicts ANTERIEURE_V1 posés)"] = len(p.verdicts)
    return journal


def _empreintes(conn) -> dict[str, str]:
    return {t: cut.empreinte(conn, t) for t in TABLES_PROTEGEES}


def _compter(conn, sql: str, args: tuple = ()) -> int:
    try:
        return conn.execute(sql, args).fetchone()[0]
    except sqlite3.Error:
        return 0


def etat_comptable(conn, debut: str) -> dict[str, int]:
    """Ce qui reste, tous runs confondus, de la comptabilité d'avant la V1 — doit être 0 partout."""
    mois_v1 = debut[:7]
    etat = {t: _compter(conn, f'SELECT COUNT(*) FROM "{t}" WHERE substr(mois,1,7) < ? '
                              "AND mois <> ''", (mois_v1,))
            for t in LOT10_PAR_MOIS + LOT12_PAR_MOIS + (LOT12_ENTETES,) if t != LOT10_PROVENANCE}
    # Provenance : seule une ligne qui ANNONCE un mois calculé serait un reliquat ; la trace
    # d'exclusion `ANTERIEUR_V1` est attendue (un par mois antérieur vu par le moteur).
    etat[LOT10_PROVENANCE] = _compter(
        conn, f"SELECT COUNT(*) FROM {LOT10_PROVENANCE} WHERE substr(mois,1,7) < ? AND mois <> '' "
              "AND COALESCE(mode_traitement, '') <> ?", (mois_v1, MODE_TRACE_EXCLUSION_V1))
    etat[LOT11_DASHBOARD] = _compter(conn, f"SELECT COUNT(*) FROM {LOT11_DASHBOARD} WHERE mois <> "
                                           "'TRANSVERSE' AND substr(mois,1,7) < ?", (mois_v1,))
    for t in LOT12_PAR_FACTURE:
        etat[t + " (orphelines)"] = _compter(
            conn, f'SELECT COUNT(*) FROM "{t}" x WHERE NOT EXISTS (SELECT 1 FROM {LOT12_ENTETES} e '
                  'WHERE e.run_id = x.run_id AND e.facture_id = x.facture_id)')
    etat["proprietaires_releves"] = _compter(
        conn, "SELECT COUNT(*) FROM proprietaires_releves WHERE substr(mois,1,7) < ?", (mois_v1,))
    etat["factures_proprietaires"] = _compter(conn, "SELECT COUNT(*) FROM factures_proprietaires")
    etat["proprietaire_allocations"] = _compter(conn, "SELECT COUNT(*) FROM proprietaire_allocations")
    etat["imputations_airbnb"] = _compter(conn, "SELECT COUNT(*) FROM imputations_airbnb")
    etat["mouvements_tresorerie_proprietaires"] = _compter(
        conn, "SELECT COUNT(*) FROM mouvements_tresorerie_proprietaires "
              "WHERE substr(date_mouvement,1,10) < ?", (debut,))
    etat["controles_suivi"] = _compter(
        conn, "SELECT COUNT(*) FROM controles_suivi WHERE substr(mois,1,7) < ? AND module IN "
              f"({','.join('?' * len(MODULES_SUIVI_COMPTABLES))})",
        (mois_v1, *MODULES_SUIVI_COMPTABLES))
    etat["factures_fournisseurs"] = _compter(
        conn, "SELECT COUNT(*) FROM factures WHERE substr(date_facture,1,10) < ?", (debut,))
    etat["dettes_fournisseurs_actives"] = _compter(
        conn, "SELECT COUNT(*) FROM factures WHERE substr(date_facture,1,10) < ? AND statut IN "
              "('A_CONTROLER','VALIDEE','PARTIELLEMENT_REGLEE','LITIGE')", (debut,))
    for t, cle in (("facture_lignes_menage", "facture_id_opaque"),
                   ("facture_ventilations", "facture_id_opaque"),
                   ("facture_classification", "facture_id_opaque"),
                   ("facture_evenements", "facture_id_opaque"),
                   ("facture_interpretations", "facture_id_opaque")):
        etat[t + " (orphelines)"] = _compter(
            conn, f'SELECT COUNT(*) FROM "{t}" x WHERE x.{cle} IS NOT NULL AND NOT EXISTS '
                  f"(SELECT 1 FROM factures f WHERE f.facture_id_opaque = x.{cle})")
    etat["facture_pdf_diagnostics (orphelins)"] = _compter(
        conn, "SELECT COUNT(*) FROM facture_pdf_diagnostics d WHERE d.facture_id_opaque IS NOT NULL "
              "AND NOT EXISTS (SELECT 1 FROM factures f WHERE f.facture_id_opaque = "
              "d.facture_id_opaque)")
    return etat


def _assertions(conn, p: Plan, avant: dict[str, str], rap_avant: dict[str, Any]) -> list[dict]:
    resultats: list[dict[str, Any]] = []

    def ok(code: str, libelle: str, condition: bool, detail: Any = "") -> None:
        resultats.append({"code": code, "libelle": libelle, "ok": bool(condition), "detail": detail})

    etat = etat_comptable(conn, p.debut)
    restants = {k: v for k, v in etat.items() if v}
    plan_restant = _plan(conn)
    rap = _rapprochements_charges(conn)
    apres = _empreintes(conn)
    changees = sorted(t for t in TABLES_PROTEGEES if avant.get(t) != apres.get(t))

    def intacts(groupe) -> bool:
        return not [t for t in groupe if t in changees]

    ok("A", "Factures propriétaires = 0", etat["factures_proprietaires"] == 0)
    ok("B", "Créances = 0 (aucune facture émise, aucune allocation)",
       etat["factures_proprietaires"] == 0 and etat["proprietaire_allocations"] == 0)
    ok("C", "Relevés propriétaires antérieurs = 0 (relevés et net par mois, tous runs)",
       etat["proprietaires_releves"] == 0 and etat["lot10_net_vue_mois"] == 0)
    ok("D", "Soldes propriétaires antérieurs = 0 (règlements Lot10, tous runs)",
       etat["lot10_net_reglement"] == 0)
    ok("E", "Restes à payer antérieurs = 0 (Lot10 et Lot12, tous runs)",
       etat["lot10_net_reglement"] == 0 and etat[LOT12_ENTETES] == 0
       and etat["lot12_controle_mensuel"] == 0)
    ok("F", "Acomptes hérités = 0", etat["mouvements_tresorerie_proprietaires"] == 0)
    ok("G", "Compensations anciennes = 0", etat["imputations_airbnb"] == 0)
    ok("H", "Dette fournisseur active antérieure = 0 ; factures fournisseurs antérieures = 0",
       etat["dettes_fournisseurs_actives"] == 0 and etat["factures_fournisseurs"] == 0)
    ok("I", "Charges non rapprochées héritées = 0", not rap["charges_sans_rapprochement"],
       rap["charges_sans_rapprochement"])
    ok("J", "Charges rapprochées inchangées",
       rap["nb_charges"] == rap_avant["nb_charges"] and "charges" not in changees,
       rap["nb_charges"])
    ok("K", "Rapprochements banque ↔ charges inchangés, montants égaux",
       rap["nb_rapprochements"] == rap_avant["nb_rapprochements"]
       and "banque_rapprochements" not in changees and not rap["rapprochements_orphelins"]
       and abs(rap["total_charges"] - rap["total_rapproche"]) < 0.005,
       f"{rap['nb_rapprochements']} → {rap['nb_charges']} charges ; "
       f"{rap['total_charges']} € ↔ {rap['total_rapproche']} €")
    ok("L", "Mouvements bancaires inchangés", intacts(cut.BANQUE))
    ok("M", "Hostaway inchangé", intacts(cut.HOSTAWAY))
    ok("N", "Archive des réservations et réservations hors Hostaway inchangées",
       intacts(cut.ARCHIVES_RESERVATIONS + cut.RESERVATIONS))
    ok("O", "Référentiels inchangés", intacts(cut.REFERENTIELS))
    ok("P", "Fiche société inchangée", "parametres_societe_facturation" not in changees)
    ok("Q", "Paramètre V1 en place (facturation antérieure interdite)",
       v1.debut_depuis_connexion(conn) == p.debut)
    ok("R", "Comptabilité propriétaire antérieure : aucune donnée, tous runs confondus",
       not restants and not any(c["a_supprimer"] for c in plan_restant.comptes.values()
                                if c is not None), restants)
    v1_lignes = _compter(conn, "SELECT COUNT(*) FROM lot10_net_reglement r JOIN lot10_runs u "
                               "ON u.run_id = r.run_id AND u.actif = 1 WHERE substr(r.mois,1,7) >= ?",
                         (p.mois_v1,))
    v1_avant = p.infos.get("lignes_v1_actives", v1_lignes)
    ok("S", "Comptabilité propriétaire de la période V1 intacte", v1_lignes == v1_avant, v1_lignes)
    ok("PROTEGEES", "Toutes les tables protégées intactes (empreintes)", not changees, changees)
    fk = conn.execute("PRAGMA foreign_key_check").fetchall()
    ok("U", "PRAGMA foreign_key_check : 0 erreur", not fk, len(fk))
    integ = conn.execute("PRAGMA integrity_check").fetchone()[0]
    ok("T", "PRAGMA integrity_check : ok", integ == "ok", integ)
    return resultats


def executer(*, db_path=None, confirmer: bool = False, acteur: str = "",
             _apres_purge: Callable[[sqlite3.Connection], None] | None = None) -> dict[str, Any]:
    """Purge transactionnelle. Refuse sans confirmation ni acteur, sans cutover, ou en présence d'une
    anomalie. Tout ou rien : un invariant faux = ROLLBACK intégral."""
    if not confirmer or not str(acteur or "").strip():
        return {"ok": False, "code": E_CONFIRMATION,
                "message": "La finition du cutover exige une confirmation explicite et un acteur."}
    conn = cut._connexion(db_path, lecture_seule=False)
    try:
        conn.execute("BEGIN IMMEDIATE")
        p = _plan(conn)
        if not p.debut:
            conn.execute("ROLLBACK")
            return {"ok": False, "code": E_CUTOVER_ABSENT, "message": p.anomalies[0]}
        if p.anomalies:
            conn.execute("ROLLBACK")
            return {"ok": False, "code": E_ANOMALIES, "anomalies": p.anomalies, **_rapport(p)}
        p.infos["lignes_v1_actives"] = _compter(
            conn, "SELECT COUNT(*) FROM lot10_net_reglement r JOIN lot10_runs u ON u.run_id = "
                  "r.run_id AND u.actif = 1 WHERE substr(r.mois,1,7) >= ?", (p.mois_v1,))
        avant = _empreintes(conn)
        rap_avant = p.infos["rapprochements_charges"]
        rapport = _rapport(p)
        try:
            journal = _purger(conn, p)
            if _apres_purge is not None:
                _apres_purge(conn)
            verifications = _assertions(conn, p, avant, rap_avant)
        except Exception as exc:  # noqa: BLE001 — tout ou rien
            conn.execute("ROLLBACK")
            return {"ok": False, "code": E_VERIFICATION, "message": f"{type(exc).__name__}: {exc}",
                    **rapport}
        echecs = [v for v in verifications if not v["ok"]]
        if echecs:
            conn.execute("ROLLBACK")
            return {"ok": False, "code": E_VERIFICATION, "echecs": echecs,
                    "verifications": verifications, **rapport}
        conn.execute("COMMIT")
    finally:
        conn.close()
    return {"ok": True, "mode": "EXECUTION", "acteur": acteur, **rapport,
            "journal_suppressions": journal, "verifications": verifications}


# ── Vérification après coup ───────────────────────────────────────────────────────────────────────

def verifier(*, db_path=None) -> dict[str, Any]:
    """Après COMMIT (et après tout recalcul) : rien d'antérieur n'existe, rien ne peut renaître."""
    from app.services import creances_dettes_service as cd
    from app.services import factures_proprietaires_service as fpr
    from app.services import factures_service as fs
    from app.services import proprietaires_service as ps

    conn = cut._connexion(db_path, lecture_seule=True)
    try:
        debut = v1.debut_depuis_connexion(conn) or ""
        etat = etat_comptable(conn, debut) if debut else {}
        rap = _rapprochements_charges(conn)
        provenance = [dict(r) for r in conn.execute(
            "SELECT p.mois, p.classification, p.mode_traitement FROM lot10_run_mois_provenance p "
            "JOIN lot10_runs u ON u.run_id = p.run_id AND u.actif = 1 ORDER BY p.mois")] \
            if cut._existe(conn, "lot10_run_mois_provenance") else []
        integ = conn.execute("PRAGMA integrity_check").fetchone()[0]
        fk = len(conn.execute("PRAGMA foreign_key_check").fetchall())
        mois_actifs = sorted({r[0] for r in conn.execute(
            "SELECT DISTINCT r.mois FROM lot10_net_reglement r JOIN lot10_runs u "
            "ON u.run_id = r.run_id AND u.actif = 1")})
        un_proprietaire = conn.execute(
            "SELECT proprietaire_id FROM ref_proprietaires ORDER BY proprietaire_id LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    def refuse_facture(mois: str) -> bool:
        try:
            fpr.exiger_periode_v1(mois, db_path=db_path)
            return False
        except fpr.FactureProprietaireError:
            return True

    def refus_fournisseur(date_facture: str) -> bool:
        return any(e["code"] == fs.E_DATE_ANTERIEURE_V1 for e in fs.valider(
            {"fournisseur_id_opaque": "VERIF", "facture_ref": "VERIF-NON-ECRITE",
             "montant_ttc": 1, "date_facture": date_facture}, db_path))

    mois_ante = f"{int(debut[:4]) - (1 if debut[5:7] == '01' else 0)}-" \
                f"{(int(debut[5:7]) - 2) % 12 + 1:02d}" if debut else ""
    pid = un_proprietaire[0] if un_proprietaire else ""
    releve_ante = ps.load_releve(pid, mois_ante).get("status") if pid and mois_ante else ""
    dettes_ante = [d for d in cd.dettes(db_path=db_path)
                   if str(d.get("date_facture") or "")[:10] < debut]
    checks = {
        "cutover_applique": bool(debut),
        "comptabilite_anterieure_0": bool(etat) and not any(etat.values()),
        "creances_0": not cd.creances(db_path=db_path),
        "dettes_fournisseurs_anterieures_0": not dettes_ante,
        "charges_rapprochees": rap["nb_charges"],
        "rapprochements_banque_charges": rap["nb_rapprochements"],
        "rapprochements_montants_egaux": abs(rap["total_charges"] - rap["total_rapproche"]) < 0.005,
        "charges_non_rapprochees_0": not rap["charges_sans_rapprochement"],
        "lot10_mois_actifs": mois_actifs,
        "lot10_aucun_mois_anterieur": all(m >= debut[:7] for m in mois_actifs) if debut else False,
        "lot10_provenance_anterieure_exclue": all(
            r["mode_traitement"] == "EXCLU_PERIMETRE_ANTERIEUR_V1"
            for r in provenance if r["mois"] < debut[:7]) if debut else False,
        "releve_mois_anterieur": releve_ante,
        "releve_mois_anterieur_hors_v1": releve_ante == v1.ST_HORS_V1,
        "facture_proprietaire_mois_anterieur_refusee": refuse_facture(mois_ante) if mois_ante else False,
        "facture_proprietaire_v1_autorisee": not refuse_facture(debut[:7]) if debut else False,
        "facture_fournisseur_anterieure_refusee": refus_fournisseur(f"{mois_ante}-15")
        if mois_ante else False,
        "facture_fournisseur_v1_autorisee": not refus_fournisseur(f"{debut[:7]}-15") if debut else False,
        "integrity_ok": integ == "ok",
        "foreign_key_check_0": fk == 0,
    }
    checks["ok"] = all(v for k, v in checks.items()
                       if k not in ("charges_rapprochees", "rapprochements_banque_charges",
                                    "lot10_mois_actifs", "releve_mois_anterieur"))
    return {"checks": checks, "etat_comptable_anterieur": etat, "rapprochements_charges": rap,
            "provenance_lot10_active": provenance}
