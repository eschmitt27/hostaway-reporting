"""Lot12 — préfactures propriétaires, SQLite natif (migration 0047).

Port fidèle de `02_TRAVAIL/lot12_generer_factures.py` : lit `lot10_net_reglement`/
`lot10_commissions_a_controler`/`lot10_resultats` (Lot10, SQLite), `controles_lot11_dashboard_mois`
(Lot11, SQLite, migration 0046) et le référentiel (`ref_setup_repo`, SQLite) — écrit dans les tables
0047, jamais dans `MASTER_FACT_Proprietaires.xlsx`.

RÈGLE FONDAMENTALE INCHANGÉE : PRÉFACTURES UNIQUEMENT. `statut_generation` reste toujours
`PREFACTURE_CONTROLE` — ce module ne génère, ne modifie et ne lit AUCUNE facture propriétaire
ÉMISE. La facturation-propriétaire réelle reste `factures_service`/`comptabilite_ecritures_service`
via `ventes_lot12_adapter_service` (qui lit `lot10_net_reglement` directement, pas ces tables) :
DEUX CHEMINS SÉPARÉS, jamais fusionnés — ce module ne doit jamais importer un service d'écriture
comptable ou de facture (vérifié par `tests/test_lot12_pas_de_double_comptage.py`).
"""
from __future__ import annotations

import sys
import uuid
from datetime import date
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import ref_setup_repo

_TRAVAIL_DIR = str(cfg.PROJECT_ROOT / "02_TRAVAIL")
if _TRAVAIL_DIR not in sys.path:
    sys.path.insert(0, _TRAVAIL_DIR)

from lib_parc import A_CONTROLER, STATUT_PARC_INVALIDE, is_hors_parc_technique, is_statut_parc_a_controler  # noqa: E402

SENTINEL_GLOBAL = "GLOBAL_NON_AFFECTE"
STATUT_SUCCES = "SUCCES"
STATUT_ECHEC = "ECHEC"


def _n(v) -> float:
    try:
        return round(float(v), 2) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _txt(v) -> str:
    return "" if v is None else str(v)


def _build_facture_lignes(facture_id: str, rec: dict[str, Any], statut_facture: str
                          ) -> list[dict[str, Any]]:
    """Fonction pure — reprise telle quelle de `lot12_generer_factures.build_facture_lignes`."""
    exploitation = [
        ("TOTAL_PAYOUT", "Total payout", rec["total_payout"]),
        ("MENAGE_FACTURE", "Ménage facturé", rec["total_menage"]),
        ("COMMISSION_CONCIERGERIE", "Commission conciergerie", rec["total_commission"]),
    ]
    if rec["total_preparation_canape"] > 0:
        exploitation.append(("PREPARATION_CANAPE",
                             "Préparation du canapé payée par les voyageurs",
                             rec["total_preparation_canape"]))
    exploitation += [
        ("CHARGE_FIXE", "Charge fixe mensuelle", rec["charge_fixe"]),
        ("REVENU_NET_EXPLOITATION", "Revenu net d'exploitation propriétaire",
         rec["revenu_net_exploitation"]),
    ]
    reglement = [
        ("CHARGES_EXCEPT_REFAC", "Charges / achats exceptionnels refacturés",
         rec["charges_except_refac"]),
        ("MONTANT_DU", "Montant total dû à la conciergerie", rec["montant_du"]),
        ("ACOMPTE_AIRBNB", "Acompte reçu via Airbnb", rec["airbnb_impute"]),
        ("PAIEMENT_DEJA_RECU", "Autres paiements déjà reçus", 0.0),
        ("ACOMPTES_PROPRIETAIRES", "Acomptes propriétaires (réservations hors HA)",
         rec["acomptes"]),
        ("RESTE_A_PAYER", "Reste à payer à la conciergerie", rec["reste_a_payer"]),
    ]
    out, num = [], 0
    for t, lib, mt in exploitation:
        num += 1
        out.append({"facture_id": facture_id, "ligne_num": num, "type_ligne": t, "libelle": lib,
                    "montant": mt, "bloc": "EXPLOITATION", "commentaire": ""})
    for t, lib, mt in reglement:
        num += 1
        out.append({"facture_id": facture_id, "ligne_num": num, "type_ligne": t, "libelle": lib,
                    "montant": mt, "bloc": "REGLEMENT", "commentaire": ""})
    num += 1
    out.append({"facture_id": facture_id, "ligne_num": num, "type_ligne": "STATUT_REGLEMENT",
                "libelle": "Statut règlement", "montant": None, "bloc": "REGLEMENT",
                "commentaire": statut_facture})
    return out


def construire(*, db_path=None, run_id: str | None = None) -> dict[str, Any]:
    """Recalcule les préfactures Lot12 depuis les chaînes SQLite déjà migrées et écrit le résultat.

    Même discipline que `controles_lot11_service.construire()` : tout le calcul se fait en
    mémoire, la transaction n'est ouverte qu'une fois terminée sans exception.
    """
    rid = run_id or f"L12-{uuid.uuid4().hex[:12]}"
    today = date.today().isoformat()
    try:
        conn = get_db(db_path)
        try:
            lot10_run = conn.execute(
                "SELECT run_id FROM lot10_runs WHERE actif = 1").fetchone()
            lot10_run = lot10_run[0] if lot10_run else None
            df_reg, df_ac, rslt_rows = [], [], []
            if lot10_run:
                df_reg = [dict(r) for r in conn.execute(
                    "SELECT * FROM lot10_net_reglement WHERE run_id = ?", (lot10_run,))]
                df_ac = [dict(r) for r in conn.execute(
                    "SELECT * FROM lot10_commissions_a_controler WHERE run_id = ?", (lot10_run,))]
                rslt_rows = [dict(r) for r in conn.execute(
                    "SELECT vision, resultat FROM lot10_resultats WHERE run_id = ?", (lot10_run,))]
            # Dashboard Lot11 : instantané unique (DELETE+INSERT à chaque construction, comme
            # `flux_unifies`/`controles_lot11_constats`) — toute la table EST le dernier run.
            df_dash = [dict(r) for r in conn.execute(
                "SELECT * FROM controles_lot11_dashboard_mois")]
        finally:
            conn.close()

        hc_global = 0.0
        for r in rslt_rows:
            if r.get("vision") == "HORS_COMPTA":
                hc_global += _n(r.get("resultat"))
        donnees_partielles = (round(hc_global, 2) == 0.0)

        dash_idx = {_txt(r["mois"]): {
            "bloquants": int(_n(r.get("nb_bloquants_ouverts"))),
            "a_controler": int(_n(r.get("nb_a_controler_ouverts"))),
            "ok": _txt(r.get("facturation_lot12_ok")),
            "statut_banque": _txt(r.get("statut_mois_banque")),
        } for r in df_dash}
        transverse = dash_idx.get("TRANSVERSE",
                                  {"bloquants": 0, "a_controler": 0, "ok": "NON",
                                   "statut_banque": "OUVERT"})

        df_prop = ref_setup_repo.lire_onglet("REF_Proprietaires", db_path=db_path)
        df_log = ref_setup_repo.lire_onglet("REF_Logements", db_path=db_path)
        prop_idx = {r.get("proprietaire_id"): r for r in df_prop}
        log_idx = {r.get("logement_id"): r for r in df_log}

        entetes, lignes, controle = [], [], []
        compteur: dict[str, int] = {}

        for r in df_reg:
            mois = r.get("mois")
            log_id = r.get("logement_id")
            prop_id = r.get("proprietaire_id")
            rec = {
                "mois": mois, "proprietaire_id": prop_id, "logement_id": log_id,
                "total_payout": _n(r.get("total_payout_mois")),
                "total_menage": _n(r.get("total_menage_mois")),
                "total_commission": _n(r.get("total_commission_mois")),
                "total_preparation_canape": _n(r.get("total_preparation_canape_mois")),
                "charge_fixe": _n(r.get("charge_fixe_mensuelle")),
                "charges_except_refac": _n(r.get("charges_exceptionnelles_refacturees")),
                "revenu_net_exploitation": _n(r.get("net_proprietaire_apres_charge_mois")),
                "montant_du": _n(r.get("montant_du_conciergerie")),
                "acomptes": _n(r.get("autres_acomptes_recus")),
                "airbnb_impute": _n(r.get("acompte_conciergerie_recu_via_airbnb")),
                "reste_a_payer": _n(r.get("reste_a_payer_conciergerie")),
                "credit_a_traiter": _n(r.get("credit_a_traiter")),
                "nb_reservations": int(_n(r.get("nb_reservations"))),
                "statut": ("CONTROLE_GLOBAL_NON_AFFECTE" if log_id == SENTINEL_GLOBAL
                          else "RATTACHE_PROPRIETAIRE"),
            }
            controle.append(rec)

            if log_id == SENTINEL_GLOBAL or not prop_id or prop_id == SENTINEL_GLOBAL:
                continue
            log_row = log_idx.get(log_id)
            if is_hors_parc_technique(log_row):
                rec["statut"] = "EXCLU_HORS_PARC_TECHNIQUE"
                continue
            if is_statut_parc_a_controler(log_row):
                rec["statut"] = A_CONTROLER
                rec["code_anomalie"] = STATUT_PARC_INVALIDE
                continue

            compteur[mois] = compteur.get(mois, 0) + 1
            facture_id = f"PREF-{mois}-{prop_id}-{log_id}-{compteur[mois]:03d}"

            pr, lg = prop_idx.get(prop_id, {}), log_idx.get(log_id, {})
            nom_prop = f"{pr.get('prenom_proprietaire') or ''} {pr.get('nom_proprietaire') or ''}".strip()
            adresse_prop = pr.get("adresse_facturation")
            mode_fact = _txt(pr.get("mode_facturation")) or "A_DEFINIR"

            g = dash_idx.get(_txt(mois))
            bloquants = (g["bloquants"] if g else 0) + transverse["bloquants"]
            a_controler = (g["a_controler"] if g else 0) + transverse["a_controler"]
            statut_banque = g["statut_banque"] if g else "OUVERT"
            facturable_flag = g["ok"] if g else "NON"

            if bloquants > 0:
                statut_facture = "NON_FACTURABLE_BLOQUANT"
            elif a_controler > 0 or facturable_flag != "OUI" or mode_fact == "A_DEFINIR":
                statut_facture = "NON_FACTURABLE_A_CONTROLER"
            else:
                statut_facture = "FACTURABLE"
            statut_generation = "PREFACTURE_CONTROLE"

            balises = ["{{LOGO_A_INSERER}}", "{{SIRET_A_COMPLETER}}",
                      "{{ADRESSE_SOCIETE_A_COMPLETER}}", "{{FACTURE_NON_FINALE}}"]
            if not adresse_prop or _txt(adresse_prop).strip() == "":
                balises.append("{{ADRESSE_PROPRIETAIRE_A_COMPLETER}}")
            if mode_fact == "A_DEFINIR":
                balises.append("{{MODE_FACTURATION_A_DEFINIR}}")
            if statut_banque != "CLOTURE":
                balises.append("{{BANQUE_NON_CLOTUREE}}")
            if a_controler > 0:
                balises.append("{{RESERVATIONS_A_CONTROLER}}")
            if rec["credit_a_traiter"] > 0:
                balises.append("{{TROP_PERCU_CREDIT_A_TRAITER}}")
            if rec["acomptes"] == 0.0:
                balises.append("{{ACOMPTES_NON_ALIMENTES}}")
            if donnees_partielles:
                balises.append("{{DONNEES_PARTIELLES}}")
            balises_str = " ".join(balises)

            periode_debut = f"{mois}-01"
            periode_fin = _fin_de_mois(mois)

            entetes.append({
                "facture_id": facture_id, "mois": mois, "proprietaire_id": prop_id,
                "nom_proprietaire": nom_prop or "{{NOM_PROPRIETAIRE}}",
                "adresse_proprietaire": adresse_prop or "{{ADRESSE_PROPRIETAIRE_A_COMPLETER}}",
                "logement_id": log_id,
                "nom_logement": lg.get("nom_logement_officiel") or lg.get("nom_court") or log_id,
                "periode_debut": periode_debut, "periode_fin": periode_fin,
                "nb_reservations": rec["nb_reservations"],
                "total_exploitation_net": rec["revenu_net_exploitation"],
                "total_reglement_du": rec["montant_du"], "reste_a_payer": rec["reste_a_payer"],
                "credit_a_traiter": rec["credit_a_traiter"], "mode_facturation": mode_fact,
                "statut_facture": statut_facture, "statut_generation": statut_generation,
                "balises": balises_str, "date_generation": today,
            })
            lignes.extend(_build_facture_lignes(facture_id, rec, statut_facture))

        dashboard_rows = _dashboard_facturation(entetes, dash_idx, transverse)
        ac_rows = _a_controler(df_ac, df_prop, entetes)

        conn = get_db(db_path)
        try:
            conn.execute("UPDATE lot12_runs SET actif = 0 WHERE actif = 1")
            _remplacer(conn, "lot12_prefactures_entete", rid, entetes,
                      ("facture_id", "mois", "proprietaire_id", "nom_proprietaire",
                       "adresse_proprietaire", "logement_id", "nom_logement", "periode_debut",
                       "periode_fin", "nb_reservations", "total_exploitation_net",
                       "total_reglement_du", "reste_a_payer", "credit_a_traiter",
                       "mode_facturation", "statut_facture", "statut_generation", "balises",
                       "date_generation"), run_scope=True)
            _remplacer(conn, "lot12_prefactures_lignes", rid, lignes,
                      ("facture_id", "ligne_num", "type_ligne", "libelle", "montant", "bloc",
                       "commentaire"), run_scope=True)
            _remplacer(conn, "lot12_controle_mensuel", rid, controle,
                      ("mois", "proprietaire_id", "logement_id", "total_payout", "total_menage",
                       "total_commission", "total_preparation_canape", "charge_fixe",
                       "charges_except_refac", "revenu_net_exploitation", "montant_du",
                       "acomptes", "airbnb_impute", "reste_a_payer", "credit_a_traiter",
                       "nb_reservations", "statut", "code_anomalie"), run_scope=True)
            _remplacer(conn, "lot12_dashboard_facturation", rid, dashboard_rows,
                      ("mois", "proprietaire_id", "nb_logements", "nb_bloquants_mois",
                       "nb_a_controler_mois", "facturation_lot12_ok", "mode_facturation",
                       "statut_facture", "balises_non_resolues"), run_scope=True)
            _remplacer(conn, "lot12_a_controler", rid, ac_rows,
                      ("mois", "proprietaire_id", "logement_id", "reservation", "code_anomalie",
                       "severite", "impact_facturation", "message"), run_scope=True)

            conn.execute(
                "INSERT INTO lot12_runs (run_id, statut, actif, nb_entetes, nb_lignes, "
                "nb_controle, nb_dashboard, nb_a_controler) VALUES (?,?,1,?,?,?,?,?)",
                (rid, STATUT_SUCCES, len(entetes), len(lignes), len(controle),
                 len(dashboard_rows), len(ac_rows)))
            conn.commit()
        finally:
            conn.close()

        return {"ok": True, "run_id": rid, "nb_entetes": len(entetes), "nb_lignes": len(lignes),
                "nb_controle": len(controle), "nb_dashboard": len(dashboard_rows),
                "nb_a_controler": len(ac_rows)}
    except Exception as exc:
        conn = get_db(db_path)
        try:
            conn.execute(
                "INSERT INTO lot12_runs (run_id, statut, actif, erreur_code, erreur_message) "
                "VALUES (?,?,0,?,?)",
                (rid, STATUT_ECHEC, type(exc).__name__, str(exc)[:500]))
            conn.commit()
        finally:
            conn.close()
        return {"ok": False, "run_id": rid, "code": "ERREUR_CALCUL", "message": str(exc)}


def _remplacer(conn, table: str, run_id: str, rows: list[dict], cols: tuple[str, ...],
              run_scope: bool) -> None:
    all_cols = ("run_id",) + cols
    conn.executemany(
        f"INSERT INTO {table} ({', '.join(all_cols)}) VALUES ({', '.join(['?'] * len(all_cols))})",
        [(run_id,) + tuple(r.get(c) for c in cols) for r in rows])


def _fin_de_mois(mois: str) -> str:
    import calendar
    annee, m = int(mois[:4]), int(mois[5:7])
    dernier_jour = calendar.monthrange(annee, m)[1]
    return f"{annee:04d}-{m:02d}-{dernier_jour:02d}"


def _dashboard_facturation(entetes: list[dict], dash_idx: dict, transverse: dict) -> list[dict]:
    groupes: dict[tuple, list[dict]] = {}
    for e in entetes:
        groupes.setdefault((e["mois"], e["proprietaire_id"]), []).append(e)

    rows = []
    for (mois, prop), grp in groupes.items():
        g = dash_idx.get(_txt(mois))
        bloquants = (g["bloquants"] if g else 0) + transverse["bloquants"]
        a_controler = (g["a_controler"] if g else 0) + transverse["a_controler"]
        balises_all: set[str] = set()
        for e in grp:
            balises_all.update(e["balises"].split())
        crit = [b for b in balises_all if b in (
            "{{MODE_FACTURATION_A_DEFINIR}}", "{{BANQUE_NON_CLOTUREE}}",
            "{{ADRESSE_PROPRIETAIRE_A_COMPLETER}}", "{{RESERVATIONS_A_CONTROLER}}")]
        rows.append({
            "mois": mois, "proprietaire_id": prop,
            "nb_logements": len({e["logement_id"] for e in grp}),
            "nb_bloquants_mois": bloquants, "nb_a_controler_mois": a_controler,
            "facturation_lot12_ok": (g["ok"] if g else "NON"),
            "mode_facturation": grp[0]["mode_facturation"], "statut_facture": grp[0]["statut_facture"],
            "balises_non_resolues": " ".join(sorted(crit)),
        })
    return rows


def _a_controler(df_ac: list[dict], df_prop: list[dict], entetes: list[dict]) -> list[dict]:
    rows = []
    for r in df_ac:
        rows.append({
            "mois": None, "proprietaire_id": None, "logement_id": r.get("logement_id_snapshot"),
            "reservation": r.get("reservation_id"),
            "code_anomalie": r.get("code_anomalie_lot10") or "RESERVATION_A_CONTROLER",
            "severite": "A_CONTROLER", "impact_facturation": "EXCLUE_DE_FACTURE",
            "message": f"Reservation {r.get('source')} exclue - saisie/controle requis",
        })
    n_adef = sum(1 for p in df_prop if _txt(p.get("mode_facturation")) == "A_DEFINIR")
    if n_adef > 0:
        rows.append({
            "mois": None, "proprietaire_id": None, "logement_id": None, "reservation": None,
            "code_anomalie": "MODE_FACTURATION_A_DEFINIR", "severite": "A_CONTROLER",
            "impact_facturation": "BLOQUE_FACTURE_FINALE",
            "message": f"{n_adef} proprietaires sans mode_facturation",
        })
    for e in entetes:
        if _n(e.get("credit_a_traiter")) > 0:
            rows.append({
                "mois": e.get("mois"), "proprietaire_id": e.get("proprietaire_id"),
                "logement_id": e.get("logement_id"), "reservation": None,
                "code_anomalie": "TROP_PERCU_CREDIT_A_TRAITER", "severite": "A_CONTROLER",
                "impact_facturation": "BLOQUE_FACTURE_FINALE",
                "message": f"Credit a traiter {e.get('credit_a_traiter')} EUR sur "
                          f"{e.get('facture_id')}.",
            })
    return rows
