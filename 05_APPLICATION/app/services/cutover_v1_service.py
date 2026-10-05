"""Cutover V1 — remise à zéro CONTRÔLÉE de la comptabilité applicative au 1er septembre 2026.

DÉCISION (utilisateur, 2026-10-02) : « La comptabilité applicative V1 de Chouette Patrimoine démarre
au 1er septembre 2026. Les factures propriétaires antérieures ne sont pas reprises et aucune facture
ne peut être créée ou éditée pour une période antérieure à septembre 2026. Les créances antérieures
sont purgées. Les charges héritées ne sont conservées que lorsqu'elles disposent déjà d'un
rapprochement bancaire validé. Les mouvements bancaires, les rapprochements banque ↔ charges
correspondants, les données Hostaway, les réservations, les référentiels et les données société sont
conservés. » Documentation : `00_CADRAGE/CUTOVER_V1_2026-09.md`.

CE N'EST PAS une recréation de base, ni un `DELETE … WHERE date < 2026-09-01` : chaque table est
CLASSÉE (KEEP, PURGE, KEEP_BY_DEPENDENCY, REBUILD) et chaque ligne purgée l'est pour une raison
prouvée par ses liens, jamais par sa seule date.

TROIS TEMPS
  1. `simuler()`  — lecture SEULE (connexion `mode=ro`) : matrice complète, identifiants touchés,
                    anomalies. Deux simulations successives rendent le même résultat.
  2. `executer()` — UNE transaction `BEGIN IMMEDIATE` : suppressions enfants → parents, pose du
                    paramètre `V1_ACCOUNTING_START_DATE`, puis vérifications DANS la transaction
                    (clés étrangères, intégrité, invariants métier A→Z, empreintes des données
                    conservées) ; COMMIT seulement si tout est conforme, sinon ROLLBACK intégral.
  3. `verifier()` — invariants relus sur la base engagée (après redémarrage, après actualisation).

LES DONNÉES DÉRIVÉES (flux unifié, Lot10, Lot11, Lot12, ménages calculés, réservations calculées)
ne sont pas supprimées ici : elles sont RECONSTRUITES ensuite par l'orchestrateur, depuis les sources
conservées (classe REBUILD).

AUCUNE RÈGLE N'EST INVENTÉE. Un cas que la décision ne tranche pas (argent réellement encaissé sur une
ancienne facture, justificatif rattaché à une charge à purger…) est une ANOMALIE : l'exécution est
alors refusée, rien n'est supprimé.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import app.config as cfg
from app.services import perimetre_v1_service as v1

KEEP = "KEEP"
PURGE = "PURGE"
KEEP_BY_DEPENDENCY = "KEEP_BY_DEPENDENCY"
REBUILD = "REBUILD"
ETATS = (KEEP, PURGE, KEEP_BY_DEPENDENCY, REBUILD)
NON_CLASSEE = "NON_CLASSEE"

E_CONFIRMATION = "CUTOVER_CONFIRMATION_REQUISE"
E_DEJA_APPLIQUE = "CUTOVER_DEJA_APPLIQUE"
E_ANOMALIES = "CUTOVER_ANOMALIES"
E_VERIFICATION = "CUTOVER_VERIFICATION_ECHOUEE"

TYPES_RAPPROCHEMENT_CHARGE = ("CHARGE_FOURNISSEUR", "CHARGE")

# ── Classification des tables ───────────────────────────────────────────────────────────────────
# MIXTE : la table porte plusieurs catégories, calculées ligne à ligne depuis le plan.

MIXTE = "MIXTE"


@dataclass(frozen=True)
class Table:
    etat: str
    role: str
    justification: str


def _groupe(tables: tuple[str, ...], etat: str, role: str, justification: str) -> dict:
    return {t: Table(etat, role, justification) for t in tables}


REFERENTIELS = (
    "ref_abonnements_logiciels", "ref_admin_evenements", "ref_assoc_mode", "ref_associes",
    "ref_banque_regles", "ref_bareme_ik", "ref_bareme_ik_annees", "ref_canape_parametres",
    "ref_canaux_reservation", "ref_cartes_paiement", "ref_categories_charges",
    "ref_charges_recurrentes", "ref_cloture_mensuelle", "ref_codes_impact",
    "ref_couts_menage_interne", "ref_couts_standards_menage", "ref_gestion_logements_hist",
    "ref_intervenants", "ref_logements", "ref_mapping_logements", "ref_modes_paiement",
    "ref_parametres_generaux", "ref_proprietaires", "ref_regles_versions",
    "ref_setup_import_feuilles", "ref_setup_imports", "ref_sources_systeme", "ref_statuts",
    "ref_statuts_payout", "ref_taux_commission", "ref_taux_heures_menage",
    "ref_types_affectation", "ref_types_flux", "ref_types_lignes_menage", "ref_types_logements",
    "plan_comptable", "plan_comptable_evenements", "mapping_categorie_compte",
    "mapping_comptable_regles", "mapping_produits_facture", "mapping_regle_evenements",
    "parametres_societe_facturation_historique", "ik_baremes", "ik_bareme_tranches",
    "ik_vehicules", "proprietaires_facturation", "proprietaires_facturation_evenements",
    "fournisseurs", "fournisseur_details", "fournisseur_evenements",
    "fournisseur_menage_qualification", "fournisseur_rattachements",
    "fournisseur_rattachement_evenements", "charges_justificatif_sequence",
    "justificatifs_sequence",
)
HOSTAWAY = (
    "hostaway_anomalies", "hostaway_cleaning_tasks", "hostaway_cleaning_tasks_extractions",
    "hostaway_extractions", "hostaway_listings", "hostaway_payouts", "hostaway_reservation_fees",
    "hostaway_reservation_finance_fields", "hostaway_reservations",
)
RESERVATIONS = (
    "reservations_hors_hostaway", "reservation_hh_overrides", "reservation_hh_evenements",
    "saisie_hh_writes",
)
ARCHIVES_RESERVATIONS = (
    "reservations_archives", "reservations_historique_cloture",
    "reservations_historique_corrections", "mois_classification_legacy", "mois_archive_reglement",
    "ajustements_post_cloture", "assiette_corrections_manuelles", "aircover",
)
BANQUE = (
    "qonto_accounts", "qonto_sync_runs", "qonto_transactions_raw",
    "qonto_transactions_statut_local", "banque_mouvements", "banque_classifications",
    "banque_classification_signaux", "banque_classement_decisions", "banque_controles",
    "banque_controle_runs", "banque_overrides", "banque_import_source", "caisse_transferts_banque",
)
FACTURES_FOURNISSEURS = (
    "factures", "facture_lignes", "facture_lignes_menage", "facture_lignes_menage_detail",
    "facture_lignes_menage_pdf", "facture_classification", "facture_evenements",
    "facture_interpretations", "facture_pdf_diagnostics", "facture_ventilations",
    "facture_ventilation_parts", "menages_pdf_fichiers_hash", "reglements_fournisseurs",
)
MENAGES_SOURCES = (
    "menages", "menage_overrides", "menage_evenements", "menages_declarations_internes",
    "menages_declarations_extra", "menages_declarations_historique",
    "menages_declarations_conflits", "menages_externes_historique",
    "intervenant_menage_allocations", "intervenant_menage_dettes",
    "intervenant_menage_paiements", "intervenant_menage_recalculs",
)
OBSERVABILITE = (
    "audit_events", "run_history", "moteur_runs", "moteur_run_etapes", "orchestrateur_datasets",
    "orchestrateur_dataset_evenements", "orchestrateur_verrous", "pipeline_runs",
    "calculs_indicateurs", "calculs_run_lots", "calculs_runs", "calculs_sauvegardes",
    "sauvegardes_base", "sauvegardes_base_tracabilite", "snapshots", "schema_migrations",
    "screen_states", "drafts", "controles_runs", "controles_suivi", "controles_suivi_historique",
    "menages_actualisations_hostaway", "menages_changements_mois_clotures",
    "menages_recalcul_runs", "menages_runs_cibles", "saisie_charges_writes",
    "cloture_statuts", "cloture_statut_evenements", "mois_reouvertures", "periodes_comptables",
    "periode_evenements", "periods",
    # Clôture par modules (0126) : journal en ajout seul, impossible avant la V1 (déclencheur) donc
    # jamais concerné par une purge de l'ancien modèle.
    "cloture_modules", "cloture_modules_evenements",
)
DERIVES = (
    "flux_unifies", "flux_unifies_runs", "lot10_commissions", "lot10_commissions_a_controler",
    "lot10_net_exploitation", "lot10_net_reglement", "lot10_net_vue_mois", "lot10_resultats",
    "lot10_run_mois_provenance", "lot10_runs", "lot12_a_controler", "lot12_controle_mensuel",
    "lot12_dashboard_facturation", "lot12_prefactures_entete", "lot12_prefactures_id_legacy",
    "lot12_prefactures_lignes", "lot12_runs", "controles_lot11_constats",
    "controles_lot11_constats_champs", "controles_lot11_dashboard_mois", "controles_lot11_runs",
    "reservations_calculees", "reservations_resolues", "reservations_datasets",
    "menages_cout_complet", "menages_cout_complet_provenance", "menages_gainperte",
    "menages_rapprochement", "menages_taches_enrichies",
)
FACTURATION_PROPRIETAIRE = (
    "factures_proprietaires", "factures_proprietaires_conformite",
    "factures_proprietaires_evenements", "factures_proprietaires_lignes",
    "factures_proprietaires_lignes_charge", "factures_proprietaires_lignes_detail",
    "factures_proprietaires_lignes_provenance", "factures_proprietaires_meta",
    "factures_proprietaires_reservations", "factures_proprietaires_sequence",
    "imputations_airbnb", "credits_clients", "credit_client_evenements",
    "proprietaire_allocations", "proprietaire_recalculs", "reglement_repartitions",
    "rapprochements_reglements", "rapprochement_evenements",
)
MIXTES = (
    "charges", "charge_evenements", "charges_perimetre_analytique", "charges_perimetre_menage",
    "charges_refacturation_positions", "charges_refacturation_evenements",
    "charges_affectations", "charges_affectation_evenements", "justificatifs",
    "justificatif_evenements", "ik", "ik_trajets", "ik_depenses_activite", "ik_evenements",
    "flux_propositions_refusees", "flux_lettrages", "flux_lettrage_lignes",
    "flux_lettrage_evenements", "banque_rapprochements", "banque_rapprochement_evenements",
    "banque_suggestion_decisions", "banque_imports", "ecritures", "ecriture_lignes",
    "ecriture_evenements", "ecriture_ligne_ventilation", "operations_diverses", "od_lignes",
    "operations_caisse", "mouvements_tresorerie_proprietaires",
    "mouvements_tresorerie_proprietaires_evenements", "proprietaires_releves",
    "proprietaires_paiement", "proprietaires_releve_cycle", "proprietaires_releve_evenements",
    "clotures_mensuelles", "cloture_evenements", "cloture_elements", "cloture_documents",
    "parametres_societe_facturation",
)

CLASSIFICATION: dict[str, Table] = {
    **_groupe(REFERENTIELS, KEEP, "Référentiel",
              "Référentiel nécessaire au fonctionnement futur et à l'interprétation de l'historique "
              "(périodes de validité comprises) : jamais remis à zéro."),
    **_groupe(HOSTAWAY, KEEP, "Source Hostaway",
              "Donnée source réelle (réservations, finances, payouts, listings, tâches) : "
              "historique métier conservé intégralement, toutes périodes."),
    **_groupe(RESERVATIONS, KEEP, "Réservations hors Hostaway",
              "Source métier indépendante saisie dans l'application : conservée."),
    **_groupe(ARCHIVES_RESERVATIONS, KEEP, "Archive des réservations",
              "Archive et classification historiques des réservations : conservées intactes."),
    **_groupe(BANQUE, KEEP, "Banque",
              "Mouvements bancaires et leur journal : jamais supprimés, quelle que soit la date."),
    **_groupe(FACTURES_FOURNISSEURS, KEEP, "Factures fournisseurs reçues",
              "Factures des prestataires (PDF importés) : source des coûts ménages externes, donc "
              "historique métier ; aucune écriture comptable n'en est issue. Les purger serait "
              "défait par le prochain import PDF, qui les recréerait depuis les documents sources."),
    **_groupe(MENAGES_SOURCES, KEEP, "Ménages (sources et décisions)",
              "Déclarations, historiques et arbitrages humains des ménages : historique métier."),
    **_groupe(OBSERVABILITE, KEEP, "Observabilité / technique",
              "Journal des runs, sauvegardes, migrations, verrous, suivi des contrôles : "
              "conservés pour le diagnostic."),
    **_groupe(DERIVES, REBUILD, "Données dérivées",
              "Reconstruites par l'orchestrateur depuis les sources conservées, après le cutover "
              "(les préfactures ne portent plus que sur la période V1)."),
    **_groupe(FACTURATION_PROPRIETAIRE, PURGE, "Facturation propriétaire et créances",
              "Ancienne facturation propriétaire et tout ce qui n'existe que pour elle : la V1 "
              "repart avec 0 facture et 0 créance."),
    **_groupe(MIXTES, MIXTE, "Mixte", "Catégories calculées ligne à ligne."),
}


def classer(table: str) -> Table | None:
    return CLASSIFICATION.get(table)


# ── Connexion et utilitaires ────────────────────────────────────────────────────────────────────

def _connexion(db_path, *, lecture_seule: bool) -> sqlite3.Connection:
    chemin = Path(db_path or cfg.DB_PATH)
    if lecture_seule:
        conn = sqlite3.connect(f"file:{chemin.as_posix()}?mode=ro", uri=True)
    else:
        conn = sqlite3.connect(str(chemin), isolation_level=None, timeout=30)
        conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def _tables(conn) -> list[str]:
    return [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name")]


def _existe(conn, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                        (table,)).fetchone() is not None


def _colonnes(conn, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')]


def _lignes(conn, table: str) -> list[dict[str, Any]]:
    if not _existe(conn, table):
        return []
    return [dict(r) for r in conn.execute(f'SELECT * FROM "{table}"')]


def _compte(conn, table: str) -> int:
    return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] if _existe(conn, table) \
        else 0


def empreinte(conn, table: str, *, exclure: tuple[str, set] | None = None) -> str:
    """Empreinte SHA-256 du contenu d'une table (ordre stable), éventuellement hors certaines
    lignes (`exclure` = (colonne, valeurs)) — sert à prouver que ce qui est conservé est intact."""
    if not _existe(conn, table):
        return "ABSENTE"
    cols = _colonnes(conn, table)
    ordre = ", ".join(f'"{c}"' for c in cols)
    h = hashlib.sha256()
    for r in conn.execute(f'SELECT * FROM "{table}" ORDER BY {ordre}'):
        d = dict(r)
        if exclure and d.get(exclure[0]) in exclure[1]:
            continue
        h.update(repr(tuple(d[c] for c in cols)).encode("utf-8"))
    return h.hexdigest()[:24]


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Le plan : qui est purgé, qui est conservé, et pourquoi ──────────────────────────────────────

@dataclass
class Plan:
    debut: str
    mouvements: set[str] = field(default_factory=set)
    charges_conservees: dict[str, list[str]] = field(default_factory=dict)
    charges_purgees: dict[str, dict[str, Any]] = field(default_factory=dict)
    rap_dependance: set[str] = field(default_factory=set)   # banque ↔ charge validés
    rap_conserves: set[str] = field(default_factory=set)    # autres rapprochements réels
    rap_purges: dict[str, str] = field(default_factory=dict)  # id → motif
    lettrages_dependance: set[str] = field(default_factory=set)
    lettrages_purges: set[str] = field(default_factory=set)
    ecritures_dependance: set[str] = field(default_factory=set)
    ecritures_conservees: set[str] = field(default_factory=set)
    ecritures_purgees: dict[str, str] = field(default_factory=dict)
    factures: dict[str, dict[str, Any]] = field(default_factory=dict)
    lignes_facture: set[str] = field(default_factory=set)
    mtp_purges: set[str] = field(default_factory=set)
    imputations_purgees: set[str] = field(default_factory=set)
    credits_purges: set[str] = field(default_factory=set)
    releves_purges: set[str] = field(default_factory=set)
    rapprochements_reglements_purges: set[str] = field(default_factory=set)
    clotures_purgees: set[str] = field(default_factory=set)
    imports_test: set[str] = field(default_factory=set)
    suggestions_test: set[int] = field(default_factory=set)
    positions_purgees: set[str] = field(default_factory=set)
    affectations_purgees: set[str] = field(default_factory=set)
    ik_purges: set[str] = field(default_factory=set)
    propositions_refusees_purgees: set[int] = field(default_factory=set)
    od_purgees: set[str] = field(default_factory=set)
    caisse_purgees: set[str] = field(default_factory=set)
    justificatifs_conserves: set[str] = field(default_factory=set)
    anomalies: list[str] = field(default_factory=list)

    @property
    def mois(self) -> str:
        return self.debut[:7]


def _mouvements_bancaires(conn) -> set[str]:
    """Identifiants des mouvements bancaires EXISTANTS : Qonto (statut local) et ancien import
    Crédit Mutuel. Un rapprochement vers un mouvement absent n'est pas un lien bancaire réel."""
    ids: set[str] = set()
    if _existe(conn, "qonto_transactions_statut_local"):
        ids |= {r[0] for r in conn.execute(
            "SELECT mouvement_id_opaque FROM qonto_transactions_statut_local "
            "WHERE mouvement_id_opaque IS NOT NULL")}
    if _existe(conn, "banque_mouvements"):
        ids |= {r[0] for r in conn.execute("SELECT mouvement_id_opaque FROM banque_mouvements")}
    return ids


def _liens_charges(conn, mouvements: set[str]) -> tuple[dict[str, list[str]], set[str], set[str]]:
    """Charges disposant d'un rapprochement BANCAIRE VALIDÉ, avec leurs rapprochements et
    lettrages. Validé = rapprochement CONFIRME vers un mouvement bancaire existant, dont le
    lettrage éventuel est VALIDE ; ou lettrage VALIDE réunissant un mouvement BANQUE existant et
    la charge. La catégorisation d'un mouvement n'est PAS un rapprochement."""
    lettrages_valides = {r[0] for r in conn.execute(
        "SELECT lettrage_id_opaque FROM flux_lettrages WHERE statut='VALIDE'")} \
        if _existe(conn, "flux_lettrages") else set()
    liens: dict[str, list[str]] = {}
    lettrages_charges: set[str] = set()
    raps: set[str] = set()
    for r in _lignes(conn, "banque_rapprochements"):
        if (r["type_objet"] in TYPES_RAPPROCHEMENT_CHARGE and r["statut"] == "CONFIRME"
                and r["mouvement_id_opaque"] in mouvements
                and (not r.get("lettrage_id_opaque") or r["lettrage_id_opaque"] in lettrages_valides)):
            liens.setdefault(r["objet_id"], []).append(r["rapprochement_id_opaque"])
            raps.add(r["rapprochement_id_opaque"])
            if r.get("lettrage_id_opaque"):
                lettrages_charges.add(r["lettrage_id_opaque"])
    if _existe(conn, "flux_lettrage_lignes"):
        par_lettrage: dict[str, list[dict]] = {}
        for l in _lignes(conn, "flux_lettrage_lignes"):
            par_lettrage.setdefault(l["lettrage_id_opaque"], []).append(l)
        for lid, lignes in par_lettrage.items():
            if lid not in lettrages_valides:
                continue
            banque = any(l["cote"] == "MOUVEMENT" and l["type_element"] == "BANQUE"
                         and l["element_id"] in mouvements for l in lignes)
            if not banque:
                continue
            for l in lignes:
                if l["cote"] == "OBJET" and l["type_element"] == "CHARGE":
                    liens.setdefault(l["element_id"], [])
                    lettrages_charges.add(lid)
    return liens, raps, lettrages_charges


def _plan(conn, debut: str) -> Plan:
    p = Plan(debut=debut)
    mois_v1 = p.mois
    p.mouvements = _mouvements_bancaires(conn)

    # ── Charges ────────────────────────────────────────────────────────────────────────────────
    liens, raps_charges, lettrages_charges = _liens_charges(conn, p.mouvements)
    for c in _lignes(conn, "charges"):
        cid = c["charge_id"]
        if cid in liens:
            p.charges_conservees[cid] = liens[cid]
        else:
            p.charges_purgees[cid] = {"date": c.get("date_charge"), "montant": c.get("montant"),
                                      "statut": c.get("statut"),
                                      "categorie": c.get("categorie_charge_id")}
    purge_charges = set(p.charges_purgees)
    p.rap_dependance = {r for r in raps_charges}
    p.lettrages_dependance = set(lettrages_charges)

    p.positions_purgees = {r["position_id"] for r in _lignes(conn, "charges_refacturation_positions")
                           if r["charge_id"] in purge_charges}
    p.affectations_purgees = {r["affectation_id_opaque"] for r in _lignes(conn, "charges_affectations")
                              if r["charge_id"] in purge_charges}
    p.ik_purges = {r["ik_id_opaque"] for r in _lignes(conn, "ik") if r["charge_id"] in purge_charges}
    if p.ik_purges:
        p.anomalies.append(f"IK rattachée(s) à une charge à purger : {sorted(p.ik_purges)} — "
                           "l'IK porte un relevé de trajets ; décision humaine requise.")
    for j in _lignes(conn, "justificatifs"):
        if j.get("objet_type") == "CHARGE" and j.get("objet_id") in purge_charges:
            p.anomalies.append(f"Justificatif {j['reference']} rattaché à la charge à purger "
                               f"{j['objet_id']} (la base refuse sa suppression).")
        else:
            p.justificatifs_conserves.add(j["reference"])
    for f in _lignes(conn, "factures"):
        if f.get("charge_id") in purge_charges:
            p.anomalies.append(f"Facture fournisseur {f['facture_id_opaque']} liée à la charge à "
                               f"purger {f['charge_id']}.")

    # ── Facturation propriétaire ───────────────────────────────────────────────────────────────
    for f in _lignes(conn, "factures_proprietaires"):
        p.factures[f["facture_id_opaque"]] = {
            "numero": f.get("numero_facture"), "mois": f.get("mois"), "statut": f.get("statut"),
            "type": f.get("type_document"), "montant": f.get("montant_total"),
            "document": f.get("document_nom")}
    if any((f["mois"] or "") >= mois_v1 for f in p.factures.values()):
        # La décision : « il n'existe actuellement aucune facture émise de septembre ». Si une
        # facture V1 existait, la purger serait une décision nouvelle — pas la nôtre.
        p.anomalies.append("Facture(s) propriétaire(s) de la période V1 présente(s) : "
                           + ", ".join(sorted(k for k, f in p.factures.items()
                                              if (f["mois"] or "") >= mois_v1)))
    factures = set(p.factures)
    p.lignes_facture = {r["ligne_id_opaque"] for r in _lignes(conn, "factures_proprietaires_lignes")}
    p.imputations_purgees = {r["imputation_airbnb_id"] for r in _lignes(conn, "imputations_airbnb")}
    p.credits_purges = {r["credit_id_opaque"] for r in _lignes(conn, "credits_clients")}
    if p.credits_purges:
        p.anomalies.append(f"Crédit(s) client(s) présent(s) : {sorted(p.credits_purges)} — la base "
                           "interdit leur suppression ; décision humaine requise.")

    # Objets « argent propriétaire » : purgés s'ils n'ont aucun lien bancaire réel.
    liens_objets: dict[str, list[str]] = {}
    for r in _lignes(conn, "banque_rapprochements"):
        liens_objets.setdefault(r["objet_id"], []).append(r["rapprochement_id_opaque"])
    for l in _lignes(conn, "flux_lettrage_lignes"):
        liens_objets.setdefault(l["element_id"], []).append(l["lettrage_id_opaque"])
    for m in _lignes(conn, "mouvements_tresorerie_proprietaires"):
        mid = m["mouvement_opaque"]
        bancaire = [x for x in liens_objets.get(mid, [])]
        if bancaire:
            p.anomalies.append(f"Mouvement de trésorerie propriétaire {mid} rapproché de la banque "
                               f"({bancaire}) : argent réel, décision humaine requise.")
        else:
            p.mtp_purges.add(mid)
    for fid in factures:
        if liens_objets.get(fid):
            p.anomalies.append(f"Facture propriétaire {fid} rapprochée de la banque "
                               f"({liens_objets[fid]}) : encaissement réel, décision humaine requise.")

    # Relevés / règlements propriétaires de l'ancien environnement.
    for r in _lignes(conn, "proprietaires_releves"):
        if (r.get("mois") or "") < mois_v1:
            p.releves_purges.add(r["releve_id_opaque"])
    p.rapprochements_reglements_purges = {
        r["rapprochement_id_opaque"] for r in _lignes(conn, "rapprochements_reglements")
        if r.get("releve_id_opaque") in p.releves_purges}

    # ── Rapprochements bancaires ───────────────────────────────────────────────────────────────
    objets_purges = purge_charges | factures | p.mtp_purges | p.credits_purges
    for r in _lignes(conn, "banque_rapprochements"):
        rid = r["rapprochement_id_opaque"]
        if rid in p.rap_dependance:
            continue
        if r["mouvement_id_opaque"] not in p.mouvements:
            p.rap_purges[rid] = ("donnée de recette : mouvement bancaire inexistant "
                                 f"({r['mouvement_id_opaque']}), objet {r['objet_id']}")
        elif r["objet_id"] in objets_purges:
            if r["statut"] == "CONFIRME":
                p.anomalies.append(f"Rapprochement validé {rid} vers l'objet purgé {r['objet_id']}.")
            else:
                p.rap_purges[rid] = f"proposition non validée vers l'objet purgé {r['objet_id']}"
        else:
            p.rap_conserves.add(rid)

    # Lettrages : purgés seulement s'ils portent un objet purgé (sinon conservés).
    for lid in {l["lettrage_id_opaque"] for l in _lignes(conn, "flux_lettrage_lignes")}:
        objets = {l["element_id"] for l in _lignes(conn, "flux_lettrage_lignes")
                  if l["lettrage_id_opaque"] == lid and l["cote"] == "OBJET"}
        if objets & objets_purges:
            p.lettrages_purges.add(lid)
            p.anomalies.append(f"Lettrage {lid} portant un objet purgé {sorted(objets & objets_purges)}.")
    for l in _lignes(conn, "flux_lettrages"):
        if l["lettrage_id_opaque"] not in p.lettrages_purges:
            p.lettrages_dependance.add(l["lettrage_id_opaque"])

    # Suggestions et imports de recette (documentés mission 14d : fixtures `CHG_SEED_*`,
    # fichier `releve_recette.csv`), prouvés ici par l'absence de tout mouvement bancaire.
    for s in _lignes(conn, "banque_suggestion_decisions"):
        if s["mouvement_id_opaque"] not in p.mouvements:
            p.suggestions_test.add(s["id"])
    imports_avec_mouvement = {r[0] for r in conn.execute(
        "SELECT DISTINCT import_id FROM banque_mouvements")} if _existe(conn, "banque_mouvements") \
        else set()
    for i in _lignes(conn, "banque_imports"):
        if i["import_id"] not in imports_avec_mouvement and "recette" in str(
                i.get("nom_fichier_origine") or "").lower():
            p.imports_test.add(i["import_id"])

    # ── Écritures ──────────────────────────────────────────────────────────────────────────────
    ids_purges = (objets_purges | set(p.rap_purges) | p.lettrages_purges | p.imputations_purgees
                  | p.releves_purges)
    ecritures = _lignes(conn, "ecritures")
    for e in ecritures:
        origine = e.get("origine_id_opaque")
        if origine in ids_purges:
            p.ecritures_purgees[e["ecriture_id_opaque"]] = (
                f"issue de {e.get('origine_type')} {origine} (purgé)")
    change = True
    while change:      # contrepassation d'une écriture purgée : purgée avec elle
        change = False
        for e in ecritures:
            if (e["ecriture_id_opaque"] not in p.ecritures_purgees
                    and e.get("contrepasse_de") in p.ecritures_purgees):
                p.ecritures_purgees[e["ecriture_id_opaque"]] = (
                    f"contrepassation de {e['contrepasse_de']} (purgée)")
                change = True
    for e in ecritures:
        eid = e["ecriture_id_opaque"]
        if eid in p.ecritures_purgees:
            continue
        origine = e.get("origine_id_opaque")
        if origine in p.rap_dependance or origine in p.lettrages_dependance:
            p.ecritures_dependance.add(eid)
        else:
            p.ecritures_conservees.add(eid)
        if (e.get("periode") or "") < mois_v1:
            p.anomalies.append(f"Écriture {eid} de la période {e.get('periode')} conservée "
                               "(chaîne conservée) : elle polluerait la comptabilité V1.")

    # OD et caisse de l'ancienne période.
    for o in _lignes(conn, "operations_diverses"):
        if (o.get("date_operation") or "")[:7] < mois_v1:
            p.od_purgees.add(o["od_id_opaque"])
    for o in _lignes(conn, "operations_caisse"):
        if (o.get("date_operation") or "")[:7] < mois_v1:
            p.caisse_purgees.add(o["operation_id_opaque"])

    # Clôtures de l'ancien modèle.
    for c in _lignes(conn, "clotures_mensuelles"):
        if (c.get("mois") or "") < mois_v1:
            p.clotures_purgees.add(c["cloture_id_opaque"])

    # Propositions refusées qui ne désignent qu'un objet purgé.
    for r in _lignes(conn, "flux_propositions_refusees"):
        texte = f"{r.get('mouvements_json') or ''} {r.get('objets_json') or ''}"
        if any(oid in texte for oid in objets_purges):
            p.propositions_refusees_purgees.add(r["id"])
    return p


# ── Matrice ─────────────────────────────────────────────────────────────────────────────────────

def _cat(categorie: str, etat: str, nb: int, justification: str) -> dict[str, Any]:
    return {"categorie": categorie, "etat": etat, "nb": nb, "justification": justification}


def _compter_par(conn, table: str, colonne: str, ids: set) -> int:
    if not ids or not _existe(conn, table):
        return 0
    return sum(1 for r in conn.execute(f'SELECT "{colonne}" FROM "{table}"') if r[0] in ids)


def _mixte(conn, table: str, p: Plan) -> list[dict[str, Any]]:
    """Catégories d'une table mixte, avec leur nombre de lignes."""
    n = _compte(conn, table)
    charges_p, charges_k = set(p.charges_purgees), set(p.charges_conservees)

    def partage(colonne: str, purges: set, lib_purge: str, just_purge: str,
                etat_reste: str, lib_reste: str, just_reste: str) -> list[dict]:
        np = _compter_par(conn, table, colonne, purges)
        return [_cat(lib_purge, PURGE, np, just_purge),
                _cat(lib_reste, etat_reste, n - np, just_reste)]

    regle_charge_k = ("Charge héritée disposant d'un rapprochement bancaire validé : conservée "
                      "avec toute sa chaîne.")
    regle_charge_p = ("Charge héritée sans rapprochement bancaire validé : purgée, quels que "
                      "soient sa date, son statut ou son montant.")
    if table == "charges":
        return [_cat("charges rapprochées", KEEP, len(charges_k), regle_charge_k),
                _cat("charges non rapprochées", PURGE, len(charges_p), regle_charge_p)]
    if table in ("charge_evenements", "charges_perimetre_analytique", "charges_perimetre_menage",
                 "charges_affectations", "ik"):
        return partage("charge_id", charges_p, "rattachées à une charge purgée", regle_charge_p,
                       KEEP_BY_DEPENDENCY, "rattachées à une charge conservée", regle_charge_k)
    if table in ("charges_refacturation_positions", "charges_refacturation_evenements"):
        return partage("position_id", p.positions_purgees, "position d'une charge purgée",
                       regle_charge_p, KEEP_BY_DEPENDENCY, "position d'une charge conservée",
                       regle_charge_k)
    if table == "charges_affectation_evenements":
        return partage("affectation_id_opaque", p.affectations_purgees,
                       "affectation d'une charge purgée", regle_charge_p, KEEP_BY_DEPENDENCY,
                       "affectation d'une charge conservée", regle_charge_k)
    if table in ("ik_trajets", "ik_depenses_activite", "ik_evenements"):
        return partage("ik_id_opaque", p.ik_purges, "IK d'une charge purgée", regle_charge_p,
                       KEEP_BY_DEPENDENCY, "IK d'une charge conservée", regle_charge_k)
    if table == "justificatifs":
        return [_cat("justificatifs de charges conservées", KEEP_BY_DEPENDENCY, n,
                     "Pièce d'une charge conservée (la base interdit toute suppression).")]
    if table == "justificatif_evenements":
        return [_cat("événements de justificatifs conservés", KEEP_BY_DEPENDENCY, n,
                     "Historique d'une pièce conservée.")]
    if table == "flux_propositions_refusees":
        np = len(p.propositions_refusees_purgees)
        return [_cat("refus visant un objet purgé", PURGE, np, "N'a plus d'objet."),
                _cat("autres refus", KEEP, n - np, "Mémoire des refus de rapprochement.")]
    if table in ("flux_lettrages", "flux_lettrage_lignes", "flux_lettrage_evenements"):
        return partage("lettrage_id_opaque", p.lettrages_purges, "lettrage d'un objet purgé",
                       "Objet purgé.", KEEP_BY_DEPENDENCY, "lettrages banque ↔ charge conservés",
                       "Lettrage validé d'une charge conservée : la chaîne banque ↔ charge reste "
                       "lisible.")
    if table == "banque_rapprochements":
        return [_cat("banque ↔ charge validés", KEEP_BY_DEPENDENCY, len(p.rap_dependance),
                     "Règle absolue : un rapprochement validé mouvement ↔ charge n'est jamais "
                     "supprimé."),
                _cat("autres rapprochements réels", KEEP, len(p.rap_conserves),
                     "Apport en compte courant, retrait d'espèces… : opérations bancaires réelles."),
                _cat("données de recette / orphelines", PURGE, len(p.rap_purges),
                     "Mouvement bancaire inexistant et objet de recette (CHG_SEED_*, "
                     "RES_HOSTAWAY_2026_06_05…), documentés comme fixtures (mission 14d).")]
    if table == "banque_rapprochement_evenements":
        return partage("rapprochement_id_opaque", set(p.rap_purges),
                       "événements des rapprochements de recette", "Suivent leur rapprochement.",
                       KEEP_BY_DEPENDENCY, "événements des rapprochements conservés",
                       "Suivent leur rapprochement.")
    if table == "banque_suggestion_decisions":
        return [_cat("décisions de recette", PURGE, len(p.suggestions_test),
                     "Visent un mouvement inexistant et des charges CHG_SEED_* (fixtures)."),
                _cat("autres décisions", KEEP, n - len(p.suggestions_test), "Mémoire réelle.")]
    if table == "banque_imports":
        return [_cat("imports de recette", PURGE, len(p.imports_test),
                     "Fichier « releve_recette.csv », aucun mouvement en base (fixture 14d)."),
                _cat("autres imports", KEEP, n - len(p.imports_test), "Journal bancaire réel.")]
    if table in ("ecritures", "ecriture_lignes", "ecriture_evenements",
                 "ecriture_ligne_ventilation"):
        purgees = set(p.ecritures_purgees)
        dep = p.ecritures_dependance
        ndep = _compter_par(conn, table, "ecriture_id_opaque", dep)
        np = _compter_par(conn, table, "ecriture_id_opaque", purgees)
        return [_cat("écritures des factures/objets purgés", PURGE, np,
                     "Purement dérivées de la facturation propriétaire purgée."),
                _cat("écritures banque ↔ charges conservées", KEEP_BY_DEPENDENCY, ndep,
                     "Règlement d'une charge conservée : la chaîne reste lisible."),
                _cat("autres écritures V1", KEEP, n - np - ndep,
                     "Opérations bancaires réelles de septembre (apport, retrait).")]
    if table in ("operations_diverses", "od_lignes"):
        return partage("od_id_opaque", p.od_purgees, "OD de l'ancienne période",
                       "Antérieure à la V1.", KEEP, "OD de la période V1", "Période V1.")
    if table == "operations_caisse":
        return partage("operation_id_opaque", p.caisse_purgees, "caisse de l'ancienne période",
                       "Antérieure à la V1.", KEEP, "caisse de la période V1", "Période V1.")
    if table == "mouvements_tresorerie_proprietaires":
        return partage("mouvement_opaque", p.mtp_purges, "acomptes/reversements sans lien bancaire",
                       "Ancien acompte lié à une facture purgée : jamais transformé en crédit V1.",
                       KEEP, "liés à la banque", "Argent réel.")
    if table == "mouvements_tresorerie_proprietaires_evenements":
        return partage("mouvement_id", p.mtp_purges, "événements des mouvements purgés",
                       "Suivent leur mouvement.", KEEP, "autres", "Suivent leur mouvement.")
    if table in ("proprietaires_releves", "proprietaires_paiement", "proprietaires_releve_cycle",
                 "proprietaires_releve_evenements"):
        return partage("releve_id_opaque", p.releves_purges, "cycle de règlement d'avant la V1",
                       "État de recouvrement de l'ancien environnement.", KEEP,
                       "cycle de la période V1", "Période V1.")
    if table in ("clotures_mensuelles", "cloture_evenements", "cloture_elements",
                 "cloture_documents"):
        return partage("cloture_id_opaque", p.clotures_purgees, "clôtures d'avant la V1",
                       "Clôtures de l'ancien modèle (mois antérieurs à la V1).", KEEP,
                       "clôtures de la période V1", "Premier mois V1 : 2026-09.")
    if table == "parametres_societe_facturation":
        return [_cat("fiche société et facturation", KEEP, n,
                     "Fiche canonique de Chouette Patrimoine : conservée à l'identique."),
                _cat("paramètre de cutover V1", REBUILD, 0,
                     "V1_ACCOUNTING_START_DATE posé par le cutover (une ligne ajoutée).")]
    return [_cat("non traitée", NON_CLASSEE, n, "Catégories non décrites.")]


def matrice(conn, p: Plan) -> list[dict[str, Any]]:
    out = []
    for t in _tables(conn):
        spec = classer(t)
        n = _compte(conn, t)
        if spec is None:
            out.append({"table": t, "role": "?", "nb": n,
                        "categories": [_cat("table non classée", NON_CLASSEE, n,
                                            "Inconnue de la classification : à examiner.")]})
            continue
        if spec.etat == MIXTE:
            cats = _mixte(conn, t, p)
        else:
            cats = [_cat("toutes les lignes", spec.etat, n, spec.justification)]
        out.append({"table": t, "role": spec.role, "nb": n, "categories": cats})
    return out


# ── Simulation ──────────────────────────────────────────────────────────────────────────────────

def _volumetrie_cle(conn) -> dict[str, int]:
    def q(sql: str) -> int:
        try:
            return conn.execute(sql).fetchone()[0]
        except sqlite3.Error:
            return 0
    return {
        "factures_emises": q("SELECT COUNT(*) FROM factures_proprietaires WHERE statut='EMIS'"),
        "factures_proprietaires": q("SELECT COUNT(*) FROM factures_proprietaires"),
        "mouvements_bancaires": q("SELECT COUNT(*) FROM qonto_transactions_raw")
        + q("SELECT COUNT(*) FROM banque_mouvements"),
        "rapprochements_bancaires": q("SELECT COUNT(*) FROM banque_rapprochements"),
        "charges": q("SELECT COUNT(*) FROM charges"),
        "reservations_hostaway": q("SELECT COUNT(*) FROM hostaway_reservations"),
        "reservations_hors_hostaway": q("SELECT COUNT(*) FROM reservations_hors_hostaway"),
        "proprietaires": q("SELECT COUNT(*) FROM ref_proprietaires"),
        "logements": q("SELECT COUNT(*) FROM ref_logements"),
        "intervenants_fournisseurs": q("SELECT COUNT(*) FROM ref_intervenants")
        + q("SELECT COUNT(*) FROM fournisseurs"),
        "ecritures": q("SELECT COUNT(*) FROM ecritures"),
    }


def _rapport(conn, p: Plan) -> dict[str, Any]:
    m = matrice(conn, p)
    totaux = {e: 0 for e in (*ETATS, NON_CLASSEE)}
    for t in m:
        for c in t["categories"]:
            totaux[c["etat"]] = totaux.get(c["etat"], 0) + c["nb"]
    non_classees = [t["table"] for t in m
                    if any(c["etat"] == NON_CLASSEE and c["nb"] for c in t["categories"])
                    or t["role"] == "?"]
    anomalies = list(p.anomalies)
    if non_classees:
        anomalies.append(f"Table(s) non classée(s) : {non_classees}")
    nb_ref = sum(_compte(conn, t) for t in REFERENTIELS)
    return {
        "debut_v1": p.debut,
        "tables_analysees": len(m),
        "lignes": totaux,
        "factures_supprimees": len(p.factures),
        "factures_detail": p.factures,
        "creances_supprimees": sum(1 for f in p.factures.values() if f["statut"] == "EMIS"),
        "charges_supprimees": len(p.charges_purgees),
        "charges_supprimees_detail": p.charges_purgees,
        "charges_conservees": len(p.charges_conservees),
        "charges_conservees_detail": p.charges_conservees,
        "rapprochements_banque_charges_conserves": len(p.rap_dependance),
        "rapprochements_autres_conserves": len(p.rap_conserves),
        "rapprochements_purges": p.rap_purges,
        "ecritures_purgees": p.ecritures_purgees,
        "ecritures_conservees": sorted(p.ecritures_dependance | p.ecritures_conservees),
        "mouvements_bancaires_conserves": _volumetrie_cle(conn)["mouvements_bancaires"],
        "reservations_conservees": _compte(conn, "hostaway_reservations")
        + _compte(conn, "reservations_hors_hostaway"),
        "referentiels_conserves": nb_ref,
        "acomptes_purges": sorted(p.mtp_purges),
        "imputations_purgees": sorted(p.imputations_purgees),
        "clotures_purgees": sorted(p.clotures_purgees),
        "releves_purges": sorted(p.releves_purges),
        "volumetrie": _volumetrie_cle(conn),
        "matrice": m,
        "anomalies": anomalies,
    }


def simuler(*, db_path=None, debut: str = v1.DATE_V1_DECIDEE) -> dict[str, Any]:
    """CUTOVER V1 — SIMULATION. Lecture seule (la connexion ne peut pas écrire)."""
    conn = _connexion(db_path, lecture_seule=True)
    try:
        p = _plan(conn, debut)
        rapport = _rapport(conn, p)
    finally:
        conn.close()
    rapport["mode"] = "SIMULATION"
    rapport["empreinte_rapport"] = hashlib.sha256(json.dumps(
        {k: v for k, v in rapport.items() if k != "mode"}, sort_keys=True, default=str,
        ensure_ascii=False).encode("utf-8")).hexdigest()[:24]
    return rapport


# ── Exécution ───────────────────────────────────────────────────────────────────────────────────

def _supprimer(conn, table: str, colonne: str, ids, journal: dict[str, int]) -> None:
    ids = list(ids)
    if not ids or not _existe(conn, table):
        return
    n = 0
    for i in range(0, len(ids), 400):
        lot = ids[i:i + 400]
        cur = conn.execute(f'DELETE FROM "{table}" WHERE "{colonne}" IN ({",".join("?" * len(lot))})',
                           lot)
        n += cur.rowcount
    journal[table] = journal.get(table, 0) + n


def _vider(conn, table: str, journal: dict[str, int]) -> None:
    if _existe(conn, table):
        journal[table] = journal.get(table, 0) + conn.execute(f'DELETE FROM "{table}"').rowcount


def _purger(conn, p: Plan) -> dict[str, int]:
    """Suppressions dans l'ordre enfants → parents. Aucune ici n'est « par date » seule."""
    j: dict[str, int] = {}
    charges = set(p.charges_purgees)
    # Charges : dépendances d'abord.
    _supprimer(conn, "charges_refacturation_evenements", "position_id", p.positions_purgees, j)
    _supprimer(conn, "charges_refacturation_positions", "position_id", p.positions_purgees, j)
    _supprimer(conn, "charge_evenements", "charge_id", charges, j)
    _supprimer(conn, "charges_perimetre_analytique", "charge_id", charges, j)
    _supprimer(conn, "charges_perimetre_menage", "charge_id", charges, j)
    _supprimer(conn, "charges_affectation_evenements", "affectation_id_opaque",
               p.affectations_purgees, j)
    _supprimer(conn, "charges_affectations", "affectation_id_opaque", p.affectations_purgees, j)
    # Facturation propriétaire et tout ce qui n'existe que pour elle.
    _vider(conn, "factures_proprietaires_lignes_charge", j)
    _supprimer(conn, "factures_proprietaires_lignes_detail", "ligne_id_opaque", p.lignes_facture, j)
    _vider(conn, "factures_proprietaires_lignes_provenance", j)
    _vider(conn, "factures_proprietaires_lignes", j)
    _vider(conn, "factures_proprietaires_reservations", j)
    _vider(conn, "factures_proprietaires_meta", j)
    _vider(conn, "factures_proprietaires_conformite", j)
    _vider(conn, "factures_proprietaires_evenements", j)
    _vider(conn, "factures_proprietaires", j)
    _vider(conn, "factures_proprietaires_sequence", j)
    _supprimer(conn, "imputations_airbnb", "imputation_airbnb_id", p.imputations_purgees, j)
    _supprimer(conn, "mouvements_tresorerie_proprietaires_evenements", "mouvement_id",
               p.mtp_purges, j)
    _supprimer(conn, "mouvements_tresorerie_proprietaires", "mouvement_opaque", p.mtp_purges, j)
    _vider(conn, "proprietaire_allocations", j)
    _vider(conn, "proprietaire_recalculs", j)
    _vider(conn, "reglement_repartitions", j)
    _supprimer(conn, "rapprochement_evenements", "rapprochement_id_opaque",
               p.rapprochements_reglements_purges, j)
    _supprimer(conn, "rapprochements_reglements", "rapprochement_id_opaque",
               p.rapprochements_reglements_purges, j)
    for t in ("proprietaires_paiement", "proprietaires_releve_cycle",
              "proprietaires_releve_evenements", "proprietaires_releves"):
        _supprimer(conn, t, "releve_id_opaque", p.releves_purges, j)
    # Banque : uniquement les données de recette prouvées (jamais un mouvement).
    _supprimer(conn, "banque_rapprochement_evenements", "rapprochement_id_opaque",
               set(p.rap_purges), j)
    _supprimer(conn, "banque_rapprochements", "rapprochement_id_opaque", set(p.rap_purges), j)
    _supprimer(conn, "banque_suggestion_decisions", "id", p.suggestions_test, j)
    _supprimer(conn, "banque_imports", "import_id", p.imports_test, j)
    # Écritures purement dérivées de ce qui est purgé.
    ecr = set(p.ecritures_purgees)
    for t in ("ecriture_evenements", "ecriture_ligne_ventilation", "ecriture_lignes", "ecritures"):
        _supprimer(conn, t, "ecriture_id_opaque", ecr, j)
    _supprimer(conn, "od_lignes", "od_id_opaque", p.od_purgees, j)
    _supprimer(conn, "operations_diverses", "od_id_opaque", p.od_purgees, j)
    _supprimer(conn, "operations_caisse", "operation_id_opaque", p.caisse_purgees, j)
    # Clôtures de l'ancien modèle.
    for t in ("cloture_evenements", "cloture_elements", "cloture_documents", "clotures_mensuelles"):
        _supprimer(conn, t, "cloture_id_opaque", p.clotures_purgees, j)
    _supprimer(conn, "flux_propositions_refusees", "id", p.propositions_refusees_purgees, j)
    # Les charges elles-mêmes, en dernier.
    _supprimer(conn, "charges", "charge_id", charges, j)
    return j


TABLES_A_PRESERVER = REFERENTIELS + HOSTAWAY + RESERVATIONS + ARCHIVES_RESERVATIONS + BANQUE \
    + FACTURES_FOURNISSEURS + MENAGES_SOURCES + OBSERVABILITE + DERIVES


def _empreintes_conservees(conn, p: Plan) -> dict[str, str]:
    """Empreintes de tout ce qui doit sortir du cutover À L'IDENTIQUE."""
    e = {t: empreinte(conn, t) for t in TABLES_A_PRESERVER}
    e["charges(conservées)"] = empreinte(conn, "charges", exclure=("charge_id",
                                                                    set(p.charges_purgees)))
    e["banque_rapprochements(conservés)"] = empreinte(
        conn, "banque_rapprochements", exclure=("rapprochement_id_opaque", set(p.rap_purges)))
    e["ecritures(conservées)"] = empreinte(
        conn, "ecritures", exclure=("ecriture_id_opaque", set(p.ecritures_purgees)))
    e["ecriture_lignes(conservées)"] = empreinte(
        conn, "ecriture_lignes", exclure=("ecriture_id_opaque", set(p.ecritures_purgees)))
    for t in ("flux_lettrages", "flux_lettrage_lignes", "flux_lettrage_evenements"):
        e[f"{t}(conservés)"] = empreinte(conn, t, exclure=("lettrage_id_opaque",
                                                           p.lettrages_purges))
    for t in ("charges_perimetre_analytique", "charges_perimetre_menage", "charge_evenements"):
        e[f"{t}(conservés)"] = empreinte(conn, t, exclure=("charge_id", set(p.charges_purgees)))
    e["justificatifs"] = empreinte(conn, "justificatifs")
    e["justificatif_evenements"] = empreinte(conn, "justificatif_evenements")
    e["parametres_societe(hors V1)"] = empreinte(
        conn, "parametres_societe_facturation", exclure=("cle", {v1.CLE_PARAMETRE}))
    return e


def _assertions(conn, p: Plan, avant: dict[str, str]) -> list[dict[str, Any]]:
    """Invariants A→Z vérifiables en base, évalués sur l'état courant de `conn`."""
    res: list[dict[str, Any]] = []

    def ok(code: str, libelle: str, vrai: bool, detail: Any = "") -> None:
        res.append({"code": code, "libelle": libelle, "ok": bool(vrai), "detail": detail})

    def n(sql: str, *params) -> int:
        return conn.execute(sql, params).fetchone()[0]

    ok("A", "Factures propriétaires existantes = 0", n("SELECT COUNT(*) FROM factures_proprietaires") == 0)
    ok("B", "Créances existantes = 0 (aucune facture émise, aucune imputation, aucun acompte sans "
            "lien bancaire)",
       n("SELECT COUNT(*) FROM factures_proprietaires WHERE statut='EMIS'") == 0
       and n("SELECT COUNT(*) FROM imputations_airbnb") == 0
       and n("SELECT COUNT(*) FROM proprietaire_allocations") == 0
       and n("SELECT COUNT(*) FROM credits_clients") == 0
       and not any(m["mouvement_opaque"] in p.mtp_purges
                   for m in _lignes(conn, "mouvements_tresorerie_proprietaires")))
    liens, raps, _lets = _liens_charges(conn, _mouvements_bancaires(conn))
    charges = [r[0] for r in conn.execute("SELECT charge_id FROM charges")]
    sans = [c for c in charges if c not in liens]
    ok("C", "Charges héritées non rapprochées = 0", not sans, sans)
    ok("D", "Chaque charge conservée a son rapprochement bancaire validé",
       set(charges) == set(p.charges_conservees) and all(c in liens for c in charges))
    presents = {r[0] for r in conn.execute("SELECT rapprochement_id_opaque FROM banque_rapprochements")}
    ok("E", "Tous les rapprochements banque ↔ charges validés d'avant le cutover sont présents",
       p.rap_dependance <= presents and p.rap_dependance <= raps,
       sorted(p.rap_dependance - presents))
    apres = _empreintes_conservees(conn, p)

    def meme(*tables: str) -> bool:
        return all(apres.get(t) == avant.get(t) for t in tables)
    ok("F", "Mouvements bancaires sources inchangés", meme(*BANQUE))
    ok("G", "Identifiants bancaires stables inchangés",
       meme("qonto_transactions_statut_local", "qonto_transactions_raw", "banque_mouvements"))
    ok("H", "Réservations Hostaway inchangées", meme(*HOSTAWAY))
    ok("I", "Archive des réservations intacte", meme(*ARCHIVES_RESERVATIONS))
    ok("J", "Réservations hors Hostaway conservées", meme(*RESERVATIONS))
    ok("K", "Propriétaires conservés", meme("ref_proprietaires", "proprietaires_facturation"))
    ok("L", "Logements conservés", meme("ref_logements", "ref_mapping_logements"))
    ok("M", "Logements archivés et historique de gestion conservés",
       meme("ref_logements", "ref_gestion_logements_hist"))
    ok("N", "Fournisseurs et prestataires conservés",
       meme("fournisseurs", "fournisseur_details", "ref_intervenants"))
    ok("O", "Référentiels conservés", meme(*REFERENTIELS))
    ok("P", "Fiche société conservée", meme("parametres_societe(hors V1)",
                                            "parametres_societe_facturation_historique"))
    ok("Q", "Taux de commission historiques conservés", meme("ref_taux_commission"))
    ok("R", "Coûts ménage historiques conservés",
       meme("ref_couts_standards_menage", "ref_couts_menage_interne"))
    ok("S", "Aucune créance résiduelle (allocations FIFO, journal des recalculs, crédits)",
       n("SELECT COUNT(*) FROM proprietaire_allocations") == 0
       and n("SELECT COUNT(*) FROM proprietaire_recalculs") == 0)
    ok("V", "Aucune écriture antérieure à la V1",
       n("SELECT COUNT(*) FROM ecritures WHERE periode < ?", p.mois) == 0)
    ok("KEEP", "Chaînes conservées intactes (charges, rapprochements, lettrages, écritures, "
               "justificatifs, factures fournisseurs, ménages, observabilité, dérivés)",
       meme(*[k for k in avant]), sorted(k for k in avant if apres.get(k) != avant.get(k)))
    ok("PARAM", "Paramètre V1_ACCOUNTING_START_DATE posé",
       n("SELECT COUNT(*) FROM parametres_societe_facturation WHERE cle=? AND valeur=?",
         v1.CLE_PARAMETRE, p.debut) == 1)
    fk = conn.execute("PRAGMA foreign_key_check").fetchall()
    ok("Z", "PRAGMA foreign_key_check : 0 erreur", not fk, [tuple(r) for r in fk][:10])
    integrite = conn.execute("PRAGMA integrity_check").fetchone()[0]
    ok("Y", "PRAGMA integrity_check : ok", integrite == "ok", integrite)
    return res


def _documents_a_archiver(p: Plan) -> list[Path]:
    racine = (Path(cfg.FACTURES_PROPRIETAIRES_DIR) if getattr(cfg, "FACTURES_PROPRIETAIRES_DIR", None)
              else Path(cfg.DATA_DIR) / "factures_proprietaires")
    noms = {f["document"] for f in p.factures.values() if f.get("document")}
    if not noms or not racine.exists():
        return []
    return sorted(x for x in racine.rglob("*") if x.is_file() and x.name in noms)


def executer(*, db_path=None, debut: str = v1.DATE_V1_DECIDEE, confirmer: bool = False,
             acteur: str = "", archive_dir: Path | None = None,
             _apres_purge: Callable[[sqlite3.Connection], None] | None = None) -> dict[str, Any]:
    """Exécute le cutover en UNE transaction. Refuse sans confirmation, si déjà appliqué, ou si la
    simulation signale une anomalie. Rollback intégral au premier invariant non vérifié."""
    if not confirmer:
        return {"ok": False, "code": E_CONFIRMATION,
                "message": "Cutover refusé sans confirmation explicite (confirmer=True)."}
    if not acteur.strip():
        return {"ok": False, "code": E_CONFIRMATION, "message": "Auteur du cutover requis."}

    conn = _connexion(db_path, lecture_seule=False)
    archives: list[tuple[Path, Path]] = []
    try:
        conn.execute("BEGIN IMMEDIATE")
        deja = conn.execute("SELECT valeur FROM parametres_societe_facturation WHERE cle=?",
                            (v1.CLE_PARAMETRE,)).fetchone()
        if deja:
            conn.execute("ROLLBACK")
            return {"ok": False, "code": E_DEJA_APPLIQUE,
                    "message": f"Cutover V1 déjà appliqué (début {deja[0]})."}
        p = _plan(conn, debut)
        rapport_avant = _rapport(conn, p)
        if rapport_avant["anomalies"]:
            conn.execute("ROLLBACK")
            return {"ok": False, "code": E_ANOMALIES, "message": "Anomalies : exécution refusée.",
                    "anomalies": rapport_avant["anomalies"]}
        avant = _empreintes_conservees(conn, p)

        # Documents générés par l'application pour les seules factures purgées : COPIÉS d'abord
        # (l'original n'est retiré qu'après le COMMIT), jamais un document source externe.
        for doc in _documents_a_archiver(p):
            if archive_dir is None:
                conn.execute("ROLLBACK")
                return {"ok": False, "code": E_CONFIRMATION,
                        "message": "Dossier d'archive requis pour les PDF de factures purgées."}
            cible = Path(archive_dir) / "factures_proprietaires" / doc.name
            cible.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(doc, cible)
            archives.append((doc, cible))

        journal = _purger(conn, p)
        conn.execute(
            "INSERT INTO parametres_societe_facturation (cle, valeur, maj_par, origine) "
            "VALUES (?,?,?,?)", (v1.CLE_PARAMETRE, debut, acteur, "CUTOVER_V1"))
        conn.execute(
            "INSERT INTO parametres_societe_facturation_historique (cle, ancienne_valeur, "
            "nouvelle_valeur, modifie_par, motif) VALUES (?,?,?,?,?)",
            (v1.CLE_PARAMETRE, None, debut, acteur,
             "Cutover V1 : début de la comptabilité applicative V1."))
        # La fiche société historisée gagne une ligne : on l'exclut de la comparaison à l'identique.
        avant["parametres_societe_facturation_historique"] = empreinte(
            conn, "parametres_societe_facturation_historique")
        if _apres_purge is not None:
            _apres_purge(conn)
        verifs = _assertions(conn, p, avant)
        echecs = [v for v in verifs if not v["ok"]]
        if echecs:
            conn.execute("ROLLBACK")
            for _src, cible in archives:
                cible.unlink(missing_ok=True)
            return {"ok": False, "code": E_VERIFICATION, "message": "Vérification échouée : ROLLBACK.",
                    "echecs": echecs, "verifications": verifs, "journal": journal}
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        for _src, cible in archives:
            cible.unlink(missing_ok=True)
        raise
    finally:
        conn.close()

    retires = []
    for src, cible in archives:
        src.unlink(missing_ok=True)
        retires.append({"document": src.name, "archive": str(cible)})
    rapport_avant.update({"mode": "EXECUTION", "ok": True, "journal_suppressions": journal,
                          "verifications": verifs, "documents_archives": retires,
                          "execute_le": _maintenant(), "acteur": acteur})
    return rapport_avant


# ── Vérification après engagement (redémarrage, actualisation) ─────────────────────────────────

def verifier(*, db_path=None) -> dict[str, Any]:
    """Invariants relus sur la base ENGAGÉE, par les services de l'application eux-mêmes."""
    from app.services import creances_dettes_service as cd
    from app.services import factures_proprietaires_service as fpr

    debut = v1.debut(db_path=db_path)
    conn = _connexion(db_path, lecture_seule=True)
    try:
        liens, _r, _l = _liens_charges(conn, _mouvements_bancaires(conn))
        charges = [r[0] for r in conn.execute("SELECT charge_id FROM charges")]
        ecritures_avant = conn.execute("SELECT COUNT(*) FROM ecritures WHERE periode < ?",
                                       ((debut or "0000-00")[:7],)).fetchone()[0]
        prefactures_avant = conn.execute(
            "SELECT COUNT(*) FROM lot12_prefactures_entete e JOIN lot12_runs r "
            "ON r.run_id = e.run_id AND r.actif = 1 WHERE e.mois < ?",
            ((debut or "0000-00")[:7],)).fetchone()[0] if _existe(conn, "lot12_runs") else 0
        integrite = conn.execute("PRAGMA integrity_check").fetchone()[0]
        fk = conn.execute("PRAGMA foreign_key_check").fetchall()
        volumetrie = _volumetrie_cle(conn)
    finally:
        conn.close()
    try:
        fpr.exiger_periode_v1("2026-08", db_path=db_path)
        aout_refuse = False
    except fpr.FacturationAvantV1:
        aout_refuse = True
    try:
        fpr.exiger_periode_v1("2026-09", db_path=db_path)
        septembre_autorise = True
    except fpr.FactureProprietaireError:
        septembre_autorise = False
    checks = {
        "cutover_applique": debut == v1.DATE_V1_DECIDEE,
        "factures_emises_0": volumetrie["factures_emises"] == 0,
        "factures_0": volumetrie["factures_proprietaires"] == 0,
        "creances_0": cd.creances(db_path=db_path) == [],
        "charges_non_rapprochees_0": all(c in liens for c in charges),
        "ecritures_avant_v1_0": ecritures_avant == 0,
        "prefactures_avant_v1_0": prefactures_avant == 0,
        "facture_aout_refusee": aout_refuse,
        "facture_septembre_autorisee": septembre_autorise,
        "prochain_numero_septembre": fpr.prochain_numero("2026-09", db_path=db_path),
        "integrity_ok": integrite == "ok",
        "foreign_key_check_0": not fk,
    }
    checks["ok"] = all(v for k, v in checks.items() if k != "prochain_numero_septembre") \
        and checks["prochain_numero_septembre"] == "2026-09-001"
    return {"debut_v1": debut, "volumetrie": volumetrie, "checks": checks}
