"""Lot11 — contrôles de cohérence transverses, SQLite natif (migration 0045).

Port fidèle des groupes de `02_TRAVAIL/lot11_controles_coherence.py` dont TOUTES les sources sont
déjà SQLite : réservations (Lot4quater `reservations_resolues`), payouts/anomalies Hostaway, Lot9
(`flux_unifies`), Lot10 (`lot10_*`), référentiel (`REF_Setup` via `ref_setup_repo`, migration 0029),
Banque (`banque_vues_service`/`ref_cloture_mensuelle`). Même statut architectural que
`flux_unifie_service.py` pour Lot9 : un port direct dans la couche application plutôt qu'un ajout
`--source SQLITE` au script `02_TRAVAIL` (accepté après parité réelle prouvée, cf. doc 95 §15-16).

COUVERTURE : TOUS LES GROUPES DU LEGACY SONT PORTÉS (fermeture complète de Lot11).
  - Groupe 0  référentiels historisés (taux commission, gestion logements)
  - Groupe 0B AirCover / imputations Airbnb
  - Groupe 0C ajustements post-clôture (constat seul — une période close n'est jamais réécrite)
  - Groupe 1  PK manquantes/dupliquées (FLUX/RES/PAY/COM)
  - Groupe 2  jointures transverses
  - Groupe 3  cohérence exploitation / règlement
  - Groupe 4  cohérence arithmétique commissions + invariant REEL=COMPTABLE+HC
  - Groupe 5  sources métier non alimentées (HC_ZERO_SOURCES_VIDES)
  - Groupe 6  référentiel + anomalies Hostaway
  - Groupe 6f rapprochement ménages externes ↔ Hostaway (Lot6 SQLite, plus aucun classeur Lot6c)
  - Groupe 7  référentiel propriétaires
  - Groupe 8  banque (payout déjà Hostaway, clôture bancaire impossible)
  - Lot7C     suivi associé (avantages HR)
  - MENAGES   provenance de la source (cache/réseau Google Sheet — manifeste JSON, pas un classeur)
  - DASHBOARD_MOIS et CAISSE_THEORIQUE

Les sources métier encore NON MIGRÉES (Charges/Lot3, Acomptes/Lot5-master, IK/Lot7, HH/Lot4-master,
AirCover, ImputationsAirbnb, AjustementsPostCloture) sont toutes vides aujourd'hui : le contrôle
porté produit exactement le constat que le legacy produit pour une source vide (même code, même
message), sans jamais ouvrir de classeur. Les branches conditionnées à des données inexistantes
(schéma incomplet, cross-contrôles avantages, impact AirCover automatique) seront portées AVEC la
migration de ces saisies : les écrire à vide produirait des règles qui ne s'exécutent jamais.
Aucune règle métier n'est modifiée ni inventée.

ÉCRITURE — MÊMES TABLES QUE LA REPRISE CLASSEUR (0041/0042), UN RUN TRACÉ (0045)
`construire()` calcule TOUT en mémoire, puis n'écrit qu'une fois le calcul terminé sans exception —
DELETE + INSERT dans une seule transaction (jamais de remplacement partiel du dernier jeu valide).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import banque_vues_service
from app.services import ref_setup_repo
from app.services import reservations_dataset_service as res_ds

# Les bibliothèques de règles (`lib_*`) vivent dans le 02_TRAVAIL DU WORKTREE, à côté du paquet
# `app` — c'est `APP_ROOT.parent` qui les localise, jamais `cfg.PROJECT_ROOT`. Les deux coïncident
# en exécution normale, mais `PROJECT_ROOT` est redirigé (variable d'environnement, tests isolant
# un faux arbre) : l'ancrer ici rendait l'import dépendant d'une valeur qui n'a rien à voir avec
# l'emplacement du code.
_TRAVAIL_DIR = str(cfg.APP_ROOT.parent / "02_TRAVAIL")
if _TRAVAIL_DIR not in sys.path:
    sys.path.insert(0, _TRAVAIL_DIR)

from lib_controls import default_impact_facture  # noqa: E402
from lib_ref_history import REF_GESTION_LOGEMENTS_HIST_SHEET, resolve_management_period  # noqa: E402

SOURCE_SQLITE_NATIF = "SQLITE_NATIF"
STATUT_SUCCES = "SUCCES"
STATUT_ECHEC = "ECHEC"
TOLERANCE = 0.10


def _num(v, default=0.0) -> float:
    if v is None or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _txt(v) -> str:
    return "" if v is None else str(v)


def _norm_id(v) -> str:
    s = _txt(v).strip()
    return s[:-2] if s.endswith(".0") else s


class _Ctrl:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def add(self, source_module, source_table, source_pk, code_controle, severity, message, *,
           mois=None, logement_id=None, proprietaire_id=None, reservation_id=None,
           document_id=None, impact_facture=None, commentaire=None) -> None:
        pk = f"{source_pk or source_module}||{code_controle}"
        self.rows.append({
            "ctrl_pk": pk, "source_module": source_module, "source_table": source_table,
            "source_pk": str(source_pk) if source_pk else None,
            "reservation_id": reservation_id, "code_controle": code_controle, "severity": severity,
            "impact_facture": default_impact_facture(severity, impact_facture), "message": message,
            "mois": mois, "logement_id": logement_id, "proprietaire_id": proprietaire_id,
            "document_id": document_id, "statut_resolution": "OUVERT", "commentaire": commentaire,
            "date_detection": self.today,
        })


def _groupe0_ref_historiques(ctrl: _Ctrl, df_res: list[dict], db_path) -> None:
    taux_hist = ref_setup_repo.lire_onglet("REF_Taux_Commission", db_path=db_path)
    if not taux_hist:
        ctrl.add("REF", "REF_Taux_Commission", None,
                 "TAUX_COMMISSION_HISTORIQUE_ABSENT_TRANSITOIRE", "A_CONTROLER",
                 "REF_Taux_Commission absent ou vide. Aucun taux non date autorise; "
                 "facture finale interdite.",
                 commentaire="Creer les taux dates avant cloture/facture finale.")

    gest_hist = ref_setup_repo.lire_onglet(REF_GESTION_LOGEMENTS_HIST_SHEET, db_path=db_path)
    if not gest_hist:
        ctrl.add("REF", REF_GESTION_LOGEMENTS_HIST_SHEET, None,
                 "GESTION_LOGEMENT_HISTORIQUE_ABSENT_TRANSITOIRE", "A_CONTROLER",
                 "REF_Gestion_Logements_Hist absent ou vide. Le proprietaire applicable ne peut "
                 "pas etre resolu; facture finale interdite.",
                 commentaire="Creer les periodes de gestion datees avant cloture/facture finale.")
    else:
        for r in df_res:
            if r.get("statut_controle") != "VALIDE":
                continue
            res = resolve_management_period(
                gest_hist, logement_id=r.get("logement_id"), date_arrivee=r.get("date_arrivee"),
                date_depart=r.get("date_depart"))
            if res.status != "OK":
                ctrl.add("RESERVATIONS", "reservations_resolues", r.get("reservation_calc_id"),
                         f"GESTION_LOGEMENT_{res.status}", "BLOQUANT",
                         f"Reservation hors periode/proprietaire de gestion: {res.message}",
                         mois=r.get("mois"), logement_id=r.get("logement_id"),
                         proprietaire_id=r.get("proprietaire_id"))


def _groupe1_pk_doublons(ctrl: _Ctrl, df_flux, df_res, df_pay, df_com) -> None:
    tables = [
        ("FLUX", df_flux, "flux_id", "flux_unifies"),
        ("RESERVATIONS", df_res, "reservation_calc_id", "reservations_resolues"),
        ("PAYOUT", df_pay, "reservation_id", "hostaway_payouts"),
        ("COMMISSIONS", df_com, "flux_source_pk", "lot10_commissions"),
    ]
    for module, rows, pk_col, tbl in tables:
        vus, doublons_ex, nulls = set(), [], 0
        for r in rows:
            v = r.get(pk_col)
            if v is None or v == "":
                nulls += 1
                continue
            if v in vus and len(doublons_ex) < 3:
                doublons_ex.append(v)
            vus.add(v)
        if nulls:
            ctrl.add(module, tbl, None, "PK_MANQUANTE_OU_DOUBLONNEE", "BLOQUANT",
                     f"{tbl}: {nulls} lignes sans PK '{pk_col}'.")
        n_dupes = len(rows) - len(vus) - nulls
        if n_dupes > 0:
            ctrl.add(module, tbl, None, "PK_MANQUANTE_OU_DOUBLONNEE", "BLOQUANT",
                     f"{tbl}: {n_dupes} PK dupliquees (ex: {doublons_ex}).")


def _groupe2_jointures(ctrl: _Ctrl, df_flux, df_res, df_pay, df_hh) -> None:
    flux017 = [r for r in df_flux if r.get("type_flux_id") == "TYPE_FLUX_017"]
    vus, doublons = set(), []
    non_null_pks = [r.get("source_pk") for r in flux017 if r.get("source_pk")]
    for pk in non_null_pks:
        if pk in vus and len(doublons) < 3:
            doublons.append(pk)
        vus.add(pk)
    n_dupes_017 = len(non_null_pks) - len(vus)
    if n_dupes_017 > 0:
        ctrl.add("FLUX", "flux_unifies", None, "DOUBLON_RESERVATION_FLUX", "BLOQUANT",
                 f"{n_dupes_017} source_pk TYPE_FLUX_017 dupliquees (ex: {doublons}).")

    res_ids = {r.get("reservation_calc_id") for r in df_res if r.get("reservation_calc_id")}
    flux017_pks = {r.get("source_pk") for r in flux017 if r.get("source_pk")}
    manquants_res = sorted(flux017_pks - res_ids)
    for pk in manquants_res:
        row = next(r for r in flux017 if r.get("source_pk") == pk)
        ctrl.add("FLUX", "flux_unifies", pk, "JOINTURE_RESERVATIONS_MANQUANTE", "BLOQUANT",
                 f"source_pk '{pk}' TYPE_FLUX_017 absent de reservations_resolues.",
                 mois=row.get("mois"), logement_id=row.get("logement_id"))

    pay_ids = {_txt(r.get("reservation_id")) for r in df_pay if r.get("reservation_id")}
    res_normal = [r for r in df_res if r.get("source") in ("HOSTAWAY_AIRBNB", "HOSTAWAY_BOOKING")]
    for r in res_normal:
        rid = _norm_id(r.get("reservation_id_hostaway"))
        if rid not in pay_ids:
            ctrl.add("RESERVATIONS", "reservations_resolues", r.get("reservation_calc_id"),
                     "JOINTURE_PAYOUT_MANQUANTE", "BLOQUANT",
                     f"reservation_id_hostaway '{r.get('reservation_id_hostaway')}' absent de "
                     "hostaway_payouts.",
                     mois=r.get("mois"), logement_id=r.get("logement_id"))

    # 2d — réservations hors Hostaway. Position et vocabulaire du legacy conservés : quand la
    # source HH est vide, le constat émis est `HC_ZERO_SOURCES_VIDES` (groupe 2, pas groupe 5) ;
    # sinon on cherche une réservation présente à la fois en HH et côté Hostaway (double comptage).
    #
    # En SQLite, les réservations HH ne vivent plus dans un classeur séparé : Lot4bis les fusionne
    # dans la table commune, où elles portent un `reservation_hh_id`. L'intersection se fait donc
    # entre les lignes HH et les lignes NON-HH — comparer les lignes HH à elles-mêmes produirait un
    # faux BLOQUANT systématique.
    if not df_hh:
        ctrl.add("HH", "MASTER_FACT_MAN_ReservationsHorsHostaway", None, "HC_ZERO_SOURCES_VIDES",
                 "INFO",
                 "MASTER_FACT_MAN_ReservationsHorsHostaway vide. Controles HH non representatifs.",
                 commentaire="Sources non encore saisies.")
    else:
        hh_res_ids = {_norm_id(r.get("reservation_id_hostaway")) for r in df_hh
                      if r.get("reservation_id_hostaway")}
        hh_res_ids.discard("")
        res_ha_ids = {_norm_id(r.get("reservation_id_hostaway")) for r in df_res
                      if r.get("reservation_id_hostaway")
                      and not _txt(r.get("reservation_hh_id")).strip()}
        res_ha_ids.discard("")
        for rid in sorted(hh_res_ids & res_ha_ids):
            ctrl.add("HH", "MASTER_FACT_MAN_ReservationsHorsHostaway", rid,
                     "RESERVATION_DOUBLON_HOSTAWAY_HH", "BLOQUANT",
                     f"reservation_id_hostaway '{rid}' present dans HH ET Reservations HA.")


def _groupe3_exploitation_reglement(ctrl: _Ctrl, exploit_rows: list[dict],
                                    reglement_rows: list[dict]) -> None:
    for r in exploit_rows:
        payout = _num(r.get("payout_calcule"))
        menage = _num(r.get("menage_retenu"))
        commission = _num(r.get("commission_conciergerie"))
        canape = _num(r.get("preparation_canape_voyageurs"))
        charge_fixe = _num(r.get("charge_fixe_mensuelle"))
        net_calcule = round(payout - menage - commission - canape - charge_fixe, 2)
        # Relecture de la valeur DÉJÀ calculée par Lot10 (`lot10_net_exploitation.
        # revenu_net_exploitation`) pour la comparer à sa propre formule — un contrôle de
        # cohérence, jamais un second calcul métier (Lot10 reste l'unique moteur de calcul).
        net_stocke = _num(r.get("revenu_net_exploitation"))
        if abs(net_stocke - net_calcule) > TOLERANCE:
            ctrl.add("EXPLOITATION", "lot10_net_exploitation", r.get("flux_source_pk"),
                     "REVENU_NET_EXPLOITATION_INCOHERENT", "BLOQUANT",
                     f"Ecart {abs(net_stocke - net_calcule):.4f}E: revenu_net={net_stocke} "
                     f"!= calcule={net_calcule}",
                     mois=r.get("mois"), logement_id=r.get("logement_id"),
                     proprietaire_id=r.get("proprietaire_id"))

    for r in reglement_rows:
        net = _num(r.get("net_proprietaire_apres_charge_mois"))
        rp = _num(r.get("reste_a_payer_conciergerie"))
        acompte = _num(r.get("acompte_conciergerie_recu_via_airbnb"))
        if acompte > 0 and abs(net - rp) < TOLERANCE:
            ctrl.add("EXPLOITATION", "lot10_net_reglement",
                     f"{r.get('mois')}|{r.get('logement_id')}",
                     "ACOMPTE_AIRBNB_INCLUS_NET_EXPLOITATION", "BLOQUANT",
                     f"net_proprietaire_apres_charge={net} == reste_a_payer={rp} alors qu'un "
                     f"acompte de {acompte} est present -> confusion possible.",
                     mois=r.get("mois"), logement_id=r.get("logement_id"),
                     proprietaire_id=r.get("proprietaire_id"))

        payout_mois = _num(r.get("total_payout_mois"))
        if payout_mois > 0 and rp == payout_mois:
            ctrl.add("EXPLOITATION", "lot10_net_reglement",
                     f"{r.get('mois')}|{r.get('logement_id')}", "CONFUSION_PAYOUT_SOLDE_FACTURE",
                     "BLOQUANT",
                     f"reste_a_payer={rp} == total_payout={payout_mois} -> commission/charge fixe "
                     "non deduits du solde.",
                     mois=r.get("mois"), logement_id=r.get("logement_id"),
                     proprietaire_id=r.get("proprietaire_id"))

        pdr = _num(r.get("paiement_deja_recu"))
        montant_du = _num(r.get("montant_du_conciergerie"))
        autres_acomptes = _num(r.get("autres_acomptes_recus"))
        rp_calcule = round(montant_du - acompte - autres_acomptes - pdr, 2)
        if abs(rp - rp_calcule) > TOLERANCE:
            ctrl.add("EXPLOITATION", "lot10_net_reglement",
                     f"{r.get('mois')}|{r.get('logement_id')}",
                     "PAIEMENT_DEJA_RECU_DEDUIT_DU_PAYOUT", "BLOQUANT",
                     f"Ecart {abs(rp - rp_calcule):.4f}E dans formule reste_a_payer.",
                     mois=r.get("mois"), logement_id=r.get("logement_id"),
                     proprietaire_id=r.get("proprietaire_id"))


def _groupe4_commissions(ctrl: _Ctrl, df_com: list[dict], rslt_global: dict[str, float]) -> None:
    for r in df_com:
        taux = _num(r.get("taux_commission"))
        assiette = _num(r.get("assiette_commission"))
        payout = _num(r.get("payout_calcule"))
        menage = _num(r.get("menage_retenu"))
        commission = _num(r.get("commission_conciergerie"))
        if taux > 0:
            # Mission 14f-bis : la commission decoule de l'assiette RETENUE (plafonnee a 0 si
            # l'assiette brute est negative), jamais de l'assiette brute stockee telle quelle.
            assiette_retenue = assiette if assiette > 0 else 0.0
            com_calcule = round(assiette_retenue * taux, 2)
            if abs(commission - com_calcule) > TOLERANCE:
                ctrl.add("COMMISSIONS", "lot10_commissions", r.get("flux_source_pk"),
                         "COMMISSION_INCOHERENTE", "BLOQUANT",
                         f"commission={commission} != assiette({assiette})*taux({taux})="
                         f"{com_calcule}",
                         mois=r.get("mois"), logement_id=r.get("logement_id"),
                         proprietaire_id=r.get("proprietaire_id"))
        assiette_calcule = round(payout - menage, 2)
        if abs(assiette - assiette_calcule) > TOLERANCE:
            ctrl.add("COMMISSIONS", "lot10_commissions", r.get("flux_source_pk"),
                     "ASSIETTE_COMMISSION_INCOHERENTE", "BLOQUANT",
                     f"assiette={assiette} != payout({payout})-menage({menage})="
                     f"{assiette_calcule}",
                     mois=r.get("mois"), logement_id=r.get("logement_id"),
                     proprietaire_id=r.get("proprietaire_id"))
        # Mission 14f-bis : assiette BRUTE negative (menage retenu > payout — sejour tres court a
        # faible payout, cas reel audite reservation_id=65060946) -> Lot10 plafonne la commission
        # a 0 mais conserve l'assiette brute inchangee dans `assiette_commission` (colonne stockee
        # ici = la brute, jamais reecrite : c'est elle qui prouve le cas). Ce controle rend le cas
        # visible, non bloquant (A_CONTROLER) — il ne doit jamais disparaitre silencieusement.
        if assiette < 0:
            ecart = round(menage - payout, 2)
            ctrl.add("COMMISSIONS", "lot10_commissions", r.get("reservation_calc_id"),
                     "ASSIETTE_NEGATIVE_RAMENEE_ZERO", "A_CONTROLER",
                     f"assiette brute={assiette} (payout={payout}, menage={menage}, "
                     f"ecart={ecart}) ramenee a 0 pour le calcul de commission ; "
                     f"commission={commission}",
                     mois=r.get("mois"), logement_id=r.get("logement_id"),
                     proprietaire_id=r.get("proprietaire_id"),
                     reservation_id=r.get("reservation_id_hostaway") or r.get("reservation_calc_id"),
                     commentaire="Assiette brute conservee dans lot10_commissions.assiette_"
                                 "commission ; commission plafonnee a 0, jamais negative.")

    reel_val, comp_val, hc_val = (rslt_global.get(v) for v in ("REEL", "COMPTABLE", "HORS_COMPTA"))
    if reel_val is not None and comp_val is not None and hc_val is not None:
        expected = round(comp_val + hc_val, 2)
        if abs(reel_val - expected) > 1.00:
            ctrl.add("RESULTATS", "lot10_resultats", "GLOBAL",
                     "REEL_INCOHERENT_VS_COMPTABLE_PLUS_HC", "BLOQUANT",
                     f"REEL={reel_val} != COMPTABLE({comp_val})+HC({hc_val})={expected}")


def _groupe6_referentiel_hostaway(ctrl: _Ctrl, ref_log: list[dict], ref_map: list[dict],
                                  df_flux: list[dict], df_res: list[dict], df_ha_ano: list[dict],
                                  df_com_ac: list[dict]) -> None:
    from lib_parc import A_CONTROLER, STATUT_PARC_INVALIDE, is_gere, is_statut_parc_a_controler  # noqa: E402

    log_ref = {r.get("logement_id"): r for r in ref_log}
    for lid, row_log in log_ref.items():
        if is_statut_parc_a_controler(row_log):
            ctrl.add("REFERENTIEL", "ref_logements", lid, STATUT_PARC_INVALIDE, A_CONTROLER,
                     f"Logement {lid}: statut_parc vide ou invalide; aucun calcul economique "
                     "autorise.", logement_id=lid,
                     commentaire="Corriger REF_Logements.statut_parc avec GERE ou "
                                "HORS_PARC_TECHNIQUE.")

    forfait_ref = {r.get("logement_id"): _num(r.get("forfait_logiciel_consommables_mensuel"))
                  for r in ref_log}
    logements_avec_forfait = {lid: v for lid, v in forfait_ref.items()
                              if v > 0 and is_gere(log_ref.get(lid))}
    logements_en_flux017 = {r.get("logement_id") for r in df_flux
                            if r.get("type_flux_id") == "TYPE_FLUX_017" and r.get("logement_id")}
    for lid, forfait in logements_avec_forfait.items():
        if lid not in logements_en_flux017:
            actif = log_ref.get(lid, {}).get("actif", "?")
            ctrl.add("RESULTATS", "flux_unifies", lid, "LOG_SANS_FLUX_017", "A_CONTROLER",
                     f"Logement {lid} a forfait={forfait}E mais 0 flux TYPE_FLUX_017 dans "
                     f"flux_unifies. actif={actif}.", logement_id=lid,
                     commentaire="Confirmer si logement actif. Correction: referentiel logements "
                                "ou saisie reservations.")

    resolved_listings = {_norm_id(r.get("hostaway_listing_id")) for r in ref_log
                         if r.get("hostaway_listing_id")}
    resolved_listings.discard("")
    for m in ref_map:
        if _txt(m.get("actif")).upper() != "OUI":
            continue
        if "listingmapid" in _txt(m.get("champ_source")).lower():
            v = _norm_id(m.get("valeur_source"))
            if v:
                resolved_listings.add(v)
    resolved_res = {_norm_id(r.get("reservation_id_hostaway")) for r in df_res
                    if r.get("logement_id")}
    resolved_res.discard("")

    import re as _re
    for row in df_ha_ano:
        if row.get("code_anomalie") != "LISTING_ORPHELIN_A_CONTROLER":
            continue
        desc = _txt(row.get("description"))
        m = _re.search(r"\d{4,}", desc)
        listing_id = m.group(0) if m else ""
        res_id = _norm_id(row.get("reservation_id"))
        if (listing_id and listing_id in resolved_listings) or (res_id and res_id in resolved_res):
            continue
        ctrl.add("HOSTAWAY", "hostaway_anomalies", _txt(row.get("reservation_id")) or "listing",
                 "LISTING_ORPHELIN_A_CONTROLER", "A_CONTROLER", desc,
                 commentaire="Resolution: ajouter listing au REF_Logements ou confirmer inactif.")

    n_vrbo = sum(1 for r in df_res if r.get("source") == "HOSTAWAY_VRBO_A_CONTROLER")
    if n_vrbo > 0:
        ctrl.add("RESERVATIONS", "reservations_resolues", None, "VRBO_MONTANT_NON_RENSEIGNE",
                 "A_CONTROLER",
                 f"{n_vrbo} reservations HOSTAWAY_VRBO_A_CONTROLER sans montant valide. "
                 "Saisie manuelle requise au Lot 4.",
                 commentaire="Lots 4/4bis: saisir montants VRBO dans "
                            "SAISIE_ReservationsHorsHostaway.xlsx")

    for r in df_com_ac:
        if r.get("code_anomalie_lot10") == "GUEST_COUNT_MANQUANT_PREPARATION_CANAPE":
            ctrl.add("COMMISSIONS", "lot10_commissions_a_controler", r.get("reservation_id"),
                     "GUEST_COUNT_MANQUANT_PREPARATION_CANAPE", "A_CONTROLER",
                     "guestCount manquant pour un logement soumis a preparation canape; montant "
                     "non facture tant que non resolu.",
                     mois=r.get("mois") if "mois" in r else None,
                     logement_id=r.get("logement_id_snapshot"),
                     reservation_id=r.get("reservation_id"),
                     commentaire="Corriger guestCount dans la source reservation; ne jamais "
                                "deduire depuis prix/linge/nom voyageur.")

    n_ac = len(df_com_ac)
    if n_ac > 0:
        ctrl.add("COMMISSIONS", "lot10_commissions_a_controler", None,
                 "RESERVATION_A_CONTROLER_SANS_COMMISSION", "A_CONTROLER",
                 f"{n_ac} reservations A_CONTROLER exclues du calcul automatique des commissions "
                 f"(VRBO + Direct). Net proprietaire incomplet pour ces {n_ac} reservations.",
                 commentaire="Requiert saisie manuelle ou decision par reservation. Lots 4/4bis.")


def _groupe7_referentiel_proprietaires(ctrl: _Ctrl, ref_prop: list[dict]) -> None:
    n_adef = sum(1 for r in ref_prop if r.get("mode_facturation") == "A_DEFINIR")
    if n_adef > 0:
        ctrl.add("TRANSVERSE", "ref_proprietaires", None, "MODE_FACTURATION_A_DEFINIR", "INFO",
                 f"{n_adef} proprietaires avec mode_facturation=A_DEFINIR. Bloquant pour Lot 12 "
                 "(facturation). Non bloquant pour Lots 9-11.",
                 commentaire="Definir mode_facturation avant Lot 12.")

    # REF_Proprietaires ne porte pas toujours 'taux_commission' (le taux vit surtout dans
    # REF_Taux_Commission, historisé) — même garde de présence de colonne que le legacy
    # (`if "taux_commission" in df_prop.columns`), pour ne jamais transformer une colonne absente
    # en faux BLOQUANT systématique.
    if any("taux_commission" in r for r in ref_prop):
        for r in ref_prop:
            if _txt(r.get("actif")).upper() != "OUI":
                continue
            taux = r.get("taux_commission")
            if taux is None or _txt(taux) == "":
                ctrl.add("TRANSVERSE", "ref_proprietaires", r.get("proprietaire_id"),
                         "COMMISSION_SANS_TAUX", "BLOQUANT",
                         f"Proprietaire {r.get('proprietaire_id')} actif sans taux_commission.",
                         proprietaire_id=r.get("proprietaire_id"))


def _groupe8_banque(ctrl: _Ctrl, mouvements: list[dict], cloture_rows: list[dict],
                    df_flux: list[dict]) -> bool:
    if not mouvements and not cloture_rows:
        ctrl.add("BANQUE", "banque_mouvements", None, "BANQUE_NON_DISPONIBLE_GIT", "INFO",
                 "Aucun mouvement bancaire ni cloture en base. Controles banque non executes.")
        return False

    flux_from_banque = [r for r in df_flux
                        if "BANQUE" in _txt(r.get("source_table")).upper()
                        and r.get("sens") == "PRODUIT"]
    for r in flux_from_banque:
        ctrl.add("BANQUE", "flux_unifies", r.get("flux_id"),
                 "BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY", "BLOQUANT",
                 f"Flux PRODUIT source banque detecte: {r.get('source_pk')} "
                 f"type={r.get('type_flux_id')}. Un payout banque ne doit pas creer un PRODUIT "
                 "en double avec Hostaway.",
                 mois=r.get("mois"), logement_id=r.get("logement_id"))

    from lib_cloture import normalise_cloture_status, validate_cloture_status  # noqa: E402

    for row_clo in cloture_rows:
        mois_clo = row_clo.get("mois")
        if not mois_clo:
            continue
        mois_str = _txt(mois_clo)[:7]
        statut_clo = row_clo.get("statut_mois")
        ok_status, status_or_code = validate_cloture_status(statut_clo)
        if not ok_status:
            ctrl.add("BANQUE", "ref_cloture_mensuelle", mois_str, status_or_code, "BLOQUANT",
                     f"Mois {mois_str}: statut_mois interdit '{statut_clo}'. Statuts autorises: "
                     "OUVERT, EN_CONTROLE, CLOTURE.", mois=mois_str)
            statut_clo = "OUVERT"
        else:
            statut_clo = status_or_code

        n_non_classe_mois = sum(
            1 for m in mouvements
            if _txt(m.get("date_operation"))[:7] == mois_str
            and m.get("statut_classification") == "RAPPROCHEMENT_REQUIS")
        if n_non_classe_mois > 0:
            ctrl.add("BANQUE", "ref_cloture_mensuelle", mois_str,
                     "CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE", "A_CONTROLER",
                     f"Mois {mois_str} statut={statut_clo}: {n_non_classe_mois} lignes "
                     "RAPPROCHEMENT_REQUIS. Cloture impossible tant que rapprochements non "
                     "resolus.", mois=mois_str,
                     commentaire="Exporter donnees Airbnb et rapprocher RAPPROCH_AIRBNB_ATTENTE "
                                "(Lot 8c).")
    return True


# ── Groupes migrés lors de la fermeture complète de Lot11 ───────────────────────────────────────
#
# Ces groupes étaient les derniers servis par le moteur legacy. Aucune règle métier n'est modifiée :
# seules les ENTRÉES changent (SQLite au lieu d'un classeur). Chaque source encore non migrée
# (Charges/Lot3, Acomptes/Lot5-master, IK/Lot7, HH/Lot4-master, AirCover, ImputationsAirbnb,
# AjustementsPostCloture) est traitée comme le legacy la traitait quand elle est vide : le MÊME
# constat INFO, avec le MÊME code et le MÊME message. Constater « la source n'est pas alimentée »
# ne demande pas d'ouvrir la source — c'est précisément ce que ces contrôles disent.


def _dataset_non_alimente(db_path, table: str, condition: str = "") -> bool:
    """Vrai si la table n'existe pas ou ne porte aucune ligne (métier) exploitable."""
    conn = get_db(db_path)
    try:
        if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                        (table,)).fetchone() is None:
            return True
        sql = f"SELECT COUNT(*) FROM {table}" + (f" WHERE {condition}" if condition else "")
        return conn.execute(sql).fetchone()[0] == 0
    finally:
        conn.close()


def _groupe0b_aircover_airbnb(ctrl: _Ctrl, db_path) -> None:
    """Groupe 0B — imputations Airbnb et AirCover.

    Les deux saisies (`SAISIE_ImputationsAirbnb.xlsx`, `SAISIE_AirCover.xlsx`) n'ont aucune table
    SQLite : ce sont des saisies humaines non encore migrées, et leur onglet MASTER réel est vide
    (0 ligne). Le legacy émet dans ce cas un constat INFO par source — repris ici à l'identique
    (code, sévérité et message inchangés). Les branches « schéma incomplet » / « imputation non
    certaine » / « impact automatique interdit » du legacy ne peuvent pas se déclencher sur une
    source vide : les porter à vide produirait du code mort qui prétendrait contrôler quelque chose.
    Elles seront portées avec la migration de ces saisies (règle métier non modifiée, non inventée).
    """
    if _dataset_non_alimente(db_path, "imputations_airbnb"):
        ctrl.add("REGLEMENT", "MASTER_FACT_MAN_ImputationsAirbnb", None,
                 "AIRBNB_IMPUTATIONS_ABSENTES", "INFO",
                 "Aucune imputation Airbnb validee disponible. Les virements Airbnb bancaires ne "
                 "reduisent pas le reste a payer.")
    if _dataset_non_alimente(db_path, "aircover"):
        ctrl.add("AIRCOVER", "MASTER_FACT_AirCover", None, "AIRCOVER_SOURCE_ABSENTE", "INFO",
                 "Aucune table AirCover dediee disponible. Aucun AirCover ne peut modifier "
                 "automatiquement payout, commission ou net.")


def _groupe0c_ajustements_post_cloture(ctrl: _Ctrl, db_path) -> None:
    """Groupe 0C — corrections post-clôture tracées.

    Principe métier préservé : une période close n'est JAMAIS réécrite, le contrôle ne fait que
    constater. La saisie (`SAISIE_Ajustements_PostCloture.xlsx`) est vide et non migrée : le legacy
    émet alors ce constat INFO, repris à l'identique.
    """
    if _dataset_non_alimente(db_path, "ajustements_post_cloture"):
        ctrl.add("CLOTURE", "SAISIE_Ajustements_PostCloture", None,
                 "AJUSTEMENTS_POST_CLOTURE_SOURCE_ABSENTE", "INFO",
                 "Aucune saisie d'ajustement post-cloture disponible. Une correction future devra "
                 "etre append-only.")


def _groupe5_sources_vides(ctrl: _Ctrl, db_path, rslt_global: dict[str, float]) -> None:
    """Groupe 5 — sources métier non alimentées, documentées en INFO (vocabulaire inchangé).

    `HC_ZERO_SOURCES_VIDES` est le code unique du legacy pour « cette source n'est pas alimentée,
    donc ses contrôles ne sont pas représentatifs ». Émis par source, exactement comme le legacy.

    HH n'est PAS traité ici : le legacy l'émet dans le groupe 2 (`if SOURCES_VIDES["HH"]` y est
    suivi d'un `pass  # Deja traite en groupe 2` ici). Position conservée.
    """
    if _dataset_non_alimente(db_path, "charges"):
        ctrl.add("CHARGES", "MASTER_FACT_MAN_Charges", None, "HC_ZERO_SOURCES_VIDES", "INFO",
                 "MASTER_FACT_MAN_Charges vide. Controles CHARGE_LOGEMENT_SANS_LOGEMENT_ID / "
                 "CHARGE_PERSO_SANS_ASSOCIE non representatifs.",
                 commentaire="SAISIE_Charges_Flux.xlsx non peuplee.")
    if _dataset_non_alimente(db_path, "menages_declarations_internes"):
        ctrl.add("M04", "M04_MENAGES_PowerQuery", None, "HC_ZERO_SOURCES_VIDES", "INFO",
                 "M04_MENAGES_PowerQuery SOURCE_RAW vide. Controles M04 non representatifs. "
                 "MENAGE_INTERNE_CODE_IMPACT_NON_HC / ACHATS_DEJA_EN_SAISIE_CHARGES = 0 "
                 "(faux negatif).",
                 commentaire="Google Sheet Suivi menage non colle.")
    if _dataset_non_alimente(db_path, "acomptes_proprietaires"):
        ctrl.add("ACOMPTES", "MASTER_FACT_MAN_AcomptesProprietaires", None,
                 "HC_ZERO_SOURCES_VIDES", "INFO",
                 "MASTER_FACT_MAN_AcomptesProprietaires vide. Controle "
                 "ACOMPTE_NON_RATTACHE_FACTURE = 0.",
                 commentaire="Acomptes non encore saisis.")
    if _dataset_non_alimente(db_path, "ik_avantages"):
        ctrl.add("IK", "MASTER_FACT_MAN_IK_Avantages", None, "HC_ZERO_SOURCES_VIDES", "INFO",
                 "MASTER_FACT_MAN_IK_Avantages vide. Controle "
                 "MONTANT_RECUPERE_HH_NON_REPRIS_AVANTAGES = 0.",
                 commentaire="IK et avantages non encore saisis.")
    hc_val = rslt_global.get("HORS_COMPTA")
    ctrl.add("TRANSVERSE", "lot10_resultats", "GLOBAL", "HC_ZERO_SOURCES_VIDES", "INFO",
             f"HORS_COMPTA = {hc_val} EUR. M04/Charges/IK non alimentes. "
             "Resultat HC = 0 attendu dans l'etat actuel.",
             commentaire="Normal jusqu'a alimentation M04, Charges, IK, Acomptes.")


def _groupe_lot7c_suivi_associe(ctrl: _Ctrl, db_path) -> None:
    """Lot7C — suivi associé (avantages HR). Seul le CONTRÔLE nécessaire à Lot11 est porté.

    Le legacy active 7 contrôles intrinsèques + 2 cross-contrôles UNIQUEMENT si le suivi calculé
    (`MASTER_CALC_AVANTAGES`) est non vide ; sinon il émet `SUIVI_ASSOCIE_NON_GENERE` et diffère
    les cross-contrôles. Le suivi n'est pas généré (flags d'écriture réelle désactivés) et n'a
    aucune table SQLite : l'état porté est donc exactement celui-là. Les 7 contrôles intrinsèques
    portent sur des lignes de suivi inexistantes — les porter maintenant reviendrait à écrire des
    règles qui ne s'exécutent jamais ; ils seront portés avec la migration de Lot7.
    """
    if _dataset_non_alimente(db_path, "avantages_calcules"):
        ctrl.add("AVANTAGES_ASSOCIES", "MASTER_CALC_AVANTAGES", None, "SUIVI_ASSOCIE_NON_GENERE",
                 "INFO",
                 "MASTER_CALC_AVANTAGES vide/non regenere depuis SAISIE : cross-controles "
                 "AVANTAGE_ABSENT_DU_SUIVI et CHARGE_PAYEE_NON_REPRISE differes (prepares, non "
                 "actifs).",
                 commentaire="Lot7C : cross-controles actives quand le suivi est genere.")


def _groupe6f_menages_externes(ctrl: _Ctrl, db_path) -> None:
    """Groupe 6f — rapprochement ménages externes ↔ Hostaway, entièrement SQLite.

    Lot6 est migré : le comptage Hostaway vient de `menages_taches_enrichies` (0038, via
    `menages_reader.hostaway_comptage`, même règle de comptage que `lot6a`) et le volume facturé de
    `facture_lignes_menage` (0037/0039/0040, via `facture_lignes_menage_service`). La logique de
    classement (4 codes, mêmes seuils, mêmes libellés) est reprise telle quelle de
    `lot6c_menages_externes.py` — c'est elle qui produisait `VUE_ECART_HOSTAWAY`, que Lot11 se
    contentait d'agréger. Plus aucun classeur Lot6c au runtime.

    `type_ligne_menage_id in (TLM_001, TLM_002)` du legacy (ligne de ménage standard ou mensuelle,
    par opposition aux frais) correspond en SQLite à `type_ligne = MENAGE_EXTERNE`, déjà filtré par
    `lignes_externes_pour_reader()` : les frais non affectés y sont d'un autre type_ligne.
    """
    from app.services import menages_ecarts_service

    # Le calcul de l'écart vit dans `menages_ecarts_service` : le DÉTAIL affiché à l'écran et les
    # CONSTATS ci-dessous doivent répondre la même chose. Le dupliquer ici garantirait qu'ils
    # divergent un jour.
    par_code = menages_ecarts_service.logements_par_code(db_path=db_path)

    def _emettre(code: str, severity: str, gabarit: str, commentaire: str) -> None:
        logements = par_code.get(code, [])
        if not logements:
            return
        ctrl.add("MENAGES_EXT", "VUE_ECART_HOSTAWAY", None, code, severity,
                 gabarit.format(n=len(logements), logs=", ".join(logements)),
                 commentaire=commentaire)

    _emettre("MENAGE_EXTERNE_ECART_HOSTAWAY", "A_CONTROLER",
             "{n} logement(s) avec ecart volume facture vs Hostaway: {logs}.",
             "Ecart a valider (interne, hors HA, decalage mois ou saisie). Pas une accusation "
             "prestataire.")
    _emettre("MENAGE_EXTERNE_LOGEMENT_HORS_HA", "A_CONTROLER",
             "{n} logement(s) facture(s) absent(s) du comptage Hostaway: {logs}.",
             "Logement archive/hors HA/mapping. A valider, pas une erreur prestataire.")
    _emettre("MENAGE_HA_SANS_FACTURE_EXTERNE", "INFO",
             "{n} logement(s) avec menages Hostaway sans facture externe: {logs}.",
             "Probable menage interne / a croiser avec M04.")
    _emettre("MENAGE_EXTERNE_RAPPROCHE_HOSTAWAY", "INFO",
             "{n} logement(s) rapproche(s) facture = Hostaway.",
             "Volume mensuel coherent ; date jour non requise.")


def _groupe_menages_provenance(ctrl: _Ctrl, db_path) -> None:
    """Provenance de la source ménage (cache vs réseau Google Sheet), DEF-1.

    Ce contrôle ne lit AUCUN classeur : il lit le manifeste de provenance déposé par lot6b/lot6f
    (`_cache_google_sheet/last_resolution.json`) et l'existence des sorties. La règle est reprise
    telle quelle : sortie présente + provenance CACHE → `SOURCE_SHEET_CACHE_UTILISE` ; sortie
    présente + provenance absente/illisible/PENDING → `SOURCE_SHEET_PROVENANCE_INCOMPLETE` ;
    aucune sortie → aucun contrôle. Jamais d'assimilation silencieuse à RESEAU.

    Les « sorties » sont désormais les DATASETS SQLite (`menages_declarations_internes` pour lot6b,
    `menages_cout_complet` pour lot6f), plus les classeurs qu'ils remplacent.
    """
    import json

    base = cfg.PROJECT_ROOT
    cache_dir = base / "02_DONNEES_NORMALISEES" / "menages" / "_cache_google_sheet"
    manifeste = cache_dir / "last_resolution.json"
    etapes = {"lot6b": "menages_declarations_internes", "lot6f": "menages_cout_complet"}

    produced = {}
    conn = get_db(db_path)
    try:
        for etape, table in etapes.items():
            existe = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                                  (table,)).fetchone() is not None
            produced[etape] = bool(existe and conn.execute(
                f"SELECT COUNT(*) FROM {table}").fetchone()[0] > 0)
    finally:
        conn.close()

    steps: dict[str, Any] = {}
    if manifeste.exists():
        try:
            prov = json.loads(manifeste.read_text(encoding="utf-8"))
            if isinstance(prov.get("steps"), dict):
                steps = prov["steps"]
            elif prov.get("resolution_source") in ("CACHE", "RESEAU"):
                st = prov.get("script_or_step")
                steps = {st: prov} if st in etapes else {}
        except Exception:
            steps = {}

    cache, unproven, pending = [], [], []
    for etape in etapes:
        if not produced.get(etape):
            continue
        if (cache_dir / f"pending_resolution_{etape}.json").exists():
            pending.append(etape)
            continue
        src = str((steps.get(etape) or {}).get("resolution_source"))
        if src == "CACHE":
            cache.append(etape)
        elif src != "RESEAU":
            unproven.append(etape)

    if cache:
        detail = "; ".join(
            f"{k}=CACHE({(steps.get(k) or {}).get('cache_date_extraction_utc')},"
            f"{(steps.get(k) or {}).get('cache_age_h')}h)" for k in cache)
        ctrl.add("MENAGES", "GOOGLE_SHEET_M04", None, "SOURCE_SHEET_CACHE_UTILISE", "A_CONTROLER",
                 f"Sortie(s) ménage encore issue(s) du CACHE Google Sheet : {detail}. "
                 "Disparaît seulement quand TOUTES les étapes produites sont régénérées en RESEAU. "
                 "NE PAS clôturer le mois tant que ce contrôle est ouvert.",
                 commentaire="Rafraîchir la Google Sheet (réseau) puis relancer lot6b ET lot6f, "
                            "puis lot11.")
    if unproven or pending:
        msg = ""
        if unproven:
            msg += f"provenance absente/illisible/invalide : {unproven}. "
        if pending:
            msg += (f"transaction provenance NON finalisée (marqueur PENDING présent) : {pending} "
                    "— le manifeste ne prouve PAS que la sortie courante est RESEAU. ")
        ctrl.add("MENAGES", "GOOGLE_SHEET_M04", None, "SOURCE_SHEET_PROVENANCE_INCOMPLETE",
                 "A_CONTROLER",
                 f"Sortie(s) ménage présente(s) sans provenance prouvable : {msg}"
                 "Aucune assimilation silencieuse à RESEAU. NE PAS clôturer tant que ce contrôle "
                 "est ouvert.",
                 commentaire="Relancer l'étape concernée avec le réseau (lot6b/lot6f) pour "
                            "régénérer la provenance ; un marqueur PENDING résiduel se résout par "
                            "une exécution complète réussie.")


def _caisse_theorique(db_path, df_hh: list[dict]) -> list[dict]:
    """CAISSE_THEORIQUE (D-LOT11-05) — produite même si les sources sont vides.

    Réutilise le modèle de caisse SQLite existant (`operations_caisse`, service
    `operations_caisse_service`) : aucune comptabilité parallèle n'est créée. Les postes dont la
    source n'est pas migrée (HH, Charges, Acomptes) restent à 0 avec le statut du legacy
    `CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES` — un zéro déclaré non représentatif, jamais un zéro
    présenté comme un solde vérifié.
    """
    liquide_recupere_hh = sum(
        _num(r.get("montant_retenu")) for r in df_hh
        if _txt(r.get("mode_paiement_id")).upper() in ("ESPECES", "LIQUIDE", "CASH"))

    liquide_utilise_charges = 0.0
    charges_vides = _dataset_non_alimente(db_path, "charges")

    # `liquide_verse_proprietaires` et `liquide_affecte_acomptes` : le legacy les initialise à 0.0
    # et ne les alimente JAMAIS (aucune source ne les porte aujourd'hui). Repris tel quel — leur
    # inventer une dérivation depuis `operations_caisse` changerait une règle métier, et
    # `operations_caisse` (module Caisse applicatif) enregistre des encaissements/remboursements
    # associés, pas des reversements de liquide propriétaire. C'est ce modèle SQLite existant qui
    # portera ces postes le jour où ces flux y seront réellement enregistrés : aucune comptabilité
    # parallèle n'est créée ici.
    liquide_verse = 0.0
    liquide_acomptes = 0.0

    statut_hh = "CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES" if not df_hh else "OK"
    statut_chg = "CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES" if charges_vides else "OK"
    solde = liquide_recupere_hh - liquide_verse - liquide_utilise_charges - liquide_acomptes
    return [
        {"poste": "Liquide recupere (HH/direct)", "montant": round(liquide_recupere_hh, 2),
         "statut": statut_hh},
        {"poste": "Liquide verse proprietaires", "montant": round(-liquide_verse, 2),
         "statut": statut_hh},
        {"poste": "Liquide utilise charges", "montant": round(-liquide_utilise_charges, 2),
         "statut": statut_chg},
        {"poste": "Liquide affecte acomptes", "montant": round(-liquide_acomptes, 2),
         "statut": "CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES"},
        {"poste": "=== SOLDE THEORIQUE ===", "montant": round(solde, 2),
         "statut": "CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES"},
    ]


def _dashboard_mois(ctrl_rows: list[dict], cloture_rows: list[dict],
                    banque_dispo: bool) -> list[dict]:
    from lib_controls import facture_control_counts

    mois_avec_ctrl = {_txt(r.get("mois")) for r in ctrl_rows if r.get("mois")}
    mois_banque = sorted({_txt(r.get("mois"))[:7] for r in cloture_rows if r.get("mois")}) \
        if banque_dispo else []
    mois_all = sorted(mois_avec_ctrl | set(mois_banque) | {"TRANSVERSE"})

    cloture_par_mois = {_txt(r.get("mois"))[:7]: r for r in cloture_rows} if banque_dispo else {}

    dashboard = []
    for m in mois_all:
        if m == "TRANSVERSE":
            subset = [r for r in ctrl_rows if not r.get("mois")]
            nb_bl = sum(1 for r in subset if r.get("severity") == "BLOQUANT"
                       and r.get("impact_facture") == "BLOQUANT_FACTURE")
            nb_ac = sum(1 for r in subset if r.get("severity") == "A_CONTROLER"
                       and r.get("impact_facture") != "NON_BLOQUANT_FACTURE")
            nb_inf = sum(1 for r in subset if r.get("severity") == "INFO")
        else:
            counts = facture_control_counts(ctrl_rows, m)
            nb_bl, nb_ac, nb_inf = counts["nb_bloquants"], counts["nb_a_controler"], counts["nb_info"]

        row_clo = cloture_par_mois.get(m)
        statut_clo = "OUVERT"
        a_enregistrement = row_clo is not None
        if row_clo is not None:
            from lib_cloture import normalise_cloture_status
            statut_clo = normalise_cloture_status(row_clo.get("statut_mois", "OUVERT")) or "OUVERT"
        banque_cloturee = statut_clo == "CLOTURE"

        if m == "TRANSVERSE":
            cloture_possible = "NON_DONNEES_INCOMPLETES"
            facturation_lot12 = "NON_SOUS_RESERVE_A_CONTROLER" if (nb_bl + nb_ac) > 0 \
                else "NON_DONNEES_INCOMPLETES"
        elif nb_bl > 0:
            cloture_possible, facturation_lot12 = "NON", "NON_BLOQUANT_OUVERT"
        elif nb_ac > 0:
            cloture_possible, facturation_lot12 = "NON_A_CONTROLER_OUVERTS", "NON_SOUS_RESERVE_A_CONTROLER"
        elif not banque_cloturee:
            cloture_possible, facturation_lot12 = "NON_CLOTURE_INCOMPLETE", "NON_CLOTURE_INCOMPLETE"
        else:
            cloture_possible, facturation_lot12 = "OUI", "OUI"

        dashboard.append({
            "mois": m, "nb_bloquants_ouverts": nb_bl, "nb_a_controler_ouverts": nb_ac,
            "nb_info": nb_inf, "statut_mois_banque": statut_clo,
            "cloture_possible": cloture_possible, "facturation_lot12_ok": facturation_lot12,
        })
    return dashboard


def construire(*, db_path=None, run_id: str | None = None) -> dict[str, Any]:
    """Recalcule les contrôles Lot11 depuis les chaînes SQLite déjà migrées et écrit le résultat.

    Jamais d'exception qui laisse la base à moitié écrite : tout le calcul se fait en mémoire, et
    la transaction DELETE+INSERT n'est ouverte qu'une fois le calcul terminé. Un run raté (exception
    pendant le calcul) enregistre son échec dans `controles_lot11_runs` SANS toucher aux constats
    existants.
    """
    import uuid

    rid = run_id or f"L11-{uuid.uuid4().hex[:12]}"
    try:
        from app.services import hostaway_raw_service as raw

        df_res = res_ds.lignes(res_ds.ETAPE_RESOLUES, db_path=db_path)
        df_pay = raw.payouts(db_path=db_path)

        conn = get_db(db_path)
        try:
            # `flux_unifies` est un instantané unique (DELETE+INSERT à chaque construction, comme
            # `controles_lot11_constats` — aucune colonne `actif` sur `flux_unifies_runs`) : toute
            # la table EST le run courant, pas de filtre à appliquer.
            df_flux = [dict(r) for r in conn.execute("SELECT * FROM flux_unifies")]

            def _run_actif(runs_table):
                r = conn.execute(f"SELECT run_id FROM {runs_table} WHERE actif = 1").fetchone()
                return r[0] if r else None

            lot10_run = _run_actif("lot10_runs")
            df_com, df_com_ac, exploit_rows, reglement_rows, rslt_rows = [], [], [], [], []
            if lot10_run:
                df_com = [dict(r) for r in conn.execute(
                    "SELECT * FROM lot10_commissions WHERE run_id = ?", (lot10_run,))]
                df_com_ac = [dict(r) for r in conn.execute(
                    "SELECT * FROM lot10_commissions_a_controler WHERE run_id = ?", (lot10_run,))]
                exploit_rows = [dict(r) for r in conn.execute(
                    "SELECT * FROM lot10_net_exploitation WHERE run_id = ?", (lot10_run,))]
                reglement_rows = [dict(r) for r in conn.execute(
                    "SELECT * FROM lot10_net_reglement WHERE run_id = ?", (lot10_run,))]
                rslt_rows = [dict(r) for r in conn.execute(
                    "SELECT vision, resultat FROM lot10_resultats WHERE run_id = ?", (lot10_run,))]
        finally:
            conn.close()

        rslt_global: dict[str, float] = {}
        for r in rslt_rows:
            v = r.get("vision")
            rslt_global[v] = rslt_global.get(v, 0.0) + _num(r.get("resultat"))
        rslt_global = {k: round(v, 2) for k, v in rslt_global.items()}

        df_ha_ano = raw.anomalies(db_path=db_path)
        ref_log = ref_setup_repo.lire_onglet("REF_Logements", db_path=db_path)
        ref_prop = ref_setup_repo.lire_onglet("REF_Proprietaires", db_path=db_path)
        ref_map = ref_setup_repo.lire_onglet("REF_Mapping_Logements", db_path=db_path)

        mouvements = banque_vues_service.mouvements_normalises(db_path=db_path)
        cloture_rows = ref_setup_repo.lire_onglet("REF_Cloture_Mensuelle", db_path=db_path) \
            if _ref_cloture_disponible(db_path) else []

        # Réservations hors Hostaway : Lot4bis les fusionne dans la table commune, où elles
        # portent un `reservation_hh_id`. C'est le dataset HH — plus aucun classeur à ouvrir pour
        # savoir si la source est alimentée.
        df_hh = [r for r in df_res if _txt(r.get("reservation_hh_id")).strip()]

        # Ordre STRICTEMENT celui du legacy (`lot11_controles_coherence.main`) : le MASTER produit
        # est comparé ligne à ligne en parité, et l'ordre des groupes est l'ordre des constats.
        ctrl = _Ctrl()
        _groupe0_ref_historiques(ctrl, df_res, db_path)
        _groupe0b_aircover_airbnb(ctrl, db_path)
        _groupe0c_ajustements_post_cloture(ctrl, db_path)
        _groupe1_pk_doublons(ctrl, df_flux, df_res, df_pay, df_com)
        _groupe2_jointures(ctrl, df_flux, df_res, df_pay, df_hh)
        _groupe3_exploitation_reglement(ctrl, exploit_rows, reglement_rows)
        _groupe4_commissions(ctrl, df_com, rslt_global)
        _groupe5_sources_vides(ctrl, db_path, rslt_global)
        _groupe_lot7c_suivi_associe(ctrl, db_path)
        _groupe6_referentiel_hostaway(ctrl, ref_log, ref_map, df_flux, df_res, df_ha_ano, df_com_ac)
        _groupe6f_menages_externes(ctrl, db_path)
        _groupe7_referentiel_proprietaires(ctrl, ref_prop)
        banque_dispo = _groupe8_banque(ctrl, mouvements, cloture_rows, df_flux)
        _groupe_menages_provenance(ctrl, db_path)

        dashboard = _dashboard_mois(ctrl.rows, cloture_rows, banque_dispo)
        caisse = _caisse_theorique(db_path, df_hh)

        n_bloquant = sum(1 for r in ctrl.rows if r["severity"] == "BLOQUANT")
        n_ac = sum(1 for r in ctrl.rows if r["severity"] == "A_CONTROLER")
        n_info = sum(1 for r in ctrl.rows if r["severity"] == "INFO")

        conn = get_db(db_path)
        try:
            conn.execute("DELETE FROM controles_lot11_constats")
            cols = ("ctrl_pk", "source_module", "source_table", "source_pk", "code_controle",
                    "severity", "message", "impact_facture", "statut_resolution", "commentaire")
            conn.executemany(
                f"INSERT INTO controles_lot11_constats ({', '.join(cols)}) "
                f"VALUES ({', '.join(['?'] * len(cols))})",
                [tuple(r.get(c) for c in cols) for r in ctrl.rows])

            conn.execute("DELETE FROM controles_lot11_constats_champs")
            champs_cols = ("ctrl_pk", "mois", "logement_id", "proprietaire_id", "reservation_id",
                          "document_id", "date_detection")
            conn.executemany(
                f"INSERT OR IGNORE INTO controles_lot11_constats_champs "
                f"({', '.join(champs_cols)}) VALUES ({', '.join(['?'] * len(champs_cols))})",
                [tuple(r.get(c) for c in champs_cols) for r in ctrl.rows])

            conn.execute("DELETE FROM controles_lot11_dashboard_mois")
            dash_cols = ("run_id", "mois", "nb_bloquants_ouverts", "nb_a_controler_ouverts",
                        "nb_info", "statut_mois_banque", "cloture_possible", "facturation_lot12_ok")
            conn.executemany(
                f"INSERT INTO controles_lot11_dashboard_mois ({', '.join(dash_cols)}) "
                f"VALUES ({', '.join(['?'] * len(dash_cols))})",
                [tuple({**d, "run_id": rid}.get(c) for c in dash_cols) for d in dashboard])

            conn.execute(
                "INSERT INTO controles_lot11_runs (run_id, source, statut, nb_constats, "
                "nb_bloquants, nb_a_controler, nb_info) VALUES (?,?,?,?,?,?,?)",
                (rid, SOURCE_SQLITE_NATIF, STATUT_SUCCES, len(ctrl.rows), n_bloquant, n_ac, n_info))
            conn.commit()
        finally:
            conn.close()

        return {"ok": True, "run_id": rid, "nb_constats": len(ctrl.rows), "nb_bloquants": n_bloquant,
                "nb_a_controler": n_ac, "nb_info": n_info, "dashboard": dashboard,
                "caisse_theorique": caisse}
    except Exception as exc:
        conn = get_db(db_path)
        try:
            conn.execute(
                "INSERT INTO controles_lot11_runs (run_id, source, statut, erreur_code, "
                "erreur_message) VALUES (?,?,?,?,?)",
                (rid, SOURCE_SQLITE_NATIF, STATUT_ECHEC, type(exc).__name__, str(exc)[:500]))
            conn.commit()
        finally:
            conn.close()
        return {"ok": False, "run_id": rid, "code": "ERREUR_CALCUL", "message": str(exc)}


def _ref_cloture_disponible(db_path) -> bool:
    conn = get_db(db_path)
    try:
        return conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='ref_cloture_mensuelle'"
        ).fetchone() is not None
    finally:
        conn.close()
