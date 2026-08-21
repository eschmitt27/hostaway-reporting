"""Lot9 — flux économique unifié, SQLite (`flux_unifies`, migration 0043).

Reconstruit ce que `02_TRAVAIL/lot9_construire_flux.py` produisait dans `MASTER_CALC_Flux.xlsx`,
mais lu directement depuis les sources SQLite déjà migrées, sans détour par un classeur :

    RES → `reservations_adaptateur_moteur._vue_flux` + `reservations_dataset_service` (0034)
    MEN → `facture_lignes_menage_service.lignes_externes_pour_reader` (0037/0040)
    BNQ → `banque_vues_service.mouvements_normalises` (0032/0033)
    GPM → `menages_cout_complet` (0038), lu directement (table de résultat, pas de logique à réécrire)
    CHG → `charges_reader.read_charges` (table `charges`, migration 0052 — plus aucun classeur ;
          le lecteur existant est réutilisé tel quel, sa logique n'est pas réimplémentée ici)

Chaque module a déjà sa logique métier ailleurs (adaptateur/service) : ce module ne fait QUE la
fusion au format `flux_unifies`, il ne réimplémente aucun calcul de classification/résolution.

`MASTER_CALC_Flux.xlsx` reste `LEGACY_PARITE_TEMPORAIRE` — outil de comparaison, plus une source.

IDENTITÉ STABLE : le legacy dérivait `flux_id` d'un compteur positionnel par module
(`FLUX-{mois}-{code_impact}-{module}-{n:04d}`), qui change si une ligne apparaît/disparaît avant une
autre. Ici `flux_id` est dérivé de `ROW_HASH`, lui-même dérivé de `source_pk` (clé métier stable de
chaque module — `reservation_calc_id`, `ligne_id_opaque`, `mouvement_id`, `charge_id`, ou
`mois|logement_id|intervenant_id` pour GPM) : une même ligne source garde toujours le même `flux_id`.
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from app.db.connection import get_db
from app.readers import charges_reader
from app.services import banque_vues_service, facture_lignes_menage_service
from app.services import reservations_adaptateur_moteur as res_moteur
from app.services import reservations_dataset_service as res_ds

SOURCE_MODULE = "lot9"

TYPE_FLUX_RES = "TYPE_FLUX_017"
TYPE_FLUX_MEN = "TYPE_FLUX_014"
TYPE_FLUX_BNQ = "TYPE_FLUX_016"
TYPE_FLUX_GPM_STD = "TYPE_FLUX_019"
TYPE_FLUX_GPM_ECART = "TYPE_FLUX_018"

_IMPACT_FLAGS = {
    "IC": ("OUI", "OUI", "NON"),
    "HC": ("OUI", "NON", "OUI"),
    "HR": ("NON", "NON", "NON"),
}

# Statuts facture (`factures_service.STATUTS`) considérés « validés » pour l'injection en flux —
# équivalent du filtre legacy `statut_controle == 'VALIDE'` sur le MASTER Excel, transposé aux
# statuts réels du workflow facture (une facture BROUILLON/A_CONTROLER/LITIGE/ANNULEE n'entre pas).
_STATUTS_FACTURE_VALIDES = {"VALIDEE", "PARTIELLEMENT_REGLEE", "REGLEE"}


def _impact_flags(code_impact: str) -> tuple[str, str, str]:
    return _IMPACT_FLAGS.get(code_impact, ("NON", "NON", "NON"))


def _row_hash(source_table: str, source_pk: Any, type_flux_id: str, sens: str) -> str:
    pk = "" if source_pk is None else str(source_pk)
    s = f"{SOURCE_MODULE}|{source_table}|{pk}|{type_flux_id}|{sens}"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def _mois_premier_jour(mois: str | None) -> str | None:
    if mois and len(mois) == 7:
        return f"{mois}-01"
    return mois


def _construire_flux(*, module_code: str, source_table: str, source_pk: Any, date_flux, mois,
                     logement_id, proprietaire_id, associe_id, type_flux_id: str, sens: str,
                     montant, code_impact: str, statut_controle, commentaire,
                     niveau_anomalie: str = "INFO", code_anomalie: str | None = None,
                     seen: set, doublons: list) -> dict[str, Any] | None:
    key = (SOURCE_MODULE, source_table, "" if source_pk is None else str(source_pk),
          type_flux_id, sens)
    if key in seen:
        doublons.append(key)
        return None
    seen.add(key)

    row_hash = _row_hash(source_table, source_pk, type_flux_id, sens)
    reel, compta, hors = _impact_flags(code_impact)
    return {
        "flux_id": f"FLUX-{code_impact}-{module_code}-{row_hash}",
        "row_hash": row_hash,
        "source_module": SOURCE_MODULE,
        "source_table": source_table,
        "source_pk": None if source_pk is None else str(source_pk),
        "date_flux": date_flux,
        "mois": mois,
        "logement_id": logement_id,
        "proprietaire_id": proprietaire_id,
        "associe_id": associe_id,
        "type_flux_id": type_flux_id,
        "sens": sens,
        "montant": abs(float(montant)) if montant is not None else None,
        "code_impact": code_impact,
        "inclure_resultat_reel": reel,
        "inclure_resultat_comptable": compta,
        "inclure_resultat_hors_compta": hors,
        "statut_controle": statut_controle,
        "niveau_anomalie": niveau_anomalie,
        "code_anomalie": code_anomalie,
        "commentaire": commentaire,
        "date_integration": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _module_res(seen, doublons, db_path) -> list[dict[str, Any]]:
    lignes = res_ds.lignes(res_ds.ETAPE_RESOLUES, db_path=db_path)
    vue_flux = res_moteur._vue_flux(lignes)
    out = []
    for r in sorted(vue_flux, key=lambda r: r.get("reservation_calc_id") or ""):
        date_flux = r.get("date_arrivee") or _mois_premier_jour(r.get("mois"))
        f = _construire_flux(
            module_code="RES", source_table="reservations_resolues",
            source_pk=r.get("reservation_calc_id"), date_flux=date_flux, mois=r.get("mois"),
            logement_id=r.get("logement_id"), proprietaire_id=r.get("proprietaire_id"),
            associe_id=None, type_flux_id=TYPE_FLUX_RES, sens="PRODUIT",
            montant=r.get("montant_retenu"), code_impact=r.get("code_impact") or "IC",
            statut_controle=r.get("statut_controle") or "A_CONTROLER",
            commentaire=r.get("commentaire"), seen=seen, doublons=doublons)
        if f:
            out.append(f)
    return out


def _module_men(seen, doublons, db_path) -> list[dict[str, Any]]:
    lignes = facture_lignes_menage_service.lignes_externes_pour_reader(db_path=db_path)
    # `montant_ligne_ttc` en (None, 0) : ligne sans donnée exploitable (ex. anomalie d'extraction
    # PDF). Le legacy (`lot6c_menages_externes.py`) exclut ces lignes du flux (statut_controle
    # per-ligne A_CONTROLER, jamais VALIDE) ; `facture_lignes_menage` n'a pas cette notion par ligne
    # (seul le statut de la FACTURE existe), d'où l'exclusion explicite ici plutôt qu'un flux à 0€
    # qui gonflerait le COUNT sans le SUM — bug de port trouvé lors de la parité Lot9 (mission
    # Ménages/Lot9, baseline MEN 12 lignes/2 381,00€ : 13 lignes réelles, 1 à 0€ exclue en legacy).
    valides = [r for r in lignes
              if r.get("statut_controle") in _STATUTS_FACTURE_VALIDES
              and r.get("montant_ligne_ttc") not in (None, 0)]
    out = []
    for r in sorted(valides, key=lambda r: r.get("menage_externe_id") or ""):
        date_flux = r.get("date_facture") or r.get("date_menage") or _mois_premier_jour(r.get("mois"))
        f = _construire_flux(
            module_code="MEN", source_table="facture_lignes_menage",
            source_pk=r.get("menage_externe_id"), date_flux=date_flux, mois=r.get("mois"),
            logement_id=r.get("logement_id"), proprietaire_id=None, associe_id=None,
            type_flux_id=TYPE_FLUX_MEN, sens="CHARGE", montant=r.get("montant_ligne_ttc"),
            code_impact="IC", statut_controle="VALIDE",
            commentaire="Ménage externe validé (facture)", seen=seen, doublons=doublons)
        if f:
            out.append(f)
    return out


def _module_bnq(seen, doublons, db_path) -> list[dict[str, Any]]:
    lignes = banque_vues_service.mouvements_normalises(db_path=db_path)
    valides = [r for r in lignes
              if r.get("type_flux_id") == TYPE_FLUX_BNQ and r.get("statut_controle") == "VALIDE"]
    out = []
    for r in sorted(valides, key=lambda r: r.get("mouvement_id") or ""):
        date_op = r.get("date_operation")
        mois = str(date_op)[:7] if date_op else None
        f = _construire_flux(
            module_code="BNQ", source_table="banque_mouvements",
            source_pk=r.get("mouvement_id"), date_flux=date_op, mois=mois,
            logement_id=None, proprietaire_id=None, associe_id=None,
            type_flux_id=TYPE_FLUX_BNQ, sens="CHARGE", montant=r.get("montant"),
            code_impact="IC", statut_controle="VALIDE",
            commentaire="Frais bancaires validés", seen=seen, doublons=doublons)
        if f:
            out.append(f)
    return out


def _module_chg(seen, doublons, db_path) -> list[dict[str, Any]]:
    # `db_path` est propagé : les charges viennent désormais de SQLite (0052), et un module qui
    # lirait la base par défaut ferait entrer les données d'une AUTRE instance dans ce calcul.
    rows = charges_reader.read_charges(db_path=db_path)
    valides = [r for r in rows if r.get("statut_controle") == "VALIDE"]
    out = []
    for r in sorted(valides, key=lambda r: str(r.get("charge_id") or "")):
        date_charge = r.get("date_charge")
        mois = r.get("mois") or (str(date_charge)[:7] if date_charge else None)
        date_flux = date_charge or _mois_premier_jour(mois)
        f = _construire_flux(
            module_code="CHG", source_table="MASTER_FACT_MAN_Charges",
            source_pk=r.get("charge_id"), date_flux=date_flux, mois=mois,
            logement_id=r.get("logement_id"), proprietaire_id=r.get("proprietaire_id"),
            associe_id=r.get("associe_id"), type_flux_id=r.get("type_flux_id"),
            sens=r.get("sens") or r.get("sens_flux") or "CHARGE", montant=r.get("montant"),
            code_impact=r.get("code_impact") or "IC", statut_controle="VALIDE",
            commentaire=r.get("commentaire"), seen=seen, doublons=doublons)
        if f:
            out.append(f)
    return out


def _module_gpm(seen, doublons, db_path) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT mois, logement_id, proprietaire_id, intervenant_id, cout_standard_total, "
            "ecart_vs_standard_total, statut_ecart FROM menages_cout_complet").fetchall()]
    finally:
        conn.close()

    out = []
    for r in sorted(rows, key=lambda x: (str(x.get("mois")), str(x.get("logement_id")),
                                         str(x.get("intervenant_id")))):
        mois = r.get("mois")
        pk = f"{mois}|{r.get('logement_id')}|{r.get('intervenant_id')}"
        std = r.get("cout_standard_total")
        if std is not None and float(std) != 0:
            f = _construire_flux(
                module_code="GPM", source_table="menages_cout_complet", source_pk=pk,
                date_flux=_mois_premier_jour(mois), mois=mois, logement_id=r.get("logement_id"),
                proprietaire_id=r.get("proprietaire_id"), associe_id=None,
                type_flux_id=TYPE_FLUX_GPM_STD, sens="CHARGE", montant=std, code_impact="HC",
                statut_controle="VALIDE",
                commentaire="Ménage standard analytique (coût standard par appartement)",
                seen=seen, doublons=doublons)
            if f:
                out.append(f)
        ec = r.get("ecart_vs_standard_total")
        if ec is not None and float(ec) != 0:
            ec = float(ec)
            f = _construire_flux(
                module_code="GPM", source_table="menages_cout_complet", source_pk=pk,
                date_flux=_mois_premier_jour(mois), mois=mois, logement_id=r.get("logement_id"),
                proprietaire_id=r.get("proprietaire_id"), associe_id=None,
                type_flux_id=TYPE_FLUX_GPM_ECART, sens="PRODUIT" if ec > 0 else "CHARGE",
                montant=abs(ec), code_impact="HC", statut_controle="VALIDE",
                commentaire=f"Écart analytique ménage {r.get('statut_ecart') or ''} "
                           f"(standard - coût complet)", seen=seen, doublons=doublons)
            if f:
                out.append(f)
    return out


CODE_BANQUE_INTERDITS = {"libelle", "libelle_brut", "compte_id", "iban"}


def construire(*, db_path=None) -> dict[str, Any]:
    """Recalcule l'intégralité de `flux_unifies` depuis les sources SQLite/Excel courantes.

    Remplacement intégral (DELETE + INSERT), comme `controles_lot11_service.construire` — un
    instantané du dernier calcul, pas un historique.
    """
    seen: set = set()
    doublons: list = []

    res_rows = _module_res(seen, doublons, db_path)
    men_rows = _module_men(seen, doublons, db_path)
    bnq_rows = _module_bnq(seen, doublons, db_path)
    chg_rows = _module_chg(seen, doublons, db_path)
    gpm_rows = _module_gpm(seen, doublons, db_path)

    flux_rows = res_rows + men_rows + bnq_rows + chg_rows + gpm_rows

    neg = [r for r in flux_rows if r.get("montant") is not None and r["montant"] < 0]
    if neg:
        return {"ok": False, "code": "CTR_9_004_MONTANT_NEGATIF", "nb_montants_negatifs": len(neg)}

    ids = [r["flux_id"] for r in flux_rows]
    if len(ids) != len(set(ids)):
        return {"ok": False, "code": "CTR_9_007_FLUX_ID_DUPLIQUE"}

    conn = get_db(db_path)
    try:
        run_id = "RUN9-" + uuid.uuid4().hex[:12].upper()
        cols = ("flux_id", "row_hash", "source_module", "source_table", "source_pk", "date_flux",
               "mois", "logement_id", "proprietaire_id", "associe_id", "type_flux_id", "sens",
               "montant", "code_impact", "inclure_resultat_reel", "inclure_resultat_comptable",
               "inclure_resultat_hors_compta", "statut_controle", "niveau_anomalie",
               "code_anomalie", "commentaire", "date_integration")
        conn.execute("DELETE FROM flux_unifies")
        conn.executemany(
            f"INSERT INTO flux_unifies ({', '.join(cols)}, run_id) "
            f"VALUES ({', '.join(['?'] * len(cols))}, ?)",
            [tuple(r.get(c) for c in cols) + (run_id,) for r in flux_rows])
        conn.execute(
            "INSERT INTO flux_unifies_runs (run_id, nb_res, nb_men, nb_bnq, nb_chg, nb_gpm, "
            "nb_total, nb_doublons, statut) VALUES (?,?,?,?,?,?,?,?,?)",
            (run_id, len(res_rows), len(men_rows), len(bnq_rows), len(chg_rows), len(gpm_rows),
             len(flux_rows), len(doublons), "OK"))
        conn.commit()
    finally:
        conn.close()

    return {"ok": True, "run_id": run_id, "nb_res": len(res_rows), "nb_men": len(men_rows),
           "nb_bnq": len(bnq_rows), "nb_chg": len(chg_rows), "nb_gpm": len(gpm_rows),
           "nb_total": len(flux_rows), "nb_doublons": len(doublons)}


def lire(*, mois: str = "", db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        if mois:
            rows = conn.execute(
                "SELECT * FROM flux_unifies WHERE mois = ? ORDER BY flux_id", (mois,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM flux_unifies ORDER BY flux_id").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
