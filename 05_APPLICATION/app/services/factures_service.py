"""Factures fournisseurs — dette et pièce fournisseur (module Fournisseurs/Factures/Règlements).

Quatre objets distincts, jamais confondus (cf. `32_AUDIT_FOURNISSEURS_FACTURES.md`) :
facture (ici) / charge (Excel, parcours Charges existant) / règlement
(`reglements_service.py`) / rapprochement (`banques_rapprochement_service.py`).

Ce service **ne crée jamais de charge** : il enregistre au mieux le lien vers une charge produite
par le parcours Charges existant, sans contourner ses contrôles.

`montant_regle` et `solde_restant` ne sont jamais stockés : ils sont recalculés depuis les
répartitions de règlements, pour qu'ils ne puissent pas dériver de la vérité.
"""
from __future__ import annotations

import hashlib
import sqlite3
import uuid
from datetime import date, datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import facture_lignes_menage_service as flm

ST_BROUILLON = "BROUILLON"
ST_A_CONTROLER = "A_CONTROLER"
ST_VALIDEE = "VALIDEE"
ST_PARTIELLEMENT_REGLEE = "PARTIELLEMENT_REGLEE"
ST_REGLEE = "REGLEE"
ST_ANNULEE = "ANNULEE"
ST_LITIGE = "LITIGE"

STATUTS = [ST_BROUILLON, ST_A_CONTROLER, ST_VALIDEE, ST_PARTIELLEMENT_REGLEE, ST_REGLEE,
           ST_ANNULEE, ST_LITIGE]
# Statuts « vivants » : la facture représente encore une dette potentielle.
STATUTS_OUVERTS = (ST_A_CONTROLER, ST_VALIDEE, ST_PARTIELLEMENT_REGLEE, ST_LITIGE)

E_FLAGS = "E_FLAGS_DESACTIVES"
E_FOURNISSEUR_MANQUANT = "V01_FOURNISSEUR_MANQUANT"
E_FOURNISSEUR_ARCHIVE = "V02_FOURNISSEUR_ARCHIVE"
E_REF_MANQUANTE = "V03_REFERENCE_MANQUANTE"
E_MONTANT_INVALIDE = "V04_MONTANT_INVALIDE"
E_DOUBLON_CERTAIN = "V05_DOUBLON_CERTAIN"
E_TVA_INCOHERENTE = "V06_HT_PLUS_TVA_DIFFERENT_TTC"
E_DATE_INCOHERENTE = "V07_ECHEANCE_ANTERIEURE_FACTURE"
E_DATE_INVALIDE = "V10_DATE_CALENDAIRE_INVALIDE"
E_INTROUVABLE = "E01_FACTURE_INTROUVABLE"
E_STATUT = "E02_TRANSITION_INTERDITE"
E_CHARGE_DEJA_LIEE = "E03_CHARGE_DEJA_LIEE"
E_LIGNE_CHARGE_MANQUANTE = "V08_LIGNE_CHARGE_MANQUANTE"
E_LIGNE_MONTANT_INVALIDE = "V09_LIGNE_MONTANT_INVALIDE"
E_LIGNE_FACTURE_MONO_CHARGE = "E04_FACTURE_DEJA_MONO_CHARGE"
#: §28 — la somme des lignes doit reconstituer le total du document pour valider la facture.
E_ECART_LIGNES_TOTAL = "V11_ECART_LIGNES_TOTAL_DOCUMENT"
#: §15 — une ligne dont la nature n'a pas été tranchée par un humain interdit la validation.
E_LIGNE_NON_CLASSEE = "V12_LIGNE_SANS_NATURE"
#: §15 — une ligne qui compte pour un ménage sans logement désigné interdit la validation.
E_LIGNE_SANS_LOGEMENT = "V13_LIGNE_MENAGE_SANS_LOGEMENT"

MESSAGES = {
    E_FLAGS: "Écriture désactivée sur cette installation : l'enregistrement est impossible.",
    E_FOURNISSEUR_MANQUANT: "Le fournisseur est obligatoire.",
    E_FOURNISSEUR_ARCHIVE: "Ce fournisseur est archivé : il ne peut plus recevoir de nouvelle facture.",
    E_REF_MANQUANTE: "La référence de facture est obligatoire.",
    E_MONTANT_INVALIDE: "Le montant TTC doit être un nombre différent de zéro.",
    E_DOUBLON_CERTAIN: "Une facture avec cette référence existe déjà pour ce fournisseur.",
    E_TVA_INCOHERENTE: "Montant HT + TVA ne correspond pas au montant TTC.",
    E_DATE_INCOHERENTE: "La date d'échéance est antérieure à la date de facture.",
    E_DATE_INVALIDE: "Date de facture ou d'échéance invalide (calendaire impossible).",
    E_INTROUVABLE: "Facture introuvable.",
    E_STATUT: "Transition de statut interdite.",
    E_CHARGE_DEJA_LIEE: "Cette charge est déjà rattachée à une autre facture.",
    E_LIGNE_CHARGE_MANQUANTE: "La charge est obligatoire pour ajouter une ligne.",
    E_LIGNE_MONTANT_INVALIDE: "Le montant TTC de la ligne doit être un nombre différent de zéro.",
    E_LIGNE_FACTURE_MONO_CHARGE: "Cette facture porte déjà une charge unique (charge_id) : "
                                  "utiliser soit charge_id, soit des lignes, jamais les deux.",
}

# Transitions autorisées. Une facture VALIDEE n'est jamais supprimée : elle est ANNULEE (tracée).
TRANSITIONS: dict[str, set[str]] = {
    ST_BROUILLON: {ST_A_CONTROLER, ST_VALIDEE, ST_ANNULEE},
    ST_A_CONTROLER: {ST_VALIDEE, ST_LITIGE, ST_ANNULEE},
    # `VALIDEE -> A_CONTROLER` : rouvrir le contrôle d'une facture. Autorisé UNIQUEMENT tant que
    # rien d'irréversible n'en découle — aucune écriture d'achat, aucune dette, aucun règlement
    # (vérifié par `rouvrir_controle`, pas par cette table). Sans cette transition, une facture
    # validée à tort restait validée pour toujours : c'est exactement la situation trouvée sur les
    # deux factures PDF réelles, validées alors que leurs lignes ne reconstituaient pas leur total.
    ST_VALIDEE: {ST_A_CONTROLER, ST_PARTIELLEMENT_REGLEE, ST_REGLEE, ST_LITIGE, ST_ANNULEE},
    ST_PARTIELLEMENT_REGLEE: {ST_REGLEE, ST_LITIGE, ST_ANNULEE},
    ST_REGLEE: {ST_LITIGE},
    ST_LITIGE: {ST_VALIDEE, ST_PARTIELLEMENT_REGLEE, ST_REGLEE, ST_ANNULEE},
    ST_ANNULEE: set(),
}


# ── Niveau d'écriture exigé, par STATUT VISÉ (cf. `config.ECRITURE_OPERATIONNELLE_ENABLED`) ────
# Le niveau ne dépend pas de la FONCTION appelée mais du statut VISÉ : recevoir, annuler ou mettre
# en litige une facture reste la gestion d'un document ; la valider ou la marquer réglée l'engage
# en comptabilité. Découper par fonction aurait laissé passer `creer(statut="VALIDEE")` — une
# facture validée d'emblée, sans aucun contrôle humain, par le simple dépôt d'un PDF.
STATUTS_OPERATIONNELS: set[str] = {ST_BROUILLON, ST_A_CONTROLER, ST_LITIGE, ST_ANNULEE}
STATUTS_COMPTABLES: set[str] = {ST_VALIDEE, ST_PARTIELLEMENT_REGLEE, ST_REGLEE}


def _flags_actifs() -> bool:
    """NIVEAU B — écriture comptable / financière validée (double verrou conservé)."""
    return bool(cfg.FACTURES_REAL_WRITE_ENABLED and cfg.FACTURES_REAL_WRITE_CONFIRMATION_ENABLED)


def _niveau_operationnel_actif() -> bool:
    """NIVEAU A — enregistrement d'un document de travail, hors comptabilité."""
    return bool(getattr(cfg, "ECRITURE_OPERATIONNELLE_ENABLED", False))


def _niveau_requis_ok(statut: str) -> bool:
    """Niveau exigé pour amener une facture à `statut`. Un statut inconnu est traité comme
    comptable : à défaut de certitude, c'est le verrou le plus strict qui s'applique."""
    if statut in STATUTS_OPERATIONNELS:
        return _niveau_operationnel_actif()
    return _flags_actifs()


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _nombre(v: Any) -> float | None:
    if v is None or _txt(v) == "":
        return None
    try:
        return float(str(v).replace(",", ".").replace(" ", ""))
    except (TypeError, ValueError):
        return None


def empreinte(fournisseur: str, ref: str, montant_ttc: Any, date_facture: str) -> str:
    base = f"{fournisseur}|{ref.upper()}|{montant_ttc}|{date_facture}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


def valider(form: dict[str, Any], db_path=None, fournisseur_actif: bool | None = None,
            numero_reutilise_autorise: bool = False) -> list[dict[str, str]]:
    """Contrôles métier AVANT écriture. Liste vide = valide."""
    erreurs: list[dict[str, str]] = []

    def err(code: str, detail: str = ""):
        erreurs.append({"code": code, "message": MESSAGES.get(code, code), "detail": detail})

    frs = _txt(form.get("fournisseur_id_opaque"))
    if not frs:
        err(E_FOURNISSEUR_MANQUANT)
    elif fournisseur_actif is False:
        err(E_FOURNISSEUR_ARCHIVE, frs)

    ref = _txt(form.get("facture_ref"))
    if not ref:
        err(E_REF_MANQUANTE)

    ttc = _nombre(form.get("montant_ttc"))
    if ttc is None or abs(ttc) < 0.005:
        err(E_MONTANT_INVALIDE, _txt(form.get("montant_ttc")))

    ht, tva = _nombre(form.get("montant_ht")), _nombre(form.get("montant_tva"))
    if ht is not None and tva is not None and ttc is not None:
        if abs((ht + tva) - ttc) > 0.02:
            err(E_TVA_INCOHERENTE, f"{ht} + {tva} != {ttc}")

    # Mission 11 : une date_facture/date_echeance présente mais calendairement impossible
    # (ex. "2026-02-30") était auparavant silencieusement ignorée (`except ValueError: pass`) —
    # ni signalée, ni bloquée — et aurait circulé telle quelle jusqu'à SQLite/Lot9/Lot10. Les deux
    # champs restent optionnels (formulaire réel : `type="date"` sans `required`) ; seule une
    # valeur PRÉSENTE mais invalide est désormais rejetée.
    d_fac, d_ech = _txt(form.get("date_facture")), _txt(form.get("date_echeance"))
    fac_ok = ech_ok = True
    if d_fac:
        try:
            date.fromisoformat(d_fac)
        except ValueError:
            err(E_DATE_INVALIDE, f"date_facture={d_fac!r}")
            fac_ok = False
    if d_ech:
        try:
            date.fromisoformat(d_ech)
        except ValueError:
            err(E_DATE_INVALIDE, f"date_echeance={d_ech!r}")
            ech_ok = False
    if d_fac and d_ech and fac_ok and ech_ok:
        if date.fromisoformat(d_ech) < date.fromisoformat(d_fac):
            err(E_DATE_INCOHERENTE, f"{d_ech} < {d_fac}")

    mois = d_fac[:7] if (numero_reutilise_autorise and d_fac and fac_ok) else None
    if frs and ref and _doublon_certain(frs, ref, db_path, mois=mois):
        err(E_DOUBLON_CERTAIN, ref)

    return erreurs


def _doublon_certain(fournisseur: str, ref: str, db_path=None, *, mois: str | None = None) -> bool:
    """Même fournisseur + même référence. Avec `mois` (import d'un numéro que le fournisseur a
    RÉUTILISÉ sur une autre période), seule une facture du même mois est un doublon."""
    conn = get_db(db_path)
    try:
        sql = ("SELECT 1 FROM factures WHERE fournisseur_id_opaque=? AND facture_ref=? "
               "AND statut <> ?")
        params: tuple = (fournisseur, ref, ST_ANNULEE)
        if mois:
            sql += " AND substr(COALESCE(date_facture,''),1,7) = ?"
            params += (mois,)
        row = conn.execute(sql, params).fetchone()
        return row is not None
    finally:
        conn.close()


def doublons_probables(fournisseur: str, montant_ttc: Any, date_facture: str,
                       db_path=None) -> list[dict[str, Any]]:
    """Même fournisseur + même montant + date proche (±5 j), référence différente."""
    ttc = _nombre(montant_ttc)
    if ttc is None:
        return []
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM factures WHERE fournisseur_id_opaque=? AND statut <> ?",
            (fournisseur, ST_ANNULEE)).fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        if abs((r["montant_ttc"] or 0) - ttc) > 0.01:
            continue
        if date_facture and r["date_facture"]:
            try:
                ecart = abs((date.fromisoformat(date_facture) -
                             date.fromisoformat(r["date_facture"])).days)
                if ecart > 5:
                    continue
            except ValueError:
                pass
        out.append(dict(r))
    return out


def _evenement(conn, opaque: str, type_evt: str, ancien: str | None, nouveau: str | None,
               commentaire: str, acteur: str) -> None:
    conn.execute(
        "INSERT INTO facture_evenements (facture_id_opaque, type_evenement, ancien_statut, "
        "nouveau_statut, commentaire, acteur) VALUES (?,?,?,?,?,?)",
        (opaque, type_evt, ancien, nouveau, commentaire, acteur or "local"))


def creer(form: dict[str, Any], *, acteur: str = "", db_path=None,
          fournisseur_actif: bool | None = None,
          numero_reutilise_autorise: bool = False) -> dict[str, Any]:
    # Le statut visé est résolu AVANT le contrôle de niveau : créer une facture À CONTRÔLER est une
    # écriture opérationnelle (niveau A, production normale), la créer déjà VALIDEE engage la
    # comptabilité (niveau B, double verrou).
    statut = _txt(form.get("statut")) or ST_A_CONTROLER
    if statut not in STATUTS:
        statut = ST_A_CONTROLER
    if not _niveau_requis_ok(statut):
        return _refus(E_FLAGS)

    erreurs = valider(form, db_path, fournisseur_actif, numero_reutilise_autorise)
    if erreurs:
        return {"ok": False, "code": "V_INVALIDE", "message": "Saisie invalide.", "erreurs": erreurs}

    opaque = "FAC-" + uuid.uuid4().hex[:12].upper()
    frs = _txt(form.get("fournisseur_id_opaque"))
    ref = _txt(form.get("facture_ref"))
    ttc = _nombre(form.get("montant_ttc"))

    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
            "date_facture, date_echeance, montant_ht, montant_tva, montant_ttc, devise, statut, "
            "justificatif, source, empreinte, commentaire, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (opaque, frs, ref, _txt(form.get("date_facture")) or None,
             _txt(form.get("date_echeance")) or None, _nombre(form.get("montant_ht")),
             _nombre(form.get("montant_tva")), ttc, _txt(form.get("devise")) or "EUR", statut,
             _txt(form.get("justificatif")) or None, _txt(form.get("source")) or "SAISIE",
             empreinte(frs, ref, ttc, _txt(form.get("date_facture"))),
             _txt(form.get("commentaire")) or None, acteur or "local"))
        _evenement(conn, opaque, "CREATION", None, statut, _txt(form.get("commentaire")), acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "facture_id_opaque": opaque, "statut": statut}


def charger(opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM factures WHERE facture_id_opaque=?", (opaque,)).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    f = dict(row)
    f.update(solde(opaque, db_path))
    f["lignes"] = lignes(opaque, db_path)
    # LA somme canonique (recette 4, §6) : `flm.somme_lignes_effectives`, exactement celle que
    # lit le contrôle V11 avant de valider. Un seul calcul — jamais un pour l'écran, un autre pour
    # la validation, qui pouvaient diverger dès que l'un évoluait sans l'autre.
    f["montant_lignes_ttc"] = flm.somme_lignes_effectives(opaque, db_path)
    return f


def solde(opaque: str, db_path=None) -> dict[str, Any]:
    """Montant réglé et solde restant — TOUJOURS recalculés, jamais lus d'une colonne stockée."""
    conn = get_db(db_path)
    try:
        f = conn.execute("SELECT montant_ttc, statut FROM factures WHERE facture_id_opaque=?",
                         (opaque,)).fetchone()
        if f is None:
            return {"montant_regle": 0.0, "solde_restant": 0.0}
        rows = conn.execute(
            "SELECT r.montant FROM reglement_repartitions r "
            "JOIN reglements_fournisseurs g ON g.reglement_id_opaque = r.reglement_id_opaque "
            "WHERE r.facture_id_opaque=? AND g.statut <> 'ANNULE'", (opaque,)).fetchall()
    finally:
        conn.close()
    regle = round(sum(r["montant"] for r in rows), 2)
    total = f["montant_ttc"] or 0
    return {"montant_regle": regle, "solde_restant": round(total - regle, 2)}


def lister(*, statut: str = "", fournisseur: str = "", echues_seulement: bool = False,
           db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT * FROM factures ORDER BY id DESC").fetchall()
    finally:
        conn.close()
    out = []
    aujourdhui = date.today().isoformat()
    for r in rows:
        f = dict(r)
        if statut and f["statut"] != statut:
            continue
        if fournisseur and f["fournisseur_id_opaque"] != fournisseur:
            continue
        f.update(solde(f["facture_id_opaque"], db_path))
        f["echue"] = bool(f["date_echeance"] and f["date_echeance"] < aujourdhui
                          and f["statut"] in STATUTS_OUVERTS and f["solde_restant"] > 0.005)
        if echues_seulement and not f["echue"]:
            continue
        out.append(f)
    return out


def changer_statut(opaque: str, nouveau: str, *, commentaire: str = "", acteur: str = "",
                   db_path=None) -> dict[str, Any]:
    # Niveau exigé par le statut VISÉ : annuler une facture remplacée (V1->V2) ou la mettre en
    # litige reste opérationnel ; la VALIDER — seul acte qui ouvre le workflow comptable — exige le
    # niveau B, donc une décision humaine explicite sur une installation habilitée.
    if not _niveau_requis_ok(nouveau):
        return _refus(E_FLAGS)

    # ── §28 — Σ lignes == total document, sinon NON VALIDABLE ────────────────────────────────
    # Contrôle absent jusqu'ici : les deux factures PDF réelles ont été validées le 2026-09-11
    # avec un écart de -89,00 € et +36,00 € entre la somme de leurs lignes et le total du
    # document. Une facture dont les lignes ne reconstituent pas le total n'est pas contrôlée :
    # il manque une ligne, ou une ligne a été mal extraite. Aucun « forcer valide » n'est
    # proposé — la résolution passe par les workflows tracés (ligne manquante / extraction
    # incorrecte), jamais par une tolérance cachée.
    if nouveau == ST_VALIDEE:
        f = charger(opaque, db_path)
        if f is None:
            return _refus(E_INTROUVABLE, opaque)
        if f.get("lignes"):
            ecart = round(f["montant_lignes_ttc"] - round(f["montant_ttc"], 2), 2)
            if abs(ecart) > 0.005:
                return _refus(
                    E_ECART_LIGNES_TOTAL,
                    f"somme des lignes {f['montant_lignes_ttc']:.2f} € vs total document "
                    f"{f['montant_ttc']:.2f} € (écart {ecart:+.2f} €)")
            # §15 — chaque ligne doit AVOIR ÉTÉ CONTRÔLÉE : sa nature choisie, et son logement
            # désigné dès lors qu'elle compte pour un ménage. Une ligne dont le libellé n'a pas
            # été compris existe (elle est dans le document), mais elle interdit la validation
            # tant qu'un humain ne l'a pas classée. Aucun « forcer valide ».
            # Seules les lignes VENUES DU DOCUMENT sont concernées : une ligne ajoutée à la main
            # porte déjà l'intention de celui qui l'a saisie. Et une nature proposée par le
            # parseur avec certitude n'a pas besoin d'être reconfirmée — c'est le « je n'ai pas
            # compris ce libellé » (confiance AUCUN) qui exige une décision humaine.
            a_classer = [l for l in f["lignes"]
                         if not l.get("neutralisee") and l.get("categorie_a_classer")
                         and l.get("origine_ligne") == "PDF"
                         and str(l.get("source") or "") == flm.SOURCE_PDF
                         and _nomenclature_disponible(db_path)]
            if a_classer:
                return _refus(
                    E_LIGNE_NON_CLASSEE,
                    f"{len(a_classer)} ligne(s) sans nature : "
                    + ", ".join((l.get("libelle_source") or l.get("description") or "?")[:40]
                                for l in a_classer[:3]))
            sans_logement = [l for l in f["lignes"]
                             if not l.get("neutralisee") and l.get("categorie_compte_menage")
                             and not l.get("logement_id")]
            if sans_logement:
                return _refus(
                    E_LIGNE_SANS_LOGEMENT,
                    f"{len(sans_logement)} ligne(s) comptant un ménage sans logement désigné")

    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT statut FROM factures WHERE facture_id_opaque=?",
                           (opaque,)).fetchone()
        if row is None:
            return _refus(E_INTROUVABLE, opaque)
        ancien = row["statut"]
        if nouveau != ancien and nouveau not in TRANSITIONS.get(ancien, set()):
            return _refus(E_STATUT, f"{ancien} -> {nouveau}")
        conn.execute("UPDATE factures SET statut=?, date_modification=?, version=version+1 "
                     "WHERE facture_id_opaque=?", (nouveau, _now(), opaque))
        _evenement(conn, opaque, "VALIDATION" if nouveau == ST_VALIDEE else "MODIFICATION",
                   ancien, nouveau, commentaire, acteur)
        conn.commit()
    finally:
        conn.close()

    # ── §36/§77 — la validation CRÉE la dette fournisseur, sans attendre la banque ────────────
    # La validation ne faisait qu'un UPDATE de statut : deux factures fournisseurs VALIDÉES
    # (1 576 € au total) ne produisaient AUCUNE écriture d'achat et AUCUNE dette. Le journal
    # ACHATS restait vide, et la dépense n'existait comptablement qu'au moment du débit
    # bancaire — l'achat était constaté au paiement, ce qui n'est pas le fait générateur.
    # `generer_ecriture_achat` est idempotent : un rerun ne crée jamais une seconde écriture.
    ecriture = None
    if nouveau == ST_VALIDEE:
        try:
            from app.services import comptabilite_ecritures_service as compta
            ecriture = compta.generer_ecriture_achat(opaque, acteur=acteur or "local",
                                                     db_path=db_path)
        except Exception as exc:      # noqa: BLE001 — la facture reste validée ; l'écriture est tracée
            ecriture = {"ok": False, "message": str(exc)}

    resultat = {"ok": True, "facture_id_opaque": opaque, "statut": nouveau}
    if ecriture is not None:
        resultat["ecriture_achat"] = ecriture
    return resultat


#: Refus de rouvrir le contrôle d'une facture dont des conséquences sont déjà constatées.
E_CONSEQUENCES_IRREVERSIBLES = "E05_CONSEQUENCES_DEJA_CONSTATEES"


def consequences_constatees(opaque: str, db_path=None) -> dict[str, Any]:
    """Ce qui a déjà découlé de la validation d'une facture — écriture, dette, règlement.

    Sert à décider si rouvrir son contrôle est encore un geste RÉVERSIBLE. Aucune de ces
    conséquences ne se défait par une simple mise à jour de statut : elles se corrigent par
    contrepassation.
    """
    conn = get_db(db_path)
    try:
        ecritures = [dict(r) for r in conn.execute(
            "SELECT ecriture_id_opaque, journal, statut FROM ecritures "
            "WHERE origine_id_opaque = ?", (opaque,))]
        reglements = conn.execute(
            "SELECT COUNT(*) n FROM reglement_repartitions WHERE facture_id_opaque = ?",
            (opaque,)).fetchone()["n"]
    finally:
        conn.close()
    return {"ecritures": ecritures, "nb_reglements": int(reglements or 0),
            "reversible": not ecritures and not reglements}


E_SUPPRESSION_INTERDITE = "E06_SUPPRESSION_INTERDITE"
E_MOIS_CLOTURE = "E07_MOIS_CLOTURE"
E_MOTIF_OBLIGATOIRE = "E08_MOTIF_OBLIGATOIRE"
E_RIEN_A_CONTREPASSER = "E09_RIEN_A_CONTREPASSER"

#: Tables filles d'une facture fournisseur, dans l'ORDRE de suppression. Aucune clé étrangère ne
#: relie ces tables à `factures` (le durcissement 0055/0060 n'a porté que sur la chaîne des
#: factures propriétaires) : `PRAGMA foreign_keys=ON` ne cascade donc rien, et un DELETE naïf
#: laisserait neuf tables orphelines. L'ordre va des feuilles vers la racine.
_TABLES_FILLES = (
    ("facture_lignes_menage_detail",
     "ligne_id_opaque IN (SELECT ligne_id_opaque FROM facture_lignes_menage "
     "WHERE facture_id_opaque = ?)"),
    ("facture_lignes_menage_pdf",
     "ligne_id_opaque IN (SELECT ligne_id_opaque FROM facture_lignes_menage "
     "WHERE facture_id_opaque = ?)"),
    ("facture_lignes_menage", "facture_id_opaque = ?"),
    ("facture_lignes", "facture_id_opaque = ?"),
    # Les PARTS d'une ventilation référencent leur ventilation par clé étrangère : elles doivent
    # partir d'abord, sinon SQLite refuse la suppression de la ventilation elle-même.
    ("facture_ventilation_parts",
     "ventilation_id_opaque IN (SELECT ventilation_id_opaque FROM facture_ventilations "
     "WHERE facture_id_opaque = ?)"),
    ("facture_ventilations", "facture_id_opaque = ?"),
    ("facture_classification", "facture_id_opaque = ?"),
    ("facture_pdf_diagnostics", "facture_id_opaque = ?"),
    ("facture_evenements", "facture_id_opaque = ?"),
)


def _mois_facture(row) -> str:
    return _txt(row["date_facture"])[:7]


def _mois_est_cloture(mois: str, db_path=None) -> bool:
    """Clôture MÉTIER du mois, lue là où elle est réellement peuplée (`ref_cloture_mensuelle`).

    `comptabilite_periodes_service.est_fermee` existe aussi, sur `periodes_comptables`, table vide
    en production : s'y fier seul laisserait passer une suppression dans un mois clôturé.
    """
    from app.services import menages_declarations_service as decl

    try:
        return bool(decl.mois_cloture(mois, db_path=db_path))
    except Exception:      # noqa: BLE001 — table absente d'une base de test minimale
        return False


def _recalculer_menages(mois: str, motif: str, db_path=None) -> dict[str, Any] | None:
    """Défait les impacts ménages en RECALCULANT le mois, jamais par des DELETE à la main.

    lot6f réécrit `menages_cout_complet` intégralement par mois ; c'est donc le recalcul qui fait
    disparaître la contribution d'une facture retirée, et lui seul reste cohérent avec le reste.
    """
    if not mois:
        return None
    try:
        from app.services import orchestrateur_moteur

        return orchestrateur_moteur.executer_menages_cible(mois=mois, declencheur=motif,
                                                           db_path=db_path)
    except Exception as exc:      # noqa: BLE001 — l'écriture métier est faite, le recalcul suivra
        return {"ok": False, "code": type(exc).__name__}


def supprimer(opaque: str, *, motif: str, acteur: str = "", db_path=None) -> dict[str, Any]:
    """RÈGLE A — supprimer une facture À CONTRÔLER dont le document n'a plus lieu d'être.

    Cas réel : le PDF a été retiré du dossier, la facture reste en base et aucun écran ne la
    signale. Une facture à contrôler n'a AUCUN impact économique — `lib_db_moteur` ne compte que
    VALIDEE / PARTIELLEMENT_REGLEE / REGLEE — la suppression ne défait donc que des données
    provisoires. C'est ce qui la rend acceptable ici, et interdite ailleurs.

    Trois verrous : le statut (seules BROUILLON et A_CONTROLER), l'absence de conséquence
    constatée (écriture, dette, règlement), et le mois non clôturé. La trace est écrite AVANT la
    suppression, dans la même transaction, sinon elle disparaîtrait avec la facture.
    """
    motif = _txt(motif)
    if not motif:
        return {"ok": False, "code": E_MOTIF_OBLIGATOIRE,
                "message": "Supprimer une facture exige d'en donner la raison."}
    if not _niveau_operationnel_actif():
        return _refus(E_FLAGS)

    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT facture_id_opaque, statut, date_facture, facture_ref, montant_ttc "
            "FROM factures WHERE facture_id_opaque = ?", (opaque,)).fetchone()
        if row is None:
            return _refus(E_INTROUVABLE, opaque)
        if row["statut"] not in (ST_BROUILLON, ST_A_CONTROLER):
            return {"ok": False, "code": E_SUPPRESSION_INTERDITE,
                    "message": ("Une facture " + row["statut"] + " ne se supprime pas : elle se "
                                "contrepasse, pour que son annulation reste lisible.")}
        mois = _mois_facture(row)
        if _mois_est_cloture(mois, db_path=db_path):
            return {"ok": False, "code": E_MOIS_CLOTURE,
                    "message": f"Le mois {mois} est clôturé : passer par les règles de correction."}
    finally:
        conn.close()

    consequences = consequences_constatees(opaque, db_path=db_path)
    if not consequences["reversible"]:
        return {"ok": False, "code": E_CONSEQUENCES_IRREVERSIBLES,
                "message": "Cette facture a déjà produit une écriture ou un règlement.",
                "consequences": consequences}

    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _evenement(conn, opaque, "SUPPRESSION", row["statut"], None,
                   f"Facture {_txt(row['facture_ref'])} ({row['montant_ttc']} €) supprimée : {motif}",
                   acteur)
        # Le hash du fichier est indexé par NOM : sans ce retrait, le PDF resterait classé « déjà
        # importé » et ne pourrait plus jamais être réimporté après correction.
        fichiers = [r["nom_fichier"] for r in conn.execute(
            "SELECT nom_fichier FROM facture_pdf_diagnostics WHERE facture_id_opaque = ?",
            (opaque,))]
        supprimees: dict[str, int] = {}
        for table, condition in _TABLES_FILLES:
            try:
                cur = conn.execute(f"DELETE FROM {table} WHERE {condition}", (opaque,))
                supprimees[table] = cur.rowcount
            except sqlite3.OperationalError:      # table absente d'une base partielle
                continue
        for nom in fichiers:
            try:
                conn.execute("DELETE FROM menages_pdf_fichiers_hash WHERE nom_fichier = ?", (nom,))
            except sqlite3.OperationalError:
                break
        conn.execute("DELETE FROM factures WHERE facture_id_opaque = ?", (opaque,))
        conn.commit()
    except sqlite3.IntegrityError as exc:
        # Une dépendance non prévue : mieux vaut refuser proprement, en nommant l'obstacle, que
        # laisser remonter une erreur technique ou, pire, supprimer à moitié.
        conn.rollback()
        return {"ok": False, "code": E_SUPPRESSION_INTERDITE,
                "message": "Une donnée liée empêche la suppression : " + str(exc)}
    finally:
        conn.close()

    return {"ok": True, "facture_id_opaque": opaque, "mois": mois, "motif": motif,
            "tables_nettoyees": supprimees, "fichiers_liberes": fichiers,
            "recalcul_menages": _recalculer_menages(mois, "FACTURE_SUPPRIMEE", db_path=db_path)}


def contrepasser(opaque: str, *, motif: str, acteur: str = "", db_path=None) -> dict[str, Any]:
    """RÈGLE B — annuler une facture VALIDÉE sans jamais l'effacer.

    La facture reste, son annulation se lit : écriture d'achat contrepassée par une écriture
    miroir (mécanisme unique du projet, `comptabilite_ecritures_service.contrepasser`, déjà
    réutilisé par la caisse), lignes de ménage neutralisées pour ne plus peser sur le
    rapprochement, statut terminal, et recalcul du mois pour défaire les impacts ménages.

    IDEMPOTENTE : rejouée sur une facture déjà contrepassée, elle ne refait rien et le dit. Une
    reprise après incident ne doit pas échouer sur un travail à moitié fait.
    """
    motif = _txt(motif)
    if not motif:
        return {"ok": False, "code": E_MOTIF_OBLIGATOIRE,
                "message": "Contrepasser une facture exige d'en donner la raison."}
    if not _flags_actifs():
        return _refus(E_FLAGS)

    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT facture_id_opaque, statut, date_facture, facture_ref FROM factures "
            "WHERE facture_id_opaque = ?", (opaque,)).fetchone()
        if row is None:
            return _refus(E_INTROUVABLE, opaque)
        statut, mois = row["statut"], _mois_facture(row)
        deja = conn.execute(
            "SELECT COUNT(*) n FROM facture_evenements WHERE facture_id_opaque = ? "
            "AND type_evenement = 'CONTREPASSATION'", (opaque,)).fetchone()["n"]
    finally:
        conn.close()

    if deja:
        return {"ok": True, "facture_id_opaque": opaque, "deja_contrepassee": True,
                "message": "Cette facture a déjà été contrepassée."}
    if statut == ST_A_CONTROLER or statut == ST_BROUILLON:
        return {"ok": False, "code": E_RIEN_A_CONTREPASSER,
                "message": ("Une facture à contrôler n'a rien engagé : elle se supprime, elle ne "
                            "se contrepasse pas.")}
    if statut == ST_ANNULEE:
        return {"ok": False, "code": E_STATUT, "message": "Facture déjà annulée."}
    if _mois_est_cloture(mois, db_path=db_path):
        return {"ok": False, "code": E_MOIS_CLOTURE,
                "message": f"Le mois {mois} est clôturé : passer par les règles de correction."}

    # L'écriture d'achat n'est contrepassée que si elle a réellement été postée : générée au statut
    # PROPOSEE, elle ne pèse sur aucun solde tant qu'elle n'est pas VALIDEE.
    from app.services import comptabilite_ecritures_service as compta

    miroirs = []
    for ecriture in consequences_constatees(opaque, db_path=db_path)["ecritures"]:
        if ecriture["statut"] != compta.ST_VALIDEE:
            continue
        resultat = compta.contrepasser(ecriture["ecriture_id_opaque"],
                                       commentaire=f"Contrepassation facture : {motif}",
                                       acteur=acteur, db_path=db_path)
        if not resultat.get("ok"):
            return {"ok": False, "code": resultat.get("code", "E_CONTREPASSATION"),
                    "message": resultat.get("message", "Contrepassation comptable refusée."),
                    "ecriture_id_opaque": ecriture["ecriture_id_opaque"]}
        miroirs.append(resultat.get("miroir_id_opaque"))

    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        # Les lignes de ménage d'une facture annulée continuaient d'alimenter le rapprochement et
        # les écarts : lot6d et lib_db_moteur lisent `facture_lignes_menage` sans filtrer le statut
        # du document. Les neutraliser — jamais les supprimer — remet le compte à zéro tout en
        # laissant lisible ce que la facture portait.
        conn.execute(
            "UPDATE facture_lignes_menage SET statut_ligne = ?, motif_correction = ? "
            "WHERE facture_id_opaque = ? AND COALESCE(statut_ligne, ?) = ?",
            (flm.STATUT_LIGNE_EXTRACTION_INCORRECTE, f"Facture contrepassée : {motif}", opaque,
             flm.STATUT_LIGNE_ACTIVE, flm.STATUT_LIGNE_ACTIVE))
        conn.execute("UPDATE factures SET statut = ? WHERE facture_id_opaque = ?",
                     (ST_ANNULEE, opaque))
        _evenement(conn, opaque, "CONTREPASSATION", statut, ST_ANNULEE,
                   f"Contrepassation : {motif}" + (f" — miroir {', '.join(m for m in miroirs if m)}"
                                                   if miroirs else " — aucune écriture postée"),
                   acteur)
        conn.commit()
    finally:
        conn.close()

    return {"ok": True, "facture_id_opaque": opaque, "statut": ST_ANNULEE, "mois": mois,
            "motif": motif, "miroirs": [m for m in miroirs if m],
            "recalcul_menages": _recalculer_menages(mois, "FACTURE_CONTREPASSEE", db_path=db_path)}


def rouvrir_controle(opaque: str, *, motif: str, acteur: str = "",
                     db_path=None) -> dict[str, Any]:
    """`VALIDEE` → `A_CONTROLER` : rouvrir le contrôle d'une facture fournisseur.

    Une facture dont la somme des lignes ne reconstitue pas le total du document ne peut pas
    rester validée (§28). Encore faut-il pouvoir l'en sortir — c'est l'objet de cette fonction.

    REFUSÉ si des conséquences sont déjà constatées : écriture d'achat générée, règlement
    imputé. Elles ne se défont pas par un changement de statut ; la correction passe alors par
    une contrepassation, qui laisse la trace de ce qui a eu lieu.

    Le motif est obligatoire, et il est journalisé dans `facture_evenements`.
    """
    motif = str(motif or "").strip()
    if not motif:
        return _refus("MOTIF_OBLIGATOIRE",
                      "Rouvrir le contrôle d'une facture exige d'en donner la raison.")
    if not _niveau_requis_ok(ST_A_CONTROLER):
        return _refus(E_FLAGS)

    etat = consequences_constatees(opaque, db_path)
    if not etat["reversible"]:
        details = []
        if etat["ecritures"]:
            details.append("écriture(s) " + ", ".join(
                f"{e['ecriture_id_opaque']} ({e['journal']})" for e in etat["ecritures"]))
        if etat["nb_reglements"]:
            details.append(f"{etat['nb_reglements']} règlement(s) imputé(s)")
        return _refus(E_CONSEQUENCES_IRREVERSIBLES,
                      "Conséquences déjà constatées : " + " · ".join(details)
                      + ". La correction passe par une contrepassation, pas par un retour de "
                        "statut.")

    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT statut FROM factures WHERE facture_id_opaque=?",
                           (opaque,)).fetchone()
        if row is None:
            return _refus(E_INTROUVABLE, opaque)
        ancien = row["statut"]
        if ancien == ST_A_CONTROLER:
            return {"ok": True, "facture_id_opaque": opaque, "statut": ST_A_CONTROLER,
                    "inchange": True}
        if ST_A_CONTROLER not in TRANSITIONS.get(ancien, set()):
            return _refus(E_STATUT, f"{ancien} -> {ST_A_CONTROLER}")
        conn.execute("UPDATE factures SET statut=?, date_modification=?, version=version+1 "
                     "WHERE facture_id_opaque=?", (ST_A_CONTROLER, _now(), opaque))
        _evenement(conn, opaque, "RETOUR_A_CONTROLER", ancien, ST_A_CONTROLER, motif, acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "facture_id_opaque": opaque, "statut": ST_A_CONTROLER,
            "inchange": False, "motif": motif}


def lier_charge(opaque: str, charge_id: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Rattache une charge DÉJÀ créée par le parcours Charges. Ne crée jamais la charge elle-même,
    et refuse qu'une charge soit rattachée à deux factures."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    charge_id = _txt(charge_id)
    conn = get_db(db_path)
    try:
        if conn.execute("SELECT 1 FROM factures WHERE facture_id_opaque=?", (opaque,)).fetchone() is None:
            return _refus(E_INTROUVABLE, opaque)
        if conn.execute("SELECT 1 FROM facture_lignes WHERE facture_id_opaque=?",
                        (opaque,)).fetchone():
            return _refus(E_LIGNE_FACTURE_MONO_CHARGE, opaque)
        if conn.execute("SELECT 1 FROM factures WHERE charge_id=? AND facture_id_opaque<>? "
                        "AND statut <> ?", (charge_id, opaque, ST_ANNULEE)).fetchone():
            return _refus(E_CHARGE_DEJA_LIEE, charge_id)
        conn.execute("UPDATE factures SET charge_id=?, date_modification=?, version=version+1 "
                     "WHERE facture_id_opaque=?", (charge_id, _now(), opaque))
        _evenement(conn, opaque, "CHARGE_LIEE", None, None, f"charge {charge_id}", acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "facture_id_opaque": opaque, "charge_id": charge_id}


#: Au-delà de cet écart au tarif standard du logement, l'écran ALERTE — il ne bloque pas, et il ne
#: remplace jamais le montant réel de la facture par le tarif de référence : c'est le prestataire
#: qui dit ce qu'il facture, le référentiel ne sert qu'à faire remarquer l'inhabituel.
ECART_TARIF_RELATIF = 0.20
ECART_TARIF_ABSOLU = 5.0


def _nomenclature_disponible(db_path=None) -> bool:
    """La nomenclature des natures de prestation est-elle chargée ?

    Elle vient du RÉFÉRENTIEL (REF_Setup), pas des migrations. Exiger un classement alors que la
    liste des natures possibles n'existe pas reviendrait à interdire toute validation pour une
    raison que l'utilisateur ne peut pas corriger depuis cet écran : la règle ne s'applique donc
    que là où elle est applicable.
    """
    conn = get_db(db_path)
    try:
        return bool(conn.execute(
            "SELECT 1 FROM ref_types_lignes_menage WHERE actif = 'OUI' LIMIT 1").fetchone())
    except sqlite3.OperationalError:
        return False
    finally:
        conn.close()


def _cout_standard_du_logement(conn, logement_id: str, date_facture: str) -> float | None:
    """Tarif de ménage de référence du logement, à la date de la facture. None si inconnu."""
    if not logement_id:
        return None
    try:
        row = conn.execute(
            "SELECT c.cout_standard_menage FROM ref_logements l "
            "JOIN ref_couts_standards_menage c ON c.type_logement_id = l.type_logement_id "
            "WHERE l.logement_id = ? AND UPPER(COALESCE(c.actif,'OUI')) = 'OUI' "
            "AND (COALESCE(c.date_debut_validite,'') = '' OR c.date_debut_validite <= ?) "
            "AND (COALESCE(c.date_fin_validite,'') = '' OR c.date_fin_validite >= ?) "
            "ORDER BY c.date_debut_validite DESC LIMIT 1",
            (logement_id, date_facture or "9999-12-31", date_facture or "0001-01-01")).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None or row["cout_standard_menage"] in (None, ""):
        return None
    try:
        return float(str(row["cout_standard_menage"]).replace(",", "."))
    except ValueError:
        return None


def _enrichir_categorie(conn, d: dict[str, Any]) -> None:
    """Nature de la prestation, état du classement, et alerte de tarif (§9 à §11).

    `type_ligne` dit si un logement a été reconnu ; c'est `ref_types_lignes_menage` qui dit ce qui
    a été fait. Une ligne dont la nature n'a pas été comprise doit rester visiblement à classer :
    c'est ce qui empêche de valider une facture sur un classement que personne n'a fait.
    """
    d["categorie"] = None
    d["categorie_compte_menage"] = None
    try:
        if d.get("type_ligne_menage_id"):
            row = conn.execute(
                "SELECT type_ligne_menage, compte_comme_menage FROM ref_types_lignes_menage "
                "WHERE type_ligne_menage_id = ?", (d["type_ligne_menage_id"],)).fetchone()
            if row is not None:
                d["categorie"] = row["type_ligne_menage"]
                d["categorie_compte_menage"] = row["compte_comme_menage"] == "OUI"
    except sqlite3.OperationalError:      # référentiel absent d'une base partielle
        pass
    d["categorie_a_classer"] = (not d.get("categorie")
                                or str(d.get("type_ligne_menage_confiance") or "") == "AUCUN")

    # §10 — le tarif standard sert de CONTRÔLE, jamais de substitution. Et il ne s'applique qu'au
    # ménage courant : une remise en état est par nature à prix variable (§11).
    d["alerte_tarif"] = None
    if d.get("categorie") != "MENAGE_STANDARD" or d.get("neutralisee"):
        return
    quantite = d.get("quantite") or 0
    unitaire = d.get("prix_unitaire")
    if unitaire is None and quantite:
        unitaire = round(float(d.get("montant_ttc") or 0) / quantite, 2)
    standard = _cout_standard_du_logement(conn, str(d.get("logement_id") or ""),
                                          str(d.get("date_facture") or ""))
    if unitaire is None or standard in (None, 0):
        return
    ecart = round(float(unitaire) - standard, 2)
    if abs(ecart) > max(ECART_TARIF_ABSOLU, standard * ECART_TARIF_RELATIF):
        d["alerte_tarif"] = {"unitaire": float(unitaire), "standard": standard, "ecart": ecart}


def lignes(opaque: str, db_path=None) -> list[dict[str, Any]]:
    """Lignes de la facture fournisseur — UNE seule chaîne, extraction PDF incluse.

    BUG D'INTÉGRATION CORRIGÉ (recette utilisateur n°3, §21). Deux tables de lignes coexistaient :
    `facture_lignes_menage` (+ `_detail` quantité/prix unitaire, + `_pdf` traçabilité), ALIMENTÉE
    par l'extracteur PDF (`facture_menage_pdf_service`) et lue par lot6c/6d/6e/6f ; et
    `facture_lignes`, lue par CET écran et alimentée uniquement par « rattacher une charge ».
    Résultat : 11 lignes réellement extraites de deux PDF, et une facture fournisseur affichant
    « 0 ligne » — l'extracteur d'un côté, la facture vide de l'autre.

    La ligne extraite du PDF EST la ligne de la facture : elle est donc rendue ici, avec son
    libellé d'origine, sa quantité et son prix unitaire (§22 — le libellé et la quantité source
    sont IMMUABLES). `facture_lignes` reste lue pour ne rien masquer des lignes historiques
    éventuellement créées par l'ancien parcours de rattachement de charge.
    """
    conn = get_db(db_path)
    try:
        resultat: list[dict[str, Any]] = []
        entete = conn.execute("SELECT date_facture FROM factures WHERE facture_id_opaque=?",
                              (opaque,)).fetchone()
        date_facture = _txt(entete["date_facture"]) if entete else ""
        for r in conn.execute(
                "SELECT l.*, d.quantite, d.prix_unitaire, d.quantite_source, "
                "d.prix_unitaire_source, p.nom_prestataire, p.date_menage "
                "FROM facture_lignes_menage l "
                "LEFT JOIN facture_lignes_menage_detail d ON d.ligne_id_opaque = l.ligne_id_opaque "
                "LEFT JOIN facture_lignes_menage_pdf p ON p.ligne_id_opaque = l.ligne_id_opaque "
                "WHERE l.facture_id_opaque=? ORDER BY l.id", (opaque,)).fetchall():
            d = dict(r)
            source = str(d.get("source") or "")
            d["origine_ligne"] = ("CORRECTIVE" if source == "SAISIE_MANUELLE_CORRECTIVE"
                                  else "REPARTITION" if source == "REPARTITION_MULTI_LOGEMENTS"
                                  else "PDF")
            # Une ligne marquée EXTRACTION_INCORRECTE (ou remplacée par ses parts) reste VISIBLE —
            # la donnée brute ne disparaît jamais — mais ne compte plus dans le total.
            d["neutralisee"] = str(d.get("statut_ligne") or "ACTIVE") != "ACTIVE"
            # §27 — quantité × prix unitaire redonne-t-il le montant ? C'est ce contrôle qui
            # distingue une ligne mal lue d'une ligne absente.
            d["coherence"] = flm.coherence_ligne(d)
            # §27 — la quantité a-t-elle été corrigée depuis l'extraction ?
            d["quantite_corrigee"] = (d.get("quantite_source") is not None
                                      and d.get("quantite") != d.get("quantite_source"))
            d["date_facture"] = date_facture
            _enrichir_categorie(conn, d)
            # UNE vérité canonique pour « cette ligne compte-t-elle un ménage ? » (recette 4, §4) :
            # la NATURE choisie (`categorie_compte_menage`), jamais `type_ligne` — une colonne SQL
            # que les moteurs lisent, tenue synchronisée en écriture, mais qui n'a plus voix au
            # chapitre ici. Le repli sur `type_ligne` ne sert QUE pour les lignes jamais classées
            # (antérieures à cette mission) : la seule information qui existe alors pour elles.
            d["est_menage"] = (d["categorie_compte_menage"] if d["categorie_compte_menage"] is not None
                               else str(d.get("type_ligne") or "") in ("MENAGE_EXTERNE",
                                                                       "MENAGE_INTERNE"))
            resultat.append(d)
        for r in conn.execute(
                "SELECT * FROM facture_lignes WHERE facture_id_opaque=? ORDER BY id",
                (opaque,)).fetchall():
            d = dict(r)
            d["origine_ligne"] = "CHARGE"
            d["est_menage"] = False
            d["neutralisee"] = False
            # Mêmes clés pour toutes les lignes, quelle que soit leur table d'origine : le gabarit
            # n'a pas à savoir laquelle il affiche (et Jinja lève sur un attribut absent).
            d.setdefault("description", d.get("commentaire"))
            d.setdefault("quantite", None)
            d.setdefault("prix_unitaire", None)
            d.setdefault("quantite_source", None)
            d.setdefault("statut_ligne", "ACTIVE")
            d.setdefault("motif_correction", None)
            d.setdefault("logement_confiance", None)
            d.setdefault("logement_methode", None)
            d.setdefault("ligne_parente_id_opaque", None)
            d["quantite_corrigee"] = False
            d["coherence"] = flm.coherence_ligne(d)
            resultat.append(d)
        return resultat
    finally:
        conn.close()


def ajouter_ligne(opaque: str, charge_id: str, *, logement_id: str = "",
                  montant_ttc: Any = None, montant_ht: Any = None, montant_tva: Any = None,
                  commentaire: str = "", acteur: str = "", db_path=None) -> dict[str, Any]:
    """Ajoute une ligne de facture rattachant UNE charge (et, si connu, un logement) à une facture
    existante. Permet le cas multi-charges/multi-logements sans toucher au cas mono-charge
    historique (`factures.charge_id`) : une facture utilise l'un OU l'autre mécanisme, jamais les
    deux. Ne crée jamais la charge elle-même (même garde-fou que `lier_charge`)."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    charge_id = _txt(charge_id)
    if not charge_id:
        return _refus(E_LIGNE_CHARGE_MANQUANTE)
    ttc = _nombre(montant_ttc)
    if ttc is None or ttc == 0:
        return _refus(E_LIGNE_MONTANT_INVALIDE)
    conn = get_db(db_path)
    try:
        f = conn.execute("SELECT charge_id FROM factures WHERE facture_id_opaque=?",
                         (opaque,)).fetchone()
        if f is None:
            return _refus(E_INTROUVABLE, opaque)
        if f["charge_id"]:
            return _refus(E_LIGNE_FACTURE_MONO_CHARGE, opaque)
        if conn.execute("SELECT 1 FROM factures WHERE charge_id=? AND statut <> ?",
                        (charge_id, ST_ANNULEE)).fetchone():
            return _refus(E_CHARGE_DEJA_LIEE, charge_id)
        if conn.execute(
                "SELECT 1 FROM facture_lignes fl "
                "JOIN factures fc ON fc.facture_id_opaque = fl.facture_id_opaque "
                "WHERE fl.charge_id=? AND fc.statut <> ?", (charge_id, ST_ANNULEE)).fetchone():
            return _refus(E_CHARGE_DEJA_LIEE, charge_id)
        ligne_id = "FACL-" + uuid.uuid4().hex[:12].upper()
        conn.execute(
            "INSERT INTO facture_lignes (ligne_id_opaque, facture_id_opaque, charge_id, "
            "logement_id, montant_ht, montant_tva, montant_ttc, commentaire, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (ligne_id, opaque, charge_id, _txt(logement_id) or None,
             _nombre(montant_ht), _nombre(montant_tva), ttc,
             _txt(commentaire) or None, acteur or "local"))
        _evenement(conn, opaque, "LIGNE_AJOUTEE", None, None,
                  f"ligne {ligne_id} charge {charge_id}", acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "facture_id_opaque": opaque, "ligne_id_opaque": ligne_id,
            "charge_id": charge_id}


def historique(opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM facture_evenements WHERE facture_id_opaque=? ORDER BY id DESC",
            (opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def solde_fournisseur(fournisseur_opaque: str, db_path=None) -> dict[str, Any]:
    """Vue de synthèse — recalculée, jamais dénormalisée."""
    factures = lister(fournisseur=fournisseur_opaque, db_path=db_path)
    vivantes = [f for f in factures if f["statut"] != ST_ANNULEE]
    total_facture = round(sum(f["montant_ttc"] or 0 for f in vivantes), 2)
    total_regle = round(sum(f["montant_regle"] for f in vivantes), 2)
    return {
        "total_facture": total_facture,
        "total_regle": total_regle,
        "solde_a_payer": round(total_facture - total_regle, 2),
        "nb_factures": len(vivantes),
        "nb_echues": sum(1 for f in vivantes if f["echue"]),
        "nb_litige": sum(1 for f in vivantes if f["statut"] == ST_LITIGE),
        "factures": factures,
    }
