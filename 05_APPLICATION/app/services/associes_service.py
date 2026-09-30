"""Associés — IK (indemnités kilométriques), avantages et compte courant, en un écran consolidé.

CE QUE CE MODULE SAISIT, ET CE QU'IL NE FAIT QUE LIRE
Il ne ressaisit rien de ce qui existe ailleurs. Il LIT :
    · les charges (source de vérité comptable, montants, catégorie « avantage associé ») ;
    · les apports et remboursements de compte courant (rapprochements bancaires APPORT_ASSOCIE /
      REMBOURSEMENT_ASSOCIE, opérations de caisse REMBOURSEMENT_ASSOCIE) ;
    · le solde comptable 455100 par associé (auxiliaire).
Il SAISIT seulement ce que personne d'autre ne porte, et qui est purement analytique :
    · la fiche IK d'une charge : associé, période couverte, statut de contrôle ;
    · le relevé de trajets qui la justifie ;
    · la part de l'IK engagée ensuite pour l'activité (dépenses payées sur le compte personnel).

UNE IK EST UNE CHARGE
Son montant, son compte et son écriture sont ceux de la charge référencée — jamais recopiés.
La ventilation analytique ne modifie JAMAIS la charge comptable : IK 400 €, dont 200 € engagés
pour l'activité, reste une charge de 400 € ; l'analytique dit « avantage réel 200 € ».

AUCUNE IK N'EST ATTENDUE
L'absence d'IK un mois donné n'est ni une anomalie ni un blocage (clôture comprise).

RÈGLES DE CALCUL (analytiques)
    IK brute du mois        = montant de l'IK × jours couverts dans le mois / jours couverts
                              (bornes incluses ; centimes répartis pour retomber exactement).
    Dépenses activité       = lignes datées du débit réel sur le compte personnel (jamais
                              proratisées).
    Avantage réel IK        = IK brute du mois − dépenses activité du mois.
    Autres avantages        = charges « avantage associé » hors IK, au mois de la charge.
    Avantages totaux        = avantage réel IK + autres avantages.
    Apport CCA net          = apports − remboursements.
    Position nette avantage = avantages totaux − apport CCA net.
"""
from __future__ import annotations

import calendar
import uuid
from datetime import date, datetime, timezone
from typing import Any

from app.db.connection import get_db

EPS = 0.005
COMPTE_CCA = "455100"

ST_BROUILLON = "BROUILLON"
ST_A_CONTROLER = "A_CONTROLER"
ST_VALIDEE = "VALIDEE"
STATUTS_IK = (ST_BROUILLON, ST_A_CONTROLER, ST_VALIDEE)
LIBELLES_STATUT_IK = {ST_BROUILLON: "Brouillon", ST_A_CONTROLER: "À contrôler",
                      ST_VALIDEE: "Validée"}
TRANSITIONS_IK = {ST_BROUILLON: (ST_A_CONTROLER, ST_VALIDEE),
                  ST_A_CONTROLER: (ST_BROUILLON, ST_VALIDEE),
                  ST_VALIDEE: (ST_A_CONTROLER,)}

MOTIFS_TRAJET = {
    "ACHATS": "Achats en magasin",
    "LOGEMENT": "Logement",
    "RDV_PROPRIETAIRE": "Rendez-vous propriétaire",
    "RDV_CLIENT": "Rendez-vous client",
    "ADMINISTRATIF": "Déplacement administratif",
    "INTERVENTION": "Intervention",
    "AUTRE": "Autre déplacement professionnel",
}
NATURES_DEPENSE = {
    "MENAGE_PRESTATAIRE_INTERNE": "Ménage / prestataire interne",
    "FONCTIONNEMENT": "Autre charge de fonctionnement",
    "FONCTIONNEMENT_NON_AFFECTABLE": "Charge de fonctionnement non affectable",
}

#: Catégorie de charge qui porte une IK : « Déplacement professionnel ». Une IK n'invente pas de
#: catégorie : elle se greffe sur la charge de déplacement déjà saisie dans Charges.
CATEGORIES_IK = ("CHG_009",)

E_INTROUVABLE = "AS01_IK_INTROUVABLE"
E_CHARGE = "AS02_CHARGE_INVALIDE"
E_DEJA_IK = "AS03_CHARGE_DEJA_IK"
E_ASSOCIE = "AS04_ASSOCIE_INCONNU"
E_PERIODE = "AS05_PERIODE_INVALIDE"
E_MONTANT = "AS06_MONTANT_INVALIDE"
E_DEPASSEMENT = "AS07_DEPASSEMENT_MONTANT_IK"
E_STATUT = "AS08_TRANSITION_INTERDITE"
E_VERROUILLEE = "AS09_IK_VALIDEE_NON_MODIFIABLE"
E_TRAJET = "AS10_TRAJET_INVALIDE"
E_NATURE = "AS11_NATURE_INVALIDE"
E_DATE = "AS12_DATE_INVALIDE"
E_LIGNE = "AS13_LIGNE_INTROUVABLE"

MESSAGES = {
    E_INTROUVABLE: "IK introuvable.",
    E_CHARGE: "La charge choisie n'existe pas, est annulée, ou n'est pas une charge de déplacement.",
    E_DEJA_IK: "Cette charge porte déjà une IK.",
    E_ASSOCIE: "Associé inconnu ou inactif.",
    E_PERIODE: "Période invalide : la date de fin doit suivre la date de début.",
    E_MONTANT: "Le montant doit être un nombre strictement positif.",
    E_DEPASSEMENT: ("La part engagée pour l'activité dépasserait le montant de l'IK : "
                    "la somme des dépenses ne peut jamais dépasser l'IK."),
    E_STATUT: "Changement de statut interdit.",
    E_VERROUILLEE: "Une IK validée ne se modifie plus : repassez-la « à contrôler » d'abord.",
    E_TRAJET: "Trajet incomplet : date, motif et kilomètres (> 0) sont obligatoires.",
    E_NATURE: "Nature de dépense inconnue.",
    E_DATE: "Date invalide.",
    E_LIGNE: "Ligne introuvable.",
}


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _r(v: Any) -> float:
    return round(float(v or 0), 2)


def _txt(v: Any) -> str:
    return str(v if v is not None else "").strip()


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _date(v: Any) -> date | None:
    try:
        return date.fromisoformat(_txt(v)[:10])
    except ValueError:
        return None


def _nombre(v: Any) -> float | None:
    try:
        x = float(_txt(v).replace(",", ".").replace(" ", ""))
    except ValueError:
        return None
    return x if x == x else None


def _tables(conn) -> set[str]:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _colonnes(conn, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


# ── Référentiel ─────────────────────────────────────────────────────────────────────────────────

def associes(*, db_path=None) -> list[dict[str, str]]:
    """Associés actifs du référentiel (`ref_associes`). Aucun nom n'est deviné."""
    conn = get_db(db_path)
    try:
        if "ref_associes" not in _tables(conn):
            return []
        return [{"id": r["personne_id"], "nom": r["nom_personne"] or r["personne_id"]}
                for r in conn.execute("SELECT personne_id, nom_personne FROM ref_associes "
                                      "WHERE actif = 'OUI' ORDER BY nom_personne")]
    finally:
        conn.close()


def nom_associe(associe_id: str, *, db_path=None) -> str:
    return next((a["nom"] for a in associes(db_path=db_path) if a["id"] == associe_id),
                associe_id or "")


# ── Prorata calendaire ──────────────────────────────────────────────────────────────────────────

def jours_par_mois(debut: date, fin: date) -> list[tuple[str, int]]:
    """Jours couverts par mois, bornes incluses."""
    out: list[tuple[str, int]] = []
    courant = debut
    while courant <= fin:
        dernier = date(courant.year, courant.month,
                       calendar.monthrange(courant.year, courant.month)[1])
        borne = min(dernier, fin)
        out.append((f"{courant.year:04d}-{courant.month:02d}", (borne - courant).days + 1))
        courant = date.fromordinal(borne.toordinal() + 1)
    return out


def prorata_mensuel(montant: float, debut: date, fin: date) -> list[dict[str, Any]]:
    """Répartit `montant` au prorata des jours calendaires couverts (bornes incluses).

    Calcul en CENTIMES : chaque mois reçoit la partie entière de sa part, puis les centimes
    restants vont aux plus grands restes (à égalité, au mois le plus ancien). La somme retombe
    donc EXACTEMENT sur le montant d'origine — jamais un centime de trop ou de moins."""
    parts = jours_par_mois(debut, fin)
    total_jours = sum(j for _, j in parts)
    if total_jours <= 0:
        return []
    centimes = int(round(float(montant) * 100))
    bruts = [(mois, jours, centimes * jours / total_jours) for mois, jours in parts]
    entiers = [int(b // 1) for _, _, b in bruts]
    reste = centimes - sum(entiers)
    ordre = sorted(range(len(bruts)), key=lambda i: (-(bruts[i][2] - entiers[i]), i))
    for i in ordre[:reste]:
        entiers[i] += 1
    return [{"mois": mois, "jours": jours, "total_jours": total_jours,
             "montant": round(entiers[i] / 100, 2)} for i, (mois, jours, _) in enumerate(bruts)]


# ── IK : lecture ────────────────────────────────────────────────────────────────────────────────

def _ik_depuis_ligne(conn, r) -> dict[str, Any]:
    ik = dict(r)
    trajets = [dict(t) for t in conn.execute(
        "SELECT * FROM ik_trajets WHERE ik_id_opaque = ? AND actif = 1 "
        "ORDER BY date_trajet, id", (ik["ik_id_opaque"],))]
    depenses = [dict(d) for d in conn.execute(
        "SELECT * FROM ik_depenses_activite WHERE ik_id_opaque = ? AND actif = 1 "
        "ORDER BY date_debit, id", (ik["ik_id_opaque"],))]
    for t in trajets:
        t["motif_libelle"] = MOTIFS_TRAJET.get(t["motif"], t["motif"])
    for d in depenses:
        d["nature_libelle"] = NATURES_DEPENSE.get(d["nature"], d["nature"])
    montant = _r(ik.get("montant"))
    engage = _r(sum(d["montant"] for d in depenses))
    debut, fin = _date(ik["date_debut"]), _date(ik["date_fin"])
    ik.update({
        "montant": montant,
        "trajets": trajets,
        "depenses": depenses,
        "nb_trajets": len(trajets),
        "km_total": round(sum(float(t["km"] or 0) for t in trajets), 1),
        "depenses_total": engage,
        "avantage_reel": _r(montant - engage),
        "disponible_depenses": _r(montant - engage),
        "statut_libelle": LIBELLES_STATUT_IK.get(ik["statut"], ik["statut"]),
        "prorata": prorata_mensuel(montant, debut, fin) if debut and fin else [],
        "modifiable": ik["statut"] != ST_VALIDEE,
        # Charge corrigée à la baisse après coup : l'IK doit être revue (jamais corrigée seule).
        "depassement": engage > montant + EPS,
    })
    return ik


_SELECT_IK = ("SELECT ik.*, c.montant AS montant, c.date_charge AS date_charge, "
              "c.statut AS statut_charge, c.commentaire AS commentaire_charge, "
              "c.justificatif_reference AS justificatif_reference "
              "FROM ik JOIN charges c ON c.charge_id = ik.charge_id ")


def charger_ik(ik_id: str, *, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        if "ik" not in _tables(conn):
            return None
        r = conn.execute(_SELECT_IK + "WHERE ik.ik_id_opaque = ?", (ik_id,)).fetchone()
        if r is None:
            return None
        ik = _ik_depuis_ligne(conn, r)
        ik["historique"] = [dict(e) for e in conn.execute(
            "SELECT evenement, detail, acteur, horodatage FROM ik_evenements "
            "WHERE ik_id_opaque = ? ORDER BY id DESC", (ik_id,))]
        return ik
    finally:
        conn.close()


def lister_ik(*, associe_id: str = "", db_path=None) -> list[dict[str, Any]]:
    """IK des charges ACTIVES (une charge annulée n'est plus une IK)."""
    conn = get_db(db_path)
    try:
        if "ik" not in _tables(conn):
            return []
        sql = _SELECT_IK + "WHERE c.statut = 'ACTIVE'"
        params: list = []
        if associe_id:
            sql += " AND ik.associe_id = ?"
            params.append(associe_id)
        return [_ik_depuis_ligne(conn, r)
                for r in conn.execute(sql + " ORDER BY ik.date_debut DESC, ik.id DESC", params)]
    finally:
        conn.close()


def charges_candidates_ik(*, db_path=None) -> list[dict[str, Any]]:
    """Charges de déplacement ACTIVES qui ne portent pas encore d'IK."""
    conn = get_db(db_path)
    try:
        if "ik" not in _tables(conn):
            return []
        marques = ",".join("?" * len(CATEGORIES_IK))
        return [dict(r) for r in conn.execute(
            f"SELECT charge_id, date_charge, montant, commentaire, associe_id, avantage_associe_id, "
            f"justificatif_reference FROM charges WHERE statut = 'ACTIVE' "
            f"AND categorie_charge_id IN ({marques}) "
            f"AND charge_id NOT IN (SELECT charge_id FROM ik) ORDER BY date_charge DESC",
            CATEGORIES_IK)]
    finally:
        conn.close()


# ── IK : écriture (analytique uniquement — aucune écriture comptable) ──────────────────────────

def _evenement(conn, ik_id: str, evenement: str, detail: str, acteur: str) -> None:
    conn.execute("INSERT INTO ik_evenements (ik_id_opaque, evenement, detail, acteur) "
                 "VALUES (?,?,?,?)", (ik_id, evenement, detail or None, acteur or None))


def creer_ik(charge_id: str, associe_id: str, date_debut: str, date_fin: str, *,
             commentaire: str = "", acteur: str = "", db_path=None) -> dict[str, Any]:
    """Déclare la charge de déplacement comme IK de l'associé, sur la période donnée."""
    if associe_id not in {a["id"] for a in associes(db_path=db_path)}:
        return _refus(E_ASSOCIE, associe_id)
    debut, fin = _date(date_debut), _date(date_fin)
    if debut is None or fin is None or fin < debut:
        return _refus(E_PERIODE)
    conn = get_db(db_path)
    try:
        c = conn.execute("SELECT statut, categorie_charge_id, montant FROM charges "
                         "WHERE charge_id = ?", (charge_id,)).fetchone()
        if (c is None or c["statut"] != "ACTIVE" or c["categorie_charge_id"] not in CATEGORIES_IK
                or _r(c["montant"]) <= 0):
            return _refus(E_CHARGE, charge_id)
        if conn.execute("SELECT 1 FROM ik WHERE charge_id = ?", (charge_id,)).fetchone():
            return _refus(E_DEJA_IK, charge_id)
        ik_id = "IK-" + uuid.uuid4().hex[:12].upper()
        conn.execute("INSERT INTO ik (ik_id_opaque, charge_id, associe_id, date_debut, date_fin, "
                     "statut, commentaire, cree_par) VALUES (?,?,?,?,?,?,?,?)",
                     (ik_id, charge_id, associe_id, debut.isoformat(), fin.isoformat(),
                      ST_BROUILLON, _txt(commentaire) or None, acteur or None))
        _evenement(conn, ik_id, "CREATION",
                   f"Charge {charge_id} · {debut.isoformat()} → {fin.isoformat()}", acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ik_id_opaque": ik_id}


def _ik_modifiable(conn, ik_id: str) -> dict[str, Any] | None:
    r = conn.execute("SELECT statut FROM ik WHERE ik_id_opaque = ?", (ik_id,)).fetchone()
    if r is None:
        return _refus(E_INTROUVABLE, ik_id)
    if r["statut"] == ST_VALIDEE:
        return _refus(E_VERROUILLEE)
    return None


def ajouter_trajets(ik_id: str, trajets: list[dict[str, Any]], *, acteur: str = "",
                    db_path=None) -> dict[str, Any]:
    """Ajoute autant de trajets que fournis. Les lignes entièrement vides sont ignorées ; une ligne
    commencée mais incomplète refuse TOUT l'envoi (rien n'est enregistré à moitié)."""
    propres = []
    for t in trajets:
        if not any(_txt(t.get(k)) for k in ("date_trajet", "depart", "destination", "km",
                                              "commentaire", "vehicule")):
            continue
        d, km = _date(t.get("date_trajet")), _nombre(t.get("km"))
        motif = _txt(t.get("motif")).upper() or "AUTRE"
        if d is None or km is None or km <= 0 or motif not in MOTIFS_TRAJET:
            return _refus(E_TRAJET, _txt(t.get("date_trajet")))
        propres.append((d.isoformat(), motif, _txt(t.get("depart")) or None,
                        _txt(t.get("destination")) or None, round(km, 1),
                        _txt(t.get("vehicule")) or None, _txt(t.get("commentaire")) or None))
    if not propres:
        return _refus(E_TRAJET, "aucune ligne")
    conn = get_db(db_path)
    try:
        refus = _ik_modifiable(conn, ik_id)
        if refus:
            return refus
        conn.executemany(
            "INSERT INTO ik_trajets (ik_id_opaque, date_trajet, motif, depart, destination, km, "
            "vehicule, commentaire, cree_par) VALUES (?,?,?,?,?,?,?,?,?)",
            [(ik_id, *p, acteur or None) for p in propres])
        _evenement(conn, ik_id, "TRAJETS", f"{len(propres)} trajet(s) ajouté(s)", acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "nb": len(propres)}


def ajouter_depense(ik_id: str, montant: Any, date_debit: str, nature: str, *,
                    commentaire: str = "", intervenant_id: str = "", prestation_ref: str = "",
                    justificatif_reference: str = "", acteur: str = "",
                    db_path=None) -> dict[str, Any]:
    """Part de l'IK engagée pour l'activité. Contrôle serveur : jamais au-delà du montant de l'IK
    (doublé par un déclencheur en base)."""
    m = _nombre(montant)
    if m is None or m <= 0:
        return _refus(E_MONTANT)
    m = round(m, 2)
    d = _date(date_debit)
    if d is None:
        return _refus(E_DATE)
    if nature not in NATURES_DEPENSE:
        return _refus(E_NATURE, nature)
    conn = get_db(db_path)
    try:
        refus = _ik_modifiable(conn, ik_id)
        if refus:
            return refus
        plafond = conn.execute("SELECT c.montant FROM ik JOIN charges c ON c.charge_id = ik.charge_id "
                               "WHERE ik.ik_id_opaque = ?", (ik_id,)).fetchone()[0]
        deja = conn.execute("SELECT COALESCE(SUM(montant), 0) FROM ik_depenses_activite "
                            "WHERE ik_id_opaque = ? AND actif = 1", (ik_id,)).fetchone()[0]
        if _r(deja) + m > _r(plafond) + EPS:
            return _refus(E_DEPASSEMENT, f"{_r(deja) + m:.2f} > {_r(plafond):.2f}")
        dep_id = "IKD-" + uuid.uuid4().hex[:12].upper()
        conn.execute(
            "INSERT INTO ik_depenses_activite (depense_id_opaque, ik_id_opaque, montant, date_debit, "
            "nature, commentaire, intervenant_id, prestation_ref, justificatif_reference, cree_par) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (dep_id, ik_id, m, d.isoformat(), nature, _txt(commentaire) or None,
             _txt(intervenant_id) or None, _txt(prestation_ref) or None,
             _txt(justificatif_reference) or None, acteur or None))
        _evenement(conn, ik_id, "DEPENSE_ACTIVITE",
                   f"{m:.2f} € · {NATURES_DEPENSE[nature]} · débit du {d.isoformat()}", acteur)
        conn.commit()
    except Exception as exc:  # noqa: BLE001 — le déclencheur en base a le dernier mot
        conn.rollback()
        if "IK_DEPENSES_SUPERIEURES_AU_MONTANT" in str(exc):
            return _refus(E_DEPASSEMENT)
        raise
    finally:
        conn.close()
    return {"ok": True, "depense_id_opaque": dep_id}


def retirer_ligne(ik_id: str, table: str, ligne_id: int, *, acteur: str = "",
                  db_path=None) -> dict[str, Any]:
    """Retire un trajet ou une dépense (actif → 0). Rien n'est supprimé."""
    if table not in ("trajet", "depense"):
        return _refus(E_LIGNE)
    nom = "ik_trajets" if table == "trajet" else "ik_depenses_activite"
    conn = get_db(db_path)
    try:
        refus = _ik_modifiable(conn, ik_id)
        if refus:
            return refus
        cur = conn.execute(f"UPDATE {nom} SET actif = 0 WHERE id = ? AND ik_id_opaque = ? "
                           f"AND actif = 1", (int(ligne_id), ik_id))
        if cur.rowcount != 1:
            return _refus(E_LIGNE)
        _evenement(conn, ik_id, "RETRAIT", f"{table} n° {ligne_id} retiré(e)", acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


def changer_statut(ik_id: str, statut: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT statut FROM ik WHERE ik_id_opaque = ?", (ik_id,)).fetchone()
        if r is None:
            return _refus(E_INTROUVABLE, ik_id)
        if statut not in TRANSITIONS_IK.get(r["statut"], ()):
            return _refus(E_STATUT, f"{r['statut']} → {statut}")
        conn.execute("UPDATE ik SET statut = ?, version = version + 1 WHERE ik_id_opaque = ?",
                     (statut, ik_id))
        _evenement(conn, ik_id, "STATUT", f"{r['statut']} → {statut}", acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


# ── Compte courant d'associé ────────────────────────────────────────────────────────────────────

def solde_cca(associe_id: str, *, db_path=None) -> float:
    """Solde comptable CRÉDITEUR du compte courant (455100, auxiliaire = associé) : ce que la
    société doit à l'associé. Toutes les écritures comptent — une écriture contrepassée l'est par
    une écriture inverse, qui annule son effet."""
    conn = get_db(db_path)
    try:
        if "ecriture_lignes" not in _tables(conn):
            return 0.0
        r = conn.execute("SELECT COALESCE(SUM(credit), 0) - COALESCE(SUM(debit), 0) "
                         "FROM ecriture_lignes WHERE compte = ? AND auxiliaire = ?",
                         (COMPTE_CCA, associe_id)).fetchone()
        return _r(r[0])
    finally:
        conn.close()


def _mouvements_cca(conn, associe_ids: set[str]) -> list[dict[str, Any]]:
    """Apports et remboursements CONFIRMÉS, datés par leur écriture quand elle existe."""
    out: list[dict[str, Any]] = []
    if "banque_rapprochements" in _tables(conn):
        dates = {}
        if "ecritures" in _tables(conn):
            dates = {r[0]: r[1] for r in conn.execute(
                "SELECT origine_id_opaque, date_ecriture FROM ecritures "
                "WHERE origine_type = 'RAPPROCHEMENT'")}
        for r in conn.execute(
                "SELECT rapprochement_id_opaque, type_objet, objet_id, montant_rapproche, "
                "date_creation, mouvement_id_opaque FROM banque_rapprochements "
                "WHERE statut = 'CONFIRME' AND type_objet IN ('APPORT_ASSOCIE', "
                "'REMBOURSEMENT_ASSOCIE')"):
            if r["objet_id"] not in associe_ids:
                continue
            d = dates.get(r["rapprochement_id_opaque"]) or _txt(r["date_creation"])[:10]
            out.append({"type": "APPORT" if r["type_objet"] == "APPORT_ASSOCIE" else "REMBOURSEMENT",
                        "associe_id": r["objet_id"], "date": d, "mois": d[:7],
                        "montant": _r(r["montant_rapproche"]), "source": "Banque",
                        "reference": r["rapprochement_id_opaque"]})
    if "operations_caisse" in _tables(conn):
        for r in conn.execute(
                "SELECT operation_id_opaque, date_operation, montant, tiers_id FROM operations_caisse "
                "WHERE type_operation = 'REMBOURSEMENT_ASSOCIE' AND statut IN ('VALIDE', 'ENREGISTREE')"):
            if r["tiers_id"] not in associe_ids:
                continue
            d = _txt(r["date_operation"])[:10]
            out.append({"type": "REMBOURSEMENT", "associe_id": r["tiers_id"], "date": d,
                        "mois": d[:7], "montant": _r(r["montant"]), "source": "Caisse",
                        "reference": r["operation_id_opaque"]})
    return sorted(out, key=lambda x: (x["date"], x["reference"]))


# ── Autres avantages ────────────────────────────────────────────────────────────────────────────

def _autres_avantages(conn, associe_ids: set[str]) -> list[dict[str, Any]]:
    """Charges ACTIVES marquées « avantage associé », HORS IK (une IK est comptée comme IK)."""
    if "avantage_associe" not in _colonnes(conn, "charges"):
        return []
    ik = "AND charge_id NOT IN (SELECT charge_id FROM ik)" if "ik" in _tables(conn) else ""
    out = []
    for r in conn.execute(
            f"SELECT charge_id, date_charge, mois, montant, categorie_charge_id, commentaire, "
            f"avantage_associe_id FROM charges WHERE statut = 'ACTIVE' AND avantage_associe = 'OUI' "
            f"{ik} ORDER BY date_charge, charge_id"):
        if r["avantage_associe_id"] not in associe_ids:
            continue
        d = _txt(r["date_charge"])[:10]
        out.append({**dict(r), "associe_id": r["avantage_associe_id"], "date": d,
                    "mois": _txt(r["mois"]) or d[:7], "montant": _r(r["montant"])})
    return out


# ── Suivi consolidé ─────────────────────────────────────────────────────────────────────────────

_CLES = ("ik_brute", "depenses_activite", "avantage_ik", "autres_avantages", "avantages_totaux",
         "apports_cca", "remboursements_cca", "apport_cca_net", "position_nette")


def _ligne_vide(mois: str) -> dict[str, Any]:
    return {"mois": mois, **{k: 0.0 for k in _CLES}}


def _finaliser(l: dict[str, Any]) -> dict[str, Any]:
    for k in ("ik_brute", "depenses_activite", "autres_avantages", "apports_cca",
              "remboursements_cca"):
        l[k] = _r(l[k])
    l["avantage_ik"] = _r(l["ik_brute"] - l["depenses_activite"])
    l["avantages_totaux"] = _r(l["avantage_ik"] + l["autres_avantages"])
    l["apport_cca_net"] = _r(l["apports_cca"] - l["remboursements_cca"])
    l["position_nette"] = _r(l["avantages_totaux"] - l["apport_cca_net"])
    return l


def _elements(associe_ids: set[str], *, db_path=None) -> dict[str, list[dict[str, Any]]]:
    """Tout ce qui alimente le suivi, pour les associés demandés — lecture seule."""
    ik_parts, depenses = [], []
    for ik in lister_ik(db_path=db_path):
        if ik["associe_id"] not in associe_ids:
            continue
        for p in ik["prorata"]:
            ik_parts.append({**p, "ik_id_opaque": ik["ik_id_opaque"], "charge_id": ik["charge_id"],
                             "associe_id": ik["associe_id"], "montant_global": ik["montant"],
                             "date_debut": ik["date_debut"], "date_fin": ik["date_fin"],
                             "statut": ik["statut"], "statut_libelle": ik["statut_libelle"]})
        for d in ik["depenses"]:
            depenses.append({**d, "associe_id": ik["associe_id"], "mois": d["date_debit"][:7],
                             "charge_id": ik["charge_id"]})
    conn = get_db(db_path)
    try:
        autres = _autres_avantages(conn, associe_ids)
        cca = _mouvements_cca(conn, associe_ids)
    finally:
        conn.close()
    return {"ik": ik_parts, "depenses": depenses, "autres": autres, "cca": cca}


def _dans_periode(mois: str, debut: str, fin: str) -> bool:
    return (not debut or mois >= debut) and (not fin or mois <= fin)


def suivi(*, associe_id: str = "", mois_debut: str = "", mois_fin: str = "",
          db_path=None) -> dict[str, Any]:
    """Une ligne par mois (période filtrée), le total de la période et le cumul historique."""
    tous = associes(db_path=db_path)
    ids = {associe_id} if associe_id else {a["id"] for a in tous}
    el = _elements(ids, db_path=db_path)

    par_mois: dict[str, dict[str, Any]] = {}

    def ligne(mois):
        return par_mois.setdefault(mois, _ligne_vide(mois))

    for p in el["ik"]:
        ligne(p["mois"])["ik_brute"] += p["montant"]
    for d in el["depenses"]:
        ligne(d["mois"])["depenses_activite"] += d["montant"]
    for a in el["autres"]:
        ligne(a["mois"])["autres_avantages"] += a["montant"]
    for m in el["cca"]:
        ligne(m["mois"])["apports_cca" if m["type"] == "APPORT" else "remboursements_cca"] += \
            m["montant"]

    toutes = [_finaliser(par_mois[k]) for k in sorted(par_mois)]
    lignes = [l for l in toutes if _dans_periode(l["mois"], mois_debut, mois_fin)]

    def _total(ls, mois=""):
        t = _ligne_vide(mois)
        for l in ls:
            for k in ("ik_brute", "depenses_activite", "autres_avantages", "apports_cca",
                      "remboursements_cca"):
                t[k] += l[k]
        return _finaliser(t)

    par_associe = []
    for a in tous:
        if associe_id and a["id"] != associe_id:
            continue
        s = suivi_associe_total(a["id"], el, mois_debut, mois_fin)
        par_associe.append({"associe_id": a["id"], "nom": a["nom"], **s,
                            "solde_cca": solde_cca(a["id"], db_path=db_path)})
    return {
        "associes": tous,
        "associe_id": associe_id,
        "mois_debut": mois_debut,
        "mois_fin": mois_fin,
        "lignes": lignes,
        "total_periode": _total(lignes),
        "cumul_historique": _total(toutes),
        "par_associe": par_associe,
        "nb_ik": len({p["ik_id_opaque"] for p in el["ik"]}),
    }


def suivi_associe_total(aid: str, el: dict[str, list], mois_debut: str, mois_fin: str) -> dict:
    t = _ligne_vide("")
    for p in el["ik"]:
        if p["associe_id"] == aid and _dans_periode(p["mois"], mois_debut, mois_fin):
            t["ik_brute"] += p["montant"]
    for d in el["depenses"]:
        if d["associe_id"] == aid and _dans_periode(d["mois"], mois_debut, mois_fin):
            t["depenses_activite"] += d["montant"]
    for x in el["autres"]:
        if x["associe_id"] == aid and _dans_periode(x["mois"], mois_debut, mois_fin):
            t["autres_avantages"] += x["montant"]
    for m in el["cca"]:
        if m["associe_id"] == aid and _dans_periode(m["mois"], mois_debut, mois_fin):
            t["apports_cca" if m["type"] == "APPORT" else "remboursements_cca"] += m["montant"]
    return _finaliser(t)


def detail_mois(mois: str, *, associe_id: str = "", db_path=None) -> dict[str, Any]:
    """Tout ce qui compose la ligne d'un mois — chaque montant renvoie à sa source."""
    tous = associes(db_path=db_path)
    ids = {associe_id} if associe_id else {a["id"] for a in tous}
    el = _elements(ids, db_path=db_path)
    ik = [p for p in el["ik"] if p["mois"] == mois]
    dep = [d for d in el["depenses"] if d["mois"] == mois]
    autres = [a for a in el["autres"] if a["mois"] == mois]
    cca = [m for m in el["cca"] if m["mois"] == mois]
    ligne = _ligne_vide(mois)
    ligne["ik_brute"] = sum(p["montant"] for p in ik)
    ligne["depenses_activite"] = sum(d["montant"] for d in dep)
    ligne["autres_avantages"] = sum(a["montant"] for a in autres)
    ligne["apports_cca"] = sum(m["montant"] for m in cca if m["type"] == "APPORT")
    ligne["remboursements_cca"] = sum(m["montant"] for m in cca if m["type"] == "REMBOURSEMENT")
    return {"mois": mois, "associe_id": associe_id, "associes": tous, "ligne": _finaliser(ligne),
            "ik": ik, "depenses": dep, "autres": autres, "cca": cca,
            "soldes_cca": {a["id"]: solde_cca(a["id"], db_path=db_path) for a in tous
                           if a["id"] in ids}}
