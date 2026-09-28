"""Propositions de rapprochement des flux financiers — le moteur PROPOSE, un humain DÉCIDE.

RIEN N'EST ÉCRIT ICI. Une proposition se recalcule à chaque lecture : elle ne peut donc jamais
« traîner » en base et passer pour une décision. Seul le refus humain laisse une trace
(`flux_propositions_refusees`), pour ne pas reproposer la même combinaison.

CE QUI EST CHERCHÉ, DANS CET ORDRE (§9 du cadrage) :
  1. le montant exact, sur les RESTES à rapprocher (jamais le total d'un objet déjà en partie réglé) ;
  2. la cohérence du tiers et des références (numéro de facture dans le libellé bancaire…) ;
  3. la proximité des dates ;
  4. les combinaisons de montants : 1↔1, 1↔2, 1↔3, 2↔1, 3↔1, 2↔2 — recherche BORNÉE.

CE QUI EST EXCLU D'EMBLÉE : le mauvais sens (un encaissement ne règle pas une dette), un objet
payable hors de la source du mouvement (une charge réglée en espèces ne se rapproche pas d'un
débit bancaire), une charge hors comptabilité, une facture encore à contrôler.

LES CENTIMES. Sans correspondance exacte, un très petit écart (`ECART_MAX`) peut être PROPOSÉ —
jamais absorbé : l'écart est affiché, et son traitement est demandé à la validation.

CONFIANCE : FORTE · MOYENNE · FAIBLE, toujours avec ses raisons en clair. Quand plusieurs
propositions reposent sur la même preuve, elles descendent en FAIBLE : une preuve partagée par
plusieurs candidats n'est pas une preuve.
"""
from __future__ import annotations

import hashlib
import itertools
import json
import re
import unicodedata
from typing import Any

from app.db.connection import get_db
from app.services import flux_financiers_service as flux

FORTE = "FORTE"
MOYENNE = "MOYENNE"
FAIBLE = "FAIBLE"
RANG_CONFIANCE = {FORTE: 0, MOYENNE: 1, FAIBLE: 2}
LIBELLES_CONFIANCE = {FORTE: "Confiance forte", MOYENNE: "Confiance moyenne",
                      FAIBLE: "Confiance faible"}
BADGE_CONFIANCE = {FORTE: "success", MOYENNE: "info", FAIBLE: "warning"}

#: Écart maximal PROPOSABLE sans correspondance exacte (« quelques centimes »). Au-delà, rien
#: n'est proposé ; en deçà, l'écart est montré et son traitement exigé — jamais absorbé.
ECART_MAX = 0.10
#: Au-delà de cette distance, une date ne rapproche plus rien.
FENETRE_JOURS = 60
#: Les combinaisons (échéancier en plusieurs virements) s'étalent plus longtemps.
FENETRE_COMBO_JOURS = 120
#: Bornes de la recherche combinatoire : jamais une exploration exhaustive de la base.
MAX_CANDIDATS_COMBO = 12
MAX_OBJETS_COMBO = 3
MAX_PROPOSITIONS_PAR_MOUVEMENT = 3

EPS = flux.EPS


def _sans_accent(texte: str) -> str:
    decompose = unicodedata.normalize("NFKD", str(texte or ""))
    return "".join(c for c in decompose if not unicodedata.combining(c)).lower()


def _mots(texte: str) -> set[str]:
    return {m for m in re.split(r"[^a-z0-9]+", _sans_accent(texte)) if len(m) >= 4}


def empreinte(mouvements: list[tuple[str, str, float]], objets: list[tuple[str, str, float]]) -> str:
    """Identité d'une combinaison — la même que le lettrage garde pour son idempotence."""
    charge = json.dumps({
        "m": sorted([s, i, round(float(v), 2)] for s, i, v in mouvements),
        "o": sorted([t, i, round(float(v), 2)] for t, i, v in objets),
    }, sort_keys=True)
    return "PRP-" + hashlib.sha256(charge.encode("utf-8")).hexdigest()[:20]


def compatibles(m: dict, o: dict) -> bool:
    return (o["sens"] == m["sens"] and m["source"] in o["sources"]
            and not o.get("non_rapprochable"))


def _evaluer(ms: list[dict], os_: list[dict]) -> dict | None:
    total_m = round(sum(m["restant"] for m in ms), 2)
    total_o = round(sum(o["reste"] for o in os_), 2)
    ecart = round(total_m - total_o, 2)
    if abs(ecart) > ECART_MAX + EPS:
        return None

    raisons: list[str] = []
    exact = abs(ecart) <= EPS
    if exact:
        raisons.append("Montant exact")
    else:
        raisons.append(f"Écart de {abs(ecart):.2f} € à traiter")

    textes = " ".join(m.get("texte_recherche", "") + " " + m.get("libelle", "") for m in ms)
    texte_norm = _sans_accent(textes)
    ids_m = {m["id"] for m in ms}
    lien = any(o.get("lien_mouvement") in ids_m for o in os_)
    if lien:
        raisons.append("Charge créée depuis ce mouvement")
    reference = next((o["numero"] for o in os_ if len(o.get("numero") or "") >= 4
                      and _sans_accent(o["numero"]) in texte_norm), "")
    if reference:
        raisons.append(f"Référence « {reference} » dans le libellé")
    tiers = next((o["tiers"] for o in os_ if o.get("tiers")
                  and _mots(o["tiers"]) & _mots(textes)), "")
    if tiers:
        raisons.append(f"Tiers concordant ({tiers})")

    # Écart de date : pour chaque objet, le mouvement le plus proche.
    ecarts = []
    for o in os_:
        js = [flux.jours_entre(o["date"], m["date"]) for m in ms]
        js = [j for j in js if j is not None]
        if js:
            ecarts.append(min(js))
    ecart_jours = max(ecarts) if ecarts else None
    combinaison = len(ms) > 1 or len(os_) > 1
    fenetre = FENETRE_COMBO_JOURS if combinaison else FENETRE_JOURS
    if ecart_jours is not None and ecart_jours > fenetre and not (lien or reference):
        return None
    proche = ecart_jours is not None and ecart_jours <= 7
    if proche:
        raisons.append(f"Dates proches ({ecart_jours} j)")
    if combinaison:
        raisons.append(f"Combinaison {len(ms)} mouvement(s) ↔ {len(os_)} objet(s)")

    preuve_forte = lien or bool(reference)
    if exact and (preuve_forte or (tiers and not combinaison)):
        confiance = FORTE
    elif exact and (tiers or (proche and not combinaison)):
        confiance = MOYENNE
    elif not exact and (preuve_forte or tiers):
        confiance = MOYENNE
    else:
        confiance = FAIBLE
    score = (3 if exact else 1) + (5 if lien else 0) + (3 if reference else 0) + \
        (2 if tiers else 0) + (1 if proche else 0)

    mouvements = [{"source": m["source"], "id": m["id"], "montant": m["restant"],
                   "libelle": m["libelle"], "date": m["date"], "date_fr": m["date_fr"]}
                  for m in ms]
    objets = [{"type": o["type"], "id": o["id"], "montant": o["reste"], "libelle": o["libelle"],
               "tiers": o.get("tiers", ""), "date_fr": o["date_fr"],
               "type_libelle": o["type_libelle"]} for o in os_]
    return {
        "empreinte": empreinte([(m["source"], m["id"], m["restant"]) for m in ms],
                               [(o["type"], o["id"], o["reste"]) for o in os_]),
        "mouvements": mouvements, "objets": objets,
        "forme": f"{len(ms)} ↔ {len(os_)}",
        "total_mouvements": total_m, "total_objets": total_o, "ecart": ecart,
        "exact": exact, "confiance": confiance,
        "confiance_libelle": LIBELLES_CONFIANCE[confiance],
        "confiance_badge": BADGE_CONFIANCE[confiance],
        "raisons": raisons, "score": score,
    }


def refus_actifs(*, db_path=None) -> set[str]:
    conn = get_db(db_path)
    try:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                            "AND name='flux_propositions_refusees'").fetchone():
            return set()
        return {r[0] for r in conn.execute(
            "SELECT empreinte FROM flux_propositions_refusees WHERE actif=1")}
    finally:
        conn.close()


def _proches(reference: dict, pool: list[dict], cle_date: str = "date") -> list[dict]:
    def distance(x):
        j = flux.jours_entre(reference["date"], x[cle_date])
        return 10_000 if j is None else j
    return sorted(pool, key=distance)[:MAX_CANDIDATS_COMBO]


def calculer(mouvements: list[dict], objets: list[dict], *, refus: set[str] | None = None) -> list[dict]:
    """Le cœur, sans base : testable sur des listes construites à la main."""
    refus = refus or set()
    mvts = [m for m in mouvements if m.get("lettrable", True) and m.get("restant", 0) > EPS]
    objs = [o for o in objets if not o.get("non_rapprochable") and o.get("reste", 0) > EPS]
    props: list[dict] = []

    # ── 1 ↔ 1 ────────────────────────────────────────────────────────────────────────────────
    for m in mvts:
        for o in objs:
            if compatibles(m, o):
                e = _evaluer([m], [o])
                if e:
                    props.append(e)
    exacts_m = {p["mouvements"][0]["id"] for p in props if p["exact"]}
    exacts_o = {(p["objets"][0]["type"], p["objets"][0]["id"]) for p in props if p["exact"]}

    # ── 1 mouvement ↔ 2 ou 3 objets (un virement règle plusieurs factures) ───────────────────
    for m in mvts:
        if m["id"] in exacts_m:
            continue
        pool = [o for o in objs if compatibles(m, o) and (o["type"], o["id"]) not in exacts_o
                and o["reste"] < m["restant"] + ECART_MAX]
        for taille in range(2, MAX_OBJETS_COMBO + 1):
            for combo in itertools.combinations(_proches(m, pool), taille):
                # Des objets de même nature, et du même tiers quand il est connu : un virement
                # unique ne paie pas un loyer, une facture de ménage et un propriétaire à la fois.
                if len({o["type"] for o in combo}) > 1:
                    continue
                tiers = {o["tiers_id"] for o in combo if o.get("tiers_id")}
                if len(tiers) > 1:
                    continue
                e = _evaluer([m], list(combo))
                if e:
                    props.append(e)

    # ── 2 ou 3 mouvements ↔ 1 objet (échéancier : 300 + 300 + 400 pour 1 000) ────────────────
    for o in objs:
        if (o["type"], o["id"]) in exacts_o:
            continue
        pool = [m for m in mvts if compatibles(m, o) and m["id"] not in exacts_m
                and m["restant"] < o["reste"] + ECART_MAX]
        for taille in range(2, MAX_OBJETS_COMBO + 1):
            for combo in itertools.combinations(_proches(o, pool), taille):
                if len({m["source"] for m in combo}) > 1:
                    continue
                e = _evaluer(list(combo), [o])
                if e:
                    props.append(e)

    # ── 2 mouvements ↔ 2 objets ──────────────────────────────────────────────────────────────
    libres_m = [m for m in mvts if m["id"] not in exacts_m][:MAX_CANDIDATS_COMBO]
    libres_o = [o for o in objs if (o["type"], o["id"]) not in exacts_o][:MAX_CANDIDATS_COMBO * 2]
    for pm in itertools.combinations(libres_m, 2):
        if pm[0]["sens"] != pm[1]["sens"] or pm[0]["source"] != pm[1]["source"]:
            continue
        somme_m = pm[0]["restant"] + pm[1]["restant"]
        pool = [o for o in libres_o if compatibles(pm[0], o) and o["reste"] < somme_m]
        for po in itertools.combinations(pool[:MAX_CANDIDATS_COMBO], 2):
            if po[0]["type"] != po[1]["type"]:
                continue
            if abs(somme_m - po[0]["reste"] - po[1]["reste"]) > ECART_MAX + EPS:
                continue
            e = _evaluer(list(pm), list(po))
            if e:
                props.append(e)

    # ── Refus humains, doublons, ambiguïtés ──────────────────────────────────────────────────
    vues: dict[str, dict] = {}
    for p in props:
        if p["empreinte"] in refus:
            continue
        vues.setdefault(p["empreinte"], p)
    props = list(vues.values())
    _signaler_ambiguites(props)
    props.sort(key=lambda p: (RANG_CONFIANCE[p["confiance"]], -p["score"],
                              len(p["mouvements"]) + len(p["objets"]), p["empreinte"]))

    # Au plus N propositions par mouvement : la liste est une aide, pas un inventaire.
    compte: dict[str, int] = {}
    retenues = []
    for p in props:
        ids = [m["id"] for m in p["mouvements"]]
        if any(compte.get(i, 0) >= MAX_PROPOSITIONS_PAR_MOUVEMENT for i in ids):
            continue
        for i in ids:
            compte[i] = compte.get(i, 0) + 1
        retenues.append(p)
    return retenues


def _signaler_ambiguites(props: list[dict]) -> None:
    """Deux propositions 1↔1 qui visent le même mouvement (ou le même objet) avec le même score :
    rien ne les départage. Elles descendent en FAIBLE, et l'écran le dit."""
    par_element: dict[str, list[dict]] = {}
    for p in props:
        if p["forme"] != "1 ↔ 1":
            continue
        par_element.setdefault("M:" + p["mouvements"][0]["id"], []).append(p)
        o = p["objets"][0]
        par_element.setdefault(f"O:{o['type']}:{o['id']}", []).append(p)
    for groupe in par_element.values():
        if len(groupe) < 2:
            continue
        meilleur = max(g["score"] for g in groupe)
        ex_aequo = [g for g in groupe if g["score"] == meilleur]
        if len(ex_aequo) < 2:
            continue
        for g in ex_aequo:
            if g["confiance"] != FAIBLE:
                g["confiance"] = FAIBLE
                g["confiance_libelle"] = LIBELLES_CONFIANCE[FAIBLE]
                g["confiance_badge"] = BADGE_CONFIANCE[FAIBLE]
            if not any("équivalentes" in r for r in g["raisons"]):
                g["raisons"].append("Plusieurs propositions équivalentes : contrôle humain nécessaire")


def propositions(*, mouvement_id: str = "", db_path=None) -> list[dict]:
    """Propositions sur l'état courant de la base. N'écrit rien, ne décide rien."""
    mvts = flux.mouvements(avec_propositions=False, db_path=db_path)
    objs = flux.objets(db_path=db_path)
    props = calculer(mvts, objs, refus=refus_actifs(db_path=db_path))
    if mouvement_id:
        props = [p for p in props if any(m["id"] == mouvement_id for m in p["mouvements"])]
    return props


def proposition(empreinte_: str, *, db_path=None) -> dict | None:
    return next((p for p in propositions(db_path=db_path) if p["empreinte"] == empreinte_), None)
