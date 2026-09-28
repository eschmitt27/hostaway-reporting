"""Lettrage des flux financiers — rapprocher ET comptabiliser, en une seule décision humaine.

CE QUE FAIT UN LETTRAGE, ET RIEN D'AUTRE
  · relie N mouvements (banque ou caisse) à M objets métier (1↔1, 1↔N, N↔1, N↔M) ;
  · montre, AVANT la validation, l'écriture comptable qui va constater l'opération — comptes,
    sens, auxiliaires — et laisse l'utilisateur la corriger ;
  · à la validation, écrit DANS UNE SEULE TRANSACTION : le lettrage, les liens de rapprochement
    (`banque_rapprochements`), le règlement canonique de l'objet (règlement fournisseur,
    encaissement propriétaire) et l'écriture. Si l'écriture est refusée, RIEN n'est gardé : un
    rapprochement « validé » sans son écriture n'existe pas.

CE QU'IL NE FAIT JAMAIS
  · valider tout seul : il n'y a pas d'autre point d'entrée qu'une action humaine nommée ;
  · recréer une charge déjà constatée. Une facture fournisseur VALIDÉE a déjà son écriture
    d'achat (6xx / 401) : son paiement ne produit que 401 / 512. Une facture propriétaire ÉMISE a
    déjà sa vente (411 / 706) : son encaissement ne produit que 512 / 411 ;
  · inventer une dette : une charge payée directement par la banque se comptabilise 6xx / 512,
    sans 401. Si l'utilisateur choisit réellement un 401, un fournisseur nommé est exigé ;
  · absorber un écart : il est affiché, et son traitement (solde laissé ouvert ou écart
    comptabilisé sur un compte CHOISI) est une décision explicite ;
  · écrire vers la banque. Qonto reste en lecture seule : tout ceci se passe dans SQLite.

AUCUN SECOND MOTEUR COMPTABLE : l'insertion passe par `comptabilite_ecritures_service`
(équilibre, période close, comptes actifs, événements) ; le règlement fournisseur par
`reglements_fournisseurs_service` ; l'encaissement propriétaire par
`proprietaires_tresorerie_service` ; le compte d'une charge par `comptabilite_mappings_service`.
"""
from __future__ import annotations

import json
import logging
import re
import unicodedata
import uuid
from datetime import datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import flux_financiers_service as flux
from app.services import flux_matching_service as matching

log = logging.getLogger(__name__)

EPS = flux.EPS

SOLDE_OUVERT = "SOLDE_OUVERT"
COMPTABILISE = "COMPTABILISE"
TRAITEMENTS_ECART = (SOLDE_OUVERT, COMPTABILISE)
LIBELLES_TRAITEMENT_ECART = {
    SOLDE_OUVERT: "Laisser l'écart ouvert (règlement partiel, rien n'est absorbé)",
    COMPTABILISE: "Comptabiliser l'écart sur un compte choisi",
}

ROLE_TRESORERIE = "TRESORERIE"
ROLE_OBJET = "OBJET"
ROLE_ECART = "ECART"
ROLE_SAISIE = "SAISIE"

COMPTE_FOURNISSEURS = "401000"
COMPTE_PROPRIETAIRES = "411000"
COMPTE_ASSOCIES = "455100"

E_SELECTION = "L01_SELECTION_INCOMPLETE"
E_MOUVEMENT = "L02_MOUVEMENT_NON_LETTRABLE"
E_MELANGE = "L03_MOUVEMENTS_INCOMPATIBLES"
E_OBJET = "L04_OBJET_NON_RAPPROCHABLE"
E_SENS = "L05_SENS_INCOMPATIBLE"
E_PERIODE = "L06_PERIODE_CLOTUREE"
E_ECART = "L07_TRAITEMENT_ECART_REQUIS"
E_COMPTE_ECART = "L08_COMPTE_ECART_INVALIDE"
E_ACTEUR = "L09_ACTEUR_OBLIGATOIRE"
E_VERROU = "L10_ECRITURES_DESACTIVEES"
E_ECRITURE = "L11_ECRITURE_INVALIDE"
E_DOUBLE = "L12_DOUBLE_COMPTABILISATION"
E_AUXILIAIRE = "L13_AUXILIAIRE_OBLIGATOIRE"
E_ECHEC = "L14_ECHEC_COMPTABILISATION"
E_CONCURRENCE = "L15_MOUVEMENT_DEJA_AFFECTE"
E_INTROUVABLE = "L16_LETTRAGE_INTROUVABLE"
E_MOTIF = "L17_MOTIF_OBLIGATOIRE"
E_DOUBLON_SELECTION = "L18_ELEMENT_EN_DOUBLE"
E_COMPTE_A_DEFINIR = "L19_COMPTE_DE_CHARGE_A_DEFINIR"

E_NOM_REQUIS = "F01_NOM_OBLIGATOIRE"
E_NOM_GENERIQUE = "F02_NOM_GENERIQUE_INTERDIT"
E_IDENTIFIANT = "F03_IDENTIFIANT_LEGAL_INVALIDE"
E_DOUBLON = "F04_FOURNISSEUR_DEJA_CONNU"


class _Echec(Exception):
    def __init__(self, code: str, message: str, detail: str = ""):
        super().__init__(message)
        self.code, self.message, self.detail = code, message, detail


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _r(v: Any) -> float:
    return flux._r(v)


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _nombre(v: Any) -> float:
    t = _txt(v).replace(" ", "").replace(" ", "").replace(",", ".")
    if not t:
        return 0.0
    try:
        return round(float(t), 2)
    except ValueError:
        return float("nan")


# ══ Verrous d'écriture ════════════════════════════════════════════════════════════════════════

def verrous_fermes(source: str, types_objets: set[str]) -> list[str]:
    """Ce qui empêcherait la validation d'écrire, dit AVANT le clic. Mêmes leviers que les
    services canoniques : aucun verrou n'est créé ni contourné ici."""
    from app.services import qonto_validation_service as qv
    fermes = []
    if source == flux.BANQUE and not qv._verrou_banque():
        fermes.append(qv.MESSAGES[qv.E_VERROU_BANQUE])
    if not qv._verrou_comptabilite():
        fermes.append(qv.MESSAGES[qv.E_VERROU_COMPTA])
    if flux.FACTURE_FOURNISSEUR in types_objets and not (
            getattr(cfg, "FACTURES_REAL_WRITE_ENABLED", False)
            and getattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", False)):
        fermes.append("Le règlement d'une facture fournisseur écrit un règlement : renseignez "
                      "FACTURES_REAL_WRITE_ENABLED et FACTURES_REAL_WRITE_CONFIRMATION_ENABLED "
                      "dans « .env », puis redémarrez.")
    return fermes


# ══ Préparation : ventilation + écriture proposée ═════════════════════════════════════════════

def _split(cle: str) -> tuple[str, str]:
    t, _, i = _txt(cle).partition(":")
    return t.upper(), i.strip()


def _charges(ids: list[str], db_path=None) -> dict[str, dict]:
    if not ids:
        return {}
    conn = get_db(db_path)
    try:
        marques = ",".join("?" * len(ids))
        return {r["charge_id"]: dict(r) for r in conn.execute(
            f"SELECT * FROM charges WHERE charge_id IN ({marques})", ids)}
    finally:
        conn.close()


def _allouer(mvts: list[dict], objs: list[dict], montant_m: dict, montant_o: dict) -> list[dict]:
    """Ventilation déterministe mouvement → objet : dans l'ordre des dates, chacun consomme le
    suivant. Les sommes s'équilibrent par construction ; aucune part n'est inventée."""
    reste_m = [[m, montant_m[m["id"]]] for m in sorted(mvts, key=lambda x: (x["date"], x["id"]))]
    reste_o = [[o, montant_o[o["cle"]]] for o in sorted(objs, key=lambda x: (x["date"], x["cle"]))]
    aretes = []
    i = j = 0
    while i < len(reste_m) and j < len(reste_o):
        part = _r(min(reste_m[i][1], reste_o[j][1]))
        if part > EPS:
            aretes.append({"mouvement": reste_m[i][0], "objet": reste_o[j][0], "montant": part})
        reste_m[i][1] = _r(reste_m[i][1] - part)
        reste_o[j][1] = _r(reste_o[j][1] - part)
        if reste_m[i][1] <= EPS:
            i += 1
        if reste_o[j][1] <= EPS:
            j += 1
    return aretes


COMPTE_A_DEFINIR = ""
LIBELLE_COMPTE_A_DEFINIR = "Compte comptable à définir"


def compte_de_charge(charge: dict, *, db_path=None) -> tuple[str, str]:
    """Compte de charge PROPOSÉ pour une charge : (compte, avertissement).

    Source canonique unique : les règles VALIDÉES de `mapping_comptable_regles` (Comptabilité ›
    Mappings), résolues par `comptabilite_mappings_service.resoudre_compte` — catégorie d'abord,
    type de flux ensuite. Aucun numéro de compte n'est inventé ici.

    Pas de règle validée, ou règle pointant vers un compte absent/inactif du plan comptable :
    le compte reste À DÉFINIR. Le filet provisoire générique (606000) n'est JAMAIS repris en
    silence — l'utilisateur choisit lui-même un compte de charge actif avant de valider."""
    from app.services import comptabilite_mappings_service as maps
    resolu = maps.resoudre_compte(categorie_charge_id=_txt(charge.get("categorie_charge_id")),
                                  type_flux_id=_txt(charge.get("type_flux_id")),
                                  date_reference=_txt(charge.get("date_charge"))[:10],
                                  db_path=db_path)
    actifs = {c["compte"] for c in flux.comptes_actifs(db_path=db_path)}
    if resolu["statut"] == maps.ST_VALIDE and resolu["compte"] in actifs             and resolu["compte"].startswith("6"):
        return resolu["compte"], ""
    if resolu["statut"] == maps.ST_VALIDE:
        return COMPTE_A_DEFINIR, (f"{LIBELLE_COMPTE_A_DEFINIR} : la règle de mapping désigne le "
                                  f"compte {resolu['compte']}, absent, inactif ou qui n'est pas un "
                                  "compte de charge. Choisissez un compte de charge actif.")
    return COMPTE_A_DEFINIR, (f"{LIBELLE_COMPTE_A_DEFINIR} : aucune règle de mapping validée pour "
                              "cette catégorie (Comptabilité › Mappings). Choisissez un compte de "
                              "charge actif avant de valider.")


def _ligne_objet(o: dict, montant: float, sens: str, charges: dict, db_path=None) -> dict:
    """La contrepartie d'un objet — celle que le modèle comptable existant désigne."""
    sortie = sens == flux.SORTIE
    base = {"debit": montant if sortie else 0.0, "credit": 0.0 if sortie else montant,
            "role": ROLE_OBJET, "objet": o["cle"], "logement_id": None, "proprietaire_id": None,
            "auxiliaire": None, "avertissement": ""}
    if o["type"] == flux.CHARGE:
        c = charges.get(o["id"], {})
        compte, avertissement = compte_de_charge(c, db_path=db_path)
        base.update(compte=compte, libelle=o["libelle"], avertissement=avertissement,
                    logement_id=c.get("logement_id") or None,
                    proprietaire_id=c.get("proprietaire_id") or None)
        return base
    if o["type"] in (flux.FACTURE_FOURNISSEUR, flux.REGLEMENT_FOURNISSEUR):
        base.update(compte=COMPTE_FOURNISSEURS, auxiliaire=o["tiers_id"],
                    libelle=f"Règlement {o['libelle']}".strip())
        return base
    # Propriétaire : créance (facture émise, encaissement reçu) ou reversement.
    base.update(compte=COMPTE_PROPRIETAIRES, auxiliaire=o["tiers_id"],
                proprietaire_id=o["tiers_id"] or None,
                libelle=(f"Encaissement {o['libelle']}" if not sortie
                         else f"Reversement {o['libelle']}").strip())
    return base


def preparer(selection_m: list[str], selection_o: list[str], *, traitement_ecart: str = "",
             compte_ecart: str = "", db_path=None) -> dict[str, Any]:
    """Tout ce que l'écran de validation montre AVANT que l'utilisateur tranche. N'écrit rien."""
    erreurs: list[dict] = []
    avertissements: list[str] = []

    def err(code, message):
        erreurs.append({"code": code, "message": message})

    sel_m = [_split(x) for x in selection_m if _txt(x)]
    sel_o = [_split(x) for x in selection_o if _txt(x)]
    if not sel_m or not sel_o:
        err(E_SELECTION, "Sélectionnez au moins un mouvement et au moins un objet à rapprocher.")
    if len(set(sel_m)) != len(sel_m) or len(set(sel_o)) != len(sel_o):
        err(E_DOUBLON_SELECTION, "Un même élément ne peut pas figurer deux fois dans un rapprochement.")

    index_m = {(m["source"], m["id"]): m for m in flux.mouvements(avec_propositions=False,
                                                                 db_path=db_path)}
    index_o = {(o["type"], o["id"]): o for o in flux.objets(db_path=db_path,
                                                           inclure_non_rapprochables=True)}
    mvts: list[dict] = []
    for cle in sel_m:
        m = index_m.get(cle)
        if m is None:
            err(E_MOUVEMENT, "Mouvement introuvable.")
            continue
        if m.get("nature") == "RETRAIT_ESPECES":
            err(E_MOUVEMENT, f"{m['libelle']} : un retrait d'espèces se traite par le transfert "
                             "Banque → Caisse, pas par un rapprochement.")
        elif m["sans_effet"]:
            err(E_MOUVEMENT, f"{m['libelle']} : opération sans effet (refusée ou contrepassée par la banque).")
        elif not m["definitif"]:
            err(E_MOUVEMENT, f"{m['libelle']} : opération encore en attente chez la banque — "
                             "son montant peut changer, elle ne peut pas être comptabilisée.")
        elif m.get("restant", 0) <= EPS:
            err(E_MOUVEMENT, f"{m['libelle']} : déjà rapproché en totalité.")
        else:
            motif = flux.mois_cloture(m["date"][:7], db_path=db_path)
            if motif:
                err(E_PERIODE, f"{m['libelle']} : {motif} Aucun rapprochement ne peut la modifier "
                               "sans passer par le workflow de réouverture existant.")
        mvts.append(m)

    sources = {m["source"] for m in mvts}
    sens_set = {m["sens"] for m in mvts}
    if len(sources) > 1 or len(sens_set) > 1:
        err(E_MELANGE, "Les mouvements d'un même rapprochement doivent venir de la même source "
                       "(banque ou caisse) et aller dans le même sens.")
    source = next(iter(sources), flux.BANQUE)
    sens = next(iter(sens_set), flux.SORTIE)

    objs: list[dict] = []
    for cle in sel_o:
        o = index_o.get(cle)
        if o is None:
            # Une charge hors comptabilité est absente des objets rapprochables : on le dit.
            if cle[0] == flux.CHARGE:
                c = _charges([cle[1]], db_path).get(cle[1])
                if c and _txt(c.get("prise_en_compta")).upper() == "NON":
                    err(E_OBJET, "Une charge hors comptabilité ne peut pas être liée à un "
                                 "mouvement réel de la société : ce mouvement doit finir "
                                 "comptabilisé.")
                    continue
            err(E_OBJET, "Objet introuvable ou déjà entièrement réglé.")
            continue
        if o.get("non_rapprochable"):
            err(E_OBJET, f"{o['libelle']} : {o['non_rapprochable']}")
        elif mvts and not matching.compatibles({"sens": sens, "source": source}, o):
            err(E_SENS, f"{o['libelle']} : ne peut pas être réglé par ce mouvement "
                        f"({'encaissement' if sens == flux.ENTREE else 'décaissement'} "
                        f"{'bancaire' if source == flux.BANQUE else 'de caisse'}).")
        objs.append(o)

    total_m = _r(sum(m.get("restant", 0) for m in mvts))
    total_o = _r(sum(o.get("reste", 0) for o in objs))
    ecart = _r(total_m - total_o)
    traitement = traitement_ecart if traitement_ecart in TRAITEMENTS_ECART else ""
    compte_ecart = _txt(compte_ecart)
    periodes = sorted({m["date"][:7] for m in mvts})

    if abs(ecart) > EPS and traitement == COMPTABILISE:
        comptes = {c["compte"]: c for c in flux.comptes_actifs(db_path=db_path)}
        if compte_ecart not in comptes:
            err(E_COMPTE_ECART, "Choisissez un compte actif du plan comptable pour l'écart.")
        elif compte_ecart in (flux.TRESORERIE_PAR_SOURCE.values()) or \
                compte_ecart.startswith(("401", "411", "455")):
            err(E_COMPTE_ECART, "Un écart ne se comptabilise ni sur un compte de trésorerie, ni "
                                "sur un compte de tiers : choisissez un compte de charge ou de produit.")
        if len(periodes) > 1:
            err(E_COMPTE_ECART, "Comptabiliser un écart suppose des mouvements du même mois.")

    # ── Montants engagés ─────────────────────────────────────────────────────────────────────
    ecart_comptabilise = abs(ecart) > EPS and traitement == COMPTABILISE
    montant_m = {m["id"]: m.get("restant", 0) for m in mvts}
    montant_o = {o["cle"]: o.get("reste", 0) for o in objs}
    if abs(ecart) > EPS and not ecart_comptabilise:
        # Solde ouvert : seul le plus petit des deux côtés est engagé ; le reste demeure ouvert.
        cible = min(total_m, total_o)
        if total_m > total_o:
            montant_m = _repartir(mvts, montant_m, cible, cle="id")
        else:
            montant_o = _repartir(objs, montant_o, cible, cle="cle")
    aretes = _allouer(mvts, objs, montant_m, montant_o) if mvts and objs else []
    if ecart_comptabilise and ecart > 0:
        # Surplus de mouvement : il est rattaché à l'écart, pas à un objet.
        consomme: dict[str, float] = {}
        for a in aretes:
            consomme[a["mouvement"]["id"]] = consomme.get(a["mouvement"]["id"], 0) + a["montant"]
        for m in mvts:
            surplus = _r(montant_m[m["id"]] - consomme.get(m["id"], 0))
            if surplus > EPS:
                aretes.append({"mouvement": m, "objet": None, "montant": surplus})

    # ── Écritures proposées : une par mois de mouvement ──────────────────────────────────────
    charges = _charges([o["id"] for o in objs if o["type"] == flux.CHARGE], db_path)
    ecritures = []
    for periode in periodes:
        du_mois = [m for m in mvts if m["date"][:7] == periode]
        ids = {m["id"] for m in du_mois}
        lignes = []
        for m in sorted(du_mois, key=lambda x: (x["date"], x["id"])):
            montant = _r(montant_m.get(m["id"], 0))
            if montant <= EPS:
                continue
            entree = sens == flux.ENTREE
            lignes.append({"compte": flux.TRESORERIE_PAR_SOURCE[source], "auxiliaire": None,
                           "debit": montant if entree else 0.0, "credit": 0.0 if entree else montant,
                           "libelle": f"{m['libelle']} — {m['date_fr']}", "role": ROLE_TRESORERIE,
                           "objet": f"{source}:{m['id']}", "logement_id": None,
                           "proprietaire_id": None, "avertissement": ""})
        par_objet: dict[str, float] = {}
        for a in aretes:
            if a["objet"] is not None and a["mouvement"]["id"] in ids:
                par_objet[a["objet"]["cle"]] = _r(par_objet.get(a["objet"]["cle"], 0) + a["montant"])
        if ecart_comptabilise and ecart < 0:
            # Objets réglés au-delà de ce que le mouvement apporte : le complément va à l'objet.
            for o in objs:
                engage = sum(a["montant"] for a in aretes if a["objet"] is o)
                complement = _r(montant_o[o["cle"]] - engage)
                if complement > EPS:
                    par_objet[o["cle"]] = _r(par_objet.get(o["cle"], 0) + complement)
        for o in objs:
            if par_objet.get(o["cle"], 0) > EPS:
                ligne = _ligne_objet(o, par_objet[o["cle"]], sens, charges, db_path)
                if ligne["avertissement"]:
                    avertissements.append(ligne["avertissement"])
                lignes.append(ligne)
        if ecart_comptabilise:
            # Sortie payée en plus / entrée reçue en moins : c'est un coût (débit). Sinon un gain.
            debit_ecart = (sens == flux.SORTIE and ecart > 0) or (sens == flux.ENTREE and ecart < 0)
            lignes.append({"compte": compte_ecart, "auxiliaire": None,
                           "debit": abs(ecart) if debit_ecart else 0.0,
                           "credit": 0.0 if debit_ecart else abs(ecart),
                           "libelle": "Écart de règlement", "role": ROLE_ECART, "objet": "",
                           "logement_id": None, "proprietaire_id": None, "avertissement": ""})
        date_ecr = max(m["date"] for m in du_mois)
        ecritures.append({
            "periode": periode, "date": date_ecr, "date_fr": flux.date_fr(date_ecr),
            "journal": flux.JOURNAL_PAR_SOURCE[source],
            "libelle": _libelle_ecriture(objs, sens),
            "lignes": lignes,
            "total_debit": _r(sum(l["debit"] for l in lignes)),
            "total_credit": _r(sum(l["credit"] for l in lignes)),
        })

    if abs(ecart) > EPS and not traitement:
        avertissements.append(
            f"Écart de {abs(ecart):.2f} € entre les mouvements et les objets : choisissez son "
            "traitement avant de valider. Rien n'est absorbé en silence.")
    for o in objs:
        if o["type"] == flux.CHARGE and _txt(o.get("statut_controle")) not in ("VALIDE",):
            avertissements.append(f"{o['libelle']} : charge encore à contrôler dans le module Charges.")
        if o["type"] == flux.FACTURE_PROPRIETAIRE:
            avertissements.append("L'encaissement d'un propriétaire s'impute sur ses factures par "
                                  "l'allocation FIFO de son compte (règle existante).")

    engage_m = {k: _r(v) for k, v in montant_m.items()}
    engage_o = {k: _r(v) for k, v in montant_o.items()}
    empreinte = matching.empreinte([(m["source"], m["id"], engage_m[m["id"]]) for m in mvts],
                                   [(o["type"], o["id"], engage_o[o["cle"]]) for o in objs])
    empreinte_selection = matching.empreinte(
        [(m["source"], m["id"], m.get("restant", 0)) for m in mvts],
        [(o["type"], o["id"], o.get("reste", 0)) for o in objs])
    types = {o["type"] for o in objs}
    return {
        "ok": not erreurs, "erreurs": erreurs, "avertissements": list(dict.fromkeys(avertissements)),
        "source": source, "sens": sens,
        "mouvements": [dict(m, engage=engage_m.get(m["id"], 0)) for m in mvts],
        "objets": [dict(o, engage=engage_o.get(o["cle"], 0)) for o in objs],
        "total_mouvements": total_m, "total_objets": total_o, "ecart": ecart,
        "ecart_a_traiter": abs(ecart) > EPS, "traitement_ecart": traitement,
        "compte_ecart": compte_ecart,
        "allocation": [{"mouvement": a["mouvement"]["libelle"], "mouvement_id": a["mouvement"]["id"],
                        "objet": (a["objet"]["libelle"] if a["objet"] else "Écart de règlement"),
                        "objet_cle": a["objet"]["cle"] if a["objet"] else "",
                        "montant": a["montant"]} for a in aretes],
        "_aretes": aretes,
        "ecritures": ecritures,
        "empreinte": empreinte, "empreinte_selection": empreinte_selection,
        "selection_m": [f"{m['source']}:{m['id']}" for m in mvts],
        "selection_o": [o["cle"] for o in objs],
        "verrous_fermes": verrous_fermes(source, types),
    }


def _repartir(elements: list[dict], montants: dict, cible: float, *, cle: str) -> dict:
    """Réduit les montants engagés à `cible`, dans l'ordre des dates (le dernier est partiel)."""
    out = {}
    reste = _r(cible)
    for e in sorted(elements, key=lambda x: (x["date"], x[cle])):
        part = _r(min(montants[e[cle]], reste))
        out[e[cle]] = part
        reste = _r(reste - part)
    return out


def _libelle_ecriture(objs: list[dict], sens: str) -> str:
    if not objs:
        return "Rapprochement"
    noms = ", ".join(dict.fromkeys(
        (o["libelle"] + (f" — {o['tiers']}" if o.get("tiers") else "")) for o in objs))
    return (("Encaissement : " if sens == flux.ENTREE else "Règlement : ") + noms)[:250]


# ══ Vérification d'une écriture (proposée ou corrigée à la main) ══════════════════════════════

def lignes_depuis_formulaire(form: dict[str, list[str]], nb_ecritures: int) -> list[list[dict]]:
    """Relit les lignes éditées. Une ligne sans compte ni montant est ignorée."""
    groupes: list[list[dict]] = [[] for _ in range(nb_ecritures)]
    champs = ("ecr", "compte", "auxiliaire", "debit", "credit", "libelle", "role", "objet",
              "logement_id", "proprietaire_id")
    colonnes = {c: form.get(f"l_{c}", []) for c in champs}
    n = max((len(v) for v in colonnes.values()), default=0)
    for i in range(n):
        v = {c: (colonnes[c][i] if i < len(colonnes[c]) else "") for c in champs}
        compte = _txt(v["compte"])
        debit, credit = _nombre(v["debit"]), _nombre(v["credit"])
        if not compte and not debit and not credit:
            continue
        try:
            g = int(v["ecr"] or 0)
        except ValueError:
            g = 0
        if 0 <= g < nb_ecritures:
            groupes[g].append({"compte": compte, "auxiliaire": _txt(v["auxiliaire"]) or None,
                               "debit": debit, "credit": credit, "libelle": _txt(v["libelle"]),
                               "role": _txt(v["role"]) or ROLE_SAISIE, "objet": _txt(v["objet"]),
                               "logement_id": _txt(v["logement_id"]) or None,
                               "proprietaire_id": _txt(v["proprietaire_id"]) or None})
    return groupes


def verifier_ecritures(prep: dict, groupes: list[list[dict]], *, db_path=None) -> list[dict]:
    """Les garde-fous d'une écriture — qu'elle vienne du moteur ou d'une correction humaine."""
    erreurs: list[dict] = []

    def err(code, message):
        erreurs.append({"code": code, "message": message})

    comptes = {c["compte"]: c for c in flux.comptes_actifs(db_path=db_path)}
    fournisseurs = {f["id"] for f in flux.fournisseurs_connus(db_path=db_path)} | {
        o["tiers_id"] for o in prep["objets"]
        if o["type"] in (flux.FACTURE_FOURNISSEUR, flux.REGLEMENT_FOURNISSEUR)}
    proprietaires = {p["id"] for p in flux.proprietaires_connus(db_path=db_path)}
    associes = {a["id"] for a in flux.associes_connus(db_path=db_path)}
    tresorerie = flux.TRESORERIE_PAR_SOURCE[prep["source"]]
    sortie = prep["sens"] == flux.SORTIE

    if len(groupes) != len(prep["ecritures"]):
        err(E_ECRITURE, "Écriture incomplète.")
        return erreurs

    for proposee, lignes in zip(prep["ecritures"], groupes):
        nom = f"Écriture du {proposee['date_fr']}"
        if not lignes:
            err(E_ECRITURE, f"{nom} : aucune ligne.")
            continue
        for l in lignes:
            if l["debit"] != l["debit"] or l["credit"] != l["credit"]:      # NaN
                err(E_ECRITURE, f"{nom} : montant illisible.")
                return erreurs
            if l["debit"] < 0 or l["credit"] < 0 or (l["debit"] > EPS and l["credit"] > EPS):
                err(E_ECRITURE, f"{nom} : chaque ligne porte soit un débit, soit un crédit positif.")
            if l["role"] == ROLE_OBJET and l["objet"].startswith(f"{flux.CHARGE}:"):
                if not l["compte"]:
                    err(E_COMPTE_A_DEFINIR, f"{nom} : {LIBELLE_COMPTE_A_DEFINIR.lower()} pour la "
                                            "charge — choisissez un compte de charge actif.")
                elif l["compte"] not in comptes or not l["compte"].startswith("6"):
                    err(E_COMPTE_A_DEFINIR, f"{nom} : la charge doit porter sur un compte de "
                                            f"charge actif (classe 6), pas « {l['compte']} ».")
            elif l["compte"] not in comptes:
                err(E_ECRITURE, f"{nom} : compte « {l['compte'] or '(vide)'} » inconnu ou inactif.")
            if l["compte"].startswith("401") and l["auxiliaire"] not in fournisseurs:
                err(E_AUXILIAIRE, f"{nom} : un compte fournisseur (401) exige un fournisseur "
                                  "nommé — choisissez-le ou créez-le.")
            if l["compte"].startswith("411") and l["auxiliaire"] not in proprietaires:
                err(E_AUXILIAIRE, f"{nom} : un compte client/propriétaire (411) exige le "
                                  "propriétaire concerné.")
            if l["compte"].startswith("455") and l["auxiliaire"] not in associes:
                err(E_AUXILIAIRE, f"{nom} : un compte d'associé (455) exige l'associé concerné.")
        total_d = _r(sum(l["debit"] for l in lignes))
        total_c = _r(sum(l["credit"] for l in lignes))
        if abs(total_d - total_c) > EPS or total_d <= EPS:
            err(E_ECRITURE, f"{nom} : déséquilibrée (débit {total_d:.2f} ≠ crédit {total_c:.2f}).")

        # La trésorerie n'est pas modifiable : elle dit ce que la banque (ou la caisse) a fait.
        attendu = _r(sum(l["debit"] - l["credit"] for l in proposee["lignes"]
                         if l["role"] == ROLE_TRESORERIE))
        saisi = _r(sum(l["debit"] - l["credit"] for l in lignes if l["compte"] == tresorerie))
        if abs(attendu - saisi) > EPS:
            err(E_ECRITURE, f"{nom} : le montant porté au compte {tresorerie} doit rester "
                            f"{abs(attendu):.2f} € — c'est le mouvement réel.")
        autres_tresorerie = [l for l in lignes if l["compte"] in flux.TRESORERIE_PAR_SOURCE.values()
                             and l["compte"] != tresorerie]
        if autres_tresorerie:
            err(E_ECRITURE, f"{nom} : un seul compte de trésorerie par rapprochement.")

        # Anti double comptabilisation : les comptes de charge/produit ne peuvent porter que les
        # charges réglées ici et l'écart choisi — jamais une facture déjà constatée en achat.
        charges_du_groupe = _r(sum(l["debit"] + l["credit"] for l in proposee["lignes"]
                                   if l["role"] == ROLE_OBJET and l["objet"].startswith("CHARGE:")))
        ecart_du_groupe = _r(sum(l["debit"] + l["credit"] for l in proposee["lignes"]
                                 if l["role"] == ROLE_ECART))
        classes_67 = _r(sum(l["debit"] + l["credit"] for l in lignes
                            if l["compte"][:1] in ("6", "7")))
        if classes_67 > charges_du_groupe + ecart_du_groupe + EPS:
            err(E_DOUBLE, f"{nom} : {classes_67:.2f} € sur des comptes de charge ou de produit, "
                          f"pour {charges_du_groupe + ecart_du_groupe:.2f} € de charges réglées ici. "
                          "Une facture déjà validée (ou émise) a déjà son écriture d'achat (ou de "
                          "vente) : son règlement ne fait que solder le compte de tiers.")

        # Les dettes et créances réglées ici doivent effectivement être soldées.
        for compte, sens_attendu, types in (
                (COMPTE_FOURNISSEURS, "debit" if sortie else "credit",
                 (flux.FACTURE_FOURNISSEUR, flux.REGLEMENT_FOURNISSEUR)),
                (COMPTE_PROPRIETAIRES, "debit" if sortie else "credit",
                 (flux.FACTURE_PROPRIETAIRE, flux.MOUVEMENT_PROPRIETAIRE))):
            du = {}
            for l in proposee["lignes"]:
                if l["role"] == ROLE_OBJET and l["compte"] == compte:
                    du[l["auxiliaire"]] = _r(du.get(l["auxiliaire"], 0) + l[sens_attendu])
            for aux, montant in du.items():
                porte = _r(sum(l[sens_attendu] for l in lignes
                               if l["compte"] == compte and l["auxiliaire"] == aux))
                if porte + EPS < montant:
                    err(E_DOUBLE, f"{nom} : le compte {compte} du tiers réglé doit être soldé de "
                                  f"{montant:.2f} € (porté : {porte:.2f} €).")
    return erreurs


def _normaliser_pour_insertion(lignes: list[dict]) -> list[dict]:
    return [{"compte": l["compte"], "auxiliaire": l.get("auxiliaire") or None,
             "debit": _r(l["debit"]), "credit": _r(l["credit"]),
             "libelle": (l.get("libelle") or "")[:250],
             "logement_id": l.get("logement_id"), "proprietaire_id": l.get("proprietaire_id")}
            for l in lignes]


def _modifiee(prep: dict, groupes: list[list[dict]]) -> bool:
    def signature(lignes):
        return sorted((l["compte"], l.get("auxiliaire") or "", _r(l["debit"]), _r(l["credit"]))
                      for l in lignes)
    return any(signature(p["lignes"]) != signature(g) for p, g in zip(prep["ecritures"], groupes))


# ══ Validation ════════════════════════════════════════════════════════════════════════════════

def valider(selection_m: list[str], selection_o: list[str], *, acteur: str,
            traitement_ecart: str = "", compte_ecart: str = "",
            lignes: list[list[dict]] | None = None, justification: str = "",
            proposition: str = "", db_path=None) -> dict[str, Any]:
    """« Valider le rapprochement et comptabiliser » — tout ou rien."""
    acteur = _txt(acteur)
    if not acteur:
        return _refus(E_ACTEUR, "Indiquez votre nom : chaque validation est signée.")

    prep = preparer(selection_m, selection_o, traitement_ecart=traitement_ecart,
                    compte_ecart=compte_ecart, db_path=db_path)
    if not prep["ok"]:
        e = prep["erreurs"][0]
        return _refus(e["code"], e["message"], erreurs=prep["erreurs"])
    if prep["ecart_a_traiter"] and not prep["traitement_ecart"]:
        return _refus(E_ECART, f"Écart de {abs(prep['ecart']):.2f} € : choisissez de le laisser "
                               "ouvert ou de le comptabiliser. Rien n'est absorbé en silence.")
    if prep["verrous_fermes"]:
        return _refus(E_VERROU, prep["verrous_fermes"][0], erreurs=[
            {"code": E_VERROU, "message": m} for m in prep["verrous_fermes"]])

    groupes = lignes if lignes is not None else [e["lignes"] for e in prep["ecritures"]]
    erreurs = verifier_ecritures(prep, groupes, db_path=db_path)
    if erreurs:
        return _refus(erreurs[0]["code"], erreurs[0]["message"], erreurs=erreurs)
    modifiee = _modifiee(prep, groupes)

    existant = _lettrage_valide_par_empreinte(prep["empreinte"], db_path=db_path)
    if existant:
        # Double clic, ou retour arrière du navigateur : la même décision n'est jamais rejouée.
        return {"ok": True, "deja_valide": True, "lettrage_id_opaque": existant}

    prop = matching.proposition(proposition, db_path=db_path) if proposition else None
    lettrage = "LET-" + uuid.uuid4().hex[:12].upper()
    crees: dict[str, list] = {"liens": [], "reglements": [], "encaissements": [],
                              "ecritures": [], "reglements_rapproches": [], "operations_caisse": [],
                              "charges": [], "qonto": []}
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _verifier_concurrence(conn, prep)
        conn.execute(
            "INSERT INTO flux_lettrages (lettrage_id_opaque, statut, source, confiance, explication, "
            "empreinte, total_mouvements, total_objets, ecart, traitement_ecart, compte_ecart, "
            "ecriture_modifiee, justification, acteur) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (lettrage, "VALIDE", "PROPOSITION" if prop else "MANUEL",
             prop["confiance"] if prop else None,
             " · ".join(prop["raisons"]) if prop else None, prep["empreinte"],
             prep["total_mouvements"], prep["total_objets"], prep["ecart"],
             prep["traitement_ecart"] or "AUCUN", prep["compte_ecart"] or None,
             1 if modifiee else 0, _txt(justification) or None, acteur))
        for m in prep["mouvements"]:
            if m["engage"] > EPS:
                conn.execute("INSERT INTO flux_lettrage_lignes (lettrage_id_opaque, cote, "
                             "type_element, element_id, montant, libelle) VALUES (?,?,?,?,?,?)",
                             (lettrage, "MOUVEMENT", m["source"], m["id"], m["engage"],
                              m["libelle"]))
        for o in prep["objets"]:
            if o["engage"] > EPS:
                conn.execute("INSERT INTO flux_lettrage_lignes (lettrage_id_opaque, cote, "
                             "type_element, element_id, montant, libelle) VALUES (?,?,?,?,?,?)",
                             (lettrage, "OBJET", o["type"], o["id"], o["engage"],
                              f"{o['libelle']} {o.get('tiers') or ''}".strip()))

        objets_rappro = _regler_objets(conn, prep, lettrage, acteur, crees, db_path=db_path)
        _deposer_liens(conn, prep, lettrage, acteur, objets_rappro, crees)
        _comptabiliser(conn, prep, groupes, lettrage, acteur, crees, db_path=db_path)
        _marquer_mouvements(conn, prep, lettrage, acteur, crees)

        conn.execute("UPDATE flux_lettrages SET ecriture_id_opaque=? WHERE lettrage_id_opaque=?",
                     (",".join(crees["ecritures"]), lettrage))
        conn.execute("INSERT INTO flux_lettrage_evenements (lettrage_id_opaque, type_evenement, "
                     "detail_json, acteur) VALUES (?,?,?,?)",
                     (lettrage, "VALIDATION", json.dumps(crees, ensure_ascii=False), acteur))
        conn.commit()
    except _Echec as exc:
        conn.rollback()
        return _refus(exc.code, exc.message, detail=exc.detail)
    except Exception as exc:      # noqa: BLE001 — tout échec annule TOUT, et se dit sans secret
        conn.rollback()
        log.exception("Lettrage %s annulé", lettrage)
        return _refus(E_ECHEC, "La comptabilisation a échoué : rien n'a été enregistré "
                               "(rapprochement non validé).", detail=type(exc).__name__)
    finally:
        conn.close()
    return {"ok": True, "lettrage_id_opaque": lettrage, "ecritures": crees["ecritures"],
            "ecriture_modifiee": modifiee}


def _refus(code: str, message: str, *, erreurs: list | None = None, detail: str = "") -> dict:
    return {"ok": False, "code": code, "message": message,
            "erreurs": erreurs or [{"code": code, "message": message}], "detail": detail}


def _lettrage_valide_par_empreinte(empreinte: str, *, db_path=None) -> str | None:
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT lettrage_id_opaque FROM flux_lettrages WHERE empreinte=? "
                         "AND statut='VALIDE'", (empreinte,)).fetchone()
        return r[0] if r else None
    finally:
        conn.close()


def _verifier_concurrence(conn, prep: dict) -> None:
    """Relu SOUS le verrou d'écriture : un autre onglet a pu affecter le même mouvement entre
    l'affichage et le clic. Rien n'est écrit dans ce cas."""
    for m in prep["mouvements"]:
        deja = conn.execute(
            "SELECT COALESCE(SUM(montant_rapproche), 0) FROM banque_rapprochements "
            "WHERE mouvement_id_opaque=? AND statut='CONFIRME'", (m["id"],)).fetchone()[0]
        if _r(deja) + m["engage"] > m["montant"] + EPS:
            raise _Echec(E_CONCURRENCE, f"{m['libelle']} : ce mouvement vient d'être affecté "
                                        "ailleurs. Rechargez la page.")


def _regler_objets(conn, prep, lettrage, acteur, crees, *, db_path=None) -> dict[str, tuple]:
    """Le règlement canonique de chaque objet, dans la transaction. Retourne, pour chaque objet,
    le (type_objet, objet_id) à inscrire dans `banque_rapprochements`."""
    from app.services import proprietaires_tresorerie_service as tres
    from app.services import reglements_fournisseurs_service as regl

    source = prep["source"]
    date_regl = max(m["date"] for m in prep["mouvements"])
    cibles: dict[str, tuple] = {}
    par_fournisseur: dict[str, list] = {}
    for o in prep["objets"]:
        if o["engage"] <= EPS:
            continue
        cle = o["cle"]
        type_rappro = flux.TYPE_RAPPROCHEMENT[o["type"]]
        if o["type"] == flux.FACTURE_FOURNISSEUR:
            par_fournisseur.setdefault(o["tiers_id"], []).append(
                {"facture_id_opaque": o["id"], "montant": o["engage"]})
            cibles[cle] = (type_rappro, o["id"])
        elif o["type"] == flux.REGLEMENT_FOURNISSEUR:
            if o["engage"] >= o["reste"] - EPS:
                conn.execute("UPDATE reglements_fournisseurs SET statut='RAPPROCHE', "
                             "mouvement_id_opaque=?, version=version+1 WHERE reglement_id_opaque=?",
                             (prep["mouvements"][0]["id"], o["id"]))
                crees["reglements_rapproches"].append(o["id"])
            cibles[cle] = (type_rappro, o["id"])
        elif o["type"] == flux.FACTURE_PROPRIETAIRE:
            # L'encaissement canonique d'un propriétaire : un mouvement de trésorerie VALIDE, que
            # l'allocation FIFO du compte propriétaire impute ensuite sur ses factures.
            res = tres.creer(o["tiers_id"], "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE",
                             o["engage"], date_regl,
                             mode_reglement="BANQUE" if source == flux.BANQUE else "CAISSE",
                             reference_metier=o.get("numero") or "",
                             justification=f"Encaissement rapproché ({lettrage})",
                             source_type="FLUX_LETTRAGE", source_id=lettrage, acteur=acteur,
                             db_path=db_path, conn=conn)
            if not res.get("ok"):
                raise _Echec(E_ECHEC, f"Encaissement refusé : {res.get('message')}",
                             res.get("code", ""))
            val = tres.valider(res["mouvement_opaque"], acteur=acteur, db_path=db_path, conn=conn)
            if not val.get("ok"):
                raise _Echec(E_ECHEC, f"Encaissement refusé : {val.get('message')}")
            crees["encaissements"].append(res["mouvement_opaque"])
            cibles[cle] = (type_rappro, res["mouvement_opaque"])
        elif o["type"] == flux.MOUVEMENT_PROPRIETAIRE:
            cibles[cle] = (type_rappro, o["id"])
        elif o["type"] == flux.CHARGE:
            avant = conn.execute("SELECT statut_rapprochement, lien_virement_banque FROM charges "
                                 "WHERE charge_id=?", (o["id"],)).fetchone()
            deja = conn.execute(
                "SELECT COALESCE(SUM(l.montant),0) FROM flux_lettrage_lignes l JOIN flux_lettrages t "
                "ON t.lettrage_id_opaque=l.lettrage_id_opaque WHERE t.statut='VALIDE' "
                "AND l.cote='OBJET' AND l.type_element='CHARGE' AND l.element_id=?",
                (o["id"],)).fetchone()[0]
            statut = "RAPPROCHE" if _r(deja) >= o["montant"] - EPS else "PARTIEL"
            lien = _txt(avant["lien_virement_banque"]) if avant else ""
            conn.execute("UPDATE charges SET statut_rapprochement=?, lien_virement_banque=?, "
                         "date_modification=? WHERE charge_id=?",
                         (statut, lien or prep["mouvements"][0]["id"], _now(), o["id"]))
            from app.services import charges_saisie_service as saisie
            saisie._journaliser(conn, o["id"], "RAPPROCHEMENT", acteur, f"Lettrage {lettrage}",
                                avant=dict(avant) if avant else None,
                                apres={"statut_rapprochement": statut, "lettrage": lettrage})
            crees["charges"].append(o["id"])
            cibles[cle] = (type_rappro, o["id"])

    for fournisseur, repartitions in par_fournisseur.items():
        res = regl.enregistrer(fournisseur, repartitions, date_reglement=date_regl,
                               moyen="BANQUE" if source == flux.BANQUE else "CAISSE",
                               commentaire=f"Rapprochement {lettrage}", acteur=acteur,
                               db_path=db_path, conn=conn)
        if not res.get("ok"):
            raise _Echec(E_ECHEC, f"Règlement fournisseur refusé : {res.get('message')}",
                         res.get("code", ""))
        conn.execute("UPDATE reglements_fournisseurs SET statut='RAPPROCHE', mouvement_id_opaque=? "
                     "WHERE reglement_id_opaque=?", (prep["mouvements"][0]["id"],
                                                     res["reglement_id_opaque"]))
        crees["reglements"].append(res["reglement_id_opaque"])
    return cibles


def _deposer_liens(conn, prep, lettrage, acteur, cibles, crees) -> None:
    from app.services import banques_rapprochement_service as rappro
    fusion: dict[tuple, float] = {}
    for a in prep["_aretes"]:
        if a["objet"] is None:
            cle = (a["mouvement"]["id"], "ECART", lettrage)
        else:
            t, i = cibles[a["objet"]["cle"]]
            cle = (a["mouvement"]["id"], t, i)
        fusion[cle] = _r(fusion.get(cle, 0) + a["montant"])
    for (mouvement_id, type_objet, objet_id), montant in fusion.items():
        crees["liens"].append(rappro.inserer_lien_confirme(
            conn, mouvement_id, type_objet, objet_id, montant, lettrage_id_opaque=lettrage,
            criteres={"lettrage": lettrage, "origine": "FLUX_FINANCIERS"},
            commentaire=f"Lettrage {lettrage}", acteur=acteur))


def _comptabiliser(conn, prep, groupes, lettrage, acteur, crees, *, db_path=None) -> None:
    from app.services import comptabilite_ecritures_service as compta
    for n, (proposee, lignes) in enumerate(zip(prep["ecritures"], groupes), start=1):
        origine = lettrage if len(groupes) == 1 else f"{lettrage}/{n}"
        res = compta._inserer_ecriture(
            proposee["journal"], proposee["date"], proposee["periode"], lettrage,
            proposee["libelle"], "LETTRAGE", origine, _normaliser_pour_insertion(lignes),
            acteur=acteur, db_path=db_path, conn=conn)
        if not res.get("ok") or res.get("deja_generee"):
            raise _Echec(E_ECHEC, "La comptabilisation a été refusée : "
                                  f"{res.get('message') or 'écriture déjà existante'} — "
                                  "le rapprochement n'est pas validé.", res.get("code", ""))
        compta.valider_dans_transaction(conn, res["ecriture_id_opaque"],
                                        commentaire=f"Validée avec le rapprochement {lettrage}",
                                        acteur=acteur)
        crees["ecritures"].append(res["ecriture_id_opaque"])


def _marquer_mouvements(conn, prep, lettrage, acteur, crees) -> None:
    for m in prep["mouvements"]:
        if m["source"] == flux.CAISSE and m.get("nature_caisse") == "OPERATION":
            conn.execute("UPDATE operations_caisse SET statut='VALIDE', version=version+1, "
                         "commentaire=COALESCE(commentaire,'') || ? WHERE operation_id_opaque=? "
                         "AND statut IN ('BROUILLON','ENREGISTREE')",
                         (f" · Comptabilisée par le rapprochement {lettrage} ({acteur})", m["id"]))
            crees["operations_caisse"].append(m["id"])
        elif m["source"] == flux.BANQUE:
            couvert = conn.execute(
                "SELECT COALESCE(SUM(montant_rapproche),0) FROM banque_rapprochements "
                "WHERE mouvement_id_opaque=? AND statut='CONFIRME'", (m["id"],)).fetchone()[0]
            if _r(couvert) >= m["montant"] - EPS:
                conn.execute("UPDATE qonto_transactions_statut_local SET statut_local='RAPPROCHE', "
                             "maj_le=? WHERE mouvement_id_opaque=?", (_now(), m["id"]))
                crees["qonto"].append(m["id"])


# ══ Refus d'une proposition ═══════════════════════════════════════════════════════════════════

def refuser(empreinte: str, *, acteur: str, motif: str = "", db_path=None) -> dict[str, Any]:
    """Refus humain : AUCUN objet ni mouvement ne change. Seule trace : ce refus, qui empêche le
    moteur de reproposer exactement la même combinaison."""
    acteur = _txt(acteur)
    if not acteur:
        return _refus(E_ACTEUR, "Indiquez votre nom.")
    prop = matching.proposition(empreinte, db_path=db_path)
    if prop is None:
        return _refus(E_INTROUVABLE, "Proposition introuvable ou déjà traitée.")
    conn = get_db(db_path)
    try:
        conn.execute("INSERT OR IGNORE INTO flux_propositions_refusees (empreinte, mouvements_json, "
                     "objets_json, motif, acteur) VALUES (?,?,?,?,?)",
                     (empreinte, json.dumps(prop["mouvements"], ensure_ascii=False),
                      json.dumps(prop["objets"], ensure_ascii=False), _txt(motif) or None, acteur))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "empreinte": empreinte}


# ══ Annulation (correction d'une erreur humaine) ══════════════════════════════════════════════

def lettrages(*, element_id: str = "", db_path=None) -> list[dict]:
    conn = get_db(db_path)
    try:
        if element_id:
            rows = conn.execute(
                "SELECT DISTINCT t.* FROM flux_lettrages t JOIN flux_lettrage_lignes l "
                "ON l.lettrage_id_opaque=t.lettrage_id_opaque WHERE l.element_id=? "
                "ORDER BY t.id DESC", (element_id,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM flux_lettrages ORDER BY id DESC").fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["lignes"] = [dict(x) for x in conn.execute(
                "SELECT * FROM flux_lettrage_lignes WHERE lettrage_id_opaque=? ORDER BY id",
                (d["lettrage_id_opaque"],))]
            out.append(d)
        return out
    finally:
        conn.close()


def annuler(lettrage_id: str, *, motif: str, acteur: str, db_path=None) -> dict[str, Any]:
    """Défait un lettrage SANS effacer l'histoire : écritures contrepassées (miroir), liens et
    règlements annulés, le tout dans une transaction. Rien n'est supprimé."""
    from app.services import banques_rapprochement_service as rappro
    from app.services import comptabilite_ecritures_service as compta
    from app.services import proprietaires_tresorerie_service as tres
    from app.services import reglements_fournisseurs_service as regl

    acteur, motif = _txt(acteur), _txt(motif)
    if not acteur:
        return _refus(E_ACTEUR, "Indiquez votre nom.")
    if not motif:
        return _refus(E_MOTIF, "Annuler un rapprochement exige un motif : il reste dans l'historique.")
    conn = get_db(db_path)
    try:
        let = conn.execute("SELECT * FROM flux_lettrages WHERE lettrage_id_opaque=?",
                           (lettrage_id,)).fetchone()
        evt = conn.execute("SELECT detail_json FROM flux_lettrage_evenements WHERE "
                           "lettrage_id_opaque=? AND type_evenement='VALIDATION'",
                           (lettrage_id,)).fetchone()
        sources = {r[0] for r in conn.execute(
            "SELECT type_element FROM flux_lettrage_lignes WHERE lettrage_id_opaque=? "
            "AND cote='MOUVEMENT'", (lettrage_id,))}
        types = {r[0] for r in conn.execute(
            "SELECT type_element FROM flux_lettrage_lignes WHERE lettrage_id_opaque=? "
            "AND cote='OBJET'", (lettrage_id,))}
    finally:
        conn.close()
    if let is None or let["statut"] != "VALIDE":
        return _refus(E_INTROUVABLE, "Rapprochement introuvable ou déjà annulé.")
    fermes = verrous_fermes(next(iter(sources), flux.BANQUE), types)
    if fermes:
        return _refus(E_VERROU, fermes[0])
    motif_cloture = flux.mois_cloture(_now()[:7], db_path=db_path)
    if motif_cloture:
        return _refus(E_PERIODE, motif_cloture)
    crees = json.loads(evt["detail_json"]) if evt and evt["detail_json"] else {}

    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        for ecr in crees.get("ecritures", []):
            res = compta.contrepasser(ecr, commentaire=f"Annulation {lettrage_id} — {motif}",
                                      acteur=acteur, db_path=db_path, conn=conn)
            if not res.get("ok"):
                raise _Echec(E_ECHEC, f"Contrepassation refusée : {res.get('message')}")
        for lien in crees.get("liens", []):
            rappro.annuler(lien, commentaire=f"Annulation {lettrage_id} — {motif}", acteur=acteur,
                           db_path=db_path, conn=conn)
        for reg in crees.get("reglements", []):
            res = regl.annuler(reg, commentaire=motif, acteur=acteur, db_path=db_path, conn=conn)
            if not res.get("ok"):
                raise _Echec(E_ECHEC, f"Annulation du règlement refusée : {res.get('message')}")
        for mtp in crees.get("encaissements", []):
            res = tres.annuler(mtp, commentaire=motif, acteur=acteur, db_path=db_path, conn=conn)
            if not res.get("ok"):
                raise _Echec(E_ECHEC, f"Annulation de l'encaissement refusée : {res.get('message')}")
        for reg in crees.get("reglements_rapproches", []):
            conn.execute("UPDATE reglements_fournisseurs SET statut='ENREGISTRE', "
                         "mouvement_id_opaque=NULL, version=version+1 WHERE reglement_id_opaque=?",
                         (reg,))
        conn.execute("UPDATE flux_lettrages SET statut='ANNULE', annule_le=?, annule_par=?, "
                     "motif_annulation=?, version=version+1 WHERE lettrage_id_opaque=?",
                     (_now(), acteur, motif, lettrage_id))
        from app.services import charges_saisie_service as saisie
        for cid in crees.get("charges", []):
            reste = conn.execute(
                "SELECT COALESCE(SUM(l.montant),0) FROM flux_lettrage_lignes l JOIN flux_lettrages t "
                "ON t.lettrage_id_opaque=l.lettrage_id_opaque WHERE t.statut='VALIDE' "
                "AND l.cote='OBJET' AND l.type_element='CHARGE' AND l.element_id=?",
                (cid,)).fetchone()[0]
            statut = "PARTIEL" if _r(reste) > EPS else "NON_RAPPROCHE"
            conn.execute("UPDATE charges SET statut_rapprochement=?, date_modification=? "
                         "WHERE charge_id=?", (statut, _now(), cid))
            saisie._journaliser(conn, cid, "RAPPROCHEMENT_ANNULE", acteur, motif,
                                apres={"statut_rapprochement": statut, "lettrage": lettrage_id})
        for op in crees.get("operations_caisse", []):
            conn.execute("UPDATE operations_caisse SET statut='BROUILLON', version=version+1, "
                         "commentaire=COALESCE(commentaire,'') || ? WHERE operation_id_opaque=?",
                         (f" · Rapprochement {lettrage_id} annulé ({acteur}) : {motif}", op))
        for mvt in crees.get("qonto", []):
            conn.execute("UPDATE qonto_transactions_statut_local SET statut_local='A_RAPPROCHER', "
                         "maj_le=? WHERE mouvement_id_opaque=?", (_now(), mvt))
        conn.execute("INSERT INTO flux_lettrage_evenements (lettrage_id_opaque, type_evenement, "
                     "detail_json, acteur) VALUES (?,?,?,?)",
                     (lettrage_id, "ANNULATION", json.dumps({"motif": motif}, ensure_ascii=False),
                      acteur))
        conn.commit()
    except _Echec as exc:
        conn.rollback()
        return _refus(exc.code, exc.message)
    except Exception as exc:      # noqa: BLE001
        conn.rollback()
        log.exception("Annulation du lettrage %s abandonnée", lettrage_id)
        return _refus(E_ECHEC, "L'annulation a échoué : rien n'a été modifié.",
                      detail=type(exc).__name__)
    finally:
        conn.close()
    return {"ok": True, "lettrage_id_opaque": lettrage_id}


# ══ Fournisseur créé à la volée ═══════════════════════════════════════════════════════════════

NOMS_GENERIQUES = {"fournisseur", "fournisseurs", "fournisseur divers", "fournisseurs divers",
                   "divers", "inconnu", "autre", "autres", "non identifie", "a identifier",
                   "sans nom", "tiers divers"}


def _normaliser_nom(nom: str) -> str:
    decompose = unicodedata.normalize("NFKD", _txt(nom))
    sans = "".join(c for c in decompose if not unicodedata.combining(c)).lower()
    return re.sub(r"[^a-z0-9]+", " ", sans).strip()


def doublons_fournisseur(nom: str, *, siren: str = "", siret: str = "", db_path=None) -> list[dict]:
    """Fournisseurs (ou prestataires) déjà connus sous ce nom ou cet identifiant légal."""
    cible = _normaliser_nom(nom)
    ident = {x for x in (re.sub(r"\D", "", siren), re.sub(r"\D", "", siret)) if x}
    trouves = []
    conn = get_db(db_path)
    try:
        tables = flux._tables(conn)
        if "fournisseurs" in tables:
            details = {}
            if "fournisseur_details" in tables:
                details = {r[0]: re.sub(r"\D", "", _txt(r[1])) for r in conn.execute(
                    "SELECT fournisseur_id_opaque, numero_entreprise FROM fournisseur_details")}
            for r in conn.execute("SELECT fournisseur_id_opaque, nom FROM fournisseurs WHERE actif=1"):
                num = details.get(r[0], "")
                if _normaliser_nom(r[1]) == cible or (num and any(
                        num.startswith(i) or i.startswith(num) for i in ident)):
                    trouves.append({"id": r[0], "nom": r[1]})
        if "ref_intervenants" in tables:
            for r in conn.execute("SELECT intervenant_id, nom_intervenant, nom_legal, societe, "
                                  "siret_rcs FROM ref_intervenants"):
                noms = {_normaliser_nom(x) for x in (r[1], r[2], r[3]) if _txt(x)}
                num = re.sub(r"\D", "", _txt(r[4]))
                if cible in noms or (num and any(num.startswith(i) or i.startswith(num)
                                                 for i in ident)):
                    trouves.append({"id": r[0], "nom": r[1]})
    finally:
        conn.close()
    return trouves


def creer_fournisseur(nom: str, *, siren: str = "", siret: str = "", acteur: str,
                      db_path=None) -> dict[str, Any]:
    """Crée un fournisseur RÉEL, nommé, dans le référentiel canonique — jamais un « divers »
    fabriqué pour équilibrer une écriture. SIREN/SIRET seulement s'ils sont connus : aucun
    identifiant légal n'est inventé ni complété."""
    from app.services import fournisseurs_referentiel_service as frs

    nom = _txt(nom)
    acteur = _txt(acteur)
    if not acteur:
        return _refus(E_ACTEUR, "Indiquez votre nom.")
    if not nom:
        return _refus(E_NOM_REQUIS, "Le nom ou la raison sociale est obligatoire.")
    if _normaliser_nom(nom) in NOMS_GENERIQUES:
        return _refus(E_NOM_GENERIQUE, "Un fournisseur générique (« divers », « inconnu »…) "
                                       "ne peut pas servir d'auxiliaire : nommez le vrai fournisseur.")
    siren_c = re.sub(r"\s", "", _txt(siren))
    siret_c = re.sub(r"\s", "", _txt(siret))
    if siren_c and not re.fullmatch(r"\d{9}", siren_c):
        return _refus(E_IDENTIFIANT, "Un SIREN compte 9 chiffres. Laissez vide s'il n'est pas connu.")
    if siret_c and not re.fullmatch(r"\d{14}", siret_c):
        return _refus(E_IDENTIFIANT, "Un SIRET compte 14 chiffres. Laissez vide s'il n'est pas connu.")
    if siren_c and siret_c and not siret_c.startswith(siren_c):
        return _refus(E_IDENTIFIANT, "Le SIRET doit commencer par le SIREN.")

    doublons = doublons_fournisseur(nom, siren=siren_c, siret=siret_c, db_path=db_path)
    if doublons:
        res = _refus(E_DOUBLON, f"« {doublons[0]['nom']} » existe déjà : sélectionnez-le plutôt "
                                "que d'en créer un second.")
        res["doublons"] = doublons
        return res

    try:
        f = frs.creer(nom, "AUTRE", commentaire="Créé depuis Flux financiers (rapprochement)",
                      acteur=acteur, db_path=db_path)
    except frs.FournisseurRefuse as exc:
        return _refus(E_NOM_REQUIS, str(exc))
    numero = siret_c or siren_c
    if numero:
        conn = get_db(db_path)
        try:
            conn.execute("INSERT OR REPLACE INTO fournisseur_details (fournisseur_id_opaque, "
                         "raison_sociale, numero_entreprise) VALUES (?,?,?)",
                         (f["fournisseur_id_opaque"], nom, numero))
            conn.commit()
        finally:
            conn.close()
    return {"ok": True, "id": f["fournisseur_id_opaque"], "nom": f["nom"]}
