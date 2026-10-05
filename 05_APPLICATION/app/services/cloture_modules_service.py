"""Modules de clôture d'un mois — la vue MÉTIER des bloqueurs, agrégée depuis les VRAIS modules.

CE QUE CE MODULE FAIT
Une clôture mensuelle n'est pas une liste plate de contrôles : c'est, pour chaque domaine de
l'application (réservations, ménages, charges, banque, factures clients, créances, comptabilité), la
question « tout ce qui concerne ce mois est-il traité ? ». Ce module répond, domaine par domaine, en
INTERROGEANT les modules eux-mêmes.

CE QU'IL NE FAIT PAS — et c'est le contrat
  · il ne copie aucune anomalie : chaque bloqueur est RECALCULÉ à chaque appel depuis sa source
    (une charge validée dans « Charges », un mouvement qualifié dans « Flux », une écriture validée
    dans « Comptabilité » : le bloqueur disparaît de lui-même, sans case à cocher) ;
  · il ne crée aucun « problème de clôture » : une table de problèmes parallèle divergerait au premier
    incident, c'est exactement ce qu'on évite ;
  · il n'écrit RIEN : lecture seule, comme `cloture_flux_service` dont il réutilise l'analyse.

SOURCES (chacune dit ce qu'elle lit)
  · contrôles du moteur (APP-5B, `controles_actionnable_service`) — rangés dans leur domaine par
    leur module d'origine, et traduits en langage métier ; aucun code technique n'est exposé ;
  · Flux financiers (`cloture_flux_service`) — mouvements bancaires et de caisse, écritures proposées,
    comptes à définir ;
  · lectures DIRECTES, faites ici parce qu'aucun contrôle moteur ne les couvre ou parce que le contrôle
    moteur n'est relu qu'au prochain calcul : charges non validées, séjours hors période de gestion,
    conflits de déclaration de ménage, factures propriétaires à créer / émettre / comptabiliser,
    calculs obsolètes, positions de compte à recalculer, anomalies comptables bloquantes.

LE LIEN « TRAITER » mène à la VRAIE page du module, déjà filtrée sur le mois (paramètres d'URL), jamais
à une copie de cette page dans l'écran de clôture.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib.parse import quote

from app.db.connection import get_db

RESERVATIONS = "RESERVATIONS"
MENAGES = "MENAGES"
CHARGES = "CHARGES"
BANQUE = "BANQUE"
FACTURES_CLIENTS = "FACTURES_CLIENTS"
CREANCES = "CREANCES"
COMPTABILITE = "COMPTABILITE"


@dataclass(frozen=True)
class Module:
    cle: str
    libelle: str
    resume: str
    #: Ce que « clôturé » veut dire concrètement — affiché à la confirmation, tenu par les gardes.
    verrouille: tuple[str, ...]
    #: Page du module, pour la CONSULTER (elle reste consultable une fois clôturé).
    consulter: str


#: Ordre d'affichage = ordre conseillé : la comptabilité se clôture en dernier, une fois que tout ce
#: qui l'alimente est arrêté.
MODULES: tuple[Module, ...] = (
    Module(RESERVATIONS, "Réservations",
           "Séjours du mois, montants retenus, commissions et résultats.",
           ("La saisie, la modification et la régularisation des réservations hors Hostaway de ce mois "
            "sont refusées.",
            "La correction manuelle de l'assiette de commission d'un séjour de ce mois est refusée.",
            "Les réservations restent consultables et continuent d'être importées : un changement reçu "
            "après la clôture fait réapparaître un bloqueur, le module doit alors être rouvert."),
           "/reservations?mois={mois}"),
    Module(MENAGES, "Ménages",
           "Ménages réalisés, déclarés et facturés, rapprochés des tâches Hostaway.",
           ("La saisie et la modification des déclarations de ménage de ce mois sont refusées.",
            "L'actualisation des sources signale les changements reçus sans modifier les déclarations. "
            "Un calcul périmé peut être actualisé depuis la clôture sans rouvrir le module."),
           "/menages?mois={mois}"),
    Module(CHARGES, "Charges et factures fournisseurs",
           "Charges du mois, contrôle et factures de vos fournisseurs.",
           ("La saisie, la validation et la réouverture des charges de ce mois sont refusées.",
            "La saisie, la modification et la suppression des factures fournisseurs de ce mois sont "
            "refusées."),
           "/flux-financiers/charges?mois={mois}"),
    Module(BANQUE, "Banque et caisse",
           "Mouvements Qonto et opérations de caisse du mois, rapprochés ou qualifiés.",
           ("Aucun rapprochement, lettrage ni nouvelle opération de caisse ne peut porter sur ce mois.",
            "Les mouvements Qonto continuent d'être lus ; ils ne modifient plus le mois clôturé."),
           "/flux-financiers/banque?mois={mois}"),
    Module(FACTURES_CLIENTS, "Factures clients",
           "Factures et avoirs adressés aux propriétaires pour ce mois.",
           ("La création, la modification, la validation, l'émission et la suppression de factures et "
            "d'avoirs propriétaires de ce mois sont refusées.",
            "Les factures restent consultables, et leur PDF reste téléchargeable."),
           "/factures-proprietaires?mois={mois}"),
    Module(CREANCES, "Créances et dettes",
           "Positions des comptes propriétaires, crédits et dettes fournisseurs.",
           ("Les reversements Airbnb, reprises de solde et imputations de crédit datés de ce mois sont "
            "refusés.",
            "Les créances et les dettes restent consultables."),
           "/creances"),
    Module(COMPTABILITE, "Comptabilité",
           "Écritures du mois, comptes à définir et équilibre des journaux.",
           ("La période comptable est CLÔTURÉE : plus d'écriture, de validation ni de contrepassation "
            "directe sur ce mois.",
            "Une correction passe par la réouverture de la période, avec justification."),
           "/comptabilite/ecritures?periode={mois}"),
)
PAR_CLE: dict[str, Module] = {m.cle: m for m in MODULES}


# ── Utilitaires ──────────────────────────────────────────────────────────────────────────────────

def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _date_fr(iso: Any) -> str:
    s = _txt(iso)[:10]
    try:
        return date.fromisoformat(s).strftime("%d/%m/%Y")
    except ValueError:
        return s


def _du_au(debut: Any, fin: Any) -> str:
    a, b = _txt(debut)[:10], _txt(fin)[:10]
    try:
        d1, d2 = date.fromisoformat(a), date.fromisoformat(b)
        return f"du {d1.strftime('%d/%m')} au {d2.strftime('%d/%m')}"
    except ValueError:
        return " → ".join(x for x in (a, b) if x)


def _tables(conn) -> set[str]:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _pluriel(n: int, singulier: str, pluriel: str) -> str:
    return f"{n} {singulier if n == 1 else pluriel}"


def _groupe(cle: str, singulier: str, pluriel: str, items: list[dict[str, Any]], *, pourquoi: str,
            action: str, lien: str = "", lien_libelle: str = "") -> dict[str, Any]:
    """Un groupe de bloqueurs de même nature : une ligne dans la carte du module, avec son lien
    « Traiter » et le détail repliable des éléments qui le composent."""
    return {"cle": cle, "libelle": _pluriel(len(items), singulier, pluriel), "nb": len(items),
            "items": items, "pourquoi": pourquoi, "action": action,
            "lien": lien, "lien_libelle": lien_libelle or action}


def _item(libelle: str, *, detail: str = "", montant: float | None = None, date_: str = "",
          lien: str = "", lien_libelle: str = "") -> dict[str, Any]:
    return {"libelle": libelle, "detail": detail, "montant": montant, "date": _date_fr(date_),
            "lien": lien, "lien_libelle": lien_libelle}


class _Noms:
    """Noms lisibles des logements et propriétaires, lus une fois : jamais un identifiant à l'écran."""

    def __init__(self, db_path=None) -> None:
        self.logements: dict[str, str] = {}
        self.proprietaires: dict[str, str] = {}
        conn = get_db(db_path)
        try:
            t = _tables(conn)
            if "ref_logements" in t:
                for r in conn.execute("SELECT logement_id, nom_court, nom_logement_officiel "
                                      "FROM ref_logements"):
                    self.logements[r[0]] = _txt(r[1]) or _txt(r[2]) or "Logement"
            if "ref_proprietaires" in t:
                for r in conn.execute("SELECT proprietaire_id, prenom_proprietaire, "
                                      "nom_proprietaire FROM ref_proprietaires"):
                    self.proprietaires[r[0]] = " ".join(
                        x for x in (_txt(r[1]), _txt(r[2])) if x) or "Propriétaire"
        finally:
            conn.close()

    def logement(self, lid: Any) -> str:
        return self.logements.get(_txt(lid), "Logement non identifié" if not _txt(lid) else "Logement")

    def proprietaire(self, pid: Any) -> str:
        return self.proprietaires.get(_txt(pid), "Propriétaire")


# ══ 1. Contrôles du moteur : rangés par domaine, traduits en langage métier ═══════════════════════

#: Domaine métier d'un contrôle d'après son module d'origine. Un module inconnu retombe sur
#: « Réservations » (le calcul économique) : un contrôle n'est JAMAIS écarté faute de domaine.
MODULE_DES_CONTROLES: dict[str, str] = {
    "RESERVATIONS": RESERVATIONS, "COMMISSIONS": RESERVATIONS, "HOSTAWAY": RESERVATIONS,
    "HH": RESERVATIONS, "FLUX": RESERVATIONS, "EXPLOITATION": RESERVATIONS, "RESULTATS": RESERVATIONS,
    "REF": RESERVATIONS, "TRANSVERSE": RESERVATIONS, "PAYOUT": RESERVATIONS, "module": RESERVATIONS,
    "CLOTURE": RESERVATIONS,
    "MENAGES": MENAGES, "MENAGES_EXT": MENAGES, "M04": MENAGES,
    "CHARGES": CHARGES,
    "BANQUE": BANQUE,
    "REGLEMENT": CREANCES, "ACOMPTES": CREANCES, "AIRCOVER": CREANCES,
    "AVANTAGES_ASSOCIES": COMPTABILITE, "IK": COMPTABILITE,
}
DOMAINE_PAR_DEFAUT = RESERVATIONS

#: Contrôles remplacés par une lecture DIRECTE du module concerné : les compter aussi ferait apparaître
#: le même problème en double — et, pire, le constat moteur resterait affiché après la décision prise
#: dans le module (charge validée, écart de ménage justifié), jusqu'au calcul suivant.
#:   · une charge non validée → relue dans Charges ;
#:   · un écart de ménages (facturé ≠ Hostaway, logement facturé absent d'Hostaway) → c'est le module
#:     Ménages qui dit s'il est « à contrôler », « justifié » (outrepassé) ou « validé » : une ligne
#:     justifiée n'est plus un bloqueur, et le constat du moteur, lui, l'ignore.
CODES_LUS_EN_DIRECT = {"CHARGE_NON_VALIDEE_HORS_CALCULS", "MENAGE_EXTERNE_ECART_HOSTAWAY",
                       "MENAGE_EXTERNE_LOGEMENT_HORS_HA"}

#: Même principe pour une DÉCISION D'EXCLUSION du périmètre de gestion : un contrôle du moteur qui porte sur un séjour
#: que l'exploitant a exclu (VRBO sans montant, commission non calculée…) ne dit plus rien d'utile — la décision est
#: prise dans les Réservations —, alors que le calcul n'est peut-être pas encore refait. La clôture lit la décision en
#: direct et ne compte plus ces contrôles (`_concerne_sejour_exclu`) ; annulée, la décision les fait revenir.

#: Les contrôles du moteur sur les séjours se traitent dans l'écran des contrôles (chaque élément y porte sa vraie
#: action : régulariser, corriger l'assiette…). La liste des « réservations » de l'application ne montre, elle,
#: que les saisies manuelles : y envoyer ne ferait rien voir.
_LIEN_RESERVATIONS = "/controles-cloture?mois={mois}&code={code}"


@dataclass(frozen=True)
class _Trad:
    singulier: str
    pluriel: str
    pourquoi: str
    action: str = "Traiter"
    lien: str = ""


#: Traduction métier d'un code de contrôle. Le code lui-même n'est jamais affiché.
TRADUCTIONS: dict[str, _Trad] = {
    "RESERVATION_A_CONTROLER_SANS_COMMISSION": _Trad(
        "réservation exclue du calcul de commission", "réservations exclues du calcul de commission",
        "Sans montant retenu, la commission et le net du propriétaire sont incomplets.",
        "Saisir les montants", _LIEN_RESERVATIONS),
    "VRBO_MONTANT_NON_RENSEIGNE": _Trad(
        "réservation VRBO sans montant", "réservations VRBO sans montant",
        "Le montant réellement versé par VRBO doit être saisi.", "Saisir les montants",
        _LIEN_RESERVATIONS),
    "GUEST_COUNT_MANQUANT_PREPARATION_CANAPE": _Trad(
        "réservation sans nombre de voyageurs", "réservations sans nombre de voyageurs",
        "Sans le nombre de voyageurs, le supplément canapé ne peut pas être calculé.",
        "Voir les réservations", _LIEN_RESERVATIONS),
    "ASSIETTE_NEGATIVE_RAMENEE_ZERO": _Trad(
        "réservation à assiette de commission négative", "réservations à assiette de commission négative",
        "L'assiette négative a été ramenée à zéro automatiquement : à confirmer ou à corriger."),
    "ASSIETTE_COMMISSION_INCOHERENTE": _Trad(
        "assiette de commission incohérente", "assiettes de commission incohérentes",
        "L'assiette ne correspond pas au payout et au ménage du séjour."),
    "COMMISSION_INCOHERENTE": _Trad(
        "commission incohérente", "commissions incohérentes",
        "Le montant de la commission ne correspond pas à son assiette et à son taux."),
    "COMMISSION_SANS_TAUX": _Trad(
        "séjour sans taux de commission", "séjours sans taux de commission",
        "Aucun taux de commission n'est applicable à la date de ce séjour.", "Voir les logements",
        "/logements"),
    "JOINTURE_PAYOUT_MANQUANTE": _Trad(
        "réservation sans paiement associé", "réservations sans paiement associé",
        "Le paiement de la plateforme n'a pas pu être rattaché à ce séjour.", "Voir les réservations",
        _LIEN_RESERVATIONS),
    "JOINTURE_RESERVATIONS_MANQUANTE": _Trad(
        "flux sans réservation associée", "flux sans réservation associée",
        "Un flux économique ne retrouve pas sa réservation."),
    "DOUBLON_RESERVATION_FLUX": _Trad(
        "réservation en double dans les flux", "réservations en double dans les flux",
        "Une même réservation est comptée plusieurs fois."),
    "RESERVATION_DOUBLON_HOSTAWAY_HH": _Trad(
        "réservation saisie à la main et reçue d'Hostaway", "réservations saisies à la main et reçues d'Hostaway",
        "Le même séjour existe en double : il serait compté deux fois.", "Voir les réservations",
        _LIEN_RESERVATIONS),
    "LISTING_ORPHELIN_A_CONTROLER": _Trad(
        "annonce Hostaway sans logement correspondant", "annonces Hostaway sans logement correspondant",
        "Ses séjours sont conservés mais exclus de l'économie tant qu'aucun logement ne lui correspond.",
        "Établir la correspondance", "/correspondances-logement"),
    "LOG_SANS_FLUX_017": _Trad(
        "logement sans aucun flux économique ce mois-ci", "logements sans aucun flux économique ce mois-ci",
        "Un logement géré n'a produit aucun flux : à vérifier (séjours, gestion, ménages).",
        "Voir le logement", "/logements"),
    "REEL_INCOHERENT_VS_COMPTABLE_PLUS_HC": _Trad(
        "écart entre résultat réel et résultat comptable", "écarts entre résultat réel et résultat comptable",
        "Les deux lectures du résultat ne s'expliquent pas l'une par l'autre."),
    "REVENU_NET_EXPLOITATION_INCOHERENT": _Trad(
        "revenu net d'exploitation incohérent", "revenus nets d'exploitation incohérents",
        "Le net d'exploitation ne correspond pas à ses composantes."),
    "ACOMPTE_AIRBNB_INCLUS_NET_EXPLOITATION": _Trad(
        "acompte Airbnb compté dans le net d'exploitation", "acomptes Airbnb comptés dans le net d'exploitation",
        "Un acompte ne doit pas figurer dans le net d'exploitation."),
    "CONFUSION_PAYOUT_SOLDE_FACTURE": _Trad(
        "confusion entre paiement et solde de facture", "confusions entre paiement et solde de facture",
        "Un paiement a été traité comme un solde de facture."),
    "PAIEMENT_DEJA_RECU_DEDUIT_DU_PAYOUT": _Trad(
        "paiement déjà reçu déduit du payout", "paiements déjà reçus déduits du payout",
        "Un paiement déjà encaissé a été retiré une seconde fois du payout."),
    "GESTION_LOGEMENT_HISTORIQUE_ABSENT_TRANSITOIRE": _Trad(
        "historique de gestion des logements absent", "historiques de gestion des logements absents",
        "Sans période de gestion, le propriétaire d'un séjour ne peut pas être déterminé.",
        "Voir les logements", "/logements"),
    "TAUX_COMMISSION_HISTORIQUE_ABSENT_TRANSITOIRE": _Trad(
        "historique des taux de commission absent", "historiques des taux de commission absents",
        "Sans taux daté, aucune commission ne peut être calculée.", "Voir les logements", "/logements"),
    "PK_MANQUANTE_OU_DOUBLONNEE": _Trad(
        "anomalie d'identification dans les calculs", "anomalies d'identification dans les calculs",
        "Des lignes de calcul n'ont pas d'identifiant unique."),
    "BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY": _Trad(
        "versement bancaire peut-être déjà compté dans Hostaway",
        "versements bancaires peut-être déjà comptés dans Hostaway",
        "Le même argent pourrait être compté deux fois.", "Voir la banque",
        "/flux-financiers/banque?mois={mois}"),
    "CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE": _Trad(
        "ligne bancaire non classée (ancien import)", "lignes bancaires non classées (ancien import)",
        "Une ligne bancaire non classée interdit la clôture.", "Classer les lignes",
        "/banques-caisse/a-classer"),
    "MENAGE_EXTERNE_RAPPROCHE_HOSTAWAY": _Trad(
        "logement dont les ménages facturés correspondent à Hostaway",
        "logements dont les ménages facturés correspondent à Hostaway",
        "Le volume mensuel est cohérent : rien à faire."),
    "MENAGE_HA_SANS_FACTURE_EXTERNE": _Trad(
        "logement avec des ménages Hostaway sans facture externe",
        "logements avec des ménages Hostaway sans facture externe",
        "Probablement des ménages internes : à croiser avec les déclarations."),
    "SOURCE_SHEET_CACHE_UTILISE": _Trad(
        "déclaration de ménage lue dans une copie locale", "déclarations de ménage lues dans une copie locale",
        "La dernière lecture du Google Sheet n'est pas à jour.", "Actualiser les ménages", "/menages"),
    "SOURCE_SHEET_PROVENANCE_INCOMPLETE": _Trad(
        "provenance des déclarations de ménage incomplète",
        "provenances des déclarations de ménage incomplètes",
        "On ne sait pas d'où viennent les déclarations de ménage lues.", "Actualiser les ménages",
        "/menages"),
}

_TRAD_DEFAUT = _Trad("contrôle automatique à traiter", "contrôles automatiques à traiter",
                     "Le moteur de contrôle a relevé une anomalie sur ce mois.", "Voir le contrôle")
_TRAD_INFO_DEFAUT = _Trad("information du moteur de contrôle", "informations du moteur de contrôle",
                          "Information du moteur : elle ne bloque pas la clôture.", "Voir le contrôle")


def _element_item(e: dict[str, Any], noms: _Noms, mois: str) -> dict[str, Any]:
    """Ligne de détail d'un contrôle détaillé : où, quand, combien — jamais un identifiant technique."""
    d = e.get("donnees") or {}
    logement = d.get("logement") or d.get("logement_id")
    morceaux = []
    if logement:
        morceaux.append(noms.logement(logement))
    if d.get("date_arrivee"):
        morceaux.append(_du_au(d.get("date_arrivee"), d.get("date_depart")))
    if d.get("nombre_facture") is not None or d.get("nombre_hostaway") is not None:
        morceaux.append(f"{d.get('nombre_facture') or 0} facturé(s) pour "
                        f"{d.get('nombre_hostaway') or 0} chez Hostaway")
    if not morceaux and d.get("mois"):
        morceaux.append(str(d["mois"]))
    montant = d.get("montant_retenu") if d.get("montant_retenu") is not None else d.get("montant")
    lien = e.get("lien_module") or f"/controles-cloture/element/{quote(_txt(e.get('ctrl_opaque')))}"
    return _item(" — ".join(morceaux) or "Détail dans le contrôle",
                 montant=montant if isinstance(montant, (int, float)) else None,
                 lien=lien, lien_libelle="Traiter")


def _concerne_sejour_exclu(e: dict[str, Any], exclues) -> bool:
    """Ce contrôle du moteur porte-t-il sur un séjour dont l'exclusion du périmètre de gestion est décidée ?"""
    if not exclues:
        return False
    d = e.get("donnees") or {}
    cites = {_txt(v) for v in (d.get("reservation_id"), e.get("entite_id")) if v}
    # Certains constats citent la clé de calcul du séjour (« RES-HA-<numéro Hostaway> »), d'autres le numéro Hostaway.
    cites |= {c[len("RES-HA-"):] for c in cites if c.startswith("RES-HA-")}
    return bool(cites & set(exclues))


def _ajouter_controles_moteur(par: dict[str, dict[str, list]], mois: str, elements: list[dict[str, Any]],
                              noms: _Noms, exclues=frozenset()) -> None:
    blocs: dict[tuple[str, str, bool], list[dict[str, Any]]] = {}
    for e in elements:
        if e["code"] in CODES_LUS_EN_DIRECT or _concerne_sejour_exclu(e, exclues):
            continue
        domaine = MODULE_DES_CONTROLES.get(_txt(e["module"]), DOMAINE_PAR_DEFAUT)
        if e["est_info"]:
            blocs.setdefault((domaine, e["code"], True), []).append(e)
            continue
        if e["etat"]["anomalie_moteur_presente"] and not e["etat"]["exception_active"]:
            blocs.setdefault((domaine, e["code"], False), []).append(e)
    for (domaine, code, info), els in blocs.items():
        trad = TRADUCTIONS.get(code, _TRAD_INFO_DEFAUT if info else _TRAD_DEFAUT)
        items = [_element_item(e, noms, mois) for e in els]
        lien = trad.lien.format(mois=mois, code=quote(code)) if trad.lien else (items[0]["lien"] if items else "")
        if info:
            par[domaine]["informatifs"].append(_groupe(
                code, trad.singulier, trad.pluriel, items, pourquoi=trad.pourquoi, action="Voir",
                lien=lien))
        else:
            par[domaine]["bloqueurs"].append(_groupe(
                code, trad.singulier, trad.pluriel, items, pourquoi=trad.pourquoi,
                action=trad.action, lien=lien))


# ══ 2. Flux financiers : mouvements, écritures proposées, comptes à définir ═══════════════════════

def _ajouter_flux(par: dict[str, dict[str, list]], mois: str, flux: dict[str, Any]) -> None:
    from app.services import cloture_flux_service as cf

    domaine_famille = {cf.F_BANQUE: BANQUE, cf.F_CAISSE: BANQUE, cf.F_ECRITURE: COMPTABILITE,
                       cf.F_COMPTE: COMPTABILITE}
    par_type: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for b in flux["bloquants"]:
        par_type.setdefault((domaine_famille.get(b["famille"], COMPTABILITE), b["type"]), []).append(b)
    for (domaine, type_), bs in par_type.items():
        items = [_item(f"{b['origine'] or type_}", detail=b["statut"], montant=b["montant"],
                       date_=b["date"], lien=b["lien"], lien_libelle=b["lien_libelle"]) for b in bs]
        famille = bs[0]["famille"]
        if famille == cf.F_ECRITURE:
            lien = f"/comptabilite/ecritures?periode={quote(mois)}&statut=PROPOSEE"
            action = "Valider les écritures"
        elif famille == cf.F_COMPTE:
            lien = "/comptabilite/mappings"
            action = "Définir les comptes"
        elif famille == cf.F_CAISSE:
            lien, action = "/flux-financiers/caisse", "Traiter la caisse"
        else:
            lien, action = f"/flux-financiers/banque?mois={quote(mois)}", "Traiter les mouvements"
        singulier, pluriel = _formes(type_)
        par[domaine]["bloqueurs"].append(_groupe(
            f"FLUX:{type_}", singulier, pluriel, items,
            pourquoi=bs[0]["raison"], action=action, lien=lien))
    domaine_info = {cf.I_FACTURE: CHARGES, cf.I_CHARGE: CHARGES, cf.I_HORS_COMPTA: CHARGES,
                    cf.I_SANS_EFFET: BANQUE}
    infos: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for i in flux["informatifs"]:
        infos.setdefault((domaine_info.get(i["famille"], BANQUE), i["type"]), []).append(i)
    for (domaine, type_), iss in infos.items():
        items = [_item(i["origine"] or type_, montant=i["montant"], date_=i["date"],
                       lien=i["lien"], lien_libelle=i["lien_libelle"]) for i in iss]
        singulier, pluriel = _formes(type_)
        par[domaine]["informatifs"].append(_groupe(
            f"FLUX:{type_}", singulier, pluriel, items,
            pourquoi=iss[0]["raison"], action="Voir", lien=items[0]["lien"] if items else ""))


#: Pluriels que la règle générale ne sait pas faire (participes, locutions, deux noms).
_PLURIELS_EXPLICITES = {
    "facture fournisseur brouillon": "factures fournisseurs brouillons",
    "facture fournisseur validée": "factures fournisseurs validées",
    "facture fournisseur partiellement réglée": "factures fournisseurs partiellement réglées",
    "charge comptable pas encore comptabilisée": "charges comptables pas encore comptabilisées",
    "retrait d'espèces : écriture à valider": "retraits d'espèces : écritures à valider",
    "mouvement bancaire : écriture à valider": "mouvements bancaires : écritures à valider",
    "opération de caisse : écriture à valider": "opérations de caisse : écritures à valider",
}
_MOTS_D_ARRET = {"de", "d'espèces", "du", "des", "à", "en", "sans", "pas", "hors", ":", "la", "le", "pour"}
_COMPLEMENTS_ACCORDES = {"bancaire", "comptable", "proposée", "fournisseur"}


def _formes(libelle: str) -> tuple[str, str]:
    """(singulier, pluriel) d'un libellé de Flux (« Mouvement bancaire à qualifier » → « mouvements
    bancaires à qualifier »).

    Les libellés viennent d'un petit nombre de gabarits (`cloture_flux_service`) : le groupe nominal de
    tête prend le s — le nom, puis ses adjectifs courants — jusqu'au premier mot d'arrêt (« de », « à »,
    « en », « sans »…) ; ce qui suit reste tel quel. Les quelques cas que cette règle manquerait
    (participes, deux noms accolés) sont listés explicitement ci-dessus."""
    singulier = libelle[:1].lower() + libelle[1:]
    if singulier in _PLURIELS_EXPLICITES:
        return singulier, _PLURIELS_EXPLICITES[singulier]
    mots, sortie, tete = singulier.split(" "), [], True
    for i, mot in enumerate(mots):
        if tete and (i == 0 or mot in _COMPLEMENTS_ACCORDES) and mot not in _MOTS_D_ARRET:
            sortie.append(mot if mot.endswith(("s", "x", "z")) else mot + "s")
        else:
            tete = tete and mot not in _MOTS_D_ARRET
            sortie.append(mot)
    return singulier, " ".join(sortie)


# ══ 3. Lectures directes : ce que les contrôles moteur ne disent pas, ou disent trop tard ═════════

def _charges(par, mois: str, noms: _Noms, db_path) -> None:
    """Charges non validées du mois — relues en DIRECT, elles disparaissent dès leur validation.

    Même règle que le contrôle du moteur (« une charge non validée ne pèse pas, mais elle ne
    disparaît pas ») sauf qu'une charge REJETÉE est une décision prise : elle ne bloque plus."""
    conn = get_db(db_path)
    try:
        if "charges" not in _tables(conn):
            return
        rows = [dict(r) for r in conn.execute(
            "SELECT charge_id, date_charge, montant, logement_id, categorie_charge_id, commentaire, "
            "COALESCE(statut_controle, 'A_CONTROLER') AS controle FROM charges "
            "WHERE statut = 'ACTIVE' AND UPPER(COALESCE(statut_controle, 'A_CONTROLER')) "
            "IN ('A_CONTROLER', 'ANOMALIE') "
            "AND COALESCE(NULLIF(mois, ''), substr(date_charge, 1, 7)) = ? "
            "ORDER BY date_charge, charge_id", (mois,))]
        categories = {}
        if "ref_categories_charges" in _tables(conn):
            categories = {r[0]: f"{_txt(r[1])} · {_txt(r[2])}" for r in conn.execute(
                "SELECT categorie_charge_id, categorie_niveau_1, categorie_niveau_2 "
                "FROM ref_categories_charges")}
    finally:
        conn.close()
    if not rows:
        return
    items = [_item(categories.get(r["categorie_charge_id"]) or "Charge", detail=_txt(r["commentaire"])
                   or ("À examiner" if r["controle"].upper() == "ANOMALIE" else ""),
                   montant=abs(float(r["montant"] or 0)), date_=r["date_charge"],
                   lien=f"/flux-financiers/charges?mois={quote(mois)}&statut_controle={quote(r['controle'])}",
                   lien_libelle="Voir la charge") for r in rows]
    par[CHARGES]["bloqueurs"].append(_groupe(
        "CHARGE_A_VALIDER", "charge à valider ou à rejeter", "charges à valider ou à rejeter", items,
        pourquoi="Une charge non validée n'entre dans aucun calcul : le mois serait arrêté sans cette dépense.",
        action="Contrôler les charges", lien=f"/flux-financiers/charges?mois={quote(mois)}&statut_controle=A_CONTROLER"))


#: Les quatre façons dont un séjour peut ne pas trouver son propriétaire (`resolve_management_period`), chacune
#: dite pour ce qu'elle est : ce que l'utilisateur doit décider n'est pas la même chose. Le détail de chaque séjour
#: (sa cause, ce qu'on peut en faire) est celui du module Réservations (`perimetre_gestion_service`).
_GESTION_TEXTES = {
    "GESTION_LOGEMENT_MISSING": (
        "séjour hors période de gestion", "séjours hors période de gestion",
        "Aucune période de gestion ne couvre ses dates : le séjour est exclu du calcul et des factures. À "
        "trancher dans les Réservations : l'exclure du périmètre de gestion (le logement n'est plus à vous) ou, "
        "si le logement revient, reprendre sa gestion."),
    "GESTION_LOGEMENT_OUT_OF_PERIOD": (
        "séjour à cheval sur la fin de gestion", "séjours à cheval sur la fin de gestion",
        "Le séjour commence pendant la gestion et se termine après sa fin : il est exclu du calcul et des "
        "factures. À trancher dans les Réservations : prolonger la gestion jusqu'à son départ s'il relève de "
        "votre gestion, ou l'exclure du périmètre de gestion."),
    "GESTION_LOGEMENT_AMBIGUOUS": (
        "séjour couvert par deux périodes de gestion", "séjours couverts par deux périodes de gestion",
        "Deux périodes de gestion se chevauchent à ses dates : on ne sait pas à quel propriétaire l'attribuer. "
        "Corriger la gestion du logement depuis sa fiche."),
    "GESTION_LOGEMENT_MISSING_OWNER": (
        "séjour sur une période de gestion sans propriétaire", "séjours sur une période de gestion sans propriétaire",
        "La période de gestion de ses dates n'a pas de propriétaire : le renseigner depuis la fiche du logement."),
}
_CODES_GESTION = tuple(_GESTION_TEXTES)


def _reservations_hors_gestion(par, mois: str, noms: _Noms, db_path) -> None:
    """Séjours du mois sans propriétaire exploitable — exclus de l'économie, et jusqu'ici muets.

    Un séjour dont les dates ne tombent dans aucune période de gestion, ou chevauchent la fin de la
    gestion, passe en A_CONTROLER et sort du calcul de commission et de la facturation. Aucun contrôle
    du moteur ne le disait : le mois pouvait se clôturer avec ces séjours absents des factures.

    LU dans le module Réservations (`perimetre_gestion_service`), jamais copié : un séjour est un bloqueur tant
    qu'il est « à trancher » ; une décision d'exclusion (justifiée, tracée, prise dans les Réservations) le sort
    des bloqueurs — il passe parmi les informations —, et prolonger la gestion du logement le fait redevenir un
    séjour géré. « Traiter » ouvre la page de ces séjours, avec leur explication et les vraies actions."""
    from app.services import perimetre_gestion_service as pg

    conn = get_db(db_path)
    try:
        t = _tables(conn)
        if "reservations_resolues" not in t:
            return
        filtre_dataset, params = "", [mois]
        if "reservations_datasets" in t:
            r = conn.execute("SELECT dataset_id FROM reservations_datasets WHERE etape = 'RESOLUES' "
                             "AND actif = 1 ORDER BY rowid DESC LIMIT 1").fetchone()
            if r:
                filtre_dataset, params = " AND dataset_id = ?", [mois, r[0]]
        autres = [dict(r) for r in conn.execute(
            "SELECT logement_id, date_arrivee, date_depart, code_anomalie "
            "FROM reservations_resolues WHERE mois = ? AND statut_controle = 'A_CONTROLER' "
            f"AND code_anomalie IN ('LOGEMENT_NON_MAPPE', 'STATUT_PARC_INVALIDE'){filtre_dataset} "
            "ORDER BY logement_id, date_arrivee", params)]
    finally:
        conn.close()

    sejours = pg.sejours_hors_gestion(mois, db_path=db_path)
    a_trancher = [s for s in sejours if s["etat"] == pg.ETAT_A_TRANCHER]
    exclus = [s for s in sejours if s["etat"] == pg.ETAT_EXCLU]
    for code, (singulier, pluriel, pourquoi) in _GESTION_TEXTES.items():
        concernes = [s for s in a_trancher if s["code"] == code]
        if not concernes:
            continue
        items = [_item(s["logement"], detail=f"{s['periode_fr']} · {s['canal']}", montant=s["montant"],
                       lien=pg.lien_page(mois, "sejour-" + s["cle"]), lien_libelle="Traiter ce séjour")
                 for s in concernes]
        par[RESERVATIONS]["bloqueurs"].append(_groupe(
            "SEJOUR_" + code, singulier, pluriel, items, pourquoi=pourquoi, action="Traiter les séjours",
            lien=pg.lien_page(mois)))
    if exclus:
        items = [_item(s["logement"], detail=f"{s['periode_fr']} · {s['canal']}",
                       lien=pg.lien_page(mois, "sejour-" + s["cle"]), lien_libelle="Voir la décision")
                 for s in exclus]
        par[RESERVATIONS]["informatifs"].append(_groupe(
            "SEJOUR_EXCLU_DECIDE", "séjour exclu du périmètre de gestion",
            "séjours exclus du périmètre de gestion", items,
            pourquoi="Une décision explicite les a sortis du calcul et des factures : ils restent visibles et "
                     "ne bloquent pas la clôture.",
            action="Voir les décisions", lien=pg.lien_page(mois, "exclus")))
    non_mappes = [r for r in autres if r["code_anomalie"] == "LOGEMENT_NON_MAPPE"]
    statut = [r for r in autres if r["code_anomalie"] == "STATUT_PARC_INVALIDE"]
    if non_mappes:
        items = [_item("Annonce sans logement", detail=_du_au(r["date_arrivee"], r["date_depart"]),
                       lien="/correspondances-logement", lien_libelle="Établir la correspondance")
                 for r in non_mappes]
        par[RESERVATIONS]["bloqueurs"].append(_groupe(
            "SEJOUR_LOGEMENT_INCONNU", "séjour d'un logement non reconnu",
            "séjours d'un logement non reconnu", items,
            pourquoi="Hostaway envoie un séjour pour une annonce qui ne correspond à aucun logement.",
            action="Établir la correspondance", lien="/correspondances-logement"))
    if statut:
        items = [_item(noms.logement(r["logement_id"]), detail=_du_au(r["date_arrivee"], r["date_depart"]),
                       lien=f"/logements/{quote(_txt(r['logement_id']))}", lien_libelle="Voir le logement")
                 for r in statut]
        par[RESERVATIONS]["bloqueurs"].append(_groupe(
            "SEJOUR_STATUT_LOGEMENT", "séjour d'un logement au statut invalide",
            "séjours d'un logement au statut invalide", items,
            pourquoi="Le statut du logement (géré, retiré, hors parc) n'est pas renseigné.",
            action="Voir le logement", lien=items[0]["lien"]))


def _calculs_obsoletes(par, mois, db_path) -> None:
    """Un calcul « à recalculer » ou en échec rend ses résultats — et donc les bloqueurs lus ici —
    peu fiables : clôturer dessus serait arrêter un mois sur des chiffres périmés.

    Seuls « à recalculer », « en échec » et « en cours » comptent. « Jamais calculé » n'est PAS un
    retard : rien n'a été produit, donc rien n'est périmé (installation neuve, base de test)."""
    from app.services import orchestrateur_dag as dag
    from app.services import orchestrateur_service as orch

    try:
        etats = {d["dataset"]: d["statut"] for d in orch.etat_datasets(db_path)}
    except Exception:       # noqa: BLE001 — table d'orchestration absente : rien à signaler
        return
    raisons = {orch.ST_A_RECALCULER: "Une donnée en amont a changé : le résultat affiché est périmé.",
               orch.ST_ECHEC: "Le dernier calcul a échoué : le résultat affiché est ancien.",
               orch.ST_EN_COURS: "Un calcul est en cours : attendre sa fin avant de clôturer."}
    from app.services import cloture_actualisation_service as refresh

    calculs = {n: domaine for domaine, noms in refresh.DATASETS_PAR_MODULE.items() for n in noms}
    groupes: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for nom, domaine in calculs.items():
        statut = etats.get(nom)
        if statut not in raisons:
            continue
        noeud = dag.NOEUDS.get(nom)
        groupes.setdefault((domaine, statut), []).append(
            _item(noeud.nom_affiche if noeud else nom))
    for (domaine, statut), items in groupes.items():       # un groupe par domaine et par état, pas par calcul
        g = _groupe(
            f"CALCUL:{statut}", "calcul à actualiser", "calculs à actualiser", items,
            pourquoi=raisons[statut], action="Actualiser")
        g["actualiser"] = statut != orch.ST_EN_COURS and all(
            refresh.calculable(n) for n, d in calculs.items() if d == domaine and etats.get(n) == statut)
        if not g["actualiser"]:
            g["action"] = "Consulter"
            g["lien_libelle"] = "Consulter"
            g["lien"] = PAR_CLE[domaine].consulter.format(mois=quote(mois))
        par[domaine]["bloqueurs"].append(g)


def _entier(v: Any) -> int:
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return 0


def _detail_menage(v: dict[str, Any]) -> str:
    """Pourquoi une ligne du module Ménages est « à contrôler », en deux faits chiffrés."""
    morceaux = []
    if v.get("identification_incomplete"):
        morceaux.append("intervenant ou logement à identifier")
    realises, declares = _entier(v.get("hostaway_realise")), _entier(v.get("total_declares"))
    if v.get("ecart") or realises != declares:
        morceaux.append(f"{_pluriel(realises, 'ménage réalisé', 'ménages réalisés')} chez Hostaway, "
                        f"{_pluriel(declares, 'déclaré ou facturé', 'déclarés ou facturés')}")
    return " · ".join(morceaux)


def _menages_a_controler(par, mois: str, noms: _Noms, db_path) -> None:
    """Ménages : les lignes que le module Ménages lui-même dit « à contrôler ».

    Même critère que la carte « À CONTRÔLER » de l'écran Ménages (`menages_service.a_controler`). Une
    ligne dont l'écart a été JUSTIFIÉ (outrepassé, avec son motif) ou validée n'est plus un bloqueur : la
    décision se prend dans le module, et c'est elle qui compte — pas le constat du moteur, qui continue de
    signaler l'écart brut. Le bloqueur disparaît donc de lui-même quand la ligne est justifiée."""
    from app.services import menages_service as men

    try:
        lignes = men.lignes_a_controler(mois, db_path=db_path)
    except Exception:       # noqa: BLE001 — une lecture qui échoue ne se tait jamais : le module est à revoir
        par[MENAGES]["bloqueurs"].append(_groupe(
            "MENAGES_ILLISIBLES", "lecture des ménages impossible", "lectures des ménages impossibles",
            [_item("Ménages du mois", detail="La lecture a échoué", lien=f"/menages?mois={quote(mois)}",
                   lien_libelle="Ouvrir")],
            pourquoi="Sans lecture des ménages, on ne sait pas s'il reste un écart à contrôler.",
            action="Ouvrir les ménages", lien=f"/menages?mois={quote(mois)}"))
        return
    if not lignes:
        return
    items = []
    for v in lignes:
        nom = _txt(v.get("nom_appartement")) or noms.logement(v.get("logement_id"))
        intervenant = _txt(v.get("nom_intervenant"))
        items.append(_item(f"{nom} — {intervenant}" if intervenant else nom, detail=_detail_menage(v),
                           lien=f"/menages/{quote(mois)}/{quote(_txt(v.get('logement_id')))}/"
                                f"{quote(_txt(v.get('intervenant_id')))}", lien_libelle="Ouvrir"))
    par[MENAGES]["bloqueurs"].append(_groupe(
        "MENAGE_A_CONTROLER", "ligne de ménages à contrôler", "lignes de ménages à contrôler", items,
        pourquoi="Le nombre de ménages réalisés ne correspond pas à celui déclaré ou facturé, et l'écart "
                 "n'est ni validé ni justifié.",
        action="Contrôler les ménages", lien=f"/menages/a-controler?mois={quote(mois)}"))


def _conflits_menages(par, mois: str, noms: _Noms, db_path) -> None:
    conn = get_db(db_path)
    try:
        if "menages_declarations_conflits" not in _tables(conn):
            return
        rows = [dict(r) for r in conn.execute(
            "SELECT logement_id, intervenant_id FROM menages_declarations_conflits "
            "WHERE statut = 'OUVERT' AND mois = ? ORDER BY logement_id", (mois,))]
    finally:
        conn.close()
    if rows:
        items = [_item(noms.logement(r["logement_id"]), detail="Déclarations contradictoires",
                       lien="/menages/conflits", lien_libelle="Résoudre") for r in rows]
        par[MENAGES]["bloqueurs"].append(_groupe(
            "MENAGE_CONFLIT", "conflit de déclaration de ménage", "conflits de déclaration de ménage",
            items, pourquoi="Deux déclarations se contredisent : le coût de ménage du mois est incertain.",
            action="Résoudre les conflits", lien="/menages/conflits"))


def _factures_clients(par, mois: str, noms: _Noms, db_path) -> None:
    """Facturation du mois : ce qui reste à créer, à valider, à émettre."""
    from app.services import factures_proprietaires_service as fpr

    try:
        factures = fpr.lister(mois=mois, db_path=db_path)
    except Exception:       # noqa: BLE001 — table absente d'une base minimale : aucune facture
        factures = []
    for statut, cle, sing, plur, pourquoi, action in (
            (fpr.ST_BROUILLON, "FACTURE_BROUILLON", "facture en brouillon", "factures en brouillon",
             "Un brouillon n'a aucun effet : tant qu'il n'est pas émis, le propriétaire n'est pas facturé.",
             "Valider et émettre"),
            (fpr.ST_VALIDE, "FACTURE_VALIDEE", "facture validée à émettre", "factures validées à émettre",
             "Une facture validée n'a pas encore de numéro : elle n'est pas adressée au propriétaire.",
             "Émettre")):
        fs = [f for f in factures if f["statut"] == statut]
        if not fs:
            continue
        items = [_item(f"{noms.proprietaire(f['proprietaire_id'])} — {noms.logement(f['logement_id'])}",
                       detail="Avoir" if f.get("type_document") == fpr.TYPE_AVOIR else "Facture",
                       montant=f.get("montant_total"),
                       lien=f"/factures-proprietaires/{quote(f['facture_id_opaque'])}",
                       lien_libelle="Ouvrir") for f in fs]
        par[FACTURES_CLIENTS]["bloqueurs"].append(_groupe(
            cle, sing, plur, items, pourquoi=pourquoi, action=action,
            lien=f"/factures-proprietaires?mois={quote(mois)}&statut={quote(statut)}"))

    try:
        from app.readers import proprietaires_reader as prop_reader
        from app.services import factures_proprietaires_source as source
        ids = [p["proprietaire_id"] for p in prop_reader.read_proprietaires() if p.get("proprietaire_id")]
        propositions = source.propositions_du_mois(mois, ids, db_path=db_path)
    except Exception:       # noqa: BLE001 — pas de préfacture lisible : rien à proposer
        propositions = []
    a_creer = [p for p in propositions if p["statut_proposition"] == "PRETE"]
    a_controler = [p for p in propositions if p["statut_proposition"] == "A_CONTROLER"]
    lien_proposer = f"/factures-proprietaires/proposer?mois={quote(mois)}"
    if a_creer:
        items = [_item(f"{noms.proprietaire(p['proprietaire_id'])} — {noms.logement(p['logement_id'])}",
                       montant=p.get("montant_total"), lien=lien_proposer, lien_libelle="Créer")
                 for p in a_creer]
        par[FACTURES_CLIENTS]["bloqueurs"].append(_groupe(
            "FACTURE_A_CREER", "facture propriétaire à créer", "factures propriétaires à créer", items,
            pourquoi="Le calcul du mois produit un montant à facturer, mais aucune facture n'existe encore.",
            action="Créer les factures", lien=lien_proposer))
    if a_controler:
        items = [_item(f"{noms.proprietaire(p['proprietaire_id'])} — {noms.logement(p['logement_id'])}",
                       detail=_txt(p.get("detail")), lien=lien_proposer, lien_libelle="Voir")
                 for p in a_controler]
        par[FACTURES_CLIENTS]["bloqueurs"].append(_groupe(
            "FACTURE_NON_PROPOSABLE", "facture impossible à proposer", "factures impossibles à proposer",
            items, pourquoi="Des contrôles empêchent de proposer la facture : le propriétaire ne serait "
                            "pas facturé.", action="Voir les propositions", lien=lien_proposer))


def _factures_a_comptabiliser(par, mois: str, noms: _Noms, db_path) -> None:
    """Facture ÉMISE mais pas encore comptabilisée — depuis le 2026-10-04, émettre ne comptabilise plus.

    Une facture dont l'écriture est seulement PROPOSÉE est déjà dite par Flux (« écriture proposée à
    valider ») : elle n'est pas comptée deux fois."""
    from app.services import comptabilite_ecritures_service as compta
    from app.services import factures_proprietaires_service as fpr

    try:
        emises = [f for f in fpr.lister(mois=mois, statut=fpr.ST_EMIS, db_path=db_path)
                  if not int(f.get("hors_compta") or 0)]
    except Exception:       # noqa: BLE001
        return
    a_faire = []
    for f in emises:
        etat = compta.etat_comptabilisation_facture(f, db_path=db_path)
        if etat["etat"] == compta.COMPTA_COMPTABILISEE or etat["ecriture"] is not None:
            continue
        a_faire.append(f)
    if a_faire:
        items = [_item(f"{noms.proprietaire(f['proprietaire_id'])} — {_txt(f.get('numero_facture')) or 'facture'}",
                       montant=f.get("montant_total"), date_=f.get("date_facture") or "",
                       lien=f"/factures-proprietaires/{quote(f['facture_id_opaque'])}/comptabiliser",
                       lien_libelle="Comptabiliser") for f in a_faire]
        par[COMPTABILITE]["bloqueurs"].append(_groupe(
            "FACTURE_A_COMPTABILISER", "facture émise à comptabiliser", "factures émises à comptabiliser",
            items, pourquoi="Une facture émise n'est dans les comptes qu'après validation de sa "
                            "comptabilisation.", action="Comptabiliser",
            lien=items[0]["lien"] if len(items) == 1 else f"/factures-proprietaires?mois={quote(mois)}&statut=EMIS&comptabilisee=non"))


def _positions_de_compte(par, noms: _Noms, db_path) -> None:
    """Créances : l'enregistrement de la position d'un compte propriétaire est-il à jour ?

    `etat_persistance` existe pour ça : l'affichage est toujours juste (calculé en mémoire), seul
    l'enregistrement peut être en retard. Clôturer sur une position périmée fixerait un état faux."""
    from app.services import compte_proprietaire_service as cpt

    try:
        proprietaires = cpt.proprietaires_concernes(db_path=db_path)
    except Exception:       # noqa: BLE001 — tables de compte absentes : aucune position
        return
    retards = []
    for pid in proprietaires:
        try:
            if not cpt.etat_persistance(pid, db_path=db_path)["a_jour"]:
                retards.append(pid)
        except Exception:   # noqa: BLE001 — une position illisible ne se lit pas : à recalculer
            retards.append(pid)
    if retards:
        items = [_item(f"Compte de {noms.proprietaire(pid)}", detail="Enregistrement en retard sur les données",
                       lien=f"/comptes-proprietaires/{quote(pid)}", lien_libelle="Recalculer")
                 for pid in retards]
        par[CREANCES]["bloqueurs"].append(_groupe(
            "POSITION_A_RECALCULER", "position de compte à recalculer", "positions de compte à recalculer",
            items, pourquoi="L'enregistrement de la position ne correspond plus aux factures et aux "
                            "règlements : la figer l'arrêterait faussée.",
            action="Recalculer les positions", lien=items[0]["lien"] if len(items) == 1 else "/comptes-proprietaires"))


_CONTROLES_COMPTABLES = {
    "CTRL_CPT_ECRITURE_DESEQUILIBREE": ("écriture déséquilibrée", "écritures déséquilibrées",
                                         "Le total des débits diffère du total des crédits."),
    "CTRL_CPT_COMPTE_ABSENT": ("ligne d'écriture sur un compte inconnu", "lignes d'écriture sur un compte inconnu",
                               "Le compte utilisé n'existe pas dans le plan comptable."),
    "CTRL_CPT_COMPTE_INACTIF": ("ligne d'écriture sur un compte désactivé",
                                "lignes d'écriture sur un compte désactivé",
                                "Le compte utilisé a été désactivé."),
    "CTRL_CPT_DOUBLON_ECRITURE": ("écriture en double", "écritures en double",
                                  "Plusieurs écritures actives existent pour la même origine."),
}


def _controles_comptables(par, mois: str, db_path) -> None:
    from app.services import comptabilite_controles_service as ctrl

    rapport = ctrl.controler(periode=mois, db_path=db_path)
    if rapport.get("statut") == "INDISPONIBLE":
        return
    par_code: dict[str, list[dict[str, Any]]] = {}
    for a in rapport["anomalies"]:
        if a["severite"] == ctrl.BLOQUANT:
            par_code.setdefault(a["code"], []).append(a)
    for code, anos in par_code.items():
        sing, plur, pourquoi = _CONTROLES_COMPTABLES.get(
            code, ("anomalie comptable bloquante", "anomalies comptables bloquantes",
                   "Un contrôle comptable bloquant est ouvert."))
        items = [_item(_txt(a["message"]), montant=a.get("montant") if isinstance(a.get("montant"), (int, float)) else None,
                       lien=f"/comptabilite/a-controler", lien_libelle="Ouvrir") for a in anos]
        par[COMPTABILITE]["bloqueurs"].append(_groupe(
            code, sing, plur, items, pourquoi=pourquoi, action="Contrôler les écritures",
            lien=f"/comptabilite/ecritures?periode={quote(mois)}"))


# ══ Assemblage ════════════════════════════════════════════════════════════════════════════════════

def analyser(mois: str, *, elements: list[dict[str, Any]] | None = None,
             flux: dict[str, Any] | None = None, contexte_flux: dict | None = None,
             db_path=None) -> dict[str, Any]:
    """Bloqueurs et informatifs de chaque module pour `mois`. LECTURE SEULE, recalculée à chaque appel.

    `elements` (contrôles du moteur du mois) et `flux` (analyse Flux du mois) peuvent être fournis par
    l'appelant qui les a déjà lus : on ne les relit pas. Un module n'a JAMAIS de bloqueur recopié —
    ce sont les sources qui sont interrogées."""
    from app.services import clotures_service as cs
    from app.services import cloture_flux_service as cf
    from app.services import perimetre_gestion_service as pg

    mois = _txt(mois)[:7]
    if elements is None:
        elements = cs.elements_du_mois(mois, db_path)
    if flux is None:
        flux = cf.analyser(mois, contexte_flux=contexte_flux, db_path=db_path)
    noms = _Noms(db_path)

    par: dict[str, dict[str, list]] = {m.cle: {"bloqueurs": [], "informatifs": []} for m in MODULES}
    _ajouter_controles_moteur(par, mois, elements, noms, pg.reservations_exclues(db_path=db_path))
    _ajouter_flux(par, mois, flux)
    _charges(par, mois, noms, db_path)
    _reservations_hors_gestion(par, mois, noms, db_path)
    _calculs_obsoletes(par, mois, db_path)
    _menages_a_controler(par, mois, noms, db_path)
    _conflits_menages(par, mois, noms, db_path)
    _factures_clients(par, mois, noms, db_path)
    _factures_a_comptabiliser(par, mois, noms, db_path)
    _positions_de_compte(par, noms, db_path)
    _controles_comptables(par, mois, db_path)

    modules = []
    for m in MODULES:
        bloqueurs, infos = par[m.cle]["bloqueurs"], par[m.cle]["informatifs"]
        modules.append({
            "cle": m.cle, "libelle": m.libelle, "resume": m.resume, "verrouille": list(m.verrouille),
            "consulter": m.consulter.format(mois=quote(mois)),
            "bloqueurs": bloqueurs, "informatifs": infos,
            "nb_bloqueurs": sum(g["nb"] for g in bloqueurs),
            "nb_informatifs": sum(g["nb"] for g in infos),
        })
    return {"mois": mois, "modules": modules, "par_cle": {m["cle"]: m for m in modules},
            "nb_bloqueurs": sum(m["nb_bloqueurs"] for m in modules),
            "nb_informatifs": sum(m["nb_informatifs"] for m in modules)}


# ══ Clôturer, rouvrir, tableau de bord ════════════════════════════════════════════════════════════
#
# L'état « clôturé » d'un module est le SEUL fait stocké (migration 0126). Il est vérifié contre le
# réel à chaque affichage : un module clôturé dont un bloqueur est réapparu (donnée reçue après coup)
# s'affiche « à rouvrir », il ne reste pas faussement vert.

from datetime import datetime                                                      # noqa: E402

ETAT_CLOS = "CLOS"
ETAT_A_TRAITER = "A_TRAITER"
ETAT_PRET = "PRET"
ETAT_A_REVOIR = "A_REVOIR"
LIBELLES_ETAT = {ETAT_CLOS: "Clôturé", ETAT_A_TRAITER: "À traiter", ETAT_PRET: "Prêt à clôturer",
                 ETAT_A_REVOIR: "À rouvrir"}

MSG_NON_DEMARREE = ("Démarrez d'abord la clôture du mois : les modules se clôturent une fois la "
                    "clôture ouverte.")
MSG_MOIS_ARCHIVE = ("Le mois est clôturé définitivement : ses modules ne se rouvrent plus un par un. Pour "
                    "corriger, rouvrez exceptionnellement le mois entier (avec une justification) depuis la "
                    "clôture mensuelle.")


def _maintenant() -> str:
    """Heure LOCALE du poste, comme `clotures_service._now()` : c'est celle que l'utilisateur lit à
    l'écran (« clôturé à 22h32 ») et rapproche de ses propres gestes."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _refus(message: str):
    from app.services import clotures_service as cs
    return cs.ClotureRefusee(message)


def _module_connu(module: str) -> Module:
    m = PAR_CLE.get(_txt(module).upper())
    if m is None:
        raise _refus("Module de clôture inconnu.")
    return m


def etats(mois: str, *, db_path=None) -> dict[str, dict[str, Any]]:
    """État enregistré des modules d'un mois — {clé: ligne}. Table absente (base non migrée) : vide."""
    conn = get_db(db_path)
    try:
        if "cloture_modules" not in _tables(conn):
            return {}
        return {r["module"]: dict(r) for r in conn.execute(
            "SELECT * FROM cloture_modules WHERE mois = ?", (_txt(mois)[:7],))}
    finally:
        conn.close()


def _periode_comptable_rouverte(mois: str, db_path) -> bool:
    """Le verrou de la Comptabilité est la PÉRIODE COMPTABLE : si elle n'est plus clôturée (rouverte
    depuis l'écran de comptabilité), le module n'est plus réellement verrouillé."""
    from app.services import comptabilite_periodes_service as per

    return not per.est_fermee(mois, db_path)


def modules_non_clos(mois: str, *, db_path=None) -> list[str]:
    """Clés des modules qui ne sont PAS clôturés pour ce mois (dans l'ordre d'affichage).

    Un module est clôturé quand sa clôture est enregistrée ET que son verrou tient encore : la
    Comptabilité, dont le verrou est la période comptable, ne l'est plus si cette période a été
    rouverte depuis. C'est le même critère que le tableau de bord — la clôture du mois côté serveur
    ne peut pas être plus indulgente que ce que l'écran affiche."""
    enregistres = etats(mois, db_path=db_path)
    restants = [m.cle for m in MODULES if enregistres.get(m.cle, {}).get("statut") != "CLOS"]
    if COMPTABILITE not in restants and _periode_comptable_rouverte(mois, db_path):
        restants.append(COMPTABILITE)
    return [m.cle for m in MODULES if m.cle in restants]


def historique(mois: str, module: str = "", *, db_path=None) -> list[dict[str, Any]]:
    """Journal des clôtures et réouvertures de modules, du plus récent au plus ancien."""
    conn = get_db(db_path)
    try:
        if "cloture_modules_evenements" not in _tables(conn):
            return []
        sql, params = "SELECT * FROM cloture_modules_evenements WHERE mois = ?", [_txt(mois)[:7]]
        if module:
            sql, params = sql + " AND module = ?", params + [module]
        rows = [dict(r) for r in conn.execute(sql + " ORDER BY id DESC", params)]
    finally:
        conn.close()
    for r in rows:
        m = PAR_CLE.get(r["module"])
        r["module_libelle"] = m.libelle if m else r["module"]
    return rows


def _garde_cloture_demarree(cloture: dict[str, Any], *, ouvrir: bool) -> None:
    """La clôture du mois est démarrée et le mois n'est pas définitivement clôturé."""
    from app.services import clotures_service as cs

    statut = cloture["statut"]
    if statut == cs.ST_ARCHIVEE:
        raise _refus(MSG_MOIS_ARCHIVE if ouvrir else cs.MSG_DEJA_ARCHIVEE)
    if statut == cs.ST_NON_DEMARREE:
        raise _refus(MSG_NON_DEMARREE)


def _cloturer_periode_comptable(mois: str, *, acteur: str, commentaire: str, db_path=None) -> None:
    """Clôturer le module Comptabilité = clôturer la PÉRIODE COMPTABLE du mois — le verrou que le
    moteur d'écritures respecte déjà. Ses étapes (en contrôle, validée, clôturée) sont enchaînées par
    le service existant, jamais contournées : il refuse lui-même une période aux contrôles bloquants."""
    from app.services import comptabilite_periodes_service as per

    if not per._flags_actifs():
        raise _refus("L'écriture comptable est désactivée sur cette installation : la période "
                     "comptable ne peut pas être clôturée.")
    periode = per.charger(mois, db_path)
    statut = periode["statut"] if periode else per.ST_OUVERTE
    if statut == per.ST_CLOTUREE:
        return
    etapes = []
    if statut == per.ST_OUVERTE:
        etapes.append(lambda: per.passer_en_controle(mois, acteur=acteur, db_path=db_path))
    elif statut == per.ST_ROUVERTE:
        etapes.append(lambda: per.rouvrir_pour_controle(mois, acteur=acteur, db_path=db_path))
    if statut in (per.ST_OUVERTE, per.ST_ROUVERTE, per.ST_EN_CONTROLE):
        etapes.append(lambda: per.valider(mois, acteur=acteur, db_path=db_path))
    etapes.append(lambda: per.cloturer(mois, acteur=acteur, commentaire=commentaire, db_path=db_path))
    for etape in etapes:
        res = etape()
        if not res.get("ok"):
            raise _refus(res.get("message") or "La période comptable n'a pas pu être clôturée.")


def cloturer_module(cloture: dict[str, Any], module: str, *, acteur: str = "", commentaire: str = "",
                    db_path=None) -> dict[str, Any]:
    """Clôture UN module du mois. Refuse — message métier, jamais une erreur SQL — si :
    la clôture n'est pas démarrée ou le mois est clôturé définitivement ; le mois n'est pas terminé
    (courant et futur ne se clôturent jamais) ; le module est déjà clôturé ; un bloqueur subsiste
    (RECALCULÉ ici, depuis les vrais modules, quelle que soit l'interface).

    Effet réel : le module est VERROUILLÉ — ses services refusent leurs écritures sur le mois (voir
    `cloture_verrous_service`) ; pour la Comptabilité, la période comptable est clôturée. Date, heure,
    acteur et résumé sont tracés, en ajout seul."""
    from app.services import clotures_service as cs

    m = _module_connu(module)
    mois = cloture["mois"]
    _garde_cloture_demarree(cloture, ouvrir=False)
    refus_temporel = cs.refus_temporel(mois)
    if refus_temporel:
        raise _refus(refus_temporel)
    if etats(mois, db_path=db_path).get(m.cle, {}).get("statut") == "CLOS":
        raise _refus(f"Le module « {m.libelle} » est déjà clôturé pour {cs.mois_fr(mois)}.")

    def _controler() -> dict[str, Any]:
        analyse = analyser(mois, db_path=db_path)["par_cle"][m.cle]
        if analyse["nb_bloqueurs"]:
            n = analyse["nb_bloqueurs"]
            raise _refus(f"Le module « {m.libelle} » ne peut pas être clôturé : {n} bloqueur"
                         f"{'s' if n > 1 else ''} reste{'nt' if n > 1 else ''} à traiter.")
        return analyse

    _controler()
    if m.cle == COMPTABILITE:
        _cloturer_periode_comptable(mois, acteur=acteur, commentaire=commentaire, db_path=db_path)

    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        actuelle = conn.execute("SELECT statut FROM clotures_mensuelles WHERE cloture_id_opaque = ? "
                                "AND actif = 1", (cloture["cloture_id_opaque"],)).fetchone()
        if actuelle is None:
            raise _refus("Clôture introuvable.")
        _garde_cloture_demarree({"statut": actuelle["statut"]}, ouvrir=False)
        analyse = _controler()                       # relu sous verrou d'écriture : aucune course
        existant = conn.execute("SELECT statut FROM cloture_modules WHERE mois = ? AND module = ?",
                                (mois, m.cle)).fetchone()
        if existant is not None and existant["statut"] == "CLOS":
            raise _refus(f"Le module « {m.libelle} » est déjà clôturé pour {cs.mois_fr(mois)}.")
        maintenant = _maintenant()
        conn.execute(
            "INSERT INTO cloture_modules (mois, module, cloture_id_opaque, statut, date_cloture, "
            "acteur_cloture, commentaire_cloture, nb_clotures) VALUES (?,?,?,'CLOS',?,?,?,1) "
            "ON CONFLICT(mois, module) DO UPDATE SET statut = 'CLOS', "
            "cloture_id_opaque = excluded.cloture_id_opaque, date_cloture = excluded.date_cloture, "
            "acteur_cloture = excluded.acteur_cloture, "
            "commentaire_cloture = excluded.commentaire_cloture, nb_clotures = nb_clotures + 1",
            (mois, m.cle, cloture["cloture_id_opaque"], maintenant, acteur, commentaire))
        n_info = analyse["nb_informatifs"]
        resume = "Aucun bloqueur" + (f" ; {n_info} information{'s' if n_info > 1 else ''} ne "
                                     f"bloque{'nt' if n_info > 1 else ''} pas" if n_info else "")
        conn.execute(
            "INSERT INTO cloture_modules_evenements (mois, module, cloture_id_opaque, type_evenement, "
            "ancien_statut, nouveau_statut, commentaire, resume, date_evenement, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (mois, m.cle, cloture["cloture_id_opaque"], "CLOTURE_MODULE",
             existant["statut"] if existant else None, "CLOS", commentaire, resume, maintenant, acteur))
        cs._journaliser_evenement(conn, cloture["cloture_id_opaque"], "MODULE_CLOTURE", None, None,
                                  commentaire=f"{m.libelle} : clôturé. {resume}.", acteur=acteur,
                                  date_evenement=maintenant)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return etats(mois, db_path=db_path)[m.cle]


def rouvrir_module(cloture: dict[str, Any], module: str, *, acteur: str = "", justification: str = "",
                   db_path=None) -> dict[str, Any]:
    """Rouvre un module clôturé — explicite, justifié, tracé. Jamais une suppression : la clôture
    d'origine reste dans le journal (date, acteur), la réouverture s'y ajoute.

    Si la clôture du mois avait déjà été validée (A_VALIDER, VALIDEE), elle revient en préparation : un
    mois dont un module est rouvert n'est plus « prêt ». Un mois clôturé DÉFINITIVEMENT ne se rouvre
    pas (contrat existant : correction rétroactive)."""
    from app.services import clotures_service as cs

    m = _module_connu(module)
    mois = cloture["mois"]
    if not _txt(justification):
        raise _refus("Une justification est obligatoire pour rouvrir un module clôturé.")
    _garde_cloture_demarree(cloture, ouvrir=True)
    if etats(mois, db_path=db_path).get(m.cle, {}).get("statut") != "CLOS":
        raise _refus(f"Le module « {m.libelle} » n'est pas clôturé : rien à rouvrir.")

    if m.cle == COMPTABILITE:
        from app.services import comptabilite_periodes_service as per
        if per.est_fermee(mois, db_path):
            res = per.rouvrir(mois, justification=justification, acteur=acteur, db_path=db_path)
            if not res.get("ok"):
                raise _refus(res.get("message") or "La période comptable n'a pas pu être rouverte.")

    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        actuelle = conn.execute("SELECT * FROM clotures_mensuelles WHERE cloture_id_opaque = ? "
                                "AND actif = 1", (cloture["cloture_id_opaque"],)).fetchone()
        if actuelle is None:
            raise _refus("Clôture introuvable.")
        actuelle = dict(actuelle)
        _garde_cloture_demarree(actuelle, ouvrir=True)
        maintenant = _maintenant()
        cur = conn.execute(
            "UPDATE cloture_modules SET statut = 'ROUVERT', date_reouverture = ?, "
            "acteur_reouverture = ?, justification_reouverture = ?, nb_reouvertures = nb_reouvertures + 1 "
            "WHERE mois = ? AND module = ? AND statut = 'CLOS'",
            (maintenant, acteur, justification, mois, m.cle))
        if cur.rowcount != 1:
            raise _refus(f"Le module « {m.libelle} » a changé d'état entretemps : rechargez la page.")
        conn.execute(
            "INSERT INTO cloture_modules_evenements (mois, module, cloture_id_opaque, type_evenement, "
            "ancien_statut, nouveau_statut, commentaire, resume, date_evenement, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (mois, m.cle, cloture["cloture_id_opaque"], "REOUVERTURE_MODULE", "CLOS", "ROUVERT",
             justification, "Module rouvert", maintenant, acteur))
        cs._journaliser_evenement(conn, cloture["cloture_id_opaque"], "MODULE_ROUVERT", None, None,
                                  commentaire=f"{m.libelle} : rouvert. {justification}", acteur=acteur,
                                  date_evenement=maintenant)
        # Une clôture déjà validée ne l'est plus : elle repasse en préparation, par son automate.
        courante = actuelle
        if courante["statut"] == cs.ST_VALIDEE:
            courante = cs._transition(courante, cs.ST_ROUVERTE, acteur=acteur,
                                      justification=f"Module {m.libelle} rouvert : {justification}",
                                      conn=conn, extra_cols={"date_reouverture": cs._now(),
                                                             "justification_reouverture": justification})
        if courante["statut"] in (cs.ST_ROUVERTE, cs.ST_A_VALIDER):
            cs._transition(courante, cs.ST_EN_PREPARATION, acteur=acteur,
                           justification=f"Module {m.libelle} rouvert : {justification}", conn=conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return etats(mois, db_path=db_path)[m.cle]


def rouvrir_tous_dans(conn, cloture_opaque: str, mois: str, *, acteur: str, justification: str,
                      maintenant: str) -> list[str]:
    """Rouvre TOUS les modules clôturés d'un mois, sur la connexion de l'appelant (aucun commit) — la
    réouverture exceptionnelle du mois : un mois rouvert dont les domaines resteraient verrouillés ne
    permettrait de corriger rien.

    Chaque module repasse à « rouvert » avec la même justification ; la clôture d'origine (date, acteur,
    commentaire) reste sur la ligne du module et dans le journal en ajout seul, auquel s'ajoute une
    ligne « réouverture » par module. Rend les clés des modules rouverts, dans l'ordre d'affichage."""
    if "cloture_modules" not in _tables(conn):
        return []
    rouverts = [r["module"] for r in conn.execute(
        "SELECT module FROM cloture_modules WHERE mois = ? AND statut = 'CLOS'", (mois,))]
    texte = f"Réouverture exceptionnelle du mois : {justification}"
    for cle in rouverts:
        conn.execute(
            "UPDATE cloture_modules SET statut = 'ROUVERT', date_reouverture = ?, acteur_reouverture = ?, "
            "justification_reouverture = ?, nb_reouvertures = nb_reouvertures + 1 "
            "WHERE mois = ? AND module = ? AND statut = 'CLOS'", (maintenant, acteur, texte, mois, cle))
        conn.execute(
            "INSERT INTO cloture_modules_evenements (mois, module, cloture_id_opaque, type_evenement, "
            "ancien_statut, nouveau_statut, commentaire, resume, date_evenement, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (mois, cle, cloture_opaque, "REOUVERTURE_MODULE", "CLOS", "ROUVERT", texte,
             "Module rouvert avec le mois", maintenant, acteur))
    return [m.cle for m in MODULES if m.cle in rouverts]


def _horodatage_fr(valeur: Any) -> str:
    """« 2026-09-11 22:32:10 » → « 11/09/2026 à 22h32 ». Une date seule reste une date."""
    s = _txt(valeur).replace("T", " ").replace("Z", "")
    if not s:
        return ""
    jour = _date_fr(s[:10])
    heure = s[11:16]
    return f"{jour} à {heure.replace(':', 'h')}" if len(heure) == 5 else jour


def _etat_de_la_cloture(statut: str) -> tuple[str, str]:
    """Où en est la clôture du mois, en deux mots — (clé, libellé) pour le bandeau de synthèse."""
    from app.services import clotures_service as cs

    if statut == cs.ST_NON_DEMARREE:
        return "NON_DEMARREE", "Clôture non démarrée"
    if statut == cs.ST_ARCHIVEE:
        return "CLOTURE", "Mois clôturé"
    if statut == cs.ST_VALIDEE:
        return "VALIDEE", "Clôture validée"
    if statut == cs.ST_ROUVERTE:
        return "EN_COURS", "Clôture rouverte"       # un mois clôturé rouvert exceptionnellement, à reclôturer
    return "EN_COURS", "Clôture en cours"


def _cloture_du_mois(cloture: dict[str, Any], db_path) -> dict[str, str]:
    """Quand et par qui le MOIS a été clôturé définitivement (dernière transition vers ARCHIVEE)."""
    from app.services import clotures_service as cs

    if not cloture.get("cloture_id_opaque"):
        return {"date": "", "acteur": ""}
    for e in cs.historique(cloture["cloture_id_opaque"], db_path):
        if e.get("type_evenement") == "TRANSITION" and e.get("nouveau_statut") == cs.ST_ARCHIVEE:
            return {"date": _horodatage_fr(e.get("date_evenement")), "acteur": _txt(e.get("acteur"))}
    return {"date": "", "acteur": ""}


def tableau_de_bord(cloture: dict[str, Any], *, progression: dict[str, Any] | None = None,
                    db_path=None) -> dict[str, Any]:
    """Le tableau de bord d'un mois : pour chaque module, son état, ses bloqueurs, ses actions
    possibles — et la possibilité de clôturer le mois entier. Tout est recalculé ; seul l'état
    « clôturé » (date, acteur) vient de la base.

    `cloture` peut être une clôture pas encore créée (`{"mois": …, "statut": "NON_DEMARREE"}`) : la page
    d'un mois montre alors ses bloqueurs avant même qu'on ait démarré la clôture."""
    from app.services import clotures_service as cs

    mois = cloture["mois"]
    prog = progression or cs.calcul_progression(mois, db_path)
    enregistres = etats(mois, db_path=db_path)
    refus_temporel = prog["refus_temporel"]
    demarree = cloture["statut"] != cs.ST_NON_DEMARREE
    archivee = cloture["statut"] == cs.ST_ARCHIVEE

    modules = []
    for m in prog["modules"]["modules"]:
        p = enregistres.get(m["cle"]) or {}
        clos = p.get("statut") == "CLOS"
        periode_rouverte = (m["cle"] == COMPTABILITE and clos and not archivee
                            and _periode_comptable_rouverte(mois, db_path))
        if archivee or (clos and not m["nb_bloqueurs"] and not periode_rouverte):
            etat = ETAT_CLOS
        elif clos:
            etat = ETAT_A_REVOIR
        elif m["nb_bloqueurs"]:
            etat = ETAT_A_TRAITER
        else:
            etat = ETAT_PRET
        n = m["nb_bloqueurs"]
        if etat == ETAT_A_REVOIR:
            motif = (f"{n} élément{'s' if n > 1 else ''} à traiter {'sont apparus' if n > 1 else 'est apparu'} "
                     "depuis la clôture : rouvrez le module pour le traiter." if n else
                     "La période comptable a été rouverte depuis la clôture de ce module.")
        elif archivee or clos:
            motif = ""
        elif n:
            motif = f"{n} bloqueur{'s' if n > 1 else ''} à traiter avant de clôturer ce module."
        elif refus_temporel:
            # Le motif complet (« le mois est encore en cours… ») est dit UNE fois, dans la synthèse. Un mois qui court
            # ne se démarre pas : « disponible une fois la clôture démarrée » ferait croire qu'un bouton existe.
            motif = "Possible une fois le mois terminé."
        elif not demarree:
            motif = "Disponible une fois la clôture démarrée."
        else:
            motif = ""
        modules.append({
            **m, "etat": etat, "etat_libelle": LIBELLES_ETAT[etat],
            "date_cloture": p.get("date_cloture") or "" if clos else "",
            "date_cloture_fr": _horodatage_fr(p.get("date_cloture")) if clos else "",
            "acteur_cloture": p.get("acteur_cloture") or "" if clos else "",
            "commentaire_cloture": p.get("commentaire_cloture") or "" if clos else "",
            "date_reouverture": p.get("date_reouverture") or "",
            "date_reouverture_fr": _horodatage_fr(p.get("date_reouverture")),
            "acteur_reouverture": p.get("acteur_reouverture") or "",
            "justification_reouverture": p.get("justification_reouverture") or "",
            "nb_reouvertures": p.get("nb_reouvertures") or 0,
            "peut_cloturer": (not archivee and demarree and not refus_temporel and not clos and not n),
            "peut_rouvrir": (not archivee and clos), "motif_indisponible": motif,
        })
    nb_total = len(modules)
    nb_clos = sum(1 for m in modules if m["etat"] == ETAT_CLOS)
    restants = [m["libelle"] for m in modules if m["etat"] != ETAT_CLOS]
    if archivee:
        motif_mois = cs.MSG_DEJA_ARCHIVEE
    elif not demarree:
        motif_mois = MSG_NON_DEMARREE
    elif refus_temporel:
        motif_mois = refus_temporel
    elif restants:
        k = len(restants)
        motif_mois = (f"{k} module{'s' if k > 1 else ''} {'restent' if k > 1 else 'reste'} à clôturer"
                      + (" : " + ", ".join(restants) if k <= 3 else "") + ".")
    elif prog["nb_bloqueurs"]:
        motif_mois = cs.message_bloquants(prog["nb_bloqueurs"])
    else:
        motif_mois = ""
    cle_etat, libelle_etat = _etat_de_la_cloture(cloture["statut"])
    finale = _cloture_du_mois(cloture, db_path) if archivee else {"date": "", "acteur": ""}
    return {
        "mois": mois, "mois_fr": prog["mois_fr"], "du_mois": cs.du_mois(mois), "modules": modules,
        "nb_clos": nb_clos, "nb_total": nb_total,
        "nb_prets": sum(1 for m in modules if m["etat"] == ETAT_PRET),
        "nb_a_traiter": sum(1 for m in modules if m["etat"] in (ETAT_A_TRAITER, ETAT_A_REVOIR)),
        "progression_libelle": f"{nb_clos} / {nb_total} modules clôturés",
        "statut": cloture["statut"], "etat_cloture": cle_etat, "etat_cloture_libelle": libelle_etat,
        "temporalite": prog["temporalite"], "refus_temporel": refus_temporel, "demarree": demarree,
        "refus_demarrage": cs.refus_demarrage(mois),
        "mois_clos": archivee, "peut_cloturer_mois": not archivee and not motif_mois,
        "motif_mois": motif_mois, "nb_bloqueurs": prog["nb_bloqueurs"],
        "nb_informatifs": prog["modules"]["nb_informatifs"],
        "date_cloture_mois_fr": finale["date"], "acteur_cloture_mois": finale["acteur"],
        "ouverte_le_fr": _date_fr(cloture.get("date_creation")),
    }
