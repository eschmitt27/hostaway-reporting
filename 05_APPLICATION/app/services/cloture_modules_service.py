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
           ("Les déclarations de ménage de ce mois et leur recalcul sont refusés.",
            "L'actualisation des ménages signale les changements reçus sans recalculer le mois."),
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

#: Contrôles remplacés par une lecture DIRECTE (même règle, mais relue en direct plutôt qu'au
#: prochain calcul du moteur) : les compter deux fois les ferait apparaître en double — et le
#: constat moteur resterait affiché après la validation de la charge, jusqu'au calcul suivant.
CODES_LUS_EN_DIRECT = {"CHARGE_NON_VALIDEE_HORS_CALCULS"}

_LIEN_RESERVATIONS = "/reservations?mois={mois}&statut_controle=A_CONTROLER"


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
    "MENAGE_EXTERNE_ECART_HOSTAWAY": _Trad(
        "ménage facturé non rapproché des ménages Hostaway",
        "ménages facturés non rapprochés des ménages Hostaway",
        "Le nombre de ménages facturés ne correspond pas à celui d'Hostaway.",
        "Rapprocher les ménages", "/menages?mois={mois}&ecart_seul=true"),
    "MENAGE_EXTERNE_LOGEMENT_HORS_HA": _Trad(
        "logement facturé absent du comptage Hostaway", "logements facturés absents du comptage Hostaway",
        "Des ménages sont facturés pour un logement qu'Hostaway ne connaît pas ce mois-ci.",
        "Rapprocher les ménages", "/menages?mois={mois}&ecart_seul=true"),
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


def _ajouter_controles_moteur(par: dict[str, dict[str, list]], mois: str, elements: list[dict[str, Any]],
                              noms: _Noms) -> None:
    blocs: dict[tuple[str, str, bool], list[dict[str, Any]]] = {}
    for e in elements:
        if e["code"] in CODES_LUS_EN_DIRECT:
            continue
        domaine = MODULE_DES_CONTROLES.get(_txt(e["module"]), DOMAINE_PAR_DEFAUT)
        if e["est_info"]:
            blocs.setdefault((domaine, e["code"], True), []).append(e)
            continue
        if e["etat"]["anomalie_moteur_presente"] and not e["etat"]["exception_active"]:
            blocs.setdefault((domaine, e["code"], False), []).append(e)
    for (domaine, code, info), els in blocs.items():
        trad = TRADUCTIONS.get(code, _TRAD_DEFAUT)
        items = [_element_item(e, noms, mois) for e in els]
        lien = trad.lien.format(mois=mois) if trad.lien else (items[0]["lien"] if items else "")
        if info:
            par[domaine]["informatifs"].append(_groupe(
                code, trad.singulier, trad.pluriel, items,
                pourquoi="Information du moteur : elle ne bloque pas la clôture.", action="Voir",
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


_CODES_GESTION = ("GESTION_LOGEMENT_MISSING", "GESTION_LOGEMENT_OUT_OF_PERIOD",
                  "GESTION_LOGEMENT_AMBIGUOUS", "GESTION_LOGEMENT_MISSING_OWNER")


def _reservations_hors_gestion(par, mois: str, noms: _Noms, db_path) -> None:
    """Séjours du mois sans propriétaire exploitable — exclus de l'économie, et jusqu'ici muets.

    Un séjour dont les dates ne tombent dans aucune période de gestion, ou chevauchent la fin de la
    gestion, passe en A_CONTROLER et sort du calcul de commission et de la facturation. Aucun contrôle
    du moteur ne le disait : le mois pouvait se clôturer avec ces séjours absents des factures
    (constaté sur un logement réactivé sans reprise de gestion). Lu ici dans la source, jamais copié."""
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
        marques = ",".join("?" * len(_CODES_GESTION))
        rows = [dict(r) for r in conn.execute(
            "SELECT logement_id, date_arrivee, date_depart, code_anomalie, montant_retenu "
            "FROM reservations_resolues WHERE mois = ? AND statut_controle = 'A_CONTROLER' "
            f"AND (code_anomalie IN ({marques}) OR code_anomalie IN ('LOGEMENT_NON_MAPPE', "
            f"'STATUT_PARC_INVALIDE')){filtre_dataset} ORDER BY logement_id, date_arrivee",
            (params[0], *_CODES_GESTION, *params[1:]))]
    finally:
        conn.close()
    gestion = [r for r in rows if r["code_anomalie"] in _CODES_GESTION]
    non_mappes = [r for r in rows if r["code_anomalie"] == "LOGEMENT_NON_MAPPE"]
    statut = [r for r in rows if r["code_anomalie"] == "STATUT_PARC_INVALIDE"]
    if gestion:
        items = [_item(noms.logement(r["logement_id"]), detail=_du_au(r["date_arrivee"], r["date_depart"]),
                       montant=r["montant_retenu"] if isinstance(r["montant_retenu"], (int, float)) else None,
                       lien=f"/logements/{quote(_txt(r['logement_id']))}", lien_libelle="Voir la gestion")
                 for r in gestion]
        par[RESERVATIONS]["bloqueurs"].append(_groupe(
            "SEJOUR_HORS_GESTION", "séjour hors période de gestion", "séjours hors période de gestion",
            items,
            pourquoi="Sans propriétaire en gestion à ses dates, un séjour est exclu du calcul et des "
                     "factures. Vérifier la période de gestion du logement (réactivation, changement "
                     "de propriétaire).",
            action="Vérifier la gestion", lien=items[0]["lien"]))
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


#: Étapes de calcul : leur domaine d'attache, et ce que l'utilisateur doit lire.
_CALCULS_PAR_DOMAINE = {"RESERVATIONS": RESERVATIONS, "FLUX_LOT9": RESERVATIONS, "LOT10": RESERVATIONS,
                        "LOT11": RESERVATIONS, "MENAGES": MENAGES, "LOT12": FACTURES_CLIENTS}


def _calculs_obsoletes(par, db_path) -> None:
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
    for nom, domaine in _CALCULS_PAR_DOMAINE.items():
        statut = etats.get(nom)
        if statut not in (orch.ST_A_RECALCULER, orch.ST_ECHEC, orch.ST_EN_COURS):
            continue
        noeud = dag.NOEUDS.get(nom)
        libelle = noeud.nom_affiche if noeud else nom
        raison = {orch.ST_A_RECALCULER: "Une donnée en amont a changé : le résultat affiché est périmé.",
                  orch.ST_ECHEC: "Le dernier calcul a échoué : le résultat affiché est ancien.",
                  orch.ST_EN_COURS: "Un calcul est en cours : attendre sa fin avant de clôturer."}[statut]
        par[domaine]["bloqueurs"].append(_groupe(
            f"CALCUL:{nom}", "calcul à actualiser", "calculs à actualiser",
            [_item(libelle, detail=raison, lien="/actualisation", lien_libelle="Actualiser")],
            pourquoi=raison, action="Actualiser", lien="/actualisation"))


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

    mois = _txt(mois)[:7]
    if elements is None:
        elements = cs.elements_du_mois(mois, db_path)
    if flux is None:
        flux = cf.analyser(mois, contexte_flux=contexte_flux, db_path=db_path)
    noms = _Noms(db_path)

    par: dict[str, dict[str, list]] = {m.cle: {"bloqueurs": [], "informatifs": []} for m in MODULES}
    _ajouter_controles_moteur(par, mois, elements, noms)
    _ajouter_flux(par, mois, flux)
    _charges(par, mois, noms, db_path)
    _reservations_hors_gestion(par, mois, noms, db_path)
    _calculs_obsoletes(par, db_path)
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
