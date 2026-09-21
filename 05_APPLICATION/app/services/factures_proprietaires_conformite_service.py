"""Conformité des factures propriétaires : contrôle de pré-émission et données réglementaires.

Un seul point d'entrée décide si une facture peut être émise : `verifier()`. Il renvoie la liste
des manques, chacun avec un code stable, plutôt qu'un simple booléen — l'utilisateur doit pouvoir
lire *pourquoi* une facture est bloquée.

Ce module ne complète jamais une donnée absente. Une information réglementaire manquante bloque
l'émission réelle ; elle n'est pas remplacée par une valeur plausible.
"""
from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Any

from app.db.connection import get_db
from app.services import facturation_config_service as conf

# Codes de contrôle stables.
C_IDENTITE_EMETTEUR = "FACTURE_IDENTITE_EMETTEUR_INCOMPLETE"
C_IDENTITE_CLIENT = "FACTURE_IDENTITE_CLIENT_INCOMPLETE"
C_TYPE_CLIENT = "FACTURE_TYPE_CLIENT_NON_DETERMINE"
C_REGIME_TVA = "FACTURE_REGIME_TVA_NON_CONFIRME"
C_MENTION_TVA = "FACTURE_MENTION_TVA_MANQUANTE"
C_PENALITES = "FACTURE_PENALITES_RETARD_NON_CONFIGUREES"
C_INDEMNITE = "FACTURE_INDEMNITE_RECOUVREMENT_NON_CONFIGUREE"
C_PERIODE = "FACTURE_PERIODE_PRESTATION_MANQUANTE"
C_LIGNES = "FACTURE_SANS_LIGNE"
C_TOTAL = "FACTURE_TOTAL_INCOHERENT"
C_ECHEANCE = "FACTURE_ECHEANCE_NON_CONFIGUREE"

PRETE = "PRETE_A_EMETTRE"
BLOQUEE = "BLOQUEE"


def periode_prestation(mois: str) -> tuple[str, str]:
    """Premier et dernier jour du mois facturé — la période des prestations, à ne pas confondre
    avec la date d'émission du document."""
    annee, _, m = str(mois).partition("-")
    a, m = int(annee), int(m)
    return f"{a:04d}-{m:02d}-01", f"{a:04d}-{m:02d}-{calendar.monthrange(a, m)[1]:02d}"


def periode_facturee(facture: dict[str, Any]) -> tuple[str, str]:
    """Période RÉELLEMENT couverte par la facture : la sienne si elle en porte une, sinon son mois.

    Une facture de période libre (§ parcours « Facturer une période libre ») couvre du 1er au 13
    septembre, pas septembre entier. Dériver la mention du seul `mois` faisait imprimer une période
    plus large que celle facturée — une mention réglementaire fausse sur un document opposable.
    Le mois reste la valeur de repli : le cycle mensuel, lui, ne porte pas de bornes.
    """
    debut, fin = str(facture.get("periode_debut") or ""), str(facture.get("periode_fin") or "")
    try:
        if debut and fin:
            return date.fromisoformat(debut[:10]).isoformat(), date.fromisoformat(fin[:10]).isoformat()
    except ValueError:      # bornes illisibles : on retombe sur le mois plutôt que d'imprimer faux
        pass
    return periode_prestation(facture["mois"])


def date_echeance(date_facture: str, delai_jours: int | None) -> str | None:
    """Échéance de règlement.

    `0` et `None` ne veulent PAS dire la même chose, et les confondre était le défaut de la version
    précédente (`if not delai_jours` traitait 0 comme une absence) :
      · `None` → délai non configuré : aucune échéance calculable, le contrôle le signale ;
      · `0`    → **paiement à réception** : l'échéance EST la date d'émission.
    """
    if delai_jours is None or not date_facture:
        return None
    try:
        d = date.fromisoformat(str(date_facture)[:10])
    except ValueError:
        return None
    return (d + timedelta(days=int(delai_jours))).isoformat()


def _noms_logements(facture: dict[str, Any], *, db_path=None) -> list[str]:
    """Noms réels des logements de la facture, dans l'ordre. Repli sur l'identifiant seulement si
    le référentiel ne le connaît pas — auquel cas mieux vaut un code qu'une ligne vide."""
    from app.services import referentiel_service as ref

    ids = [i for i in dict.fromkeys(
        [facture.get("logement_id")] + list(facture.get("logements") or [])) if i]
    noms = []
    for lid in ids:
        try:
            nom = ref.nom_logement(lid, db_path=db_path)
        except Exception:      # noqa: BLE001 — référentiel absent : on n'empêche pas la facture
            nom = ""
        noms.append(nom or str(lid))
    return noms


def client(proprietaire_id: str, *, db_path=None) -> dict[str, Any]:
    """Identité du client telle qu'elle sera figée. Le type n'est pas deviné : sans information
    explicite, il reste `A_CONTROLER` et l'émission est bloquée."""
    from app.readers import proprietaires_reader as reader
    try:
        p = reader.find_proprietaire(proprietaire_id) or {}
    except Exception:
        p = {}
    from app.services import proprietaires_facturation_service as classement

    nom = " ".join(x for x in (p.get("prenom_proprietaire"), p.get("nom_proprietaire")) if x)
    # Le classement SAISI (table compagne 0073) fait foi. On retombe sur une éventuelle colonne
    # `type_client` du référentiel si un import venait à en fournir une — sans jamais rien déduire.
    type_client = classement.type_client(proprietaire_id, db_path=db_path)
    if type_client == conf.CLIENT_A_CONTROLER:
        type_client = str(p.get("type_client") or "").upper() or conf.CLIENT_A_CONTROLER
    if type_client not in conf.TYPES_CLIENT:
        type_client = conf.CLIENT_A_CONTROLER
    enregistre = classement.lire(proprietaire_id, db_path=db_path) or {}
    return {
        "type_client": type_client,
        "denomination": nom or proprietaire_id,
        "adresse": p.get("adresse_facturation") or "",
        "adresse_facturation": p.get("adresse_facturation") or "",
        # Identifiants du client professionnel : le classement les porte s'ils ont été saisis, sinon
        # on retombe sur le référentiel. Aucun n'est fabriqué, et un particulier n'en a pas.
        "siren": enregistre.get("siren_client") or p.get("siren") or "",
        "tva_intra": enregistre.get("tva_intra_client") or p.get("tva_intra") or "",
        "numero_bon_commande": p.get("numero_bon_commande") or "",
        # Point de contact du client, s'il en existe un au référentiel. Téléphone prioritaire sur
        # l'e-mail (§24). Rien n'est inventé : un propriétaire sans contact n'en affiche aucun.
        "telephone": str(p.get("telephone") or "").strip(),
        "email": str(p.get("email") or "").strip(),
    }


def construire(facture: dict[str, Any], *, date_facture: str, db_path=None) -> dict[str, Any]:
    """Bloc de conformité complet pour une facture, sans rien écrire."""
    regime = conf.regime_tva()
    taux = conf.taux_tva_applicable(regime)
    debut, fin = periode_facturee(facture)
    cl = client(facture["proprietaire_id"], db_path=db_path)

    total_ht = round(float(facture.get("montant_total") or 0), 2)
    total_tva = round(total_ht * taux / 100.0, 2) if taux else 0.0
    delai = conf.delai_paiement_jours()

    return {
        "facture_id_opaque": facture["facture_id_opaque"],
        "nature_operation": conf.NATURE_PRESTATION,
        "adresse_livraison": conf.ADRESSE_LIVRAISON_NA,
        "periode_debut": debut,
        "periode_fin": fin,
        "emetteur": conf.emetteur(),
        "representants": conf.representants(),
        "client": cl,
        # Nom RÉEL du ou des logements facturés : `LOG_0001` n'apprend rien au destinataire.
        "logements": _noms_logements(facture, db_path=db_path),
        "regime_tva": regime,
        "mention_tva": conf.mention_tva(regime),
        "total_ht": total_ht,
        "total_tva": total_tva,
        "total_ttc": round(total_ht + total_tva, 2),
        "date_echeance": date_echeance(date_facture, delai),
        "delai_paiement_jours": delai,
        # Libellé DÉRIVÉ du délai (« Paiement à réception » quand il vaut 0) : il ne peut donc pas
        # contredire l'échéance imprimée juste à côté.
        "conditions_paiement": conf.conditions_paiement(delai),
        "conditions_escompte": conf.conditions_escompte(),
        "taux_penalites_retard": conf.taux_penalites_retard(),
        "indemnite_recouvrement": conf.indemnite_recouvrement(),
        "facturation_electronique": conf.facturation_electronique(),
    }


def verifier(facture: dict[str, Any], *, date_facture: str = "", db_path=None) -> dict[str, Any]:
    """Contrôle unique de pré-émission. Renvoie `PRETE_A_EMETTRE` ou `BLOQUEE` + les manques."""
    bloc = construire(facture, date_facture=date_facture or f"{facture['mois']}-01",
                      db_path=db_path)
    manques: list[dict[str, str]] = []

    em = bloc["emetteur"]
    absents = [c for c in conf.EMETTEUR_REQUIS if not em.get(c)]
    if absents:
        manques.append({"code": C_IDENTITE_EMETTEUR,
                        "message": "identité émetteur incomplète : " + ", ".join(absents)})

    cl = bloc["client"]
    if cl["type_client"] == conf.CLIENT_A_CONTROLER:
        # Message ACTIONNABLE : l'utilisateur doit savoir quoi faire, pas seulement qu'il manque
        # quelque chose. Le code technique reste à côté, pour les journaux et les tests.
        manques.append({"code": C_TYPE_CLIENT,
                        "message": "Indiquez si le propriétaire est un particulier ou un "
                                   "professionnel avant d'émettre la facture. Les mentions "
                                   "légales obligatoires ne sont pas les mêmes."})
    if not cl["denomination"] or not cl["adresse"]:
        manques.append({"code": C_IDENTITE_CLIENT,
                        "message": "identité client incomplète (dénomination ou adresse)"})

    if bloc["regime_tva"] == conf.TVA_A_CONTROLER:
        manques.append({"code": C_REGIME_TVA,
                        "message": "régime de TVA non confirmé — fondement juridique requis"})
    elif not bloc["mention_tva"] and bloc["regime_tva"] in (conf.TVA_FRANCHISE,
                                                           conf.TVA_EXONERATION_AUTRE):
        manques.append({"code": C_MENTION_TVA,
                        "message": f"mention réglementaire absente pour {bloc['regime_tva']}"})

    # Clauses B2B : exigées seulement face à un professionnel — les imposer à un particulier
    # serait une mention inadaptée.
    if cl["type_client"] == conf.CLIENT_PROFESSIONNEL:
        if not bloc["taux_penalites_retard"]:
            manques.append({"code": C_PENALITES,
                            "message": "taux de pénalités de retard non configuré (client professionnel)"})
        if not bloc["indemnite_recouvrement"]:
            manques.append({"code": C_INDEMNITE,
                            "message": "indemnité forfaitaire de recouvrement non configurée "
                                       "(client professionnel)"})

    if not bloc["periode_debut"] or not bloc["periode_fin"]:
        manques.append({"code": C_PERIODE, "message": "période de prestation indéterminée"})
    if not bloc["date_echeance"]:
        manques.append({"code": C_ECHEANCE,
                        "message": "Délai de paiement non configuré : l'échéance ne peut pas être "
                                   "calculée. Renseignez FACTURATION_DELAI_PAIEMENT_JOURS "
                                   "(0 = paiement à réception)."})

    lignes = facture.get("lignes") or []
    if not lignes:
        manques.append({"code": C_LIGNES, "message": "facture sans ligne facturée"})
    else:
        total_lignes = round(sum(float(l["montant"]) for l in lignes), 2)
        if abs(total_lignes - bloc["total_ht"]) > 0.01:
            manques.append({"code": C_TOTAL,
                            "message": f"lignes {total_lignes} != total {bloc['total_ht']}"})

    return {"statut": BLOQUEE if manques else PRETE, "manques": manques, "conformite": bloc}


# ── Persistance ─────────────────────────────────────────────────────────────────────────────────

def enregistrer(bloc: dict[str, Any], *, db_path=None) -> None:
    """Fige le bloc de conformité à l'émission. Écrit une seule fois : une facture émise ne voit
    jamais ses données réglementaires réécrites."""
    em, cl = bloc["emetteur"], bloc["client"]
    fe = bloc["facturation_electronique"]
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO factures_proprietaires_conformite ("
            " facture_id_opaque, nature_operation, adresse_livraison, periode_debut, periode_fin,"
            " emetteur_denomination, emetteur_forme_juridique, emetteur_capital, emetteur_siren,"
            " emetteur_siret, emetteur_rcs, emetteur_adresse_siege, emetteur_tva_intra,"
            " emetteur_contact, emetteur_coordonnees_paiement,"
            " type_client, client_denomination, client_adresse, client_adresse_facturation,"
            " client_siren, client_tva_intra, numero_bon_commande,"
            " regime_tva, mention_tva, total_ht, total_tva, total_ttc,"
            " date_echeance, delai_paiement_jours, conditions_escompte, taux_penalites_retard,"
            " indemnite_recouvrement,"
            " electronic_invoice_status, electronic_invoice_provider, electronic_invoice_format"
            ") VALUES (" + ",".join(["?"] * 35) + ")",
            (bloc["facture_id_opaque"], bloc["nature_operation"], bloc["adresse_livraison"],
             bloc["periode_debut"], bloc["periode_fin"],
             em["denomination"], em["forme_juridique"], em["capital"], em["siren"], em["siret"],
             em["rcs"], em["adresse_siege"], em["tva_intra"], em["contact"],
             em["coordonnees_paiement"],
             cl["type_client"], cl["denomination"], cl["adresse"], cl["adresse_facturation"],
             cl["siren"], cl["tva_intra"], cl["numero_bon_commande"],
             bloc["regime_tva"], bloc["mention_tva"], bloc["total_ht"], bloc["total_tva"],
             bloc["total_ttc"], bloc["date_echeance"], bloc["delai_paiement_jours"],
             bloc["conditions_escompte"], bloc["taux_penalites_retard"],
             bloc["indemnite_recouvrement"],
             fe["status"], fe["provider"], fe["format"]))
        conn.commit()
    finally:
        conn.close()


def charger(facture_id: str, *, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM factures_proprietaires_conformite WHERE facture_id_opaque=?",
            (facture_id,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None
