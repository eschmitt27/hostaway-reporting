"""Actions d'édition d'une facture propriétaire BROUILLON : charge métier, reversement Airbnb,
acompte propriétaire.

CE MODULE N'INVENTE AUCUN OBJET MÉTIER. Il ORCHESTRE des services canoniques déjà en place :

  charge métier         -> `charges_saisie_service.creer()`   (JAMAIS un INSERT dans `charges`)
  reversement Airbnb    -> `imputations_airbnb_service.creer()` (table `imputations_airbnb`, 0054)
  acompte propriétaire  -> `proprietaires_tresorerie_service.creer()/valider()` (0025)

Chaque action ajoute EN PLUS une ligne (charge) ou un rattachement (règlement) à la facture. La
facture reste un DOCUMENT : ni Lot9, ni Lot10, ni Lot12 n'est écrit ici, jamais.

ÉCRITURES COMPTABLES — INVARIANT
Créer une charge depuis un BROUILLON, y compris en mode IC (« impacte la comptabilité »), ne crée
JAMAIS d'écriture comptable. `charges_saisie_service.creer()` n'insère que dans `charges`, journalise
l'événement et synchronise la position de refacturation ; la comptabilisation est un workflow
POSTÉRIEUR et SÉPARÉ (`comptabilite_ecritures_service`), déclenché ailleurs et jamais d'ici.
« Impact comptabilité = Oui » décrit une DESTINATION future, pas un effet immédiat.

ATOMICITÉ — TRANSACTION SQLITE UNIQUE
`charges_saisie_service.creer()` accepte désormais un `conn` externe optionnel (même idiome que
`charges_refacturation_service.synchroniser_depuis_charge` : `conn=None` défaut → connexion propre
ouverte/validée/fermée par le service ; `conn` fourni → écrit DANS la transaction de l'appelant,
sans commit ni close, laissés à l'appelant). `ajouter_ligne_charge` ouvre UNE connexion pour toute
l'opération et la partage :
    1. `charges.creer(..., conn=conn)` — insère la charge et synchronise sa position de
       refacturation (`synchroniser_depuis_charge` déjà appelée depuis `creer()`, avec le même
       `conn` — jamais un second appel ici) ;
    2. `svc.ajouter_ligne(..., _conn=conn)` — insère la ligne de facture ;
    3. `INSERT INTO factures_proprietaires_lignes_charge` — le lien ligne↔charge ;
    4. `svc._journal(conn, ...)` — l'événement d'historique.
Un seul `conn.commit()` à la fin. Toute exception à n'importe quelle étape déclenche
`conn.rollback()` puis `raise` : SQLite annule TOUT ce qui a été écrit dans cette transaction,
y compris l'INSERT de la charge lui-même — il n'y a donc plus rien à compenser, et aucun appel à
`charges_saisie_service.annuler()` n'a lieu sur ce chemin. Une panne process en cours de transaction
ne laisse aucune charge orpheline : soit la transaction est validée en entier, soit elle n'a jamais
existé du point de vue de la base.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db
from app.services import charges_saisie_service as charges
from app.services import factures_proprietaires_service as svc
from app.services import proprietaires_tresorerie_service as tresorerie

# ── Les TROIS modes d'impact, et rien d'autre ───────────────────────────────────────────────────
# Vocabulaire EXISTANT, repris tel quel (`flux_unifie_service._IMPACT_FLAGS` pour la consommation
# aval, `charges_preview_service.PRISE_EN_COMPTA_BY_IMPACT` pour la saisie). Aucun quatrième mode
# n'est inventé ici : un mode de plus serait un mode que le reste de la chaîne ne saurait pas lire.
#
# Attention : le 3ᵉ élément de `_IMPACT_FLAGS` n'est PAS `prise_en_compta` mais
# `inclure_resultat_hors_compta` (vérifié dans `_construire_flux`) — d'où le tableau explicite
# ci-dessous plutôt qu'une réutilisation trompeuse.
MODES_CHARGE: dict[str, dict[str, str]] = {
    "IC": {
        "libelle": "Charge normale",
        "description": "Impacte le résultat réel et comptable",
        "impact_resultat_reel": "OUI",
        "impact_resultat_comptable": "OUI",
        "prise_en_compta": "OUI",
    },
    "HC": {
        "libelle": "Charge hors comptabilité",
        "description": "Impacte le résultat réel, hors compta",
        "impact_resultat_reel": "OUI",
        "impact_resultat_comptable": "NON",
        "prise_en_compta": "NON",
    },
    "HR": {
        "libelle": "Charge hors résultat et hors comptabilité",
        "description": "Hors résultat et hors comptabilité",
        "impact_resultat_reel": "NON",
        "impact_resultat_comptable": "NON",
        "prise_en_compta": "NON",
    },
}

AVERTISSEMENT_CHARGE = "⚠ Cette ligne créera également une charge."


def modes_charge() -> list[dict[str, str]]:
    """Les trois modes, prêts à afficher — l'ordre est stable (normale, hors compta, hors tout)."""
    return [{"code_impact": code, **valeurs} for code, valeurs in MODES_CHARGE.items()]


def categories_charges(*, db_path=None) -> list[dict[str, Any]]:
    """Catégories de charge actives, pour le sélecteur. `categorie_charge_id` est OBLIGATOIRE côté
    `charges_saisie_service` : sans elle, la charge est refusée avant toute écriture."""
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT categorie_charge_id, categorie_niveau_1, categorie_niveau_2, description "
            "FROM ref_categories_charges WHERE actif IS NULL OR UPPER(actif) <> 'NON' "
            "ORDER BY categorie_niveau_1, categorie_niveau_2, categorie_charge_id").fetchall()
    except Exception:  # noqa: BLE001 — référentiel absent = liste vide, jamais un écran cassé
        return []
    finally:
        conn.close()
    return [dict(r) for r in rows]


# ── B — Ligne CHARGE sur un BROUILLON ───────────────────────────────────────────────────────────

def ajouter_ligne_charge(facture_id: str, *, libelle: str, montant: Any, code_impact: str,
                         categorie_charge_id: str, date_charge: str = "",
                         refacturable: str = "NON", commentaire: str = "",
                         acteur: str = "", db_path=None) -> dict[str, Any]:
    """Ajoute une ligne CHARGE à un BROUILLON **et** crée la charge réelle correspondante.

    Transaction SQLite unique documentée en tête de module : la validation pure (mode d'impact,
    catégorie, libellé) s'exécute AVANT toute connexion ; toute l'écriture (charge, refacturation,
    ligne, lien, historique) partage UNE connexion et UN commit final. Un refus métier de
    `charges.creer()` (ex. contrat invalide) survient avant tout commit et n'a rien écrit : on
    ferme simplement la connexion et on refuse proprement, sans rollback nécessaire (rien à
    défaire). Toute exception après ce point déclenche un rollback total.
    """
    facture = svc.lire(facture_id, db_path=db_path)
    svc._exiger_brouillon(facture, "ajout d'une charge")

    code_impact = str(code_impact or "").strip().upper()
    if code_impact not in MODES_CHARGE:
        raise svc.FactureProprietaireError(
            f"mode d'impact inconnu : {code_impact!r}. Attendu IC, HC ou HR.")
    if not str(categorie_charge_id or "").strip():
        raise svc.FactureProprietaireError("categorie de charge obligatoire")
    libelle = str(libelle or "").strip()
    if not libelle:
        raise svc.FactureProprietaireError("libelle obligatoire")

    mode = MODES_CHARGE[code_impact]
    # Sans date explicite, la charge est datée du premier jour du mois facturé : jamais
    # « aujourd'hui », qui rattacherait la charge à un mois sans rapport avec la facture.
    date_charge = str(date_charge or "").strip() or f"{facture['mois']}-01"

    conn = get_db(db_path)
    try:
        # ── 1. Charge canonique + refacturation, DANS la transaction partagée ────────────────
        resultat = charges.creer({
            "date_charge": date_charge,
            "mois": facture["mois"],
            "montant": svc._round(montant),
            "sens_flux": "DEPENSE",
            "categorie_charge_id": str(categorie_charge_id).strip(),
            "code_impact": code_impact,
            "impact_resultat_reel": mode["impact_resultat_reel"],
            "impact_resultat_comptable": mode["impact_resultat_comptable"],
            "prise_en_compta": mode["prise_en_compta"],
            "affectation_type": "LOGEMENT",
            "logement_id": facture["logement_id"],
            "proprietaire_id": facture["proprietaire_id"],
            # `refacturable` est transmis TEL QUEL au service canonique : c'est lui, et lui seul,
            # qui alimente la file de refacturation (`synchroniser_depuis_charge`). Aucune seconde
            # alimentation ici — c'est ce qui empêche la double refacturation.
            "refacturable": "OUI" if str(refacturable).strip().upper() == "OUI" else "NON",
            "source_flux": "FACTURE_PROPRIETAIRE",
            "commentaire": commentaire or f"Saisie depuis la facture {facture_id}",
        }, acteur=acteur or "interface", conn=conn)

        if not resultat.get("ok"):
            # Refus métier pur (ex. contrat invalide) : rien n'a été écrit sur CETTE connexion
            # avant ce point (`valider()`/`_verifier_contrat()` s'exécutent avant toute requête).
            # Levée dans ce même `try` : le `except` ci-dessous fait le rollback (no-op ici, sans
            # écriture à défaire) et la fermeture, un seul chemin de sortie pour toute erreur.
            raise svc.FactureProprietaireError(
                f"charge refusee ({resultat.get('code')}) : {resultat.get('message')}")
        charge_id = resultat["charge_id"]

        # ── 2. Ligne de facture, lien, historique — MÊME connexion ───────────────────────────
        ligne = svc.ajouter_ligne(
            facture_id, type_ligne=svc.TYPE_LIGNE_CHARGE, libelle=libelle, montant=montant,
            acteur=acteur, commentaire=f"charge {charge_id} ({code_impact})", _conn=conn)
        conn.execute(
            "INSERT INTO factures_proprietaires_lignes_charge "
            "(ligne_id_opaque, facture_id_opaque, charge_id, code_impact) VALUES (?,?,?,?)",
            (ligne["ligne_id_opaque"], facture_id, charge_id, code_impact))
        svc._journal(
            conn, facture_id, svc.EVT_AJOUT_CHARGE, svc.ST_BROUILLON, svc.ST_BROUILLON,
            f"charge {charge_id} {code_impact} « {libelle} » {svc._round(montant):.2f} — "
            f"impact resultat {mode['impact_resultat_reel']}, "
            f"impact comptabilite {mode['impact_resultat_comptable']}, aucune ecriture creee",
            acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"ok": True, "charge_id": charge_id, "ligne_id_opaque": ligne["ligne_id_opaque"],
            "code_impact": code_impact}


def charge_consommee(charge_id: str, *, db_path=None) -> dict[str, Any]:
    """La charge est-elle déjà utilisée en aval ? Décide si une suppression propre reste possible.

    Trois usages bloquent la suppression : une position de refacturation déjà imputée, une écriture
    comptable, une ligne de facture propriétaire d'un AUTRE document. Dans ce cas on ne supprime
    jamais en silence — on renvoie vers le workflow de correction existant
    (`charges_saisie_service.modifier`), qui journalise l'avant/après.
    """
    from app.services import charges_refacturation_service as refac

    conn = get_db(db_path)
    try:
        def _compte(sql: str, params: tuple) -> int:
            try:
                return int(conn.execute(sql, params).fetchone()[0] or 0)
            except Exception:  # noqa: BLE001 — table absente = usage inexistant
                return 0

        # Une position dont le total imputé est > 0 a DÉJÀ servi sur une facture : la charge est
        # consommée, quel que soit son statut courant.
        position = _compte(
            "SELECT COUNT(*) FROM charges_refacturation_positions "
            "WHERE charge_id=? AND (montant_impute_total > 0 OR statut IN (?, ?))",
            (charge_id, refac.STATUT_IMPUTEE, refac.STATUT_NON_REFACTUREE))
        ecriture = _compte(
            "SELECT COUNT(*) FROM ecritures WHERE origine_id_opaque=? OR piece=?",
            (charge_id, charge_id))
    finally:
        conn.close()
    return {"consommee": bool(position or ecriture), "positions_imputees": position,
            "ecritures": ecriture}


def supprimer_ligne_charge(facture_id: str, ligne_id: str, *, acteur: str = "",
                           db_path=None) -> dict[str, Any]:
    """Supprime une ligne CHARGE et annule la charge liée si elle n'a AUCUN usage aval.

    Si la charge est déjà consommée, la ligne est quand même retirée du document (c'est une décision
    de facturation), mais la charge est CONSERVÉE et le retour l'indique explicitement : c'est au
    workflow de correction des charges de trancher, pas à l'écran de facturation.
    """
    resultat = svc.supprimer_ligne(facture_id, ligne_id, acteur=acteur, db_path=db_path)
    lien = resultat.get("charge_liee")
    if not lien:
        return {**resultat, "charge_annulee": False}

    charge_id = lien["charge_id"]
    usage = charge_consommee(charge_id, db_path=db_path)
    if usage["consommee"]:
        return {**resultat, "charge_annulee": False, "charge_id": charge_id,
                "message": (f"La charge {charge_id} est déjà utilisée en aval : elle n'a pas été "
                            "annulée. Utiliser la correction de charge existante.")}
    charges.annuler(charge_id, acteur=acteur or "interface",
                    motif=f"ligne de facture {ligne_id} supprimée ({facture_id})", db_path=db_path)
    return {**resultat, "charge_annulee": True, "charge_id": charge_id}


# ── C — Reversement Airbnb ──────────────────────────────────────────────────────────────────────

def ajouter_reversement_airbnb(facture_id: str, *, montant: Any, date_imputation: str,
                               reference_airbnb: str = "", commentaire: str = "",
                               acteur: str = "", db_path=None) -> dict[str, Any]:
    """Rattache un reversement Airbnb à la facture. Reste un PAYOUT_PLATEFORME.

    Ne crée NI réservation, NI charge, NI mouvement bancaire, NI écriture comptable : c'est un
    paiement déjà reçu, imputé sur une créance existante.
    """
    from app.services import imputations_airbnb_service as imputations

    facture = svc.lire(facture_id, db_path=db_path)
    resultat = imputations.creer(
        proprietaire_id=facture["proprietaire_id"], logement_id=facture["logement_id"],
        mois=facture["mois"], document_id=facture_id, montant_impute=montant,
        date_imputation=date_imputation, reference_airbnb=reference_airbnb,
        commentaire=commentaire, db_path=db_path)
    if not resultat.get("ok"):
        raise svc.FactureProprietaireError(
            f"reversement refuse ({resultat.get('code')}) : {resultat.get('message')}")

    conn = get_db(db_path)
    try:
        svc._journal(conn, facture_id, svc.EVT_AJOUT_REVERSEMENT_AIRBNB, facture["statut"],
                     facture["statut"],
                     f"reversement {resultat['imputation_airbnb_id']} "
                     f"{svc._round(montant):.2f} au {date_imputation}"
                     + (f" — ref {reference_airbnb}" if reference_airbnb else "")
                     + (f" ; {commentaire}" if commentaire else ""), acteur)
        conn.commit()
    finally:
        conn.close()
    return resultat


# ── D — Acompte propriétaire ────────────────────────────────────────────────────────────────────

def ajouter_acompte(facture_id: str, *, montant: Any, date_mouvement: str,
                    mode_reglement: str = "", commentaire: str = "", acteur: str = "",
                    db_path=None) -> dict[str, Any]:
    """Enregistre un acompte propriétaire rattaché à la facture, puis le VALIDE.

    DÉCISION — validation automatique. `creer()` laisse le mouvement en BROUILLON, or Lot10
    (`charger_acomptes_proprietaires_sqlite`) ne retient que `statut='VALIDE' AND actif=1`, et le
    solde de règlement applique la même règle. Un acompte créé depuis cet écran et laissé en
    BROUILLON serait donc saisi, affiché… et sans effet sur le solde : exactement le genre d'écart
    silencieux que ce projet refuse. L'utilisateur qui saisit un acompte ICI affirme un paiement
    reçu ; l'action est donc création + validation, en un geste, et les deux événements
    (`CREATION`, `VALIDATION`) restent tracés séparément dans l'historique du mouvement.

    N'écrit JAMAIS dans Lot9/Lot10/Lot12 : ne touche ni payout, ni commission, ni ménage, ni revenu
    net d'exploitation, ni résultat économique.
    """
    facture = svc.lire(facture_id, db_path=db_path)
    resultat = tresorerie.creer(
        facture["proprietaire_id"], "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE",
        montant, date_mouvement, logement_id=facture["logement_id"],
        mode_reglement=mode_reglement, reference_metier=facture_id,
        justification=commentaire, source_type="MANUEL", source_id=facture_id,
        acteur=acteur or "interface", db_path=db_path)
    if not resultat.get("ok"):
        raise svc.FactureProprietaireError(
            f"acompte refuse ({resultat.get('code')}) : {resultat.get('message')}")

    opaque = resultat["mouvement_opaque"]
    validation = tresorerie.valider(opaque, acteur=acteur or "interface", db_path=db_path)

    conn = get_db(db_path)
    try:
        svc._journal(conn, facture_id, svc.EVT_AJOUT_ACOMPTE, facture["statut"], facture["statut"],
                     f"acompte {opaque} {svc._round(montant):.2f} au {date_mouvement} "
                     f"(statut {validation.get('statut')})"
                     + (f" — {mode_reglement}" if mode_reglement else "")
                     + (f" ; {commentaire}" if commentaire else ""), acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "mouvement_opaque": opaque, "statut": validation.get("statut")}
