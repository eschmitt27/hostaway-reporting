"""Saisie des charges — UI → SQLite (migration 0052).

CE QUE CE MODULE REMPLACE
La saisie des charges passait par `SAISIE_Charges_Flux.xlsx` : l'utilisateur remplissait le
classeur, un rafraîchissement Power Query produisait `MASTER_FACT_MAN_Charges.xlsx`, et
l'application lisait ce master. Trois artefacts, deux rafraîchissements manuels, et un moteur qui
refusait de démarrer sans le classeur.

La cible est directe : **UI → service → SQLite**. Pas d'aller-retour par Excel, et surtout pas de
circuit `UI → SQLite → Excel → moteur`, qui recréerait le problème sous un autre nom.

CE QUI N'EST PAS RECALCULÉ ICI
Rien. Ce service ENREGISTRE une saisie ; il ne dérive ni le flux, ni le résultat, ni un statut de
contrôle. `code_impact` et `statut_controle` sont saisis ou laissés vides, puis lus tels quels par
le reste de la chaîne (D044). Le calcul économique reste à Lot9/Lot10, les contrôles à Lot11.

La seule exception est un REFUS, pas un calcul : un `code_impact` hors vocabulaire des charges est
rejeté (`valider()`). Enregistrer tel quel une valeur que personne en aval ne sait lire, ce n'est
pas de la neutralité, c'est un silence.

CORRECTION, PAS SUPPRESSION
Une charge peut être référencée par une écriture comptable, un rapprochement bancaire ou un flux
déjà calculé. Elle n'est donc jamais supprimée physiquement : elle est ANNULÉE, et
`charge_evenements` conserve l'avant/après.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.contrats_donnees import Charge, ContratInvalideError
from app.db.connection import get_db
from app.moteurs.charges_engine import CODES_IMPACT_CHARGE

STATUT_ACTIVE = "ACTIVE"
STATUT_ANNULEE = "ANNULEE"

EVT_CREATION = "CREATION"
EVT_MODIFICATION = "MODIFICATION"
EVT_ANNULATION = "ANNULATION"
EVT_VALIDATION_CONTROLE = "VALIDATION_CONTROLE"
EVT_ANOMALIE_CONTROLE = "ANOMALIE_CONTROLE"

# Vocabulaire du CONTRÔLE d'une charge — celui de la migration 0011, repris tel quel. À ne pas
# confondre avec `statut` (ACTIVE/ANNULEE), qui est le cycle de VIE : une charge peut être active
# et non contrôlée, ou contrôlée puis annulée. Deux axes, deux colonnes.
CONTROLE_A_CONTROLER = "A_CONTROLER"
#: « Contrôlée et acceptée ». Le mot était `CONFORME` ici et `VALIDE` sur l'écran « Charges à
#: contrôler » — deux mots pour le même acte, dans la MÊME colonne. Seul `VALIDE` était lu par la
#: chaîne économique : une charge validée depuis la fiche restait donc sans effet, en silence.
#: Unifié sur `VALIDE` par la migration 0080.
CONTROLE_VALIDE = "VALIDE"
CONTROLE_ANOMALIE = "ANOMALIE"
CONTROLE_REJETE = "REJETE"
CONTROLES = (CONTROLE_A_CONTROLER, CONTROLE_VALIDE, CONTROLE_ANOMALIE, CONTROLE_REJETE)
#: Le SEUL statut qui fait entrer une charge dans un calcul (miroir applicatif de
#: `lib_db_moteur.STATUTS_CHARGE_CALCULEE` — verrouillé par un test de synchronisation).
CONTROLES_CALCULES = (CONTROLE_VALIDE,)

E_CHAMP_MANQUANT = "CHARGE_CHAMP_MANQUANT"
E_MONTANT_INVALIDE = "CHARGE_MONTANT_INVALIDE"
E_MOIS_INVALIDE = "CHARGE_MOIS_INVALIDE"
E_INTROUVABLE = "CHARGE_INTROUVABLE"
E_DEJA_ANNULEE = "CHARGE_DEJA_ANNULEE"
E_DOUBLON = "CHARGE_ID_DEJA_UTILISE"
E_CONTRAT_INVALIDE = "CHARGE_CONTRAT_INVALIDE"
E_IMPACT_INVALIDE = "CHARGE_CODE_IMPACT_INVALIDE"

# Champs modifiables par la saisie. `charge_id`, `statut` et les horodatages n'en font pas partie :
# l'identité et le cycle de vie ne se corrigent pas comme une valeur métier.
CHAMPS_SAISIE = (
    "date_charge", "mois", "montant", "sens_flux", "sens", "categorie_charge_id",
    # `impact_resultat_reel` / `impact_resultat_comptable` ont disparu (migration 0078) : elles
    # recopiaient ce que `code_impact` dit déjà. L'impact se lit par
    # `charges_engine.impact_charge(code_impact)`, jamais dans une colonne à maintenir à jour.
    "filtre_vue_menage", "type_flux_id", "code_impact",
    "prise_en_compta", "associe_id", "mode_paiement_id", "carte_id",
    "affectation_type", "logement_id", "proprietaire_id", "reservation_id", "refacturable",
    "source_flux", "methode_traitement", "paye_avec_montant_recupere", "lien_virement_banque",
    "statut_controle", "niveau_anomalie", "code_anomalie", "statut_rapprochement", "justificatif",
    "commentaire",
    # `affectable_menage` était CALCULÉ par la prévisualisation puis perdu à l'INSERT : la colonne
    # n'existait pas et ce champ ne figurait pas ici. `lot6f_cout_complet_menages` filtre pourtant
    # dessus pour constituer ses pools — une charge ménage saisie dans l'application ne pouvait donc
    # jamais rejoindre le coût complet ménage (migration 0076).
    "affectable_menage",
)

OBLIGATOIRES = ("date_charge", "montant", "categorie_charge_id")


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "code": code, "message": message}


def _nombre(valeur: Any) -> float | None:
    if valeur is None or valeur == "":
        return None
    try:
        return float(str(valeur).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return None


def _mois_depuis_date(date_charge: str) -> str:
    return str(date_charge)[:7]


def valider(donnees: dict[str, Any]) -> dict[str, Any]:
    """Validation métier (§17). Rend un refus lisible, jamais une exception."""
    for champ in OBLIGATOIRES:
        valeur = donnees.get(champ)
        # `0` est une valeur PRÉSENTE : la tester avec `or ""` la ferait passer pour manquante et
        # afficherait « champ obligatoire manquant » là où le vrai motif est « montant nul ».
        if valeur is None or str(valeur).strip() == "":
            return _refus(E_CHAMP_MANQUANT, f"Champ obligatoire manquant : {champ}.")

    montant = _nombre(donnees.get("montant"))
    if montant is None:
        return _refus(E_MONTANT_INVALIDE, "Le montant doit être un nombre.")
    if montant == 0:
        return _refus(E_MONTANT_INVALIDE, "Une charge à 0 n'a pas d'effet : montant attendu ≠ 0.")

    mois = str(donnees.get("mois") or "").strip() or _mois_depuis_date(donnees["date_charge"])
    if len(mois) != 7 or mois[4] != "-":
        return _refus(E_MOIS_INVALIDE, f"Mois attendu au format AAAA-MM, reçu : {mois!r}.")

    # `code_impact` reste FACULTATIF (D044 : ce service enregistre, il ne dérive pas). Mais s'il
    # est fourni, il doit appartenir au vocabulaire des charges. HR y a été retiré (DÉCISION 2) :
    # une dépense sans effet sur le résultat ni sur la comptabilité n'est pas une charge. Le refus
    # est ici parce que c'est la SEULE porte d'écriture : retirer HR des formulaires sans fermer
    # le service l'aurait laissé atteignable par l'API et par tout appelant interne.
    code_impact = str(donnees.get("code_impact") or "").strip()
    if code_impact and code_impact not in CODES_IMPACT_CHARGE:
        admis = ", ".join(sorted(CODES_IMPACT_CHARGE))
        return _refus(E_IMPACT_INVALIDE,
                      f"Code d'impact {code_impact!r} inconnu pour une charge : {admis} attendus.")

    return {"ok": True, "montant": montant, "mois": mois}


def _verifier_contrat(donnees: dict[str, Any]) -> dict[str, Any] | None:
    """Mission 11 : `valider()` reste la seule source de vérité métier ; ce contrat structurel
    (`app.contrats_donnees.Charge`) rejette en plus une date_charge non calendaire ou un montant
    non numérique qui, avant cette mission, auraient été acceptés tels quels (ex. `date_charge=
    "12/06/2026"`, silencieusement tronqué en mois via `str(...)[:7]` — un mois `"12/06"` invalide
    aurait alors circulé jusqu'à Lot9/Lot10 sans jamais être détecté ici). Appelé APRÈS `valider()`,
    jamais à sa place (§docstring `contrats_donnees.py`)."""
    try:
        Charge.from_dict(donnees)
    except ContratInvalideError as exc:
        return _refus(E_CONTRAT_INVALIDE, str(exc))
    return None


def _journaliser(conn, charge_id: str, evenement: str, acteur: str, motif: str,
                 avant: Any = None, apres: Any = None) -> None:
    conn.execute(
        "INSERT INTO charge_evenements (charge_id, evenement, acteur, motif, avant_json, "
        "apres_json) VALUES (?,?,?,?,?,?)",
        (charge_id, evenement, acteur or None, motif or None,
         json.dumps(avant, default=str) if avant else None,
         json.dumps(apres, default=str) if apres else None))


def _charge(conn, charge_id: str) -> dict[str, Any] | None:
    r = conn.execute("SELECT * FROM charges WHERE charge_id = ?", (charge_id,)).fetchone()
    return dict(r) if r else None


def lire(charge_id: str, *, db_path=None) -> dict[str, Any] | None:
    """La ligne complète, cycle de vie compris. `None` si la charge n'existe pas.

    `charges_service.load_detail` reproduit la projection du lecteur moteur et n'expose donc pas
    `statut` (ACTIVE/ANNULEE) : un écran qui doit décider si une action est encore possible a
    besoin de cette colonne, et la lui ajouter là-bas romprait la parité de ce lecteur.
    """
    conn = get_db(db_path)
    try:
        return _charge(conn, str(charge_id or "").strip())
    finally:
        conn.close()


def creer(donnees: dict[str, Any], *, acteur: str = "", conn=None, perimetre=None,
          perimetre_menage=None, db_path=None) -> dict[str, Any]:
    """Crée une charge. `charge_id` fourni, ou dérivé d'un identifiant opaque — jamais un rang.

    `conn` : même idiome que `charges_refacturation_service.synchroniser_depuis_charge` — si
    fourni, la charge s'écrit DANS la transaction de l'appelant (pas de commit/close ici, c'est à
    l'appelant de gérer la transaction en entier). Si `None` (défaut, tous les appelants
    existants), comportement inchangé : connexion propre ouverte/validée/fermée ici.
    Validation métier (`valider()`, `_verifier_contrat()`) reste PURE et s'exécute AVANT toute
    ouverture de connexion — un refus métier ne touche jamais la base, conn fourni ou non.

    `perimetre` : les N logements concernés, déjà calculés par `charges_impact_service`
    (`{logement_id, proprietaire_id, quote_part_montant}`). Écrit AVANT la synchronisation de
    refacturation, parce que celle-ci en a besoin pour savoir à quels propriétaires la charge peut
    être refacturée. Sans lui, une charge multi-logements perdait son périmètre à l'écriture et sa
    position naissait `A_TRAITER`, donc invisible de toute facture.
    """
    validation = valider(donnees)
    if not validation["ok"]:
        return validation
    contrat_refus = _verifier_contrat(donnees)
    if contrat_refus is not None:
        return contrat_refus

    charge_id = str(donnees.get("charge_id") or "").strip() or f"CHG-{uuid.uuid4().hex[:12]}"
    valeurs = {c: donnees.get(c) for c in CHAMPS_SAISIE}
    valeurs["montant"] = validation["montant"]
    valeurs["mois"] = validation["mois"]

    connexion_locale = conn is None
    if connexion_locale:
        conn = get_db(db_path)
    try:
        if _charge(conn, charge_id) is not None:
            return _refus(E_DOUBLON, f"Une charge porte déjà l'identifiant {charge_id}.")
        colonnes = ["charge_id", *CHAMPS_SAISIE, "date_saisie", "source_module", "acteur"]
        params = [charge_id, *(valeurs[c] for c in CHAMPS_SAISIE), _maintenant(), "SAISIE_APP",
                  acteur or None]
        conn.execute(
            f"INSERT INTO charges ({', '.join(colonnes)}) "
            f"VALUES ({', '.join(['?'] * len(colonnes))})", params)
        _journaliser(conn, charge_id, EVT_CREATION, acteur, "", apres=valeurs)
        # Le périmètre analytique s'écrit AVANT la synchronisation de refacturation : celle-ci en
        # dérive les propriétaires éligibles quand la charge est commune à plusieurs logements.
        if perimetre:
            from app.services import charges_perimetre_service as perim
            perim.enregistrer(charge_id, perimetre, mois=valeurs["mois"], acteur=acteur, conn=conn)
        # Périmètre MÉNAGE (0076) : même principe, autre dimension — intervenants ou logements.
        if perimetre_menage and perimetre_menage.get("entrees"):
            from app.services import charges_perimetre_service as perim
            perim.enregistrer_menage(charge_id, perimetre_menage.get("mode", ""),
                                     perimetre_menage["entrees"], mois=valeurs["mois"],
                                     acteur=acteur, conn=conn)
        # Mission 15 : point d'entrée unique de la file de refacturation — une charge
        # refacturable='OUI' alimente automatiquement une position, jamais un second flux.
        from app.services import charges_refacturation_service as refac
        refac.synchroniser_depuis_charge(charge_id, acteur=acteur, conn=conn)
        if connexion_locale:
            conn.commit()
    finally:
        if connexion_locale:
            conn.close()
    return {"ok": True, "charge_id": charge_id}


def modifier(charge_id: str, donnees: dict[str, Any], *, acteur: str = "", motif: str = "",
             db_path=None) -> dict[str, Any]:
    """Corrige une charge existante. L'avant/après est journalisé."""
    validation = valider(donnees)
    if not validation["ok"]:
        return validation
    contrat_refus = _verifier_contrat(donnees)
    if contrat_refus is not None:
        return contrat_refus

    conn = get_db(db_path)
    try:
        avant = _charge(conn, charge_id)
        if avant is None:
            return _refus(E_INTROUVABLE, f"Charge inconnue : {charge_id}.")
        if avant["statut"] == STATUT_ANNULEE:
            return _refus(E_DEJA_ANNULEE, "Une charge annulée ne se corrige pas ; en créer une "
                                          "nouvelle.")
        valeurs = {c: donnees.get(c) for c in CHAMPS_SAISIE}
        valeurs["montant"] = validation["montant"]
        valeurs["mois"] = validation["mois"]
        conn.execute(
            f"UPDATE charges SET {', '.join(f'{c} = ?' for c in CHAMPS_SAISIE)}, "
            "date_modification = ? WHERE charge_id = ?",
            [*(valeurs[c] for c in CHAMPS_SAISIE), _maintenant(), charge_id])
        _journaliser(conn, charge_id, EVT_MODIFICATION, acteur, motif,
                     avant={c: avant.get(c) for c in CHAMPS_SAISIE}, apres=valeurs)
        from app.services import charges_refacturation_service as refac
        refac.synchroniser_depuis_charge(charge_id, acteur=acteur, conn=conn)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "charge_id": charge_id}


def annuler(charge_id: str, *, acteur: str = "", motif: str = "", db_path=None) -> dict[str, Any]:
    """Annule une charge — jamais de suppression physique (elle peut être déjà référencée)."""
    conn = get_db(db_path)
    try:
        avant = _charge(conn, charge_id)
        if avant is None:
            return _refus(E_INTROUVABLE, f"Charge inconnue : {charge_id}.")
        if avant["statut"] == STATUT_ANNULEE:
            return _refus(E_DEJA_ANNULEE, f"La charge {charge_id} est déjà annulée.")
        conn.execute("UPDATE charges SET statut = ?, date_modification = ? WHERE charge_id = ?",
                     (STATUT_ANNULEE, _maintenant(), charge_id))
        _journaliser(conn, charge_id, EVT_ANNULATION, acteur, motif,
                     avant={"statut": STATUT_ACTIVE}, apres={"statut": STATUT_ANNULEE})
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "charge_id": charge_id, "statut": STATUT_ANNULEE}


def statut_controle(charge: dict[str, Any] | None) -> str:
    """État de contrôle NORMALISÉ d'une charge. `NULL` vaut `A_CONTROLER`.

    Toutes les charges ne reçoivent pas cette colonne à la création : le parcours de saisie la
    pose, une création directe par le service la laisse à `NULL`, et la vraie base porte les deux
    cas. « Pas encore contrôlé » et « colonne vide » décrivent pourtant la même situation — les
    distinguer à l'écran ferait apparaître des charges dans aucun filtre, ni « à contrôler », ni
    « conforme ». On normalise donc à la LECTURE plutôt que d'imposer un défaut en base, qui
    entrerait en conflit avec la valeur calculée par le moteur.
    """
    valeur = str((charge or {}).get("statut_controle") or "").strip().upper()
    return valeur if valeur in CONTROLES else CONTROLE_A_CONTROLER


def valider_controle(charge_id: str, *, acteur: str = "", motif: str = "",
                     db_path=None) -> dict[str, Any]:
    """`A_CONTROLER` → `VALIDE`. Le geste « Valider la charge » de la fiche.

    Le vocabulaire est unifié sur celui que la chaîne économique lit (migration 0080) :
    aucun statut n'est inventé pour l'occasion. Ce qui manquait n'était pas le modèle mais la
    TRANSITION — une charge naissait `A_CONTROLER` et rien, nulle part, ne pouvait l'en sortir.

    Idempotent : valider une charge déjà `VALIDE` renvoie un succès sans réécrire ni rejournaliser
    (double-clic, rafraîchissement, double soumission). Une charge annulée n'est pas validable :
    son cycle de vie est clos.
    """
    conn = get_db(db_path)
    try:
        avant = _charge(conn, charge_id)
        if avant is None:
            return _refus(E_INTROUVABLE, f"Charge inconnue : {charge_id}.")
        if avant["statut"] == STATUT_ANNULEE:
            return _refus(E_DEJA_ANNULEE,
                          f"La charge {charge_id} est annulée : son contrôle ne peut plus changer.")
        actuel = str(avant["statut_controle"] or "").strip().upper()
        if actuel == CONTROLE_VALIDE:
            return {"ok": True, "charge_id": charge_id, "statut_controle": CONTROLE_VALIDE,
                    "inchange": True}
        conn.execute(
            "UPDATE charges SET statut_controle = ?, date_modification = ? WHERE charge_id = ?",
            (CONTROLE_VALIDE, _maintenant(), charge_id))
        _journaliser(conn, charge_id, EVT_VALIDATION_CONTROLE, acteur, motif,
                     avant={"statut_controle": actuel or None},
                     apres={"statut_controle": CONTROLE_VALIDE})
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "charge_id": charge_id, "statut_controle": CONTROLE_VALIDE,
            "inchange": False}


def signaler_anomalie(charge_id: str, *, acteur: str = "", motif: str = "",
                      db_path=None) -> dict[str, Any]:
    """`A_CONTROLER`/`VALIDE` → `ANOMALIE`. Contrepartie de `valider_controle` : un contrôle qui
    ne peut que dire « oui » n'est pas un contrôle."""
    conn = get_db(db_path)
    try:
        avant = _charge(conn, charge_id)
        if avant is None:
            return _refus(E_INTROUVABLE, f"Charge inconnue : {charge_id}.")
        if avant["statut"] == STATUT_ANNULEE:
            return _refus(E_DEJA_ANNULEE,
                          f"La charge {charge_id} est annulée : son contrôle ne peut plus changer.")
        actuel = str(avant["statut_controle"] or "").strip().upper()
        if actuel == CONTROLE_ANOMALIE:
            return {"ok": True, "charge_id": charge_id, "statut_controle": CONTROLE_ANOMALIE,
                    "inchange": True}
        conn.execute(
            "UPDATE charges SET statut_controle = ?, date_modification = ? WHERE charge_id = ?",
            (CONTROLE_ANOMALIE, _maintenant(), charge_id))
        _journaliser(conn, charge_id, EVT_ANOMALIE_CONTROLE, acteur, motif,
                     avant={"statut_controle": actuel or None},
                     apres={"statut_controle": CONTROLE_ANOMALIE})
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "charge_id": charge_id, "statut_controle": CONTROLE_ANOMALIE,
            "inchange": False}


EVT_PERIMETRE_COMPLETE = "PERIMETRE_COMPLETE"


def definir_perimetre(charge_id: str, logements: list[str], *, acteur: str = "", motif: str = "",
                      db_path=None) -> dict[str, Any]:
    """Renseigne APRÈS COUP le périmètre analytique d'une charge qui n'en a pas.

    À quoi cela sert : les charges créées AVANT la migration 0074 n'ont pas de périmètre — il était
    calculé puis jeté. Une charge refacturable dans ce cas porte une position `A_TRAITER`, que
    l'application ne propose sur aucune facture : la dépense est refacturable en théorie et
    irrécupérable en pratique. Plutôt que de deviner ses logements à sa place (ni l'événement de
    création ni la position ne les contiennent : ils n'ont jamais été écrits), on rend la saisie
    possible.

    Le montant est réparti également, comme à la création — même règle, même arithmétique. La
    position est resynchronisée dans la MÊME transaction : sans cela, le périmètre existerait sans
    que la position redevienne proposable, et rien ne le signalerait.
    """
    from app.services import charges_impact_service as impact
    from app.services import charges_perimetre_service as perim
    from app.services import charges_refacturation_service as refac

    logements = [str(l).strip() for l in (logements or []) if str(l).strip()]
    conn = get_db(db_path)
    try:
        charge = _charge(conn, str(charge_id or "").strip())
        if charge is None:
            return _refus(E_INTROUVABLE, f"Charge inconnue : {charge_id}.")
        if charge["statut"] == STATUT_ANNULEE:
            return _refus(E_DEJA_ANNULEE,
                          f"La charge {charge_id} est annulée : son périmètre ne change plus.")
        if not logements:
            return _refus(E_CHAMP_MANQUANT,
                          "Sélectionnez au moins un logement pour définir le périmètre.")

        # Propriétaire de chaque logement à la DATE DE LA CHARGE : c'est la règle déjà appliquée à
        # la création. Le recalculer à aujourd'hui ferait glisser une charge d'août vers un
        # propriétaire entré en septembre.
        gestion = [dict(r) for r in conn.execute(
            "SELECT gestion_id, logement_id, proprietaire_id, date_debut, date_fin, statut_gestion "
            "FROM ref_gestion_logements_hist")]
        mois = str(charge["mois"] or "")
        prop_par_log = {}
        for r in gestion:
            if impact.gestion_active_pour_mois(r, mois):
                prop_par_log.setdefault(str(r.get("logement_id") or "").strip(),
                                        str(r.get("proprietaire_id") or "").strip())

        parts = impact.repartir_egal(float(charge["montant"] or 0), logements)
        entrees = [{"logement_id": p["logement_id"],
                    "proprietaire_id": prop_par_log.get(p["logement_id"]) or None,
                    "mois": mois, "quote_part_montant": p["quote_part"]} for p in parts]
        perim.enregistrer(charge_id, entrees, mois=mois, acteur=acteur, conn=conn)
        refac.synchroniser_depuis_charge(charge_id, acteur=acteur, conn=conn)
        _journaliser(conn, charge_id, EVT_PERIMETRE_COMPLETE, acteur, motif,
                     avant={"perimetre": "absent"},
                     apres={"logements": logements, "nb": len(logements)})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"ok": True, "charge_id": charge_id, "nb_logements": len(logements)}


def historique(charge_id: str, *, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM charge_evenements WHERE charge_id = ? ORDER BY id", (charge_id,))]
    finally:
        conn.close()
