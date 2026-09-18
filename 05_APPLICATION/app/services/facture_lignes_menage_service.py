"""Lignes d'une facture prestataire ménage externe (`facture_lignes_menage`, migration 0037).

Satellite de `factures` (0017) — pas un en-tête concurrent — mais qui ne passe PAS par
`facture_lignes` (0022) : cette table impose un `charge_id` déjà créé par le module Charges
existant, qui reste un écrivain Excel (`saisie_charges_writer.py`), hors périmètre de cette mission.
Une ligne ménage porte donc directement `logement_id`/`montant_ttc`.

CONTRÔLE TOTAL (mission §7)
`controler_total` compare la somme des lignes (ménages affectés + frais non affectés, AVANT
ventilation — la ventilation n'ajoute jamais de montant, elle explique une répartition) au montant
TTC de la facture. Un écart signale une facture à revoir ; il ne la fait jamais passer VALIDEE tout
seul (§31 : décision humaine via `factures_service.changer_statut`).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from app.db.connection import get_db

TOLERANCE = 0.005

TYPE_MENAGE_INTERNE = "MENAGE_INTERNE"
TYPE_MENAGE_EXTERNE = "MENAGE_EXTERNE"
TYPE_FRAIS_NON_AFFECTE = "FRAIS_NON_AFFECTE"
TYPE_AUTRE = "AUTRE"
TYPES = (TYPE_MENAGE_INTERNE, TYPE_MENAGE_EXTERNE, TYPE_FRAIS_NON_AFFECTE, TYPE_AUTRE)

SOURCE_PDF = "PDF_EXTRACTION"
SOURCE_SAISIE = "SAISIE"
#: §29 — ligne que le parseur a omise, ajoutée à la main avec motif obligatoire. Volontairement
#: distincte de SOURCE_SAISIE : ce n'est pas une saisie ordinaire, c'est une CORRECTION d'un
#: document reçu, et elle doit rester identifiable comme telle pour toujours.
SOURCE_CORRECTIVE = "SAISIE_MANUELLE_CORRECTIVE"
#: §27 — part d'une ligne du document éclatée sur plusieurs logements. Ce n'est ni une ligne du
#: document (elle n'y figure pas telle quelle) ni une correction (rien n'était faux) : c'est une
#: lecture plus fine de la même dépense, et elle doit rester reconnaissable comme telle.
SOURCE_REPARTITION = "REPARTITION_MULTI_LOGEMENTS"

STATUT_LIGNE_ACTIVE = "ACTIVE"
#: §30 — ligne du document mal extraite : neutralisée, jamais supprimée.
STATUT_LIGNE_EXTRACTION_INCORRECTE = "EXTRACTION_INCORRECTE"
#: §27 — ligne remplacée par ses parts. Neutralisée pour ne pas compter deux fois, conservée pour
#: qu'on puisse toujours lire ce que le document portait avant qu'on l'éclate.
STATUT_LIGNE_REPARTIE = "REPARTIE"

#: §23 — d'où vient le logement porté par la ligne. `CONFIRME_MANUELLEMENT` n'est pas un niveau de
#: confiance du rapprochement : c'est la fin du rapprochement, un humain a tranché.
CONFIANCE_CONFIRMEE = "CONFIRME_MANUELLEMENT"

#: Nature de la prestation — vocabulaire de `ref_types_lignes_menage`, rattaché par la migration
#: 0088. À ne pas confondre avec `type_ligne`, qui dit seulement si un logement a été reconnu.
#: Les trois catégories que l'utilisateur manipule à l'écran : MÉNAGE, REMISE EN ÉTAT, et
#: AUTRE PRESTATION — cette dernière regroupant les quatre natures qui ne comptent pas un ménage.
CAT_MENAGE_STANDARD = "MENAGE_STANDARD"
CAT_REMISE_EN_ETAT = "REMISE_EN_ETAT"
CAT_FRAIS_DEPLACEMENT = "FRAIS_DEPLACEMENT"
CAT_LINGE = "LINGE"
CAT_ACHAT_PRODUIT = "ACHAT_PRODUIT"
CAT_AUTRE = "AUTRE"
CATEGORIES = (CAT_MENAGE_STANDARD, CAT_REMISE_EN_ETAT, CAT_FRAIS_DEPLACEMENT, CAT_LINGE,
              CAT_ACHAT_PRODUIT, CAT_AUTRE)


def _type_ligne_menage_id(conn, categorie: str) -> str | None:
    """Identifiant TLM_00x de la nature de prestation, lu dans le référentiel.

    Le lien se fait par le LIBELLÉ (`type_ligne_menage`) et non par un identifiant en dur : c'est
    le référentiel qui porte la nomenclature, et lui seul. Une catégorie inconnue du référentiel ne
    bloque pas l'écriture de la ligne — la ligne du document doit exister quoi qu'il arrive — elle
    laisse simplement le rattachement vide, ce qui la rendra non validable tant qu'un humain n'aura
    pas tranché.
    """
    if not categorie:
        return None
    try:
        row = conn.execute(
            "SELECT type_ligne_menage_id FROM ref_types_lignes_menage "
            "WHERE UPPER(type_ligne_menage) = UPPER(?)", (str(categorie).strip(),)).fetchone()
    except Exception:      # noqa: BLE001 — référentiel absent d'une base de test minimale
        return None
    return row["type_ligne_menage_id"] if row else None


def _compte_comme_menage(conn, type_ligne_menage_id: str | None) -> bool | None:
    """La nature de prestation compte-t-elle pour un ménage ? Rend `None` si la nature n'est pas
    connue — une ligne jamais classée n'affirme rien, elle ne dit pas « non »."""
    if not type_ligne_menage_id:
        return None
    try:
        row = conn.execute(
            "SELECT compte_comme_menage FROM ref_types_lignes_menage "
            "WHERE type_ligne_menage_id = ?", (type_ligne_menage_id,)).fetchone()
    except Exception:      # noqa: BLE001 — référentiel absent d'une base de test minimale
        return None
    return (str(row["compte_comme_menage"]).upper() == "OUI") if row else None


def _type_ligne_pour(compte_comme_menage: bool, logement_id) -> str:
    """UNE vérité canonique pour `type_ligne` — colonne SQL que lisent lot6c/6d/6e/6f et
    `lib_db_moteur` directement, sans repasser par le référentiel des natures. `type_ligne` n'est
    donc plus une décision indépendante : il est DÉRIVÉ, à chaque écriture, de la nature choisie
    (`compte_comme_menage`) et de la présence d'un logement.

    BUG RÉEL CORRIGÉ : une ligne classée MÉNAGE dont le logement était confirmé PAR LE FORMULAIRE
    « Logement » (`affecter_logement`, existant depuis la recette 3) gardait son ancien
    `type_ligne` — celui-ci n'était mis à jour que par l'ancien mécanisme « Ménage oui/non »
    (`marquer_menage`), indépendant de la nature. La colonne affichée « Ménage » restait donc à
    NON après une classification MÉNAGE, et le rapprochement (qui filtre sur `type_ligne` en SQL)
    ignorait la ligne. Un ménage SANS logement compte déjà pour la nature — l'écran l'affiche —
    mais ne peut pas encore être imputé à un coût : `TYPE_FRAIS_NON_AFFECTE` le porte en
    attendant, exactement comme une ligne PDF non rapprochée l'a toujours fait.
    """
    return TYPE_MENAGE_EXTERNE if (compte_comme_menage and str(logement_id or "").strip()) \
        else TYPE_FRAIS_NON_AFFECTE


def categories_disponibles(db_path=None) -> list[dict[str, Any]]:
    """Les natures de prestation proposées au contrôle, avec ce qu'elles impliquent.

    `compte_comme_menage` est ce qui décide, en aval, qu'une ligne vaut un ménage : une REMISE EN
    ÉTAT compte (au coût réel de la ligne), un FRAIS DE DÉPLACEMENT non. L'écran lit donc le
    référentiel plutôt que de redire la règle dans son gabarit.

    RÉSERVÉ AU RÉFÉRENTIEL, PAS À L'ÉCRAN DE CORRECTION : les six lignes de
    `ref_types_lignes_menage` sont la nomenclature du PARSEUR automatique, seul capable de
    distinguer linge, achat de produit et frais de déplacement à la lecture du document
    (`02_TRAVAIL/lib_menages_externes_pdf.py::detecter_categorie`). Un humain qui corrige une
    ligne n'a que trois décisions utiles — voir `categories_ui_disponibles`.
    """
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT type_ligne_menage_id, type_ligne_menage, compte_comme_menage, "
            "repartissable_sur_menages, commentaire FROM ref_types_lignes_menage "
            "WHERE actif = 'OUI' ORDER BY type_ligne_menage_id").fetchall()
        return [dict(r) for r in rows]
    except Exception:      # noqa: BLE001
        return []
    finally:
        conn.close()


#: §2 (vérification finale recette 4) — les TROIS catégories que l'écran propose à l'humain qui
#: classe ou corrige une ligne. `FRAIS_DEPLACEMENT`, `LINGE` et `ACHAT_PRODUIT` restent des
#: valeurs valides — le parseur automatique les écrit avec certitude d'après le texte du document
#: — mais ce sont des précisions TECHNIQUES que l'écran n'a pas à faire choisir : elles ne
#: comptent de toute façon jamais un ménage, exactement comme `AUTRE`. Un humain qui corrige une
#: ligne classée « Linge » vers « Autre prestation » ne perd rien d'utile en aval : seul
#: `compte_comme_menage` (ici NON dans les deux cas) conditionne le rapprochement.
LIBELLES_CATEGORIES_UI: dict[str, str] = {
    CAT_MENAGE_STANDARD: "Ménage",
    CAT_REMISE_EN_ETAT: "Remise en état",
    CAT_AUTRE: "Autre prestation",
}


def categories_ui_disponibles(db_path=None) -> list[dict[str, Any]]:
    """Les trois catégories métier proposées au sélecteur de nature — jamais la nomenclature
    technique complète du référentiel (six entrées, dont quatre ne concernent que le parseur)."""
    return [{**c, "libelle_ui": LIBELLES_CATEGORIES_UI[c["type_ligne_menage"]]}
            for c in categories_disponibles(db_path)
            if c["type_ligne_menage"] in LIBELLES_CATEGORIES_UI]


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "detail": detail}


def ajouter_ligne(facture_id_opaque: str, *, type_ligne: str, montant_ttc: float,
                  logement_id: str = "", menage_id_opaque: str = "", description: str = "",
                  montant_ht: float | None = None, montant_tva: float | None = None,
                  quantite: int | None = None, prix_unitaire: float | None = None,
                  date_menage: str = "", precision_date_menage: str = "",
                  nom_prestataire: str = "",
                  source: str = SOURCE_PDF, commentaire: str = "", acteur: str = "",
                  logement_confiance: str = "", logement_methode: str = "",
                  libelle_source: str = "", categorie: str = "",
                  categorie_confiance: str = "", db_path=None) -> dict[str, Any]:
    if type_ligne not in TYPES:
        return _refus("TYPE_LIGNE_INVALIDE", type_ligne)
    montant_ttc = round(float(montant_ttc or 0), 2)
    # Un 0 € lu sur une facture est une INFORMATION du document — « ce logement n'a eu aucun ménage
    # ce mois-ci » — et le prestataire prend la peine de l'écrire. Le refuser faisait disparaître la
    # ligne sans trace : deux lignes réelles manquaient en base, et le diagnostic d'écart annonçait
    # pourtant le bon nombre. Une saisie humaine à 0, elle, reste une erreur de saisie.
    if montant_ttc == 0 and source != SOURCE_PDF:
        return _refus("MONTANT_INVALIDE", str(montant_ttc))

    ligne_id = "FLM-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        tlm_id = _type_ligne_menage_id(conn, categorie)
        compte = _compte_comme_menage(conn, tlm_id)
        if compte is not None:
            # La nature est connue : `type_ligne` est DÉRIVÉ d'elle, jamais une décision séparée
            # que l'appelant pourrait faire diverger (§4/§6 recette 4).
            type_ligne = _type_ligne_pour(compte, logement_id)
        elif type_ligne != TYPE_FRAIS_NON_AFFECTE and not logement_id:
            # Nature inconnue (appelant legacy, hors modèle des catégories) : garde-fou d'origine.
            return _refus("LOGEMENT_MANQUANT", type_ligne)
        conn.execute(
            "INSERT INTO facture_lignes_menage (ligne_id_opaque, facture_id_opaque, type_ligne, "
            "logement_id, menage_id_opaque, description, montant_ht, montant_tva, montant_ttc, "
            "source, commentaire, acteur, montant_ttc_source, logement_id_source, "
            "logement_confiance, logement_methode, libelle_source, type_ligne_menage_id, "
            "type_ligne_menage_confiance) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (ligne_id, facture_id_opaque, type_ligne, logement_id or None,
             menage_id_opaque or None, description or None, montant_ht, montant_tva, montant_ttc,
             source, commentaire or None, acteur or None,
             # §27 — photographie de ce que le document portait, jamais réécrite ensuite.
             montant_ttc, logement_id or None, logement_confiance or None,
             logement_methode or None,
             # §8 — le texte du document, immuable ; `description` en est la forme métier.
             libelle_source or None,
             tlm_id, categorie_confiance or None))
        if quantite is not None or prix_unitaire is not None:
            conn.execute(
                "INSERT INTO facture_lignes_menage_detail (ligne_id_opaque, quantite, "
                "prix_unitaire, quantite_source, prix_unitaire_source) VALUES (?,?,?,?,?)",
                (ligne_id, quantite, prix_unitaire, quantite, prix_unitaire))
        if date_menage or precision_date_menage or nom_prestataire:
            conn.execute(
                "INSERT INTO facture_lignes_menage_pdf (ligne_id_opaque, date_menage, "
                "precision_date_menage, nom_prestataire) VALUES (?,?,?,?)",
                (ligne_id, date_menage or None, precision_date_menage or None,
                 nom_prestataire or None))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ligne_id_opaque": ligne_id, "facture_id_opaque": facture_id_opaque}


def lignes(facture_id_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT l.*, d.quantite, d.prix_unitaire FROM facture_lignes_menage l "
            "LEFT JOIN facture_lignes_menage_detail d ON d.ligne_id_opaque = l.ligne_id_opaque "
            "WHERE l.facture_id_opaque = ? ORDER BY l.id", (facture_id_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def ajouter_ligne_manquante(facture_id_opaque: str, *, motif: str, montant_ttc: float,
                            description: str, categorie: str = CAT_MENAGE_STANDARD,
                            logement_id: str = "", quantite: int | None = None,
                            prix_unitaire: float | None = None, acteur: str = "",
                            db_path=None) -> dict[str, Any]:
    """§29 — AJOUTER UNE LIGNE MANQUANTE : usage exceptionnel, motif OBLIGATOIRE.

    Réservé aux cas où le document contient une ligne que l'extraction n'a pas produite (parseur
    en défaut, PDF sans texte exploitable, ligne source inutilisable). Ce n'est pas un bouton
    « ajouter une ligne » générique : sans motif, rien n'est enregistré.

    La NATURE se choisit ici dans le MÊME vocabulaire que celui du contrôle d'une ligne extraite
    (`categorie`, recette 4 §9-§13) — plus l'ancien couple « Ménage oui/non » qui ne connaissait
    pas les catégories et pouvait produire une ligne durablement en désaccord avec elles (§4).
    Aucun logement n'est exigé : une ligne classée MÉNAGE sans logement reste enregistrée
    (`type_ligne` la porte en TYPE_FRAIS_NON_AFFECTE en attendant), exactement comme une ligne
    extraite du PDF non rapprochée — c'est le contrôle de validation (§15), pas la création, qui
    exige le logement.
    """
    motif = str(motif or "").strip()
    if not motif:
        return _refus("MOTIF_OBLIGATOIRE",
                      "Une ligne ajoutée à la main doit dire pourquoi elle l'a été.")
    res = ajouter_ligne(
        facture_id_opaque, type_ligne=TYPE_MENAGE_EXTERNE if logement_id else TYPE_FRAIS_NON_AFFECTE,
        montant_ttc=montant_ttc, logement_id=logement_id, description=description,
        quantite=quantite, prix_unitaire=prix_unitaire, source=SOURCE_CORRECTIVE,
        categorie=categorie, categorie_confiance=CONFIANCE_CONFIRMEE,
        commentaire=f"Ligne manquante ajoutée à la main — {motif}", acteur=acteur,
        db_path=db_path)
    if not res.get("ok"):
        return res
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE facture_lignes_menage SET motif_correction=? "
                     "WHERE ligne_id_opaque=?", (motif, res["ligne_id_opaque"]))
        conn.commit()
    finally:
        conn.close()
    res["motif"] = motif
    return res


def marquer_extraction_incorrecte(ligne_id_opaque: str, *, motif: str, acteur: str = "",
                                  db_path=None) -> dict[str, Any]:
    """§30 — MARQUER EXTRACTION INCORRECTE : la ligne est neutralisée, jamais supprimée.

    La donnée brute reste intégralement lisible (libellé d'origine, quantité, montant) ; elle
    cesse seulement de compter dans le total des lignes. Supprimer la ligne effacerait ce que le
    document disait réellement — ici, on conserve la trace ET la raison de l'écarter.
    """
    motif = str(motif or "").strip()
    if not motif:
        return _refus("MOTIF_OBLIGATOIRE",
                      "Écarter une ligne extraite exige d'en donner la raison.")
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT statut_ligne FROM facture_lignes_menage "
                           "WHERE ligne_id_opaque=?", (ligne_id_opaque,)).fetchone()
        if row is None:
            return _refus("LIGNE_INTROUVABLE", ligne_id_opaque)
        conn.execute(
            "UPDATE facture_lignes_menage SET statut_ligne=?, motif_correction=?, "
            "commentaire=COALESCE(commentaire,'') || ? WHERE ligne_id_opaque=?",
            (STATUT_LIGNE_EXTRACTION_INCORRECTE, motif,
             f" · Extraction incorrecte ({acteur or 'local'}) : {motif}", ligne_id_opaque))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ligne_id_opaque": ligne_id_opaque,
            "statut_ligne": STATUT_LIGNE_EXTRACTION_INCORRECTE, "motif": motif}


# ── §26 — cette ligne est-elle un ménage ? ──────────────────────────────────────────────────────

#: Les deux types qui alimentent la chaîne de coût ménage. Tous les autres sont des frais.
TYPES_MENAGE = (TYPE_MENAGE_INTERNE, TYPE_MENAGE_EXTERNE)


def est_menage(ligne: dict[str, Any]) -> bool:
    return str(ligne.get("type_ligne") or "") in TYPES_MENAGE


def marquer_menage(ligne_id_opaque: str, *, menage: bool, motif: str, acteur: str = "",
                   db_path=None) -> dict[str, Any]:
    """§26 — déclarer qu'une ligne EST (ou n'est pas) un ménage, avec motif obligatoire.

    Ce n'est pas un détail de présentation : le type de ligne décide si le montant entre dans le
    coût ménage du logement, donc dans ce qui sera refacturé au propriétaire. Une ligne « frais de
    déplacement » typée ménage gonfle le coût d'un logement ; une ligne de ménage typée frais l'en
    prive. Le parseur se trompe forcément un jour — l'utilisateur doit pouvoir le dire, et la
    raison doit rester lisible.

    Passer une ligne EN ménage exige un logement : un coût ménage sans logement ne veut rien dire.

    Retirée de l'écran (recette 4, §4) au profit du sélecteur « Nature de la prestation », qui
    connaît les six catégories du référentiel au lieu d'un simple oui/non — les deux ne pouvaient
    que finir par se contredire. La fonction reste, pour compatibilité, et POSE désormais aussi la
    catégorie canonique (`MENAGE_STANDARD`/`AUTRE`) en même temps que `type_ligne`, pour ne jamais
    laisser les deux diverger si quelque chose l'appelle encore.
    """
    motif = str(motif or "").strip()
    if not motif:
        return _refus("MOTIF_OBLIGATOIRE",
                      "Changer la nature d'une ligne extraite exige d'en donner la raison.")
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT type_ligne, logement_id FROM facture_lignes_menage WHERE ligne_id_opaque=?",
            (ligne_id_opaque,)).fetchone()
        if row is None:
            return _refus("LIGNE_INTROUVABLE", ligne_id_opaque)
        if menage and not (row["logement_id"] or ""):
            return _refus("LOGEMENT_MANQUANT",
                          "Une ligne de ménage doit désigner le logement nettoyé.")
        nouveau = TYPE_MENAGE_EXTERNE if menage else TYPE_FRAIS_NON_AFFECTE
        if nouveau == row["type_ligne"]:
            return {"ok": True, "inchange": True, "type_ligne": nouveau}
        categorie = CAT_MENAGE_STANDARD if menage else CAT_AUTRE
        conn.execute(
            "UPDATE facture_lignes_menage SET type_ligne=?, type_ligne_menage_id=?, "
            "type_ligne_menage_confiance=?, "
            "commentaire=COALESCE(commentaire,'') || ? WHERE ligne_id_opaque=?",
            (nouveau, _type_ligne_menage_id(conn, categorie), CONFIANCE_CONFIRMEE,
             f" · Nature corrigée {row['type_ligne']}→{nouveau} "
             f"({acteur or 'local'}) : {motif}", ligne_id_opaque))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ligne_id_opaque": ligne_id_opaque, "type_ligne": nouveau,
            "ancien_type": row["type_ligne"], "motif": motif}


# ── §27 — la quantité extraite est une donnée source ────────────────────────────────────────────

def coherence_ligne(ligne: dict[str, Any]) -> dict[str, Any]:
    """Une ligne est cohérente quand quantité × prix unitaire redonne son montant. FONCTION PURE.

    C'EST CE CONTRÔLE QUI A RÉVÉLÉ LE +36,00 € DE LA FACTURE 0005
    Cette facture porte une ligne « T.2-65 (Gabriel) » avec quantité 0, prix unitaire 32,00 € et
    montant 36,00 €. Trois valeurs qui ne peuvent pas être vraies ensemble. En écartant ce seul
    montant, la somme des lignes tombe sur 520,00 € — exactement le total du document. L'écart
    n'était donc pas une ligne absente : c'était une valeur mal lue, et l'arithmétique le dit sans
    qu'on ait besoin de rouvrir le PDF.

    Quand la quantité ou le prix unitaire manquent, il n'y a rien à vérifier : l'absence d'un
    élément n'est pas une incohérence, et la signaler comme telle serait un faux positif.
    """
    q, pu = ligne.get("quantite"), ligne.get("prix_unitaire")
    montant = round(float(ligne.get("montant_ttc") or 0), 2)
    if q is None or pu is None:
        return {"verifiable": False, "coherente": True, "attendu": None, "ecart": 0.0,
                "message": "Quantité ou prix unitaire absents du document : rien à recalculer."}
    attendu = round(float(q) * float(pu), 2)
    ecart = round(montant - attendu, 2)
    return {
        "verifiable": True, "coherente": abs(ecart) <= TOLERANCE,
        "attendu": attendu, "ecart": ecart,
        "message": ("" if abs(ecart) <= TOLERANCE else
                    f"{q} × {pu:.2f} € = {attendu:.2f} €, or la ligne porte {montant:.2f} € "
                    f"(écart {ecart:+.2f} €)."),
    }


#: Les trois natures d'écart que `diagnostic_ecart` sait distinguer.
D_COHERENT = "COHERENT"
D_LIGNES_INCOHERENTES = "LIGNES_INCOHERENTES"
D_LIGNE_MANQUANTE = "LIGNE_MANQUANTE"
D_TROP_PERCU = "LIGNES_EN_TROP"


def diagnostic_ecart(facture_id_opaque: str, db_path=None) -> dict[str, Any]:
    """Qualifie l'écart lignes/total au lieu de se contenter de l'annoncer.

    « Écart de 89,00 € » ne dit pas quoi faire. Les deux factures réelles avaient un écart, et
    pourtant deux problèmes opposés : sur l'une, une ligne mal lue ; sur l'autre, une ligne
    absente. Le geste de correction n'est pas le même (§30 neutraliser / §29 ajouter), donc
    l'écran doit nommer le problème, pas seulement le chiffrer.

    La règle de départage est arithmétique, pas heuristique : si la somme des quantités × prix
    unitaires tombe juste alors que la somme des montants ne tombe pas, ce sont les montants qui
    sont faux, et les lignes fautives sont exactement celles que `coherence_ligne` désigne.
    """
    controle = controler_total(facture_id_opaque, db_path=db_path)
    if not controle.get("ok"):
        return controle

    lignes_actives = [l for l in lignes(facture_id_opaque, db_path=db_path)
                      if str(l.get("statut_ligne") or STATUT_LIGNE_ACTIVE) == STATUT_LIGNE_ACTIVE]
    coherences = [(l, coherence_ligne(l)) for l in lignes_actives]
    incoherentes = [{**l, "coherence": c} for l, c in coherences
                    if c["verifiable"] and not c["coherente"]]
    somme_recalculee = round(sum(
        c["attendu"] if c["verifiable"] else round(float(l.get("montant_ttc") or 0), 2)
        for l, c in coherences), 2)

    ecart = controle["ecart"]
    if controle["coherent"]:
        return {**controle, "diagnostic": D_COHERENT, "lignes_incoherentes": [],
                "message": "La somme des lignes reconstitue exactement le total du document."}

    if incoherentes and abs(round(controle["montant_facture"] - somme_recalculee, 2)) <= TOLERANCE:
        noms = ", ".join(f"« {l.get('description') or l['ligne_id_opaque']} »"
                         for l in incoherentes)
        return {
            **controle, "diagnostic": D_LIGNES_INCOHERENTES,
            "lignes_incoherentes": incoherentes, "somme_recalculee": somme_recalculee,
            "message": (
                f"{len(incoherentes)} ligne(s) portent un montant qui ne correspond pas à leur "
                f"quantité × prix unitaire ({noms}). En retenant le calcul de chaque ligne, la "
                f"facture tombe exactement sur {somme_recalculee:.2f} €, le total du document. "
                f"Il s'agit d'une extraction incorrecte, pas d'une ligne manquante."),
        }

    if ecart > 0:
        return {
            **controle, "diagnostic": D_LIGNE_MANQUANTE, "lignes_incoherentes": incoherentes,
            "message": (
                f"Chaque ligne extraite est cohérente avec sa quantité et son prix unitaire, mais "
                f"il manque {ecart:.2f} € pour atteindre le total du document. Une ou plusieurs "
                f"lignes n'ont pas été extraites."),
        }
    return {
        **controle, "diagnostic": D_TROP_PERCU, "lignes_incoherentes": incoherentes,
        "message": (
            f"Les lignes extraites dépassent le total du document de {abs(ecart):.2f} €. Une ligne "
            f"a été lue deux fois, ou un montant a été mal lu."),
    }


def corriger_classification(ligne_id_opaque: str, *, categorie: str = "",
                            libelle_metier: str = "", logement_id: str | None = None,
                            acteur: str = "", db_path=None) -> dict[str, Any]:
    """Contrôle humain d'une ligne : sa nature, son libellé métier, son logement (§9-§14).

    Le logiciel propose, l'utilisateur tranche. Ce que cette fonction ne touche JAMAIS :
    `libelle_source` (le texte du document), `montant_ttc_source` et `logement_id_source` — la
    photographie de l'extraction reste lisible après toute correction, sans quoi plus personne ne
    peut distinguer ce que la facture portait de ce que nous en avons fait.

    Le montant n'est pas modifiable ici : une ligne mal lue se neutralise
    (`marquer_extraction_incorrecte`) ou se corrige par les fonctions dédiées, avec motif.
    """
    categorie = str(categorie or "").strip().upper()
    if categorie and categorie not in CATEGORIES:
        return _refus("CATEGORIE_INVALIDE", categorie)
    libelle_metier = str(libelle_metier or "").strip()

    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT type_ligne, description, logement_id, type_ligne_menage_id "
            "FROM facture_lignes_menage WHERE ligne_id_opaque=?", (ligne_id_opaque,)).fetchone()
        if row is None:
            return _refus("LIGNE_INTROUVABLE", ligne_id_opaque)

        nouveau_tlm_id = _type_ligne_menage_id(conn, categorie) if categorie else None
        if categorie:
            conn.execute(
                "UPDATE facture_lignes_menage SET type_ligne_menage_id=?, "
                "type_ligne_menage_confiance=? WHERE ligne_id_opaque=?",
                (nouveau_tlm_id, CONFIANCE_CONFIRMEE, ligne_id_opaque))
        if libelle_metier:
            conn.execute("UPDATE facture_lignes_menage SET description=? WHERE ligne_id_opaque=?",
                         (libelle_metier, ligne_id_opaque))
        if logement_id is not None:
            cible = str(logement_id).strip()
            conn.execute(
                "UPDATE facture_lignes_menage SET logement_id=?, logement_confiance=? "
                "WHERE ligne_id_opaque=?",
                (cible or None, CONFIANCE_CONFIRMEE if cible else None, ligne_id_opaque))
        if categorie or logement_id is not None:
            # `type_ligne` est DÉRIVÉ — jamais réglé indépendamment — de la nature EFFECTIVE
            # (celle qu'on vient de poser, sinon celle déjà en base) et du logement EFFECTIF
            # (idem). Recalculé dès que l'un des deux change, ici, au même endroit à chaque fois :
            # c'est ce qui garantit qu'aucun autre chemin ne peut plus les faire diverger.
            tlm_effectif = nouveau_tlm_id if categorie else row["type_ligne_menage_id"]
            logement_effectif = (str(logement_id).strip() if logement_id is not None
                                 else row["logement_id"])
            compte = _compte_comme_menage(conn, tlm_effectif)
            if compte is not None:
                conn.execute(
                    "UPDATE facture_lignes_menage SET type_ligne=? WHERE ligne_id_opaque=?",
                    (_type_ligne_pour(compte, logement_effectif), ligne_id_opaque))
        conn.execute(
            "UPDATE facture_lignes_menage SET commentaire=COALESCE(commentaire,'') || ? "
            "WHERE ligne_id_opaque=?",
            (f" · Contrôle {acteur or 'local'} : "
             + ", ".join(filter(None, [f"catégorie {categorie}" if categorie else "",
                                       "libellé métier" if libelle_metier else "",
                                       f"logement {logement_id}" if logement_id is not None else ""])),
             ligne_id_opaque))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ligne_id_opaque": ligne_id_opaque, "categorie": categorie or None,
            "libelle_metier": libelle_metier or None, "logement_id": logement_id}


def corriger_montant(ligne_id_opaque: str, *, montant_ttc: float, motif: str, acteur: str = "",
                     db_path=None) -> dict[str, Any]:
    """§3 recette 4 — corrige le montant d'une ligne AJOUTÉE À LA MAIN (SOURCE_CORRECTIVE).

    Réservé aux lignes manuelles : une ligne EXTRAITE du document a un montant qui EST le
    document — la corriger reviendrait à réécrire la pièce. Pour elle, le geste reste
    `marquer_extraction_incorrecte` (§30, neutralise sans jamais réécrire) suivi d'un nouvel ajout
    si besoin. Une ligne manuelle, elle, n'a pas de « montant source » à protéger : c'est
    l'utilisateur qui l'a saisi, il peut le corriger — avec motif, comme toute correction tracée.
    """
    motif = str(motif or "").strip()
    if not motif:
        return _refus("MOTIF_OBLIGATOIRE",
                      "Corriger le montant d'une ligne ajoutée exige d'en donner la raison.")
    try:
        montant = round(float(montant_ttc), 2)
    except (TypeError, ValueError):
        return _refus("MONTANT_INVALIDE", str(montant_ttc))
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT montant_ttc, source, statut_ligne FROM facture_lignes_menage "
            "WHERE ligne_id_opaque=?", (ligne_id_opaque,)).fetchone()
        if row is None:
            return _refus("LIGNE_INTROUVABLE", ligne_id_opaque)
        if row["source"] != SOURCE_CORRECTIVE:
            return _refus("LIGNE_NON_MODIFIABLE",
                          "Seule une ligne ajoutée à la main peut voir son montant corrigé.")
        if row["statut_ligne"] != STATUT_LIGNE_ACTIVE:
            return _refus("LIGNE_NEUTRALISEE", ligne_id_opaque)
        if montant == 0:
            return _refus("MONTANT_INVALIDE", "0")
        conn.execute(
            "UPDATE facture_lignes_menage SET montant_ttc=?, motif_correction=?, "
            "commentaire=COALESCE(commentaire,'') || ? WHERE ligne_id_opaque=?",
            (montant, motif,
             f" · Montant corrigé {row['montant_ttc']}→{montant} ({acteur or 'local'}) : {motif}",
             ligne_id_opaque))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ligne_id_opaque": ligne_id_opaque, "montant_ttc": montant,
            "ancien_montant": row["montant_ttc"], "motif": motif}


def corriger_quantite(ligne_id_opaque: str, *, quantite: int, motif: str, acteur: str = "",
                      db_path=None) -> dict[str, Any]:
    """§27 — corriger une quantité extraite : possible, mais jamais discret.

    La quantité vient du document ; la modifier revient à dire que le prestataire s'est trompé ou
    que le parseur a mal lu. Les deux arrivent, aucun des deux ne se devine plus tard : motif
    obligatoire, et la valeur lue reste conservée dans `quantite_source`, définitivement.
    """
    motif = str(motif or "").strip()
    if not motif:
        return _refus("MOTIF_OBLIGATOIRE",
                      "La quantité vient du document : la corriger exige d'en donner la raison.")
    try:
        q = int(quantite)
    except (TypeError, ValueError):
        return _refus("QUANTITE_INVALIDE", str(quantite))
    if q < 0:
        return _refus("QUANTITE_INVALIDE", "Une quantité négative n'a pas de sens sur un ménage.")

    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT d.quantite, d.quantite_source, d.prix_unitaire FROM facture_lignes_menage l "
            "LEFT JOIN facture_lignes_menage_detail d ON d.ligne_id_opaque = l.ligne_id_opaque "
            "WHERE l.ligne_id_opaque=?", (ligne_id_opaque,)).fetchone()
        if row is None:
            return _refus("LIGNE_INTROUVABLE", ligne_id_opaque)
        conn.execute(
            "INSERT INTO facture_lignes_menage_detail (ligne_id_opaque, quantite, quantite_source) "
            "VALUES (?,?,?) ON CONFLICT(ligne_id_opaque) DO UPDATE SET quantite=excluded.quantite",
            (ligne_id_opaque, q, row["quantite_source"] if row["quantite"] is not None else q))
        conn.execute(
            "UPDATE facture_lignes_menage SET motif_correction=?, "
            "commentaire=COALESCE(commentaire,'') || ? WHERE ligne_id_opaque=?",
            (motif, f" · Quantité corrigée {row['quantite']}→{q} ({acteur or 'local'}) : {motif}",
             ligne_id_opaque))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ligne_id_opaque": ligne_id_opaque, "quantite": q,
            "quantite_source": row["quantite_source"], "motif": motif}


# ── §23 — affecter le logement proposé, et apprendre du geste ───────────────────────────────────

def affecter_logement(ligne_id_opaque: str, *, logement_id: str, acteur: str = "",
                      memoriser: bool = True, db_path=None) -> dict[str, Any]:
    """Rattache une ligne au logement choisi, et enregistre la correspondance pour la suite.

    LE POINT IMPORTANT EST `memoriser`. Confirmer « T.2-65 (Gabriel) → LOG_0003 » une fois doit
    suffire : la fois d'après, le rapprochement doit être CERTAIN par le référentiel, pas
    re-déduit. Sans cela l'utilisateur reconfirmerait éternellement les mêmes libellés, et le
    logiciel n'apprendrait jamais rien de ce qu'on lui dit.

    Aucun motif n'est exigé ici, à la différence de la quantité ou de la nature : affecter un
    logement ne contredit pas le document, cela complète une lecture que le parseur n'a pas su
    faire. Le libellé source, lui, reste intact.
    """
    lid = str(logement_id or "").strip()
    if not lid:
        return _refus("LOGEMENT_MANQUANT", "Aucun logement choisi.")
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT description, logement_id, logement_id_source, type_ligne_menage_id "
            "FROM facture_lignes_menage WHERE ligne_id_opaque=?", (ligne_id_opaque,)).fetchone()
        if row is None:
            return _refus("LIGNE_INTROUVABLE", ligne_id_opaque)
        if conn.execute("SELECT 1 FROM ref_logements WHERE logement_id=?", (lid,)).fetchone() \
                is None:
            return _refus("LOGEMENT_INCONNU", lid)
        # `type_ligne` suit le logement, DÉRIVÉ de la nature déjà classée si elle l'est (§4/§6
        # recette 4) — sinon (ligne jamais classée) l'ancienne règle « logement confirmé = ménage »
        # reste la seule information disponible, et continue de s'appliquer.
        compte = _compte_comme_menage(conn, row["type_ligne_menage_id"])
        type_ligne = (_type_ligne_pour(compte, lid) if compte is not None
                     else TYPE_MENAGE_EXTERNE)
        conn.execute(
            "UPDATE facture_lignes_menage SET logement_id=?, logement_confiance=?, "
            "logement_methode=?, type_ligne=?, commentaire=COALESCE(commentaire,'') || ? "
            "WHERE ligne_id_opaque=?",
            (lid, CONFIANCE_CONFIRMEE, "CHOIX_UTILISATEUR", type_ligne,
             f" · Logement confirmé {row['logement_id'] or '—'}→{lid} ({acteur or 'local'})",
             ligne_id_opaque))
        conn.commit()
    finally:
        conn.close()

    appris = None
    if memoriser and (row["description"] or "").strip():
        from app.services import logement_matching_service as lms
        appris = lms.enregistrer_correspondance(
            row["description"], lid, acteur=acteur, db_path=db_path)
    return {"ok": True, "ligne_id_opaque": ligne_id_opaque, "logement_id": lid,
            "ancien_logement_id": row["logement_id"], "correspondance_apprise": appris}


# ── §27 — une ligne qui couvre plusieurs logements se répartit EXPLICITEMENT ─────────────────────

def repartir_ligne(ligne_id_opaque: str, repartition: list[dict[str, Any]], *, motif: str,
                   acteur: str = "", db_path=None) -> dict[str, Any]:
    """Éclate une ligne du document en une part par logement, selon des quantités DONNÉES.

    POURQUOI LE LOGICIEL NE LE FAIT PAS TOUT SEUL
    Une ligne « 4 ménages — 260,00 € » qui couvre deux logements est ambiguë : 2+2 ? 3+1 ? Le
    document ne le dit pas, et le deviner produirait un coût faux pour deux propriétaires à la
    fois, sans que personne ne s'en aperçoive. §27 l'interdit explicitement. La répartition est
    donc TOUJOURS fournie par l'utilisateur, et la somme des quantités doit retomber sur la
    quantité du document — sinon la répartition est refusée, pas ajustée.

    LE MONTANT SUIT LA MÊME RÈGLE QUE PARTOUT AILLEURS
    `lib_repartition.repartir` (centimes entiers, résidu au dernier) — la fonction canonique du
    dépôt, celle des charges et de la ventilation ménage. La somme des parts égale donc le montant
    de la ligne AU CENTIME, jamais 259,99 € pour 260,00 €.

    La ligne d'origine n'est pas supprimée : elle est neutralisée, et chaque part garde son
    `ligne_parente_id_opaque` pour qu'on puisse toujours remonter à ce que le document portait.
    """
    motif = str(motif or "").strip()
    if not motif:
        return _refus("MOTIF_OBLIGATOIRE",
                      "Répartir une ligne sur plusieurs logements exige d'en donner la raison.")
    parts = [{"logement_id": str(p.get("logement_id") or "").strip(),
              "quantite": int(p.get("quantite") or 0)} for p in (repartition or [])]
    parts = [p for p in parts if p["logement_id"] and p["quantite"] > 0]
    if len(parts) < 2:
        return _refus("REPARTITION_INSUFFISANTE",
                      "Une répartition désigne au moins deux logements avec une quantité positive.")
    if len({p["logement_id"] for p in parts}) != len(parts):
        return _refus("LOGEMENT_EN_DOUBLE", "Un même logement ne peut pas figurer deux fois.")

    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT l.facture_id_opaque, l.type_ligne, l.description, l.montant_ttc, "
            "l.statut_ligne, d.quantite, d.prix_unitaire FROM facture_lignes_menage l "
            "LEFT JOIN facture_lignes_menage_detail d ON d.ligne_id_opaque = l.ligne_id_opaque "
            "WHERE l.ligne_id_opaque=?", (ligne_id_opaque,)).fetchone()
        if row is None:
            return _refus("LIGNE_INTROUVABLE", ligne_id_opaque)
        if str(row["statut_ligne"] or STATUT_LIGNE_ACTIVE) != STATUT_LIGNE_ACTIVE:
            return _refus("LIGNE_NON_ACTIVE", "Cette ligne est déjà écartée du total.")
        quantite_doc = row["quantite"]
        total_parts = sum(p["quantite"] for p in parts)
        if quantite_doc is not None and total_parts != int(quantite_doc):
            return _refus("QUANTITE_NON_CONSERVEE", (
                f"Le document porte {quantite_doc} ménage(s) sur cette ligne ; la répartition en "
                f"totalise {total_parts}. Le logiciel n'ajuste pas tout seul : corrigez la "
                f"répartition, ou corrigez d'abord la quantité (§27)."))
        inconnus = [p["logement_id"] for p in parts if conn.execute(
            "SELECT 1 FROM ref_logements WHERE logement_id=?", (p["logement_id"],)).fetchone()
            is None]
        if inconnus:
            return _refus("LOGEMENT_INCONNU", ", ".join(inconnus))
    finally:
        conn.close()

    montant = round(float(row["montant_ttc"] or 0), 2)
    poids = {p["logement_id"]: float(p["quantite"]) for p in parts}
    montants = _repartition_canonique(montant, poids)

    creees = []
    for p in parts:
        res = ajouter_ligne(
            row["facture_id_opaque"], type_ligne=row["type_ligne"],
            montant_ttc=montants[p["logement_id"]], logement_id=p["logement_id"],
            description=f"{row['description'] or ''} — part {p['logement_id']}".strip(" —"),
            quantite=p["quantite"], prix_unitaire=row["prix_unitaire"],
            source=SOURCE_REPARTITION, acteur=acteur,
            commentaire=f"Part issue de la ligne {ligne_id_opaque} — {motif}", db_path=db_path)
        if not res.get("ok"):
            return res
        creees.append({**res, "logement_id": p["logement_id"], "quantite": p["quantite"],
                       "montant_ttc": montants[p["logement_id"]]})

    conn = get_db(db_path)
    try:
        for c in creees:
            conn.execute("UPDATE facture_lignes_menage SET ligne_parente_id_opaque=? "
                         "WHERE ligne_id_opaque=?", (ligne_id_opaque, c["ligne_id_opaque"]))
        conn.execute(
            "UPDATE facture_lignes_menage SET statut_ligne=?, motif_correction=?, "
            "commentaire=COALESCE(commentaire,'') || ? WHERE ligne_id_opaque=?",
            (STATUT_LIGNE_REPARTIE, motif,
             f" · Répartie sur {len(parts)} logements ({acteur or 'local'}) : {motif}",
             ligne_id_opaque))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ligne_id_opaque": ligne_id_opaque, "parts": creees,
            "montant_reparti": montant, "somme_parts": round(sum(montants.values()), 2),
            "motif": motif}


def _repartition_canonique(montant: float, poids: dict[str, float]) -> dict[str, float]:
    """Le seul chemin de répartition monétaire du module — `lib_repartition`, comme partout."""
    import sys

    from app import config as cfg
    travail = str(cfg.APP_ROOT.parent / "02_TRAVAIL")
    if travail not in sys.path:
        sys.path.insert(0, travail)
    import lib_repartition as rp
    return rp.repartir(montant, poids)


def cout_menages_par_logement(facture_id_opaque: str, db_path=None) -> dict[str, float]:
    """Coût des ménages de chaque logement DANS cette facture — la base de pondération de la
    ventilation (§11) : uniquement les lignes MENAGE_EXTERNE, jamais les autres factures du même
    prestataire."""
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT logement_id, SUM(montant_ttc) AS total FROM facture_lignes_menage "
            "WHERE facture_id_opaque = ? AND type_ligne = ? AND logement_id IS NOT NULL "
            "AND statut_ligne = ? GROUP BY logement_id",
            (facture_id_opaque, TYPE_MENAGE_EXTERNE, STATUT_LIGNE_ACTIVE)).fetchall()
        return {r["logement_id"]: round(r["total"], 2) for r in rows}
    finally:
        conn.close()


def lignes_externes_pour_reader(db_path=None) -> list[dict[str, Any]]:
    """Toutes les lignes MENAGE_EXTERNE, au même grain et avec les mêmes noms de champs que
    l'ancien onglet MASTER de `MASTER_FACT_MEN_MenagesExternes.xlsx` (legacy `menages_reader.
    externes()`) — pour que `menages_service.py` n'ait rien à changer.

    `mois` = mois de la FACTURE (`date_facture[:7]`), pas de la ligne : c'est la règle du moteur
    (`lot6c_menages_externes.py` : `periode_facture or date_facture[:7]`), reprise telle quelle, pas
    recalculée. `prestataire_id`/`date_facture` viennent de l'en-tête `factures` (0017) ; `date_menage`/
    `precision_date_menage`/`nom_prestataire` de `facture_lignes_menage_pdf` (0040, tels que
    l'extracteur PDF les a produits, jamais devinés ici).
    """
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT l.ligne_id_opaque, l.facture_id_opaque, l.logement_id, l.montant_ttc, "
            "d.quantite, p.date_menage, p.precision_date_menage, "
            "p.nom_prestataire, f.fournisseur_id_opaque, f.date_facture, f.facture_ref, f.commentaire, "
            "f.statut AS statut_facture "
            "FROM facture_lignes_menage l "
            "JOIN factures f ON f.facture_id_opaque = l.facture_id_opaque "
            "LEFT JOIN facture_lignes_menage_detail d ON d.ligne_id_opaque = l.ligne_id_opaque "
            "LEFT JOIN facture_lignes_menage_pdf p ON p.ligne_id_opaque = l.ligne_id_opaque "
            "WHERE l.type_ligne = ? AND l.statut_ligne = ? ORDER BY l.id",
            (TYPE_MENAGE_EXTERNE, STATUT_LIGNE_ACTIVE)).fetchall()
    finally:
        conn.close()

    out = []
    for r in rows:
        date_facture = r["date_facture"] or ""
        date_menage = r["date_menage"] or ""
        mois = str(date_facture)[:7] if date_facture else "0000-00"
        # DATE_MENAGE_ABSENTE : même condition que `date_absente` (menages_service._bloc_externe),
        # reprise du legacy lot6c (une ligne facturée sans date de ménage identifiable).
        code_anomalie = "DATE_MENAGE_ABSENTE" if not date_menage else ""
        out.append({
            "menage_externe_id": r["ligne_id_opaque"],
            "facture_id": r["facture_ref"] or "",
            "nom_fichier_source": r["commentaire"] or "",
            "nom_prestataire": r["nom_prestataire"] or "",
            "prestataire_id": r["fournisseur_id_opaque"] or "",
            "date_facture": date_facture,
            "date_menage": date_menage,
            "precision_date_menage": r["precision_date_menage"] or "",
            "mois": mois,
            "logement_id": r["logement_id"] or "",
            "nombre_menages": r["quantite"],
            "montant_ligne_ttc": r["montant_ttc"],
            "statut_controle": r["statut_facture"],
            "niveau_anomalie": "A_CONTROLER" if code_anomalie else "",
            "code_anomalie": code_anomalie,
        })
    return out


def somme_lignes_effectives(facture_id_opaque: str, db_path=None) -> float:
    """LA somme canonique des lignes d'une facture — UNE fonction, utilisée PARTOUT où « la somme
    des lignes » doit être dite : l'écran, le diagnostic d'écart, et le contrôle V11 qui
    conditionne la validation (recette 4, §6).

    BUG RÉEL CORRIGÉ : ce n'était pas le cas. `controler_total` (donc V11) sommait en SQL,
    `factures_service.charger` sommait en PYTHON sur la liste déjà chargée par `lignes()` — deux
    calculs séparés qui POUVAIENT diverger dès qu'une évolution touchait l'un sans l'autre. Il n'y
    a plus qu'un seul calcul : les autres l'appellent.

    Une ligne compte si `statut_ligne = ACTIVE` — neutralisée (§30) ou remplacée par ses parts
    (§27), elle ne compte plus, mais reste lisible. `facture_lignes` (rattachement de charge,
    §33/§34 retiré de l'écran mais dont d'anciennes factures peuvent porter des lignes) est
    additionnée aussi : ces lignes n'ont jamais eu de statut de neutralisation, elles comptent
    donc toujours.
    """
    conn = get_db(db_path)
    try:
        total_menage = conn.execute(
            "SELECT COALESCE(SUM(montant_ttc), 0) FROM facture_lignes_menage "
            "WHERE facture_id_opaque = ? AND statut_ligne = ?",
            (facture_id_opaque, STATUT_LIGNE_ACTIVE)).fetchone()[0]
        total_charge = conn.execute(
            "SELECT COALESCE(SUM(montant_ttc), 0) FROM facture_lignes "
            "WHERE facture_id_opaque = ?", (facture_id_opaque,)).fetchone()[0]
    finally:
        conn.close()
    return round((total_menage or 0) + (total_charge or 0), 2)


def controler_total(facture_id_opaque: str, db_path=None) -> dict[str, Any]:
    """Somme des lignes ménage vs montant_ttc de la facture. Écart attendu : 0,00€."""
    conn = get_db(db_path)
    try:
        f = conn.execute("SELECT montant_ttc FROM factures WHERE facture_id_opaque = ?",
                         (facture_id_opaque,)).fetchone()
        if f is None:
            return _refus("FACTURE_INTROUVABLE", facture_id_opaque)
    finally:
        conn.close()
    montant_facture = round(f["montant_ttc"] or 0, 2)
    total_lignes = somme_lignes_effectives(facture_id_opaque, db_path=db_path)
    ecart = round(montant_facture - total_lignes, 2)
    return {"ok": True, "montant_facture": montant_facture, "montant_lignes": total_lignes,
            "ecart": ecart, "coherent": abs(ecart) <= TOLERANCE}
