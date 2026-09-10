"""Composition d'une facture propriétaire : extras, réductions, charges refacturables rattachées,
et décomposition lisible du montant dû.

CE MODULE NE CALCULE AUCUNE DONNÉE MÉTIER. Il compose des éléments déjà calculés ailleurs :

    commissions        Lot10 (`lot10_commissions`), figées dans les lignes à la création
    forfait/ménage/…   Lot10, idem
    charges            `charges` (saisie canonique), RATTACHÉES ici, jamais recalculées
    acomptes           `mouvements_tresorerie_proprietaires` (0025), via l'édition existante

Il n'écrit ni dans Lot9/Lot10/Lot12, ni dans `charges` (sauf le lien de rattachement), ni dans la
comptabilité.

── LES QUATRE NATURES, ET POURQUOI ELLES NE SE CONFONDENT PAS ───────────────────────────────────
EXTRA        prestation ponctuelle facturée en plus. Montant POSITIF, entre dans `montant_total`.
REDUCTION    remise commerciale sur CETTE facture. Montant NÉGATIF, entre dans `montant_total` :
             on facture réellement moins.
CHARGE       refacturation d'une dépense déjà engagée. Montant POSITIF, RÉFÉRENCE la charge source
             — la charge n'est jamais dupliquée économiquement.
ACOMPTE      paiement DÉJÀ REÇU. N'est PAS une ligne de facture : c'est un mouvement de trésorerie
             (0025) rattaché à la facture. Il ne diminue pas ce qui est facturé, il diminue ce qui
             RESTE à payer.

D'où la distinction que tout l'écran et le PDF doivent rendre visible :

    montant_total  = commissions + ménages + canapé + forfait + refacturations + extras − réductions
    montant dû     = montant_total − acomptes déjà versés

Confondre RÉDUCTION et ACOMPTE reviendrait à dire qu'une remise est un encaissement : le chiffre
d'affaires facturé serait faux, et la comptabilité avec lui.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db
from app.services import factures_proprietaires_service as svc

TYPE_EXTRA = "EXTRA"
TYPE_REDUCTION = "REDUCTION"
TYPE_CHARGE_REFACTUREE = "CHARGE_REFACTUREE"

EVT_AJOUT_EXTRA = "AJOUT_EXTRA"
EVT_AJOUT_REDUCTION = "AJOUT_REDUCTION"
EVT_RATTACHEMENT_CHARGE = "RATTACHEMENT_CHARGE"
EVT_DETACHEMENT_CHARGE = "DETACHEMENT_CHARGE"

SOURCE_CHARGE = "CHARGE"

# Regroupement des `type_ligne` pour la décomposition affichée. L'ordre est celui de la facture.
# `MENAGE_FACTURE` et `PREPARATION_CANAPE` restent des postes distincts : ce sont des prestations
# réellement rendues, pas des variantes de commission.
GROUPES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("commissions", "Commissions de conciergerie", ("COMMISSION_CONCIERGERIE",)),
    ("menages", "Prestations de ménage", ("MENAGE_FACTURE",)),
    ("canape", "Préparation du canapé", ("PREPARATION_CANAPE",)),
    # Une SEULE ligne métier, conformément au référentiel : `REF_Charges_Recurrentes.REC_001`
    # s'appelle « Forfait client logiciel et consommables » et Lot10 la calcule d'un seul tenant
    # depuis `REF_Logements.forfait_logiciel_consommables_mensuel`. Logiciel et consommables ne
    # sont donc PAS deux postes séparables ici : les scinder inventerait une répartition que le
    # référentiel ne porte pas.
    ("forfait", "Forfait logiciel et consommables", ("CHARGE_FIXE",)),
    ("refacturations", "Charges refacturées", ("CHARGES_EXCEPT_REFAC", TYPE_CHARGE_REFACTUREE)),
    ("extras", "Extras", (TYPE_EXTRA,)),
    ("reductions", "Réductions", (TYPE_REDUCTION,)),
)


def _err(message: str) -> None:
    raise svc.FactureProprietaireError(message)


# ── EXTRA ───────────────────────────────────────────────────────────────────────────────────────

def ajouter_extra(facture_id: str, *, libelle: str, montant: Any, commentaire: str = "",
                  acteur: str = "", db_path=None) -> dict[str, Any]:
    """Ajoute un EXTRA (prestation ponctuelle facturée en plus). Montant strictement positif.

    Un extra négatif serait une réduction déguisée : refusé explicitement plutôt que silencieusement
    accepté, sinon la ligne « Extras » du récapitulatif pourrait diminuer le total sans que rien ne
    l'annonce.
    """
    valeur = svc._round(montant)
    if valeur <= 0:
        _err("un extra doit avoir un montant strictement positif "
             "(pour diminuer la facture, utiliser une reduction)")
    return svc.ajouter_ligne(facture_id, type_ligne=TYPE_EXTRA, libelle=libelle, montant=valeur,
                             objet_source_type=None, objet_source_ref=None, acteur=acteur,
                             commentaire=commentaire, db_path=db_path)


# ── RÉDUCTION ───────────────────────────────────────────────────────────────────────────────────

def ajouter_reduction(facture_id: str, *, libelle: str, montant: Any, motif: str = "",
                      acteur: str = "", db_path=None) -> dict[str, Any]:
    """Ajoute une RÉDUCTION. L'utilisateur saisit un montant POSITIF ; la ligne est stockée NÉGATIVE.

    Stocker le signe dans la donnée plutôt que dans le type évite qu'un consommateur oublie de
    soustraire : `SUM(montant)` reste le total facturé, quelle que soit la composition.
    """
    valeur = svc._round(montant)
    if valeur <= 0:
        _err("une reduction se saisit avec un montant positif (elle sera deduite du total)")
    facture = svc.lire(facture_id, db_path=db_path)
    facturable = sum(l["montant"] for l in facture["lignes"] if l["type_ligne"] != TYPE_REDUCTION)
    deja_reduit = -sum(l["montant"] for l in facture["lignes"] if l["type_ligne"] == TYPE_REDUCTION)
    if valeur + deja_reduit > svc._round(facturable) + 0.001:
        _err(f"reduction refusee : {valeur:.2f} + {deja_reduit:.2f} deja accorde depasse le "
             f"montant facturable ({svc._round(facturable):.2f}). Une facture ne peut pas devenir "
             "negative.")
    return svc.ajouter_ligne(facture_id, type_ligne=TYPE_REDUCTION, libelle=libelle,
                             montant=-valeur, acteur=acteur, commentaire=motif, db_path=db_path)


# ── CHARGES REFACTURABLES : rattacher une charge QUI EXISTE DÉJÀ ────────────────────────────────

def _colonnes_charges(conn) -> set[str]:
    return {r[1] for r in conn.execute("PRAGMA table_info(charges)")}


def charges_eligibles(facture_id: str, *, db_path=None) -> list[dict[str, Any]]:
    """Charges réellement refacturables à CE propriétaire pour CETTE facture, non encore facturées.

    Quatre filtres, tous métier — aucun n'est décoratif :
      1. `statut = ACTIVE`      : une charge annulée n'est pas refacturable ;
      2. `refacturable = OUI`   : la décision vient de la saisie/du référentiel, pas d'ici ;
      3. périmètre              : même propriétaire, et même logement quand la charge en porte un
                                  (une charge commune sans logement reste éligible au propriétaire) ;
      4. non déjà rattachée     : ni à cette facture, ni à une autre.
    Le mois n'est PAS un filtre : une dépense engagée en juin peut légitimement être refacturée sur
    la facture de juillet. C'est l'utilisateur qui décide, la liste montre la date pour qu'il puisse.
    """
    facture = svc.lire(facture_id, db_path=db_path)
    conn = get_db(db_path)
    try:
        cols = _colonnes_charges(conn)
        if not {"charge_id", "statut", "refacturable"} <= cols:
            return []
        select = [c for c in ("charge_id", "date_charge", "mois", "montant", "categorie_charge_id",
                              "logement_id", "proprietaire_id", "code_impact", "justificatif",
                              "commentaire", "statut", "refacturable") if c in cols]
        sql = (f"SELECT {', '.join(select)} FROM charges "
               "WHERE statut = 'ACTIVE' AND UPPER(COALESCE(refacturable,'')) IN ('OUI','1','TRUE') "
               "  AND COALESCE(proprietaire_id,'') = ? "
               "  AND (COALESCE(logement_id,'') = '' OR COALESCE(logement_id,'') = ?) "
               "  AND charge_id NOT IN (SELECT charge_id FROM factures_proprietaires_lignes_charge) "
               "ORDER BY date_charge DESC, charge_id")
        lignes = [dict(zip(select, r)) for r in conn.execute(
            sql, (facture["proprietaire_id"], facture["logement_id"]))]
    finally:
        conn.close()
    for l in lignes:
        l["montant"] = svc._round(l.get("montant"))
    return lignes


def charge_deja_facturee(charge_id: str, *, db_path=None) -> dict[str, Any]:
    """La charge est-elle déjà portée par une ligne de facture ? Renvoie la facture qui la porte."""
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT lc.facture_id_opaque, f.numero_facture, f.statut "
            "FROM factures_proprietaires_lignes_charge lc "
            "LEFT JOIN factures_proprietaires f ON f.facture_id_opaque = lc.facture_id_opaque "
            "WHERE lc.charge_id = ?", (charge_id,)).fetchone()
    finally:
        conn.close()
    if r is None:
        return {"facturee": False}
    return {"facturee": True, "facture_id_opaque": r[0], "numero_facture": r[1] or "",
            "statut": r[2] or ""}


def rattacher_charge(facture_id: str, charge_id: str, *, libelle: str = "", acteur: str = "",
                     db_path=None) -> dict[str, Any]:
    """Rattache une charge EXISTANTE à un BROUILLON — sans jamais la recréer ni la recalculer.

    Différence avec `factures_proprietaires_edition_service.ajouter_ligne_charge`, qui CRÉE une
    charge neuve depuis le brouillon : ici la charge préexiste (saisie ailleurs, importée d'une
    facture fournisseur…) et on ne fait que la porter sur le document. Aucun montant n'est saisi :
    il vient de la charge, sinon le document et la comptabilité diraient deux chiffres différents.

    Transaction unique : la ligne, le lien et l'événement sont écrits ensemble ou pas du tout.
    """
    facture = svc.lire(facture_id, db_path=db_path)
    svc._exiger_brouillon(facture, "rattachement d'une charge")

    conn = get_db(db_path)
    try:
        cols = _colonnes_charges(conn)
        champs = [c for c in ("charge_id", "montant", "date_charge", "proprietaire_id",
                              "logement_id", "statut", "refacturable", "categorie_charge_id",
                              "code_impact", "commentaire") if c in cols]
        row = conn.execute(
            f"SELECT {', '.join(champs)} FROM charges WHERE charge_id = ?", (charge_id,)).fetchone()
        if row is None:
            _err(f"charge introuvable : {charge_id}")
        charge = dict(zip(champs, row))

        if str(charge.get("statut") or "").upper() != "ACTIVE":
            _err(f"charge {charge_id} non ACTIVE (statut {charge.get('statut')}) : non refacturable")
        if str(charge.get("refacturable") or "").upper() not in ("OUI", "1", "TRUE"):
            _err(f"charge {charge_id} non refacturable (refacturable="
                 f"{charge.get('refacturable')!r})")
        if str(charge.get("proprietaire_id") or "") != facture["proprietaire_id"]:
            _err(f"charge {charge_id} rattachee au proprietaire "
                 f"{charge.get('proprietaire_id')!r}, la facture concerne "
                 f"{facture['proprietaire_id']!r}")
        logement_charge = str(charge.get("logement_id") or "")
        if logement_charge and logement_charge != facture["logement_id"]:
            _err(f"charge {charge_id} rattachee au logement {logement_charge!r}, la facture "
                 f"concerne {facture['logement_id']!r}")
        deja = conn.execute(
            "SELECT facture_id_opaque FROM factures_proprietaires_lignes_charge WHERE charge_id=?",
            (charge_id,)).fetchone()
        if deja is not None:
            _err(f"charge {charge_id} deja facturee (facture {deja[0]})")

        montant = svc._round(charge.get("montant"))
        if montant <= 0:
            _err(f"charge {charge_id} sans montant refacturable ({montant:.2f})")

        texte = str(libelle or "").strip() or _libelle_charge(charge)
        ligne = svc.ajouter_ligne(
            facture_id, type_ligne=TYPE_CHARGE_REFACTUREE, libelle=texte, montant=montant,
            objet_source_type=SOURCE_CHARGE, objet_source_ref=charge_id, acteur=acteur,
            commentaire=f"charge existante {charge_id} rattachee", _conn=conn)
        conn.execute(
            "INSERT INTO factures_proprietaires_lignes_charge "
            "(ligne_id_opaque, facture_id_opaque, charge_id, code_impact) VALUES (?,?,?,?)",
            (ligne["ligne_id_opaque"], facture_id, charge_id,
             str(charge.get("code_impact") or "IC")))
        svc._journal(conn, facture_id, EVT_RATTACHEMENT_CHARGE, svc.ST_BROUILLON,
                     svc.ST_BROUILLON, f"charge {charge_id} ({montant:.2f}) rattachee", acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"ok": True, "ligne_id_opaque": ligne["ligne_id_opaque"], "charge_id": charge_id,
            "montant": montant}


def _libelle_charge(charge: dict[str, Any]) -> str:
    base = str(charge.get("commentaire") or "").strip() or \
        str(charge.get("categorie_charge_id") or "").strip() or "Charge refacturée"
    date = str(charge.get("date_charge") or "").strip()
    return f"{base} ({date})" if date else base


def detacher_charge(facture_id: str, ligne_id: str, *, acteur: str = "",
                    db_path=None) -> dict[str, Any]:
    """Retire du BROUILLON une charge RATTACHÉE. La charge elle-même n'est jamais annulée.

    C'est la différence essentielle avec `supprimer_ligne_charge` (charge CRÉÉE depuis le
    brouillon, donc annulable avec lui) : une charge préexistante appartient au métier des charges,
    pas au document. On la libère seulement — elle redevient immédiatement sélectionnable, ce qui
    est exactement ce que demande le parcours de correction.
    """
    facture = svc.lire(facture_id, db_path=db_path)
    svc._exiger_brouillon(facture, "detachement d'une charge")
    conn = get_db(db_path)
    try:
        lien = conn.execute(
            "SELECT charge_id FROM factures_proprietaires_lignes_charge WHERE ligne_id_opaque=?",
            (ligne_id,)).fetchone()
        conn.execute("DELETE FROM factures_proprietaires_lignes_charge WHERE ligne_id_opaque=?",
                     (ligne_id,))
        conn.execute("DELETE FROM factures_proprietaires_lignes WHERE ligne_id_opaque=? "
                     "AND facture_id_opaque=?", (ligne_id, facture_id))
        total = svc._resynchroniser_total(conn, facture_id)
        svc._journal(conn, facture_id, EVT_DETACHEMENT_CHARGE, svc.ST_BROUILLON, svc.ST_BROUILLON,
                     f"charge {lien[0] if lien else '?'} detachee ; total {total:.2f}", acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"ok": True, "charge_id": lien[0] if lien else None, "charge_conservee": True}


# ── DÉCOMPOSITION DU MONTANT DÛ ─────────────────────────────────────────────────────────────────

def decomposition(facture_id: str, *, db_path=None) -> dict[str, Any]:
    """La formule complète, calculée CÔTÉ SERVEUR — l'écran et le PDF n'additionnent rien.

    Aucune valeur n'est stockée : tout se dérive des lignes et des mouvements de trésorerie
    rattachés. Un total stocké se désynchroniserait au premier ajout de ligne ; ici c'est
    impossible par construction.
    """
    facture = svc.lire(facture_id, db_path=db_path)
    lignes = facture["lignes"]

    postes = []
    for cle, libelle, types in GROUPES:
        concernees = [l for l in lignes if l["type_ligne"] in types]
        postes.append({
            "cle": cle, "libelle": libelle,
            "montant": svc._round(sum(l["montant"] for l in concernees)),
            "nb": len(concernees), "lignes": concernees,
        })
    connus = {t for _, _, types in GROUPES for t in types}
    autres = [l for l in lignes if l["type_ligne"] not in connus]
    if autres:
        postes.append({"cle": "autres", "libelle": "Autres", "nb": len(autres),
                       "montant": svc._round(sum(l["montant"] for l in autres)),
                       "lignes": autres})

    total_facture = svc._round(sum(l["montant"] for l in lignes))
    acomptes = svc.acomptes_proprietaire(facture_id, db_path=db_path)
    total_acomptes = svc._round(sum(float(a.get("montant") or 0) for a in acomptes))

    par_cle = {p["cle"]: p["montant"] for p in postes}
    # Sous-total = tout ce qui augmente la facture, avant les déductions consenties.
    sous_total = svc._round(sum(v for k, v in par_cle.items() if k != "reductions"))
    reductions = svc._round(-par_cle.get("reductions", 0.0))   # affiché en valeur positive

    return {
        "facture_id_opaque": facture_id,
        "postes": postes,
        "par_cle": par_cle,
        "sous_total": sous_total,
        "total_reductions": reductions,
        "total_facture": total_facture,
        "acomptes": acomptes,
        "total_acomptes": total_acomptes,
        "montant_du": svc._round(total_facture - total_acomptes),
        "devise": facture.get("devise") or "EUR",
    }


# ── DOCUMENT : une seule structure pour la prévisualisation ET le PDF ───────────────────────────

def document(facture_id: str, *, emetteur: dict[str, Any] | None = None,
             destinataire: dict[str, Any] | None = None, db_path=None) -> dict[str, Any]:
    """Contexte complet du document, dans la MÊME forme que le snapshot d'une facture émise.

    C'est ce qui garantit qu'il n'existe pas deux modèles divergents : la prévisualisation d'un
    BROUILLON et le PDF d'une facture ÉMISE sont produits par le même moteur de rendu, à partir de
    la même structure. La seule différence est la provenance :
      · BROUILLON  → reconstruit à chaud ici, il change à chaque modification (c'est le but) ;
      · ÉMISE      → lu depuis `snapshot_json`, figé, jamais recalculé.

    Une facture émise renvoie donc TOUJOURS son snapshot, même si les référentiels ont changé
    depuis : c'est la définition d'un document opposable.
    """
    facture = svc.lire(facture_id, db_path=db_path)
    if facture.get("statut") == svc.ST_EMIS and facture.get("snapshot_json"):
        import json
        fige = json.loads(facture["snapshot_json"])
        fige.setdefault("statut", facture.get("statut"))
        fige["fige"] = True
        return fige

    deco = decomposition(facture_id, db_path=db_path)
    return {
        "facture_id_opaque": facture_id,
        "numero_facture": facture.get("numero_facture") or "",
        "type_document": facture.get("type_document") or svc.TYPE_FACTURE,
        "facture_origine": facture.get("facture_origine"),
        "statut": facture.get("statut"),
        "emetteur": dict(emetteur or {}),
        "destinataire": dict(destinataire or {}),
        "proprietaire_id": facture.get("proprietaire_id"),
        "logement_id": facture.get("logement_id"),
        "mois": facture.get("mois"),
        "date_facture": facture.get("date_facture") or "",
        "devise": facture.get("devise") or "EUR",
        "lignes": facture["lignes"],
        "montant_total": svc._round(facture.get("montant_total")),
        "reservations": svc.reservations(facture_id, db_path=db_path),
        "decomposition": {**deco,
                          "postes": [{k: v for k, v in p.items() if k != "lignes"}
                                     for p in deco["postes"]]},
        "fige": False,
    }


def periode(mois: str) -> dict[str, str]:
    """Période couverte par la facture, en clair. Le grain est le MOIS — celui de Lot10/Lot12.

    Exposer des dates de début/fin libres laisserait croire qu'on peut facturer du 12 au 27 : ni
    les commissions, ni le forfait, ni les contrôles ne savent le faire. On rend donc explicites les
    bornes RÉELLES du mois plutôt que d'inventer une période que le moteur ne saurait pas honorer.
    """
    import calendar
    try:
        annee, mm = int(str(mois)[:4]), int(str(mois)[5:7])
        dernier = calendar.monthrange(annee, mm)[1]
        return {"mois": mois, "debut": f"{annee:04d}-{mm:02d}-01",
                "fin": f"{annee:04d}-{mm:02d}-{dernier:02d}"}
    except (ValueError, IndexError):
        return {"mois": str(mois or ""), "debut": "", "fin": ""}
