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

# Réexporté depuis le service canonique : la valeur est le contrat que `valider()` applique.
SOURCE_POSITION = svc.SOURCE_POSITION_REFAC

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

def charges_eligibles(facture_id: str, *, db_path=None) -> list[dict[str, Any]]:
    """Charges refacturables proposables à CETTE facture.

    DÉLÈGUE au sélecteur canonique `charges_refacturation_service.proposer_pour_facture()`, qui
    raisonne en POSITIONS DE REFACTURATION — l'objet que le reste de la chaîne connaît. Une
    première version interrogeait directement `charges` avec ses propres filtres : elle produisait
    des lignes que `factures_proprietaires_service.valider()` ne savait pas imputer, puisque celle-ci
    attend un `position_id` en `objet_source_ref`. Deux sélecteurs pour une même question, c'était
    déjà un de trop.

    Le mois n'est pas un filtre : une dépense engagée en juin peut légitimement être refacturée sur
    la facture de juillet. La date est affichée pour que l'utilisateur décide.
    """
    from app.services import charges_refacturation_service as refac

    facture = svc.lire(facture_id, db_path=db_path)
    positions = refac.proposer_pour_facture(facture["proprietaire_id"],
                                            logement_id=facture["logement_id"], db_path=db_path)
    conn = get_db(db_path)
    try:
        # Déjà portée PAR CETTE FACTURE : on ne la propose plus (une seconde ligne identique
        # serait un double-clic). Portée par une AUTRE facture : elle reste proposable tant qu'il
        # lui reste du solde — c'est précisément la refacturation partagée 350/350.
        sur_cette_facture = {r[0] for r in conn.execute(
            "SELECT objet_source_ref FROM factures_proprietaires_lignes "
            "WHERE facture_id_opaque = ? AND type_ligne = ?",
            (facture_id, TYPE_CHARGE_REFACTUREE))}
    finally:
        conn.close()
    return [p for p in positions
            if p["position_id"] not in sur_cette_facture and p["montant_restant"] > 0.001]


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


def rattacher_charge(facture_id: str, position_id: str, *, libelle: str = "", montant: Any = None,
                     acteur: str = "", db_path=None) -> dict[str, Any]:
    """Porte sur un BROUILLON une charge refacturable EXISTANTE, via sa POSITION de refacturation.

    La ligne référence le `position_id`, jamais le `charge_id` : c'est le contrat que
    `factures_proprietaires_service.valider()` applique quand elle impute chaque ligne
    `CHARGE_REFACTUREE` (`refac.imputer(objet_source_ref, ...)`). Référencer la charge produirait
    une facture qu'on ne pourrait pas valider — « Position introuvable ».

    Différence avec `factures_proprietaires_edition_service.ajouter_ligne_charge`, qui CRÉE une
    charge neuve depuis le brouillon : ici la dépense préexiste (saisie ailleurs, importée d'une
    facture fournisseur…) et on ne fait que la porter sur le document.

    LE MONTANT EST UNE DÉCISION COMMERCIALE, pas une conséquence de l'analytique. Une charge de
    700 € commune à deux logements pèse 350 € sur le résultat de chacun, mais peut être refacturée
    700 € sur une seule facture, ou 350/350, ou 500/200. Par défaut on propose le solde restant ;
    l'utilisateur peut saisir moins et reporter le reste sur une autre facture. Le cumul, lui, ne
    peut jamais dépasser le montant d'origine.

    Transaction unique : la ligne, le lien et l'événement sont écrits ensemble ou pas du tout.
    L'imputation, elle, n'a lieu qu'à la VALIDATION — un brouillon ne consomme rien, il RÉSERVE
    (cf. `charges_refacturation_service.montant_disponible`).
    """
    from app.services import charges_refacturation_service as refac

    facture = svc.lire(facture_id, db_path=db_path)
    svc._exiger_brouillon(facture, "rattachement d'une charge")
    position_id = str(position_id or "").strip()

    # Doublon sur CETTE facture : contrôlé EN PREMIER. Une fois l'élément porté, son solde tombe à
    # zéro et il cesse d'être proposable ; si l'on testait la proposabilité d'abord, un double-clic
    # répondrait « solde épuisé » — un message qui décrit la conséquence au lieu de la cause.
    conn_verif = get_db(db_path)
    try:
        double = conn_verif.execute(
            "SELECT 1 FROM factures_proprietaires_lignes WHERE facture_id_opaque = ? "
            "AND type_ligne = ? AND objet_source_ref = ?",
            (facture_id, TYPE_CHARGE_REFACTUREE, position_id)).fetchone()
    finally:
        conn_verif.close()
    if double is not None:
        _err(f"element deja porte par cette facture (position {position_id}) : "
             f"modifiez la ligne existante plutot que d'en ajouter une seconde")

    proposables = {p["position_id"]: p for p in refac.proposer_pour_facture(
        facture["proprietaire_id"], logement_id=facture["logement_id"], db_path=db_path)}
    position = proposables.get(position_id)
    if position is None:
        _err(f"position {position_id} non proposable pour cette facture : elle n'est pas "
             f"refacturable au proprietaire {facture['proprietaire_id']} / logement "
             f"{facture['logement_id']}, ou son solde est epuise")

    # Disponible = éligible − déjà imputé − déjà réservé par d'AUTRES brouillons. On exclut la
    # facture courante pour qu'une correction sur place ne se bloque pas elle-même.
    restant = svc._round(refac.montant_disponible(position["position_id"],
                                                  sauf_facture=facture_id, db_path=db_path))
    valeur = restant if montant is None else svc._round(montant)
    if valeur <= 0:
        _err(f"montant a refacturer invalide ({valeur:.2f}) : il doit etre strictement positif")
    if restant <= 0:
        _err(f"position {position_id} sans solde refacturable disponible ({restant:.2f})")
    if valeur - restant > 0.001:
        _err(f"montant {valeur:.2f} superieur au solde disponible {restant:.2f}")

    charge_id = position.get("charge_id")
    conn = get_db(db_path)
    try:
        texte = str(libelle or "").strip() or _libelle_position(position)
        # `db_path` DOIT être propagé même quand `_conn` est fourni : `ajouter_ligne` relit la
        # facture avant d'écrire, et sans lui cette relecture viserait la base par défaut au lieu
        # de celle qu'on est en train d'écrire. Le défaut ne se voyait pas tant que `cfg.DB_PATH`
        # était monkeypatché (cas des tests) ; il apparaît dès qu'on passe une base explicite.
        ligne = svc.ajouter_ligne(
            facture_id, type_ligne=TYPE_CHARGE_REFACTUREE, libelle=texte, montant=valeur,
            objet_source_type=SOURCE_POSITION, objet_source_ref=position["position_id"],
            acteur=acteur, commentaire=f"position {position['position_id']} rattachee",
            db_path=db_path, _conn=conn)
        conn.execute(
            "INSERT INTO factures_proprietaires_lignes_charge "
            "(ligne_id_opaque, facture_id_opaque, charge_id, code_impact) VALUES (?,?,?,?)",
            (ligne["ligne_id_opaque"], facture_id, charge_id, "IC"))
        svc._journal(conn, facture_id, EVT_RATTACHEMENT_CHARGE, svc.ST_BROUILLON,
                     svc.ST_BROUILLON,
                     f"position {position['position_id']} ({valeur:.2f}) rattachee", acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"ok": True, "ligne_id_opaque": ligne["ligne_id_opaque"], "charge_id": charge_id,
            "position_id": position["position_id"], "montant": valeur}


def _libelle_position(position: dict[str, Any]) -> str:
    """Le libellé imprimé sur la facture. `justificatif` est volontairement écarté : c'est une
    référence de pièce (souvent un chemin de fichier), pas un texte destiné au propriétaire."""
    base = str(position.get("description") or "").strip() or "Charge refacturée"
    date = str(position.get("date_charge") or "").strip()
    return f"{base} ({date})" if date else base


def detacher_charge(facture_id: str, ligne_id: str, *, acteur: str = "",
                    db_path=None) -> dict[str, Any]:
    """Retire du BROUILLON une charge RATTACHÉE. La charge elle-même n'est jamais annulée.

    C'est la différence essentielle avec `supprimer_ligne_charge` (charge CRÉÉE depuis le
    brouillon, donc annulable avec lui) : une charge préexistante appartient au métier des charges,
    pas au document. On la libère seulement — elle redevient immédiatement sélectionnable, ce qui
    est exactement ce que demande le parcours de correction.

    Aucune position n'est à désimputer : le rattachement n'en consomme aucune (l'imputation a lieu
    à `valider()`) et `_exiger_brouillon` garantit qu'on est encore en amont de cette étape.
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
    # Reversements Airbnb : sommes DÉJÀ DÉTENUES pour le compte du propriétaire, qui éteignent la
    # créance sans encaissement. Ils ne sont ni une ligne de facture ni un acompte — d'où un poste
    # distinct jusque sur le document (§17, §20).
    reversements = svc.reversements_airbnb(facture_id, db_path=db_path)
    total_reversements = svc._round(sum(float(r.get("montant_impute") or 0) for r in reversements))

    par_cle = {p["cle"]: p["montant"] for p in postes}
    # Sous-total = tout ce qui augmente la facture, avant les déductions consenties.
    sous_total = svc._round(sum(v for k, v in par_cle.items() if k != "reductions"))
    reductions = svc._round(-par_cle.get("reductions", 0.0))   # affiché en valeur positive

    # NET = ce qui reste à payer une fois déduits les règlements reçus et les compensations.
    # Négatif, il ne devient pas une « facture négative » : c'est un montant dû AU propriétaire.
    net = svc._round(total_facture - total_acomptes - total_reversements)

    return {
        "facture_id_opaque": facture_id,
        "postes": postes,
        "par_cle": par_cle,
        "sous_total": sous_total,
        "total_reductions": reductions,
        "total_facture": total_facture,
        "acomptes": acomptes,
        "total_acomptes": total_acomptes,
        "reversements_airbnb": reversements,
        "total_reversements_airbnb": total_reversements,
        "total_reglements": svc._round(total_acomptes + total_reversements),
        "net": net,
        "sens_net": "A_PAYER" if net > 0.005 else "A_REVERSER" if net < -0.005 else "SOLDE",
        "montant_a_reverser": svc._round(-net) if net < -0.005 else 0.0,
        # `montant_du` conservé pour compatibilité : c'est le NET quand il est positif.
        "montant_du": net,
        "devise": facture.get("devise") or "EUR",
    }


# ── DOCUMENT : une seule structure pour la prévisualisation ET le PDF ───────────────────────────

def _conformite_provisoire(facture: dict[str, Any], *, db_path=None) -> dict[str, Any]:
    """Bloc réglementaire d'un BROUILLON, recalculé à chaque aperçu.

    Provisoire par nature : il reflète la configuration et le référentiel de MAINTENANT, et changera
    si l'un des deux change avant l'émission. C'est exactement le contraire du bloc figé d'une
    facture émise, et c'est voulu — l'aperçu doit montrer ce que produirait une émission immédiate.

    Ne lève jamais : un référentiel incomplet doit dégrader l'aperçu, pas l'empêcher.
    """
    try:
        from app.services import factures_proprietaires_conformite_service as conformite
        return conformite.construire(
            facture, date_facture=facture.get("date_facture") or f"{facture.get('mois')}-01",
            db_path=db_path)
    except Exception:      # noqa: BLE001
        return {}

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
        # Bloc réglementaire, CALCULÉ À CHAUD pour un brouillon — la facture émise, elle, porte le
        # sien figé dans son snapshot. Sans lui, la prévisualisation omettait silencieusement les
        # mentions de TVA, les conditions de règlement, les représentants et le nom du logement :
        # elle affichait donc MOINS que le document final, alors qu'elle est censée le montrer tel
        # qu'il sera. Un aperçu qui ment par omission ne sert à rien.
        "conformite": _conformite_provisoire(facture, db_path=db_path),
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
