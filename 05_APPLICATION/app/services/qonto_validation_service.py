"""Validation humaine d'un mouvement Qonto — un chef d'orchestre, pas un moteur de plus.

CE MODULE N'INVENTE AUCUNE MÉCANIQUE. Il appelle celles qui existent :

  · `banques_rapprochement_service`  — le lien mouvement ↔ objet métier. Il gère déjà l'affectation
    partielle, le refus de dépassement, le refus d'un mouvement déjà affecté à 100 %, les statuts
    et le journal d'événements. Une transaction Qonto y entre comme n'importe quel mouvement.
  · `comptabilite_ecritures_service` — les écritures et leur contrepassation. Son garde
    `_deja_generee` rend la génération idempotente : regénérer pour la même origine est un no-op.
  · `reglements_fournisseurs_service` — le règlement d'une dette fournisseur, qui rafraîchit seul
    le statut de la facture.
  · `proprietaires_tresorerie_service` — l'encaissement d'un propriétaire, consommé ensuite par
    l'allocation FIFO de `compte_proprietaire_service`.

POURQUOI NE PAS RECODER L'ÉCRITURE DE RÈGLEMENT. Les services de règlement produisent déjà leur
effet bancaire. Ajouter une écriture ici la compterait deux fois — la banque serait créditée
deux fois pour un seul paiement. C'est la raison de la règle « un effet, un seul producteur ».

CE QUI RESTE INTERDIT : valider une transaction `pending`. Une autorisation de carte peut changer
de montant ou ne jamais être débitée ; en tirer une écriture comptable serait enregistrer un fait
qui n'a pas eu lieu.

QONTO RESTE EN LECTURE SEULE. Rien ici n'écrit vers la banque : le client ne sait faire que des GET
et aucun drapeau d'écriture ne change cela. Les effets décrits sont internes à SQLite.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from app.db.connection import get_db
from app.services import banques_rapprochement_service as rappro
from app.services import caisse_transferts_service as transferts
from app.services import comptabilite_ecritures_service as compta
from app.services import qonto_classification_service as classif

# ── Natures proposables à l'écran ─────────────────────────────────────────────────────────────
APPORT_ASSOCIE = "APPORT_ASSOCIE"
TRANSFERT_CAISSE = "TRANSFERT_CAISSE"
REGLEMENT_CHARGE = "REGLEMENT_CHARGE"
REVERSEMENT_PROPRIETAIRE = "REVERSEMENT_PROPRIETAIRE"

LIBELLES_NATURE = {
    APPORT_ASSOCIE: "Apport en compte courant d'associé",
    TRANSFERT_CAISSE: "Retrait d'espèces (Banque → Caisse)",
    REGLEMENT_CHARGE: "Paiement d'une facture fournisseur",
    REVERSEMENT_PROPRIETAIRE: "Encaissement d'un propriétaire",
}

# Quelle nature peut s'appliquer à quel sens. Un apport ne sort pas du compte, un paiement
# fournisseur n'y entre pas : proposer l'inverse serait offrir une erreur en un clic.
NATURES_PAR_SENS = {
    "credit": (APPORT_ASSOCIE, REVERSEMENT_PROPRIETAIRE),
    "debit": (REGLEMENT_CHARGE, TRANSFERT_CAISSE),
}

E_INTROUVABLE = "QV01_TRANSACTION_INTROUVABLE"
E_PENDING = "QV02_TRANSACTION_NON_DEFINITIVE"
E_NATURE_INCONNUE = "QV03_NATURE_INCONNUE"
E_NATURE_SENS = "QV04_NATURE_INCOMPATIBLE_AVEC_LE_SENS"
E_OBJET_REQUIS = "QV05_OBJET_OBLIGATOIRE"
E_MOTIF_REQUIS = "QV06_MOTIF_OBLIGATOIRE"
E_DEJA_AFFECTE = "QV07_MOUVEMENT_DEJA_AFFECTE_INTEGRALEMENT"

MESSAGES = {
    E_INTROUVABLE: "Cette transaction Qonto est introuvable.",
    E_PENDING: ("Cette opération est encore en attente chez Qonto : son montant peut changer ou "
                "ne jamais être débité. Elle ne peut pas être comptabilisée."),
    E_NATURE_INCONNUE: "Cette nature n'est pas reconnue.",
    E_NATURE_SENS: "Cette nature ne correspond pas au sens du mouvement.",
    E_OBJET_REQUIS: "Il faut désigner l'objet concerné (associé, facture, propriétaire).",
    E_MOTIF_REQUIS: "Annuler un rapprochement exige un motif : il reste dans l'historique.",
    E_DEJA_AFFECTE: "Ce mouvement est déjà affecté en totalité.",
}


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str, detail: str = "") -> dict:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def mouvement_opaque(transaction_id: str) -> str:
    """Identifiant de mouvement STABLE, dérivé du `transaction_id` de Qonto.

    Pas de l'UUID technique, et surtout pas du libellé : quand Qonto corrige un intitulé, la ligne
    brute change et son empreinte avec, mais `transaction_id` ne bouge pas. Un rapprochement déjà
    validé reste donc attaché à sa transaction.
    """
    empreinte = hashlib.sha256(f"QONTO|{transaction_id}".encode("utf-8")).hexdigest()
    return f"QMV-{empreinte[:12]}"


def transaction(uuid_transaction: str, *, db_path=None) -> dict | None:
    conn = get_db(db_path)
    try:
        ligne = conn.execute(
            "SELECT t.*, s.nature, s.nature_motif, s.statut_local, s.comptabilisable, "
            "       s.mouvement_id_opaque "
            "  FROM qonto_transactions_raw t "
            "  LEFT JOIN qonto_transactions_statut_local s "
            "    ON s.qonto_transaction_uuid = t.qonto_transaction_uuid "
            " WHERE t.qonto_transaction_uuid = ?", (uuid_transaction,)).fetchone()
        return dict(ligne) if ligne else None
    finally:
        conn.close()


def transaction_par_mouvement(opaque: str, *, db_path=None) -> dict | None:
    """Résout un identifiant de mouvement opaque vers sa transaction.

    C'est cet identifiant-là qui voyage dans les URL : l'UUID technique de Qonto n'a rien à faire
    dans une barre d'adresse, pas plus que le numéro de compte n'a à figurer dans un lien.
    """
    conn = get_db(db_path)
    try:
        ligne = conn.execute(
            "SELECT qonto_transaction_uuid FROM qonto_transactions_statut_local "
            "WHERE mouvement_id_opaque = ?", (opaque,)).fetchone()
    finally:
        conn.close()
    return transaction(ligne[0], db_path=db_path) if ligne else None


def associes(*, db_path=None) -> list[dict]:
    """Référentiel existant (`ref_associes`). Aucun nom n'est deviné ni codé en dur."""
    conn = get_db(db_path)
    try:
        if not conn.execute("SELECT name FROM sqlite_master WHERE type='table' "
                            "AND name='ref_associes'").fetchone():
            return []
        return [{"id": r["personne_id"], "nom": r["nom_personne"]}
                for r in conn.execute(
                    "SELECT personne_id, nom_personne FROM ref_associes "
                    "WHERE actif='OUI' ORDER BY nom_personne")]
    finally:
        conn.close()


def _effets_prevus(nature: str, montant: float, objet_libelle: str) -> list[dict]:
    """Ce qui va se passer, écrit avant que ça se passe. L'utilisateur valide en connaissance."""
    montant = round(float(montant), 2)
    if nature == APPORT_ASSOCIE:
        return [
            {"compte": compta.COMPTE_BANQUE, "libelle": "Banque", "debit": montant, "credit": 0},
            {"compte": compta.COMPTE_ASSOCIES,
             "libelle": f"Compte courant d'associé — {objet_libelle}", "debit": 0, "credit": montant},
        ]
    if nature == TRANSFERT_CAISSE:
        return [
            {"compte": compta.COMPTE_CAISSE, "libelle": "Caisse", "debit": montant, "credit": 0},
            {"compte": compta.COMPTE_BANQUE, "libelle": "Banque", "debit": 0, "credit": montant},
        ]
    if nature == REGLEMENT_CHARGE:
        return [
            {"compte": compta.COMPTE_FOURNISSEURS,
             "libelle": f"Fournisseur — {objet_libelle}", "debit": montant, "credit": 0},
            {"compte": compta.COMPTE_BANQUE, "libelle": "Banque", "debit": 0, "credit": montant},
        ]
    if nature == REVERSEMENT_PROPRIETAIRE:
        return [
            {"compte": compta.COMPTE_BANQUE, "libelle": "Banque", "debit": montant, "credit": 0},
            {"compte": compta.COMPTE_PROPRIETAIRES,
             "libelle": f"Propriétaire — {objet_libelle}", "debit": 0, "credit": montant},
        ]
    return []


def apercu(uuid_transaction: str, *, nature: str = "", objet_id: str = "", montant=None,
           db_path=None) -> dict:
    """Tout ce que l'écran de validation doit montrer AVANT que l'utilisateur tranche."""
    ligne = transaction(uuid_transaction, db_path=db_path)
    if ligne is None:
        return _refus(E_INTROUVABLE, uuid_transaction)

    sens = (ligne.get("sens") or "").lower()
    definitif = (ligne.get("statut") or "").lower() == "completed"
    opaque = ligne.get("mouvement_id_opaque") or mouvement_opaque(ligne["transaction_id"])
    montant_mouvement = abs(float(ligne.get("montant") or 0))

    deja = rappro.montant_deja_rapproche(opaque, db_path)
    restant = round(montant_mouvement - deja, 2)

    # Nature préremplie : celle déduite à l'import, quand elle correspond à une nature validable.
    prefill = {classif.RETRAIT_ESPECES: TRANSFERT_CAISSE,
               classif.APPORT_ASSOCIE: APPORT_ASSOCIE}.get(ligne.get("nature") or "", "")
    nature_choisie = nature or prefill

    objet_libelle = ""
    if nature_choisie == APPORT_ASSOCIE and objet_id:
        objet_libelle = next((a["nom"] for a in associes(db_path=db_path) if a["id"] == objet_id),
                             objet_id)

    montant_affecte = round(float(montant), 2) if montant not in (None, "") else restant

    return {
        "ok": True,
        "uuid": uuid_transaction,
        "mouvement_id_opaque": opaque,
        "definitif": definitif,
        "sens": sens,
        "montant": montant_mouvement,
        "devise": ligne.get("devise") or "EUR",
        "libelle": (ligne.get("contrepartie") or ligne.get("libelle") or "").strip(),
        "date": (ligne.get("regle_le") or ligne.get("emis_le") or "")[:10],
        "statut_qonto": (ligne.get("statut") or "").lower(),
        "nature_deduite": ligne.get("nature") or "",
        "nature_motif": ligne.get("nature_motif") or "",
        "nature_proposee": nature_choisie,
        "natures_possibles": [(n, LIBELLES_NATURE[n]) for n in NATURES_PAR_SENS.get(sens, ())],
        "associes": associes(db_path=db_path),
        "objet_id": objet_id,
        "objet_libelle": objet_libelle,
        "montant_affecte": montant_affecte,
        "deja_affecte": round(deja, 2),
        "restant": restant,
        "effets": _effets_prevus(nature_choisie, montant_affecte, objet_libelle),
        "rapprochements": rappro.lister(opaque, db_path),
        "blocage": (MESSAGES[E_PENDING] if not definitif else
                    MESSAGES[E_DEJA_AFFECTE] if restant <= 0 else ""),
    }


def valider(uuid_transaction: str, *, nature: str, objet_id: str = "", montant=None,
            acteur: str = "", commentaire: str = "", db_path=None) -> dict:
    """Enregistre la décision humaine, puis déclenche les effets canoniques.

    L'ordre compte : le rapprochement est créé CONFIRMÉ d'abord, et c'est LUI qui sert d'origine
    unique à l'écriture. Deux clics produisent donc au pire deux tentatives sur la même origine —
    la seconde est refusée par l'index unique, et la génération d'écriture est de toute façon un
    no-op pour une origine déjà servie.
    """
    ligne = transaction(uuid_transaction, db_path=db_path)
    if ligne is None:
        return _refus(E_INTROUVABLE, uuid_transaction)
    if (ligne.get("statut") or "").lower() != "completed":
        return _refus(E_PENDING)
    if nature not in LIBELLES_NATURE:
        return _refus(E_NATURE_INCONNUE, nature)

    sens = (ligne.get("sens") or "").lower()
    if nature not in NATURES_PAR_SENS.get(sens, ()):
        return _refus(E_NATURE_SENS, f"{nature} / {sens}")
    # Le transfert de caisse est déterministe : il ne se choisit pas à la main sur n'importe quoi.
    if nature == TRANSFERT_CAISSE and ligne.get("nature") != classif.RETRAIT_ESPECES:
        return _refus(E_NATURE_SENS, "seul un retrait identifié par Qonto devient un transfert")
    if nature != TRANSFERT_CAISSE and not objet_id:
        return _refus(E_OBJET_REQUIS, nature)

    opaque = ligne.get("mouvement_id_opaque") or mouvement_opaque(ligne["transaction_id"])
    montant_mouvement = abs(float(ligne.get("montant") or 0))
    deja = rappro.montant_deja_rapproche(opaque, db_path)
    if deja >= montant_mouvement - 1e-9:
        return _refus(E_DEJA_AFFECTE)
    montant_affecte = (round(float(montant), 2) if montant not in (None, "")
                       else round(montant_mouvement - deja, 2))

    # Pour un transfert de caisse, l'« objet » est la transaction elle-même : c'est ce qui rend le
    # rapprochement unique par l'index, et donc non rejouable.
    objet = objet_id or opaque

    lien = rappro.enregistrer(
        opaque, nature, objet, montant_affecte,
        montant_mouvement=montant_mouvement, statut=rappro.ST_CONFIRME, source="MANUEL",
        criteres={"origine": "QONTO", "transaction_id": ligne.get("transaction_id"),
                  "nature_deduite": ligne.get("nature") or "",
                  "nature_motif": ligne.get("nature_motif") or ""},
        commentaire=commentaire, acteur=acteur, db_path=db_path)
    if not lien.get("ok"):
        return lien

    brp = lien["rapprochement_id_opaque"]
    effets = {"rapprochement_id_opaque": brp, "montant_affecte": montant_affecte,
              "montant_restant": lien.get("montant_restant")}

    if nature == APPORT_ASSOCIE:
        effets["ecriture"] = compta.generer_ecriture_apport_associe(
            brp, acteur=acteur, db_path=db_path)
    elif nature == TRANSFERT_CAISSE:
        effets["ecriture"] = compta.generer_ecriture_transfert_caisse(
            brp, acteur=acteur, db_path=db_path)
    elif nature == REGLEMENT_CHARGE:
        # Le règlement fournisseur canonique produit lui-même son effet bancaire : on ne double
        # pas l'écriture ici. Voir la docstring du module.
        effets["reglement"] = _reglement_fournisseur(ligne, objet_id, montant_affecte,
                                                     acteur=acteur, db_path=db_path)
    elif nature == REVERSEMENT_PROPRIETAIRE:
        effets["encaissement"] = _encaissement_proprietaire(ligne, objet_id, montant_affecte,
                                                            acteur=acteur, db_path=db_path)

    _marquer_statut_local(uuid_transaction, opaque, db_path=db_path)
    effets["ok"] = True
    return effets


def _reglement_fournisseur(ligne: dict, facture_opaque: str, montant: float, *, acteur: str,
                           db_path=None) -> dict:
    """Délègue au service canonique, qui met seul à jour le statut de la facture."""
    from app.services import reglements_fournisseurs_service as regl

    conn = get_db(db_path)
    try:
        facture = conn.execute(
            "SELECT fournisseur_id_opaque, statut FROM factures WHERE facture_id_opaque=?",
            (facture_opaque,)).fetchone()
    finally:
        conn.close()
    if facture is None:
        return {"ok": False, "code": "FACTURE_INTROUVABLE", "detail": facture_opaque}
    # Une facture encore à contrôler n'est pas une dette certaine : la régler serait payer un
    # document que personne n'a validé.
    if facture["statut"] == "A_CONTROLER":
        return {"ok": False, "code": "FACTURE_NON_VALIDEE",
                "message": "Cette facture est encore À CONTRÔLER : elle ne peut pas être réglée."}
    return regl.enregistrer(
        facture["fournisseur_id_opaque"],
        [{"facture_id_opaque": facture_opaque, "montant": montant}],
        # « BANQUE » est le moyen canonique du service de règlement : l'argent sort du compte.
        # « VIREMENT » n'existe pas dans sa liste — inventer une valeur la ferait refuser.
        date_reglement=(ligne.get("regle_le") or "")[:10], moyen="BANQUE",
        acteur=acteur, db_path=db_path)


def _encaissement_proprietaire(ligne: dict, proprietaire_id: str, montant: float, *, acteur: str,
                               db_path=None) -> dict:
    """Délègue au service canonique de trésorerie propriétaire.

    L'imputation sur les factures n'est PAS choisie ici : `compte_proprietaire_service` alloue les
    sources en FIFO. Choisir la facture à la main ici créerait une seconde règle d'imputation.
    """
    from app.services import proprietaires_tresorerie_service as tres

    # `ACOMPTE_PROPRIETAIRE` est la nature canonique d'un versement du propriétaire vers la
    # société. Aucune nature nouvelle n'est inventée : la liste de `proprietaires_tresorerie_service`
    # fait foi, et l'imputation sur les factures reste l'affaire de l'allocation FIFO.
    cree = tres.creer(proprietaire_id, "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE", montant,
                      (ligne.get("regle_le") or "")[:10], mode_reglement="BANQUE",
                      reference_metier=ligne.get("transaction_id") or "",
                      source_type="QONTO", acteur=acteur, db_path=db_path)
    if not cree.get("ok"):
        return cree
    return tres.valider(cree["mouvement_opaque"], acteur=acteur, db_path=db_path)


def _marquer_statut_local(uuid_transaction: str, opaque: str, *, db_path=None) -> None:
    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE qonto_transactions_statut_local SET statut_local=?, mouvement_id_opaque=?, "
            "maj_le=? WHERE qonto_transaction_uuid=?",
            (classif.RAPPROCHE, opaque, _maintenant(), uuid_transaction))
        conn.commit()
    finally:
        conn.close()


def annuler(rapprochement_id_opaque: str, *, motif: str, acteur: str = "", db_path=None) -> dict:
    """Corrige une erreur humaine SANS effacer l'histoire.

    Le rapprochement passe à ANNULE (il reste en base, avec son journal), et l'écriture éventuelle
    est CONTREPASSÉE par une écriture miroir — jamais supprimée. Une comptabilité dont on peut
    effacer une ligne ne prouve plus rien.
    """
    if not (motif or "").strip():
        return _refus(E_MOTIF_REQUIS)

    ecriture = compta.charger_par_origine("RAPPROCHEMENT", rapprochement_id_opaque, db_path)
    resultat = {"ok": True, "rapprochement_id_opaque": rapprochement_id_opaque}

    if ecriture:
        contrepassee = compta.contrepasser(ecriture["ecriture_id_opaque"], commentaire=motif,
                                           acteur=acteur, db_path=db_path)
        resultat["contrepassation"] = contrepassee
        if not contrepassee.get("ok"):
            return contrepassee

    resultat["rapprochement"] = rappro.annuler(rapprochement_id_opaque, commentaire=motif,
                                               acteur=acteur, db_path=db_path)
    _rendre_le_mouvement_a_traiter(rapprochement_id_opaque, db_path=db_path)
    return resultat


def _rendre_le_mouvement_a_traiter(rapprochement_id_opaque: str, *, db_path=None) -> None:
    """Après annulation, la transaction redevient à traiter — sinon elle resterait « rapprochée »
    alors que plus rien ne la rapproche."""
    conn = get_db(db_path)
    try:
        rap = conn.execute(
            "SELECT mouvement_id_opaque FROM banque_rapprochements WHERE rapprochement_id_opaque=?",
            (rapprochement_id_opaque,)).fetchone()
        if rap:
            conn.execute(
                "UPDATE qonto_transactions_statut_local SET statut_local=?, maj_le=? "
                "WHERE mouvement_id_opaque=?",
                (classif.A_RAPPROCHER, _maintenant(), rap["mouvement_id_opaque"]))
            conn.commit()
    finally:
        conn.close()


def confirmer_transferts_caisse(*, acteur: str = "AUTOMATIQUE", db_path=None) -> dict:
    """Comptabilise les retraits d'espèces CONFIRMÉS — le seul automatisme autorisé.

    Un retrait au distributeur ne demande aucune décision : l'argent passe du compte au tiroir.
    Tant qu'il est provisoire, rien n'est écrit. Idempotent de bout en bout : le rapprochement est
    unique par l'index, l'écriture l'est par son origine.
    """
    bilan = {"comptabilises": 0, "ignores": 0}
    for transfert in transferts.lister(db_path=db_path):
        if transfert.get("etat") != transferts.CONFIRME:
            bilan["ignores"] += 1
            continue
        resultat = valider(transfert["qonto_transaction_uuid"], nature=TRANSFERT_CAISSE,
                           montant=transfert["montant"], acteur=acteur,
                           commentaire="Retrait d'espèces confirmé par la banque", db_path=db_path)
        if resultat.get("ok"):
            bilan["comptabilises"] += 1
        else:
            bilan["ignores"] += 1
    return bilan


def apercu_par_mouvement(opaque: str, **kwargs) -> dict:
    """Même aperçu, désigné par l'identifiant de mouvement — celui qui circule dans les URL."""
    db_path = kwargs.get("db_path")
    ligne = transaction_par_mouvement(opaque, db_path=db_path)
    if ligne is None:
        return _refus(E_INTROUVABLE, opaque)
    return apercu(ligne["qonto_transaction_uuid"], **kwargs)


def valider_par_mouvement(opaque: str, **kwargs) -> dict:
    db_path = kwargs.get("db_path")
    ligne = transaction_par_mouvement(opaque, db_path=db_path)
    if ligne is None:
        return _refus(E_INTROUVABLE, opaque)
    return valider(ligne["qonto_transaction_uuid"], **kwargs)
