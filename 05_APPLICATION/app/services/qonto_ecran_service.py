"""Ce que l'écran Banque & Caisse montre — et, tout aussi important, ce qu'il ne montre pas.

Ce module transforme la couche RAW en lignes lisibles. Il est la SEULE porte entre les tables
`qonto_*` et un gabarit : aucune vue ne lit la base directement, ce qui rend vérifiable ce qui sort.

NE SORTENT JAMAIS D'ICI :
  · `QONTO_SECRET_KEY`, l'en-tête `Authorization`, quoi que ce soit d'authentification — rien de
    tout cela n'existe en base, et rien ici ne va le chercher ;
  · l'IBAN complet — masqué en `FR76 **** **** 1234` ;
  · `charge_utile_json`, l'empreinte, les identifiants de synchronisation : de la plomberie ;
  · les identifiants techniques (UUID, `transaction_id`) dès lors qu'une information humaine
    existe. Une contrepartie et un libellé disent ce qu'est un mouvement ; un UUID ne dit rien.

TROIS SOLDES, TROIS SENS DIFFÉRENTS — les confondre serait une faute :

  · SOLDE BANQUE       = `authorized_balance` chez Qonto. C'est ce dont on dispose réellement :
                         le solde comptable diminué des opérations autorisées mais pas encore
                         réglées. C'est le chiffre qu'on regarde avant de payer quelque chose.
  · Solde comptable    = `balance`. Ce que la banque a enregistré, sans tenir compte de ce qui
                         est déjà engagé. Conservé en détail, jamais mis en avant : l'afficher
                         comme indicateur principal ferait croire à 200 € disponibles quand 20 €
                         sont déjà partis.
  · TRÉSORERIE DISPO.  = solde banque disponible + encaisse. Ce qu'on a, tous contenants confondus.

AUCUN EFFET COMPTABLE. Ce module lit.
"""
from __future__ import annotations

from app.adapters import qonto_client
from app.db.connection import get_db
from app.services import caisse_transferts_service as transferts
from app.services import qonto_classification_service as classif
from app.services import qonto_raw_service as raw
from app.services import qonto_statut_local_service as statut_local
from app.services import qonto_suggestions_service as suggestions

FILTRE_A_TRAITER = "A_TRAITER"

VUE_BANQUE = "banque"
VUE_CAISSE = "caisse"

STATUTS_QONTO = {
    "completed": "Comptabilisé",
    "pending": "En attente",
    "declined": "Refusé",
    "reversed": "Contrepassé",
}

TYPES_QONTO = {
    "transfer": "Virement",
    "card": "Carte",
    "direct_debit": "Prélèvement",
    "income": "Encaissement",
    "qonto_fee": "Frais Qonto",
    "cheque": "Chèque",
    "recall": "Rappel de fonds",
    "swift_income": "Encaissement international",
}

SENS = {"credit": "Crédit", "debit": "Débit"}


def _date_courte(valeur) -> str:
    """`2026-09-11T08:00:00.000Z` → `11/09/2026`. Absente → tiret, jamais une date inventée."""
    if not valeur:
        return "—"
    morceaux = str(valeur)[:10].split("-")
    if len(morceaux) != 3:
        return str(valeur)[:10]
    annee, mois, jour = morceaux
    return f"{jour}/{mois}/{annee}"


def _horodatage_lisible(valeur) -> str:
    if not valeur:
        return "jamais"
    texte = str(valeur)
    heure = texte[11:16] if len(texte) >= 16 else ""
    return f"{_date_courte(texte)} à {heure}" if heure else _date_courte(texte)


def _reference_utile(mouvement: dict) -> str:
    """La référence n'est montrée que si elle APPREND quelque chose.

    Qonto recopie souvent le libellé dans `reference` : l'afficher deux fois n'informe personne.
    Une note saisie à la main, elle, est de l'information humaine et prend le pas.
    """
    libelle = (mouvement.get("libelle") or "").strip()
    for champ in ("note", "reference"):
        valeur = (mouvement.get(champ) or "").strip()
        if valeur and valeur.lower() != libelle.lower():
            return valeur
    return ""


def _mois_de(mouvement: dict) -> str:
    brut = mouvement.get("regle_le") or mouvement.get("emis_le") or ""
    return str(brut)[:7]


def ligne_transaction(mouvement: dict, suggestion: dict | None = None) -> dict:
    """Une ligne prête à afficher. Ne contient aucun identifiant technique."""
    suggestion = suggestion or {"niveau": suggestions.AUCUN, "candidats": [], "message": ""}
    statut_qonto = (mouvement.get("statut") or "").lower()
    nature = mouvement.get("nature") or classif.INCONNU
    traitement = classif.determiner_traitement(mouvement, suggestion["niveau"])
    rapprochements = _rapprochements(mouvement)
    return {
        # L'identifiant qui sort d'ici est le NÔTRE (opaque et stable), jamais celui de Qonto.
        "mouvement_id": mouvement.get("mouvement_id_opaque") or "",
        "rapprochements": rapprochements,
        # Pour une opération en attente, Qonto n'a pas de date de règlement : on montre la date
        # d'émission EN LE DISANT, plutôt qu'une case vide ou une date qui ferait croire au règlement.
        "date": _date_courte(mouvement.get("regle_le") or mouvement.get("emis_le")),
        "date_est_prevue": not mouvement.get("regle_le"),
        "mois": _mois_de(mouvement),
        "montant": mouvement.get("montant"),
        "devise": mouvement.get("devise") or "EUR",
        "sens": SENS.get((mouvement.get("sens") or "").lower(), mouvement.get("sens") or "—"),
        "sens_code": (mouvement.get("sens") or "").lower(),
        "statut_qonto": STATUTS_QONTO.get(statut_qonto, mouvement.get("statut") or "—"),
        "statut_qonto_code": statut_qonto,
        "type": TYPES_QONTO.get((mouvement.get("type_operation") or "").lower(),
                                mouvement.get("type_operation") or "—"),
        "libelle": (mouvement.get("libelle") or "").strip() or "(sans libellé)",
        "contrepartie": (mouvement.get("contrepartie") or "").strip(),
        "reference": _reference_utile(mouvement),
        "nature": nature,
        "nature_libelle": classif.LIBELLES_NATURE.get(nature, nature),
        "nature_motif": mouvement.get("nature_motif") or "",
        "traitement": traitement,
        "traitement_libelle": _libelle_traitement(traitement, rapprochements),
        "definitif": bool(mouvement.get("comptabilisable")),
        "motif_non_definitif": mouvement.get("motif_non_comptabilisable") or "",
        "suggestion": suggestion,
    }


def _libelle_traitement(traitement: str, rapprochements: list[dict]) -> str:
    """« Rapproché — écriture à valider » plutôt que « Rapproché » tout court.

    Un rapprochement confirmé dont l'écriture dort en PROPOSEE n'est pas une affaire close : le
    dire évite qu'on referme l'écran en croyant le travail fini.
    """
    base = classif.LIBELLES_TRAITEMENT.get(traitement, traitement)
    if traitement != classif.RAPPROCHE:
        return base
    etats = {r.get("etat_comptable") for r in rapprochements}
    for etat in (ETAT_ECRITURE_A_VALIDER, ETAT_COMPTABILISE):
        if etat in etats:
            return f"{base} — {SUFFIXES_COMPTABLES[etat]}"
    return base


def _rapprochements(mouvement: dict) -> list[dict]:
    """À quoi ce mouvement est-il rattaché ? Répondu par le journal canonique, jamais déduit."""
    from app.services import banques_rapprochement_service as rappro
    opaque = mouvement.get("mouvement_id_opaque")
    if not opaque:
        return []
    db_path = mouvement.get("_db_path")
    actifs = [r for r in rappro.lister(opaque, db_path)
              if r.get("statut") in (rappro.ST_PROPOSE, rappro.ST_CONFIRME)]
    noms = _noms_objets(actifs, db_path)
    etats = _etats_comptables(actifs, db_path)
    return [{"type": r["type_objet"], "objet_id": r["objet_id"],
             "etat_comptable": etats.get(r["rapprochement_id_opaque"], ETAT_SANS_ECRITURE),
             # Ce que l'écran montre : « Apport compte courant — Ewan », pas
             # « APPORT_ASSOCIE — PERS_EWAN ». Un code et un identifiant technique ne disent rien
             # à qui relit son relevé ; le nom de la personne, si.
             "libelle": _libelle_rapprochement(r, noms),
             "montant": r["montant_rapproche"], "statut": r["statut"],
             "id": r["rapprochement_id_opaque"]} for r in actifs]


#: Formulation courte de chaque nature, telle qu'elle se lit dans une liste de mouvements.
# Un rapprochement confirmé ne veut pas dire « comptabilisé » : l'écriture naît PROPOSEE et
# attend la validation du workflow comptable. Afficher « Rapproché » tout court laisserait croire
# que le travail est fini alors qu'une écriture dort en attente de contrôle.
ETAT_SANS_ECRITURE = "SANS_ECRITURE"
ETAT_ECRITURE_A_VALIDER = "ECRITURE_A_VALIDER"
ETAT_COMPTABILISE = "COMPTABILISE"

SUFFIXES_COMPTABLES = {
    ETAT_ECRITURE_A_VALIDER: "écriture à valider",
    ETAT_COMPTABILISE: "comptabilisé",
}


def _etats_comptables(rapprochements: list, db_path=None) -> dict:
    """État de l'écriture attachée à chaque rapprochement, lu dans le journal comptable.

    Rien n'est déduit ni recalculé : c'est le statut que porte l'écriture. Une écriture
    contrepassée n'est pas un état d'avancement — le rapprochement correspondant est annulé, donc
    absent de cette liste.
    """
    if not rapprochements:
        return {}
    conn = get_db(db_path)
    try:
        if not conn.execute("SELECT name FROM sqlite_master WHERE type='table' "
                            "AND name='ecritures'").fetchone():
            return {}
        identifiants = [r["rapprochement_id_opaque"] for r in rapprochements]
        marques = ",".join("?" * len(identifiants))
        etats = {}
        for r in conn.execute(
                f"SELECT origine_id_opaque, statut FROM ecritures "
                f"WHERE origine_type='RAPPROCHEMENT' AND origine_id_opaque IN ({marques}) "
                f"AND statut <> 'CONTREPASSEE'", identifiants):
            etats[r["origine_id_opaque"]] = (ETAT_COMPTABILISE if r["statut"] == "VALIDEE"
                                             else ETAT_ECRITURE_A_VALIDER)
        return etats
    finally:
        conn.close()


LIBELLES_RAPPROCHEMENT = {
    "APPORT_ASSOCIE": "Apport compte courant",
    "TRANSFERT_CAISSE": "Transfert Banque → Caisse",
    "REGLEMENT_CHARGE": "Facture fournisseur",
    "REVERSEMENT_PROPRIETAIRE": "Encaissement propriétaire",
}


def _noms_objets(rapprochements: list, db_path=None) -> dict:
    """Nom lisible des objets rapprochés. Une table absente fait perdre un nom, jamais la page."""
    noms: dict[str, str] = {}
    if not rapprochements:
        return noms
    conn = get_db(db_path)
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "ref_associes" in tables:
            for r in conn.execute("SELECT personne_id, nom_personne FROM ref_associes"):
                noms[r["personne_id"]] = r["nom_personne"] or ""
        if "factures" in tables:
            for r in conn.execute("SELECT facture_id_opaque, facture_ref FROM factures"):
                noms[r["facture_id_opaque"]] = r["facture_ref"] or ""
    finally:
        conn.close()
    return noms


def _libelle_rapprochement(rapprochement: dict, noms: dict) -> str:
    nature = LIBELLES_RAPPROCHEMENT.get(rapprochement["type_objet"], rapprochement["type_objet"])
    objet = rapprochement.get("objet_id") or ""
    # Un transfert de caisse a pour « objet » le mouvement lui-même : l'afficher n'apprendrait rien.
    if not objet or objet.startswith("QMV-"):
        return nature
    return f"{nature} — {noms.get(objet) or objet}"


def _mouvements(db_path=None) -> list[dict]:
    """Jointure RAW ↔ statut applicatif, sans faire remonter les colonnes de plomberie."""
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT t.qonto_transaction_uuid, t.transaction_id, t.montant, t.devise, t.sens, "
            "       t.statut, t.type_operation, t.libelle, t.reference, t.note, t.contrepartie, "
            "       t.emis_le, t.regle_le, t.categorie, t.categorie_flux, s.statut_local, "
            "       s.comptabilisable, s.motif_non_comptabilisable, s.nature, s.nature_motif, "
            "       s.mouvement_id_opaque "
            "  FROM qonto_transactions_raw t "
            "  LEFT JOIN qonto_transactions_statut_local s "
            "    ON s.qonto_transaction_uuid = t.qonto_transaction_uuid "
            " ORDER BY COALESCE(t.regle_le, t.emis_le, t.cree_le) DESC")]
    finally:
        conn.close()


def _lignes_avec_suggestions(db_path=None) -> list[dict]:
    """Les documents candidats sont chargés UNE fois pour toute la liste, pas une fois par ligne."""
    mouvements = _mouvements(db_path)
    # La base visée voyage AVEC la ligne : sans cela, la lecture des rapprochements repartirait
    # sur la base par défaut et un test isolé lirait la vraie base.
    for mouvement in mouvements:
        mouvement["_db_path"] = db_path
    noms = suggestions._noms_tiers(db_path)
    cache_documents: dict[str, list] = {}
    lignes = []
    for mouvement in mouvements:
        # Un retrait d'espèces est traité automatiquement : lui chercher une facture n'aurait pas
        # de sens et encombrerait l'écran d'un faux choix.
        if mouvement.get("nature") == classif.RETRAIT_ESPECES:
            lignes.append(ligne_transaction(mouvement))
            continue
        sens = (mouvement.get("sens") or "").lower()
        if sens not in cache_documents:
            cache_documents[sens] = suggestions._documents(sens, db_path)
        lignes.append(ligne_transaction(
            mouvement,
            suggestions.suggerer(mouvement, documents=cache_documents[sens], noms=noms)))
    return lignes


def mois_disponibles(lignes: list[dict]) -> list[str]:
    return sorted({ligne["mois"] for ligne in lignes if ligne["mois"]}, reverse=True)


def _filtrer(lignes: list[dict], mois: str, traitement: str) -> list[dict]:
    """Les deux filtres se combinent : ils restreignent, ils ne se remplacent pas."""
    retenues = lignes
    if mois:
        retenues = [ligne for ligne in retenues if ligne["mois"] == mois]
    if traitement == FILTRE_A_TRAITER:
        # Liste de travail : tout ce qui attend une décision, sans avoir à cocher trois cases.
        retenues = [l for l in retenues if l["traitement"] in classif.A_TRAITER]
    elif traitement:
        retenues = [ligne for ligne in retenues if ligne["traitement"] == traitement]
    return retenues


def soldes(*, db_path=None) -> dict:
    """Les trois soldes, nommés sans ambiguïté."""
    comptes = raw.comptes(db_path=db_path)
    disponible = sum((c.get("solde_autorise") or 0) for c in comptes)
    comptable = sum((c.get("solde") or 0) for c in comptes)
    encaisse = encaisse_caisse(db_path=db_path)
    return {
        "banque": round(disponible, 2),
        "banque_comptable": round(comptable, 2),
        "engage": round(comptable - disponible, 2),
        "caisse": encaisse["solde"],
        "tresorerie": round(disponible + encaisse["solde"], 2),
        "devise": comptes[0]["devise"] if comptes else "EUR",
    }


def encaisse_caisse(*, db_path=None) -> dict:
    """Encaisse = caisse métier existante + retraits bancaires CONFIRMÉS.

    Les retraits provisoires sont comptés à part : tant que la banque n'a pas tranché, les faire
    entrer dans l'encaisse créerait de l'argent qui n'existe pas encore.
    """
    from app.services import operations_caisse_service as caisse

    try:
        operations = caisse.solde(db_path=db_path)
        solde_operations = float(operations.get("solde") or 0)
    except Exception:
        # La caisse métier peut être indisponible (base non migrée, module inerte) : la vue
        # bancaire ne doit pas tomber pour autant.
        solde_operations = 0.0
    totaux = transferts.totaux(db_path=db_path)
    return {
        "solde": round(solde_operations + totaux["confirme"], 2),
        "operations": round(solde_operations, 2),
        "retraits_confirmes": totaux["confirme"],
        "retraits_provisoires": totaux["provisoire"],
        "nb_retraits_confirmes": totaux["nb_confirmes"],
        "nb_retraits_provisoires": totaux["nb_provisoires"],
    }


def mouvements_caisse(*, db_path=None) -> list[dict]:
    """Ce que la vue Caisse affiche : origine tracée jusqu'à la transaction bancaire."""
    lignes = []
    for transfert in transferts.lister(db_path=db_path):
        lignes.append({
            "date": _date_courte(transfert.get("date_operation")),
            "libelle": "Retrait d'espèces au distributeur",
            "origine": f"Retrait bancaire Qonto — {transfert.get('libelle_source') or ''}".strip(" —"),
            "sens": "Entrée",
            "montant": transfert.get("montant"),
            "devise": transfert.get("devise") or "EUR",
            "etat": transfert.get("etat"),
            "etat_libelle": transferts.LIBELLES_ETAT.get(transfert.get("etat"),
                                                         transfert.get("etat")),
            "definitif": transfert.get("etat") == transferts.CONFIRME,
            "motif": transfert.get("motif_etat") or "",
        })
    return lignes


def tableau_de_bord(*, vue: str = VUE_BANQUE, mois: str = "", traitement: str = "",
                    db_path=None) -> dict:
    """Tout ce dont l'écran a besoin, déjà formaté, déjà masqué, déjà filtré."""
    if not raw.tables_presentes(db_path=db_path):
        return {"disponible": False, "vue": vue,
                "message": "Tables Qonto absentes : redémarrez l'application pour migrer la base."}

    toutes = _lignes_avec_suggestions(db_path)
    lignes = _filtrer(toutes, mois, traitement)
    valeurs = soldes(db_path=db_path)
    encaisse = encaisse_caisse(db_path=db_path)
    derniere = raw.derniere_synchronisation(db_path=db_path)

    compteurs = {code: sum(1 for ligne in toutes if ligne["traitement"] == code)
                 for code in classif.TRAITEMENTS}

    return {
        "disponible": True,
        "vue": vue if vue in (VUE_BANQUE, VUE_CAISSE) else VUE_BANQUE,
        "identifiants_presents": qonto_client.identifiants_presents(),
        "soldes": valeurs,
        "encaisse": encaisse,
        "comptes": raw.comptes(db_path=db_path),
        "transactions": lignes,
        "nb_total": len(toutes),
        "mouvements_caisse": mouvements_caisse(db_path=db_path),
        "compteurs": compteurs,
        "a_controler": compteurs[classif.A_CONTROLER],
        "a_rapprocher": compteurs[classif.A_RAPPROCHER],
        "en_attente": compteurs[classif.EN_ATTENTE_QONTO],
        "filtres": {
            "mois": mois, "traitement": traitement,
            "mois_disponibles": mois_disponibles(toutes),
            "traitements": ([(FILTRE_A_TRAITER,
                              f"À traiter ({sum(compteurs[c] for c in classif.A_TRAITER)})")]
                            + [(code, classif.LIBELLES_TRAITEMENT[code])
                               for code in classif.TRAITEMENTS]),
        },
        "derniere_synchronisation": {
            "horodatage": _horodatage_lisible(derniere.get("termine_le")
                                              or derniere.get("demarre_le")),
            "statut": derniere.get("statut") or "",
        } if derniere else {},
    }


def actualiser(*, client=None, db_path=None) -> dict:
    """Enchaîne la synchronisation complète : RAW → statuts → natures → transferts caisse.

    Un seul point d'entrée pour le bouton, pour que l'écran ne puisse pas afficher des soldes
    frais avec des natures périmées.
    """
    from app.services import qonto_sync_service as sync

    resultat = sync.synchroniser(client=client, db_path=db_path)
    if not resultat.get("ok"):
        return resultat
    statut_local.synchroniser(db_path=db_path)
    classif.synchroniser(db_path=db_path)
    transferts.synchroniser(db_path=db_path)
    return resultat
