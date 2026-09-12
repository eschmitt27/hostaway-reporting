"""Correspondances logement — le PARCOURS DÉDIÉ (§18), à la place de l'édition d'une table.

POURQUOI CE PARCOURS EXISTE
Une ligne de `ref_mapping_logements` répond à une question simple — « ce libellé venu de
l'extérieur, c'est quel logement ? » — mais l'éditer à la main oblige à en connaître quatre
autres : la source, le champ source, la valeur exacte, et l'identifiant technique du logement.
Personne ne saisit cela sans risque, et surtout : une ligne modifiée à la main n'enregistre ni QUI
a tranché, ni CONTRE QUELLE proposition. Une correspondance est une décision, pas une donnée.

Ce service pose donc la seule question qui compte, avec sous les yeux :
  — la valeur telle que la source l'écrit ;
  — le logement actuellement rattaché, s'il y en a un ;
  — le logement que le moteur propose, avec son degré de certitude ;
  — le choix humain, tracé dans `ref_admin_evenements`.

CE QU'IL COUVRE, ET QUI NE SE VOYAIT NULLE PART
Les correspondances DÉCLARÉES sont faciles à lister : elles sont en base. Les correspondances
MANQUANTES ne le sont pas — ce sont des libellés qu'une source a produits et qu'aucune ligne ne
rattache. Elles ne se voyaient qu'au moment où un moteur s'arrêtait dessus, en langage de moteur.
`a_traiter()` les remonte à l'endroit où on peut les régler :

  — une annonce Hostaway présente dans la dernière extraction, absente du parc (cas réel : le
    listing 590757 « 4 rue engalière », apparu avec quatre réservations de juillet-août que rien
    ne rattachait à un logement) ;
  — un libellé de facture de ménage externe qu'aucune correspondance ne reconnaît.

AUCUNE CORRESPONDANCE N'EST DEVINÉE
Le moteur PROPOSE (`logement_matching_service`), il ne décide jamais. Une proposition certaine est
présentée cochée ; elle attend quand même une validation. Et `LOGEMENT_DIVERS` n'est jamais offert
comme issue : masquer un mauvais rattachement derrière un logement fourre-tout est explicitement
interdit, et c'est très exactement ce que ce parcours doit rendre inutile.
"""
from __future__ import annotations

import json
from typing import Any

from app.db.connection import get_db

TABLE = "ref_mapping_logements"

SOURCE_HOSTAWAY = "Hostaway"
CHAMP_LISTING = "listingMapId"
CHAMP_NOM_LISTING = "listingName"
CHAMP_LIBELLE_FACTURE = "libelle_logement_source"

ORIGINE_HOSTAWAY = "HOSTAWAY_LISTING"
ORIGINE_FACTURE = "FACTURE_MENAGE"

ACTION_DECLARATION = "CORRESPONDANCE_DECLAREE"
ACTION_CORRECTION = "CORRESPONDANCE_CORRIGEE"
ACTION_DESACTIVATION = "CORRESPONDANCE_DESACTIVEE"

# Logements techniques : ils existent pour recevoir ce qui n'a pas de place, jamais pour servir de
# réponse à « quel logement ? ». Les proposer ici transformerait le parcours censé supprimer les
# mauvais rattachements en machine à en fabriquer.
LOGEMENTS_TECHNIQUES = ("LOGEMENT_DIVERS", "APPARTEMENT_DIVERS")

E_VALEUR_MANQUANTE = "CORRESPONDANCE_VALEUR_MANQUANTE"
E_LOGEMENT_INCONNU = "CORRESPONDANCE_LOGEMENT_INCONNU"
E_LOGEMENT_TECHNIQUE = "CORRESPONDANCE_LOGEMENT_TECHNIQUE"
E_INTROUVABLE = "CORRESPONDANCE_INTROUVABLE"
E_SANS_CHANGEMENT = "CORRESPONDANCE_SANS_CHANGEMENT"

MESSAGES = {
    E_VALEUR_MANQUANTE: "Indiquez la valeur source et le logement à rattacher.",
    E_LOGEMENT_INCONNU: "Ce logement n'existe pas dans le parc.",
    E_LOGEMENT_TECHNIQUE: ("Un logement technique (« divers ») ne peut pas servir de "
                           "correspondance : il masquerait le rattachement au lieu de le régler."),
    E_INTROUVABLE: "Cette correspondance n'existe pas.",
    E_SANS_CHANGEMENT: "Ce libellé est déjà rattaché à ce logement : rien à corriger.",
}

_COLS = ("mapping_logement_id", "source", "champ_source", "valeur_source", "logement_id",
         "niveau_confiance", "actif", "commentaire")


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _logements(db_path=None) -> list[dict[str, Any]]:
    """Parc réel, sans les logements techniques : ce sont les seules réponses recevables."""
    conn = get_db(db_path)
    try:
        return [{"logement_id": r["logement_id"], "nom_court": r["nom_court"],
                 "nom_officiel": r["nom_logement_officiel"], "ville": r["ville"],
                 "statut_parc": r["statut_parc"], "actif": r["actif"],
                 "hostaway_listing_id": _txt(r["hostaway_listing_id"])}
                for r in conn.execute(
                    "SELECT logement_id, nom_court, nom_logement_officiel, ville, statut_parc, "
                    "actif, hostaway_listing_id FROM ref_logements "
                    "WHERE logement_id NOT IN (?, ?) ORDER BY logement_id",
                    LOGEMENTS_TECHNIQUES)]
    finally:
        conn.close()


def logements_selectionnables(db_path=None) -> list[dict[str, Any]]:
    """Les choix offerts à l'écran.

    Un logement RETIRÉ du parc reste proposé : une correspondance porte souvent sur des mois
    passés, où ce logement était géré. L'exclure obligerait à laisser le rattachement faux.
    """
    return _logements(db_path=db_path)


# ── Inventaire ──────────────────────────────────────────────────────────────────────────────────

def declarees(*, source: str = "", db_path=None) -> list[dict[str, Any]]:
    """Correspondances existantes, avec le nom du logement rattaché plutôt que son identifiant."""
    noms = {l["logement_id"]: (l["nom_court"] or l["nom_officiel"] or l["logement_id"])
            for l in _logements(db_path=db_path)}
    conn = get_db(db_path)
    try:
        where, params = "", []
        if source:
            where, params = " WHERE source = ?", [source]
        lignes = [dict(zip(_COLS, r)) for r in conn.execute(
            f"SELECT {', '.join(_COLS)} FROM {TABLE}{where} "
            "ORDER BY source, champ_source, valeur_source", params)]
    finally:
        conn.close()
    for ligne in lignes:
        ligne["logement_libelle"] = noms.get(ligne["logement_id"], ligne["logement_id"])
    return lignes


def _correspondances_connues(db_path=None) -> dict[tuple[str, str], str]:
    conn = get_db(db_path)
    try:
        return {(_txt(r["champ_source"]), _txt(r["valeur_source"])): _txt(r["logement_id"])
                for r in conn.execute(
                    f"SELECT champ_source, valeur_source, logement_id FROM {TABLE} "
                    "WHERE actif = 'OUI'")}
    finally:
        conn.close()


def _listings_hostaway_non_rattaches(db_path=None) -> list[dict[str, Any]]:
    """Annonces vues dans la DERNIÈRE extraction et rattachées à aucun logement.

    C'est le point aveugle que le parcours comble : tant que personne ne regardait, une annonce
    nouvelle ne se signalait qu'en faisant échouer le calcul des réservations, avec un message de
    moteur (« LOGEMENT_NON_MAPPE — listingMapId=… ») et aucun endroit où répondre.
    """
    from app.services import hostaway_raw_service as raw

    if not raw.table_presente("hostaway_reservations", db_path=db_path):
        return []
    extraction = raw.derniere_extraction_utilisable(db_path=db_path)
    if not extraction:
        return []

    connues = _correspondances_connues(db_path=db_path)
    conn = get_db(db_path)
    try:
        # Le parc porte aussi l'identifiant Hostaway en direct sur la fiche logement : une annonce
        # rattachée par ce biais n'est pas orpheline, même sans ligne de correspondance.
        par_fiche = {_txt(r["hostaway_listing_id"]) for r in conn.execute(
            "SELECT hostaway_listing_id FROM ref_logements "
            "WHERE hostaway_listing_id IS NOT NULL AND TRIM(hostaway_listing_id) <> ''")}
        vues = list(conn.execute(
            "SELECT listing_map_id, COUNT(*) n, MIN(check_in_date) d1, MAX(check_in_date) d2 "
            "FROM hostaway_reservations WHERE extraction_id = ? AND listing_map_id IS NOT NULL "
            "GROUP BY listing_map_id ORDER BY listing_map_id", (extraction,)))
        noms = {}
        for r in conn.execute(
                "SELECT listing_map_id, nom_listing FROM hostaway_listings WHERE extraction_id = ?",
                (extraction,)):
            noms[_txt(r["listing_map_id"])] = _txt(r["nom_listing"])
    finally:
        conn.close()

    manquantes = []
    for r in vues:
        identifiant = _txt(r["listing_map_id"])
        if not identifiant or identifiant in par_fiche:
            continue
        if (CHAMP_LISTING, identifiant) in connues:
            continue
        manquantes.append({
            "origine": ORIGINE_HOSTAWAY,
            "source": SOURCE_HOSTAWAY,
            "champ_source": CHAMP_LISTING,
            "valeur_source": identifiant,
            "libelle": noms.get(identifiant, ""),
            "nb_reservations": int(r["n"] or 0),
            "premiere_arrivee": _txt(r["d1"]),
            "derniere_arrivee": _txt(r["d2"]),
            "logement_actuel": "",
            "consequence": ("Le calcul des réservations s'arrête tant que cette annonce n'est pas "
                            "rattachée : ses séjours ne sont ni facturés ni suivis."),
        })
    return manquantes


def _libelles_menage_non_rattaches(db_path=None) -> list[dict[str, Any]]:
    """Libellés de logement lus sur des factures de ménage externes, sans correspondance."""
    conn = get_db(db_path)
    try:
        if not conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='facture_lignes'"
        ).fetchone():
            return []
        colonnes = {r[1] for r in conn.execute("PRAGMA table_info(facture_lignes)")}
        if "libelle_logement_source" not in colonnes:
            return []
        connues = _correspondances_connues(db_path=db_path)
        vues = list(conn.execute(
            "SELECT libelle_logement_source v, COUNT(*) n FROM facture_lignes "
            "WHERE libelle_logement_source IS NOT NULL AND TRIM(libelle_logement_source) <> '' "
            "AND COALESCE(logement_id, '') = '' GROUP BY libelle_logement_source ORDER BY v"))
    finally:
        conn.close()

    return [{
        "origine": ORIGINE_FACTURE,
        "source": "Facture ménage externe",
        "champ_source": CHAMP_LIBELLE_FACTURE,
        "valeur_source": _txt(r["v"]),
        "libelle": _txt(r["v"]),
        "nb_lignes": int(r["n"] or 0),
        "logement_actuel": "",
        "consequence": ("Ces lignes de facture ne sont imputées à aucun logement : leur coût "
                        "n'entre dans aucun calcul."),
    } for r in vues if (CHAMP_LIBELLE_FACTURE, _txt(r["v"])) not in connues]


def a_traiter(*, db_path=None) -> list[dict[str, Any]]:
    """Ce qui attend une décision humaine, avec la proposition du moteur pour chacun.

    L'ordre place en tête ce qui bloque un calcul : une annonce Hostaway non rattachée arrête la
    chaîne des réservations, là où un libellé de facture n'empêche que l'imputation d'une ligne.
    """
    from app.services import logement_matching_service as lms

    referentiel = lms.charger_referentiel(db_path)
    elements = _listings_hostaway_non_rattaches(db_path=db_path) \
        + _libelles_menage_non_rattaches(db_path=db_path)
    for element in elements:
        libelle = element.get("libelle") or element.get("valeur_source") or ""
        proposition = lms.proposer(libelle, referentiel) if libelle else {}
        element["proposition"] = proposition
        element["propose_logement_id"] = (proposition.get("logement") or {}).get("logement_id", "") \
            if proposition.get("logement") else ""
        element["propose_libelle"] = (proposition.get("logement") or {}).get("libelle", "") \
            if proposition.get("logement") else ""
        element["confiance"] = proposition.get("confiance", "")
    return elements


def compte_a_traiter(*, db_path=None) -> int:
    try:
        return len(a_traiter(db_path=db_path))
    except Exception:      # noqa: BLE001 — un compteur d'écran ne doit jamais faire tomber la page
        return 0


# ── Décision ────────────────────────────────────────────────────────────────────────────────────

def _verifier_logement(conn, logement_id: str) -> dict[str, Any] | None:
    if logement_id in LOGEMENTS_TECHNIQUES:
        return {"ok": False, "code": E_LOGEMENT_TECHNIQUE,
                "message": MESSAGES[E_LOGEMENT_TECHNIQUE]}
    if conn.execute("SELECT 1 FROM ref_logements WHERE logement_id = ?",
                    (logement_id,)).fetchone() is None:
        return {"ok": False, "code": E_LOGEMENT_INCONNU, "message": MESSAGES[E_LOGEMENT_INCONNU]}
    return None


def _prochain_identifiant(conn) -> str:
    suivant = conn.execute(
        "SELECT COALESCE(MAX(CAST(SUBSTR(mapping_logement_id, 9) AS INTEGER)), 0) + 1 "
        f"FROM {TABLE} WHERE mapping_logement_id LIKE 'MAP_LOG_%'").fetchone()[0]
    return f"MAP_LOG_{int(suivant):04d}"


def enregistrer(*, source: str, champ_source: str, valeur_source: str, logement_id: str,
                acteur: str = "", motif: str = "", db_path=None) -> dict[str, Any]:
    """Grave la décision : création si le libellé n'était pas rattaché, correction sinon.

    Un seul point d'entrée pour les deux cas. Les séparer obligerait l'écran à savoir à l'avance
    lequel s'applique — c'est-à-dire à interroger la base avant de poser la question, et à se
    tromper dès qu'une correspondance apparaît entre-temps.

    La ligne précédente n'est jamais effacée : elle est désactivée et son état d'avant est
    journalisé. Une correspondance corrigée doit pouvoir s'expliquer plus tard.
    """
    valeur = _txt(valeur_source)
    lid = _txt(logement_id)
    champ = _txt(champ_source) or CHAMP_LIBELLE_FACTURE
    src = _txt(source) or "Application"
    if not valeur or not lid:
        return {"ok": False, "code": E_VALEUR_MANQUANTE, "message": MESSAGES[E_VALEUR_MANQUANTE]}

    from app.services import referentiel_admin_service as admin

    conn = get_db(db_path)
    try:
        refus = _verifier_logement(conn, lid)
        if refus is not None:
            return refus

        conn.execute("BEGIN IMMEDIATE")
        existante = conn.execute(
            f"SELECT {', '.join(_COLS)} FROM {TABLE} "
            "WHERE champ_source = ? AND valeur_source = ? AND actif = 'OUI'",
            (champ, valeur)).fetchone()

        if existante is not None and _txt(existante["logement_id"]) == lid:
            conn.rollback()
            return {"ok": True, "inchange": True, "code": E_SANS_CHANGEMENT,
                    "message": MESSAGES[E_SANS_CHANGEMENT],
                    "mapping_logement_id": existante["mapping_logement_id"]}

        avant = dict(zip(_COLS, existante)) if existante is not None else None
        action = ACTION_CORRECTION if existante is not None else ACTION_DECLARATION

        if existante is not None:
            # DÉSACTIVÉE, PAS SUPPRIMÉE. Effacer la ligne ferait disparaître la trace du
            # rattachement sous lequel des calculs passés ont été faits.
            conn.execute(
                f"UPDATE {TABLE} SET actif = 'NON', commentaire = ? WHERE mapping_logement_id = ?",
                (f"Remplacée le jour de la correction par {acteur or 'local'}"
                 + (f" — {motif}" if motif else "."), existante["mapping_logement_id"]))

        mid = _prochain_identifiant(conn)
        commentaire = motif or ("Correspondance corrigée dans l'application."
                                if existante is not None
                                else "Correspondance déclarée dans l'application.")
        conn.execute(
            f"INSERT INTO {TABLE} (mapping_logement_id, source, champ_source, valeur_source, "
            "logement_id, niveau_confiance, actif, commentaire, import_id) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (mid, src, champ, valeur, lid, "Fort", "OUI", commentaire, "SAISIE_APPLICATION"))

        apres = {"mapping_logement_id": mid, "source": src, "champ_source": champ,
                 "valeur_source": valeur, "logement_id": lid, "actif": "OUI"}
        admin.journaliser(conn, TABLE, valeur, action, avant, apres, acteur, commentaire)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {"ok": True, "inchange": False, "action": action, "mapping_logement_id": mid,
            "logement_id": lid, "valeur_source": valeur}


def rattacher_listing_au_parc(*, listing_map_id: str, logement_id: str, acteur: str = "",
                              db_path=None) -> dict[str, Any]:
    """Rattache une annonce Hostaway à un logement du parc — correspondance ET fiche logement.

    Les deux, parce que les deux existent et sont lus par des chemins différents : la fiche porte
    `hostaway_listing_id` (lu par les moteurs de réservation), la correspondance porte la même
    information sous forme de règle (lue par le rapprochement). N'en écrire qu'une laisserait
    l'autre chemin toujours bloqué, avec le même message d'erreur et l'impression que la
    correction n'a servi à rien.
    """
    identifiant = _txt(listing_map_id)
    lid = _txt(logement_id)
    if not identifiant or not lid:
        return {"ok": False, "code": E_VALEUR_MANQUANTE, "message": MESSAGES[E_VALEUR_MANQUANTE]}

    from app.services import referentiel_admin_service as admin

    conn = get_db(db_path)
    try:
        refus = _verifier_logement(conn, lid)
        if refus is not None:
            return refus
        occupant = conn.execute(
            "SELECT logement_id FROM ref_logements WHERE TRIM(hostaway_listing_id) = ? "
            "AND logement_id <> ?", (identifiant, lid)).fetchone()
        if occupant is not None:
            return {"ok": False, "code": "LISTING_DEJA_RATTACHE",
                    "message": (f"L'annonce {identifiant} est déjà rattachée à "
                                f"{occupant['logement_id']}. Corrigez d'abord ce rattachement.")}
        avant = conn.execute(
            "SELECT logement_id, hostaway_listing_id, sur_hostaway FROM ref_logements "
            "WHERE logement_id = ?", (lid,)).fetchone()
        conn.execute(
            "UPDATE ref_logements SET hostaway_listing_id = ?, sur_hostaway = 'OUI' "
            "WHERE logement_id = ?", (identifiant, lid))
        admin.journaliser(
            conn, "ref_logements", lid, "RATTACHEMENT_HOSTAWAY",
            dict(avant) if avant is not None else None,
            {"hostaway_listing_id": identifiant, "sur_hostaway": "OUI"}, acteur,
            f"Annonce Hostaway {identifiant} rattachée au logement.")
        conn.commit()
    finally:
        conn.close()

    correspondance = enregistrer(
        source=SOURCE_HOSTAWAY, champ_source=CHAMP_LISTING, valeur_source=identifiant,
        logement_id=lid, acteur=acteur,
        motif=f"Annonce Hostaway {identifiant} rattachée depuis l'écran des correspondances.",
        db_path=db_path)
    return {"ok": True, "logement_id": lid, "listing_map_id": identifiant,
            "correspondance": correspondance}


def desactiver(mapping_logement_id: str, *, acteur: str = "", motif: str = "",
               db_path=None) -> dict[str, Any]:
    """Retire une correspondance de l'usage, sans l'effacer."""
    mid = _txt(mapping_logement_id)
    from app.services import referentiel_admin_service as admin

    conn = get_db(db_path)
    try:
        ligne = conn.execute(
            f"SELECT {', '.join(_COLS)} FROM {TABLE} WHERE mapping_logement_id = ?",
            (mid,)).fetchone()
        if ligne is None:
            return {"ok": False, "code": E_INTROUVABLE, "message": MESSAGES[E_INTROUVABLE]}
        avant = dict(zip(_COLS, ligne))
        conn.execute(f"UPDATE {TABLE} SET actif = 'NON' WHERE mapping_logement_id = ?", (mid,))
        admin.journaliser(conn, TABLE, avant["valeur_source"], ACTION_DESACTIVATION, avant,
                          {**avant, "actif": "NON"}, acteur, motif)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "mapping_logement_id": mid}


def historique(*, limite: int = 50, db_path=None) -> list[dict[str, Any]]:
    """Décisions passées sur les correspondances — qui, quand, de quoi vers quoi."""
    conn = get_db(db_path)
    try:
        lignes = list(conn.execute(
            "SELECT horodatage, action, cle, avant_json, apres_json, acteur, commentaire "
            "FROM ref_admin_evenements WHERE table_cible IN (?, 'ref_logements') "
            "AND action IN (?, ?, ?, 'RATTACHEMENT_HOSTAWAY') "
            "ORDER BY id DESC LIMIT ?",
            (TABLE, ACTION_DECLARATION, ACTION_CORRECTION, ACTION_DESACTIVATION, limite)))
    finally:
        conn.close()

    resultat = []
    for r in lignes:
        avant = json.loads(r["avant_json"]) if r["avant_json"] else None
        apres = json.loads(r["apres_json"]) if r["apres_json"] else None
        resultat.append({
            "horodatage": r["horodatage"], "action": r["action"], "valeur_source": r["cle"],
            "logement_avant": (avant or {}).get("logement_id", ""),
            "logement_apres": (apres or {}).get("logement_id", ""),
            "acteur": r["acteur"] or "", "commentaire": r["commentaire"] or "",
        })
    return resultat
