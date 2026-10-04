"""Brouillon de facture propriétaire : séjours éditables, rechargement, suppression des annulées.

Demande utilisateur du 2026-10-03.

SÉJOURS ÉDITABLES
  Chaque séjour figé sur le brouillon porte son ménage et sa commission (migration 0122). Les
  modifier (ex. ménage offert au propriétaire) ajuste du même ÉCART les lignes « Prestations de
  ménage » et « Commission de conciergerie » de la facture : le total suit, sans avoir à faire un
  avoir après coup. Seul le DOCUMENT change — Lot10/Lot12 ne sont jamais touchés, et les valeurs
  d'origine restent affichées à côté.

RECHARGER
  Remet le brouillon d'aplomb sur le calcul ACTUEL (après une correction de réservation, une
  nouvelle extraction Hostaway…) : lignes calculées et séjours sont reconstruits ; les lignes
  ajoutées à la main (extras, réductions, charges refacturées) sont conservées.

SUPPRIMER UNE FACTURE ANNULÉE
  Seulement une facture ANNULÉE qui n'a JAMAIS été émise (aucun numéro légal) : elle n'a jamais
  existé pour personne. Une facture émise ne se supprime jamais (avoir).
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db
from app.services import factures_proprietaires_service as svc

EVT_MODIFICATION_SEJOUR = "MODIFICATION_SEJOUR"
EVT_RECHARGEMENT = "RECHARGEMENT"

#: Ligne de facture qui somme, par séjour, le champ indiqué.
LIGNE_PAR_CHAMP = {"menage": "MENAGE_FACTURE", "commission": "COMMISSION_CONCIERGERIE"}
SOURCE_SEJOURS = "SEJOURS_FACTURE"

#: Lignes reconstruites par un rechargement (celles que produit le calcul).
TYPES_CALCULES = tuple(svc.TYPES_FACTURABLES)

#: Tables portant les données propres à une facture propriétaire (feuilles d'abord).
TABLES_PROPRES = (
    "factures_proprietaires_lignes_provenance",
    "factures_proprietaires_lignes_charge",
    "factures_proprietaires_lignes",
    "factures_proprietaires_reservations",
    "factures_proprietaires_conformite",
    "factures_proprietaires_meta",
    "factures_proprietaires_evenements",
)


def _montant(valeur: Any, champ: str) -> float:
    try:
        v = round(float(str(valeur).replace(",", ".").replace("€", "").strip()), 2)
    except (TypeError, ValueError):
        raise svc.FactureProprietaireError(f"{champ} illisible : « {valeur} »")
    if v < 0:
        raise svc.FactureProprietaireError(f"{champ} négatif refusé ({v:.2f} €)")
    return v


# ── Séjours ────────────────────────────────────────────────────────────────────────────────────

def _ajuster_ligne(conn, facture_id: str, type_ligne: str, ecart: float) -> None:
    """Ajoute `ecart` à la ligne `type_ligne` du document (créée, ou retirée si elle tombe à 0)."""
    if abs(ecart) < svc.TOLERANCE:
        return
    ligne = conn.execute(
        "SELECT ligne_id_opaque, montant, objet_source_type, objet_source_ref "
        "FROM factures_proprietaires_lignes WHERE facture_id_opaque=? AND type_ligne=? "
        "ORDER BY numero_ligne LIMIT 1", (facture_id, type_ligne)).fetchone()
    if ligne is None:
        if ecart > 0:
            conn.execute(
                "INSERT INTO factures_proprietaires_lignes (ligne_id_opaque, facture_id_opaque, "
                "numero_ligne, type_ligne, libelle, montant, objet_source_type) "
                "VALUES (?,?,?,?,?,?,?)",
                (svc._opaque("FPRL"), facture_id, svc._prochain_numero_ligne(conn, facture_id),
                 type_ligne, svc.LIBELLES[type_ligne], svc._round(ecart), SOURCE_SEJOURS))
        return
    if str(ligne["objet_source_type"] or "").strip():
        # Montant d'origine conservé AVANT la première modification (même règle que
        # `modifier_ligne`) : l'écart au calcul reste lisible sur la fiche.
        conn.execute(
            "INSERT OR IGNORE INTO factures_proprietaires_lignes_provenance "
            "(ligne_id_opaque, facture_id_opaque, montant_source_initial, objet_source_type, "
            " objet_source_ref) VALUES (?,?,?,?,?)",
            (ligne["ligne_id_opaque"], facture_id, svc._round(ligne["montant"]),
             ligne["objet_source_type"], ligne["objet_source_ref"]))
    nouveau = svc._round(svc._round(ligne["montant"]) + ecart)
    if nouveau <= svc.TOLERANCE:
        conn.execute("DELETE FROM factures_proprietaires_lignes WHERE ligne_id_opaque=?",
                     (ligne["ligne_id_opaque"],))
    else:
        conn.execute("UPDATE factures_proprietaires_lignes SET montant=? WHERE ligne_id_opaque=?",
                     (nouveau, ligne["ligne_id_opaque"]))


def modifier_sejour(facture_id: str, reservation_id: str, *, menage: Any = None,
                    commission: Any = None, acteur: str = "", motif: str = "",
                    db_path=None) -> dict[str, Any]:
    """Modifie le ménage et/ou la commission d'un séjour d'un BROUILLON ; le total suit."""
    f = svc.lire(facture_id, db_path=db_path)
    svc._exiger_brouillon(f, "modification d'un séjour")
    conn = get_db(db_path)
    try:
        sejour = conn.execute(
            "SELECT * FROM factures_proprietaires_reservations "
            "WHERE facture_id_opaque=? AND reservation_id=?", (facture_id, reservation_id)).fetchone()
        if sejour is None:
            raise svc.FactureProprietaireError(f"séjour inconnu sur cette facture : {reservation_id}")
        if sejour["commission"] is None and sejour["menage"] is None:
            raise svc.FactureProprietaireError(
                "ce brouillon a été créé avant l'édition des séjours : rechargez-le d'abord")
        nouveaux = {}
        if menage is not None and str(menage).strip() != "":
            nouveaux["menage"] = _montant(menage, "ménage")
        if commission is not None and str(commission).strip() != "":
            nouveaux["commission"] = _montant(commission, "commission")
        if not nouveaux:
            raise svc.FactureProprietaireError("aucun montant saisi")

        details = []
        for champ, valeur in nouveaux.items():
            ancien = svc._round(sejour[champ])
            ecart = svc._round(valeur - ancien)
            if abs(ecart) < svc.TOLERANCE:
                continue
            conn.execute(f"UPDATE factures_proprietaires_reservations SET {champ}=? "
                         "WHERE facture_id_opaque=? AND reservation_id=?",
                         (valeur, facture_id, reservation_id))
            _ajuster_ligne(conn, facture_id, LIGNE_PAR_CHAMP[champ], ecart)
            details.append(f"{champ} {ancien:.2f} -> {valeur:.2f}")
        if not details:
            return {"ok": True, "inchange": True}
        total = svc._resynchroniser_total(conn, facture_id)
        svc._journal(conn, facture_id, EVT_MODIFICATION_SEJOUR, svc.ST_BROUILLON, svc.ST_BROUILLON,
                     f"séjour {reservation_id} : {', '.join(details)} ; total {total:.2f}"
                     + (f" — {motif}" if str(motif or "").strip() else ""), acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "total": total}


# ── Recharger ──────────────────────────────────────────────────────────────────────────────────

def _lignes_calculees_actuelles(f: dict[str, Any], db_path=None) -> list[dict[str, Any]]:
    """Lignes que le calcul ACTUEL produit pour ce brouillon (mois entier ou période libre)."""
    if f.get("periode_debut") and f.get("periode_fin"):
        from app.services import factures_proprietaires_periode_service as periode
        ap = periode.previsualiser(f["proprietaire_id"], f["periode_debut"], f["periode_fin"],
                                   logement_id=f["logement_id"], db_path=db_path)
        if not ap.get("ok"):
            raise svc.FactureProprietaireError(ap.get("message") or "période non rechargeable")
        prop = next((p for p in ap["propositions"] if p["logement_id"] == f["logement_id"]), None)
        return [{**l, "objet_source_type": svc.SOURCE_EXCEPTIONNELLE, "objet_source_ref": None}
                for l in (prop["lignes"] if prop else [])]
    if f.get("source_calcul") == svc.SOURCE_EXCEPTIONNELLE:
        raise svc.FactureProprietaireError(
            "facture ponctuelle saisie à la main : aucun calcul à recharger")
    from app.readers import proprietaires_reader as reader
    from app.services import factures_proprietaires_source as source_svc
    entetes = [e for e in reader.read_prefacture_entetes_prop_mois(f["proprietaire_id"], f["mois"])
               if e.get("logement_id") == f["logement_id"]]
    if not entetes:
        return []
    lignes = reader.read_prefacture_lignes([entetes[0].get("facture_id")])
    source = source_svc.depuis_prefacture(entetes[0], lignes.get(entetes[0].get("facture_id"), []))
    return svc.previsualiser(source)["lignes"]


def recharger(facture_id: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Reconstruit le BROUILLON sur le calcul actuel ; conserve les lignes saisies à la main."""
    f = svc.lire(facture_id, db_path=db_path)
    svc._exiger_brouillon(f, "rechargement")
    if f.get("type_document") != svc.TYPE_FACTURE:
        raise svc.FactureProprietaireError("seule une facture se recharge, pas un avoir")
    if f.get("periode_debut") and f.get("periode_fin"):
        from app.services import factures_proprietaires_periode_service as periode
        for mois in periode.mois_traverses(f["periode_debut"], f["periode_fin"]):
            svc.exiger_reservations_resolues(mois, f["logement_id"], db_path=db_path)
    else:
        svc.exiger_reservations_resolues(f["mois"], f["logement_id"], db_path=db_path)
    nouvelles = _lignes_calculees_actuelles(f, db_path=db_path)

    conn = get_db(db_path)
    try:
        charges = {r[0] for r in conn.execute(
            "SELECT ligne_id_opaque FROM factures_proprietaires_lignes_charge "
            "WHERE facture_id_opaque=?", (facture_id,))}
        a_retirer = [l["ligne_id_opaque"] for l in f["lignes"]
                     if l["type_ligne"] in TYPES_CALCULES and l["origine"] == svc.ORIGINE_CALCULEE
                     and l["ligne_id_opaque"] not in charges]
        for lid in a_retirer:
            conn.execute("DELETE FROM factures_proprietaires_lignes_provenance "
                         "WHERE ligne_id_opaque=?", (lid,))
            conn.execute("DELETE FROM factures_proprietaires_lignes WHERE ligne_id_opaque=?",
                         (lid,))
        conservees = [r[0] for r in conn.execute(
            "SELECT ligne_id_opaque FROM factures_proprietaires_lignes WHERE facture_id_opaque=? "
            "ORDER BY numero_ligne", (facture_id,))]
        # Renumérotation : lignes calculées d'abord, puis celles saisies à la main, dans leur ordre.
        # Décalage provisoire pour ne jamais heurter l'unicité (facture, numéro).
        conn.execute("UPDATE factures_proprietaires_lignes SET numero_ligne = numero_ligne + 100000 "
                     "WHERE facture_id_opaque=?", (facture_id,))
        numero = 0
        for l in nouvelles:
            numero += 1
            conn.execute(
                "INSERT INTO factures_proprietaires_lignes (ligne_id_opaque, facture_id_opaque, "
                "numero_ligne, type_ligne, libelle, montant, objet_source_type, objet_source_ref) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (svc._opaque("FPRL"), facture_id, numero, l["type_ligne"], l["libelle"],
                 svc._round(l["montant"]), l.get("objet_source_type") or SOURCE_SEJOURS,
                 l.get("objet_source_ref")))
        for lid in conservees:
            numero += 1
            conn.execute("UPDATE factures_proprietaires_lignes SET numero_ligne=? "
                         "WHERE ligne_id_opaque=?", (numero, lid))
        total_calcule = svc._round(sum(svc._round(l["montant"]) for l in nouvelles))
        conn.execute("INSERT OR REPLACE INTO factures_proprietaires_meta "
                     "(facture_id_opaque, total_source_calcule) VALUES (?,?)",
                     (facture_id, total_calcule))
        conn.execute("DELETE FROM factures_proprietaires_reservations WHERE facture_id_opaque=?",
                     (facture_id,))
        nb_sejours = svc._figer_reservations(
            conn, facture_id, f["mois"], f["proprietaire_id"], f["logement_id"],
            debut=f.get("periode_debut") or "", fin=f.get("periode_fin") or "")
        total = svc._resynchroniser_total(conn, facture_id)
        svc._journal(conn, facture_id, EVT_RECHARGEMENT, svc.ST_BROUILLON, svc.ST_BROUILLON,
                     f"{len(nouvelles)} ligne(s) recalculée(s), {len(conservees)} conservée(s), "
                     f"{nb_sejours} séjour(s) ; total {total:.2f}", acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "total": total, "lignes_calculees": len(nouvelles),
            "lignes_conservees": len(conservees), "sejours": nb_sejours}


# ── Supprimer une facture annulée jamais émise ─────────────────────────────────────────────────

def jamais_emise(f: dict[str, Any]) -> bool:
    """Aucune émission définitive, ni actuelle ni PASSÉE : pas de numéro, pas de date d'émission,
    pas de snapshot figé, et aucun événement de l'historique n'a jamais fait passer la facture au
    statut EMIS. Le statut courant seul ne suffit pas : une facture émise puis annulée reste une
    facture émise, sa trace doit être conservée."""
    if f.get("numero_facture") or f.get("date_emission") or f.get("snapshot_json"):
        return False
    evenements = f.get("evenements") or []
    return not any(e.get("nouveau_statut") == svc.ST_EMIS or e.get("type_evenement") == "EMISSION"
                   for e in evenements)


def supprimable(f: dict[str, Any]) -> bool:
    return f["statut"] == svc.ST_ANNULE and jamais_emise(f)


def supprimable_non_emise(f: dict[str, Any]) -> bool:
    """BROUILLON ou VALIDE, jamais émise : aucun numéro légal n'a été consommé (il ne l'est qu'à
    l'émission, cf. `_attribuer_numero`). Supprimer ne crée donc aucun trou dans la séquence."""
    return f["statut"] in (svc.ST_BROUILLON, svc.ST_VALIDE) and jamais_emise(f)


def supprimer_non_emise(facture_id: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Supprime définitivement une facture BROUILLON ou VALIDE jamais émise (2026-10-04).

    Une facture VALIDE repasse d'abord en brouillon (service canonique) : les positions de
    refacturation imputées à la validation sont ainsi RENDUES, jamais perdues. Une facture ÉMISE ou
    comptabilisée est refusée ici, côté serveur, quel que soit l'appelant.
    """
    f = svc.lire(facture_id, db_path=db_path)
    if not supprimable_non_emise(f):
        raise svc.FactureProprietaireError(
            "seule une facture non émise (brouillon ou validée, sans numéro) peut être supprimée ; "
            "une facture émise se corrige par un avoir")
    if f["statut"] == svc.ST_VALIDE:
        svc.repasser_en_brouillon(facture_id, acteur=acteur, motif="suppression avant émission",
                                  db_path=db_path)
    return _supprimer(facture_id, db_path=db_path)


def supprimer_annulee(facture_id: str, *, db_path=None) -> dict[str, Any]:
    """Supprime définitivement une facture ANNULÉE jamais émise, et tout ce qui lui est propre.

    Refus si elle porte un numéro, ou si un objet économique la référence encore (imputation,
    acompte, écriture comptable) : supprimer ferait alors disparaître une trace qui compte.
    """
    f = svc.lire(facture_id, db_path=db_path)
    if not supprimable(f):
        raise svc.FactureProprietaireError(
            "seule une facture ANNULÉE et jamais émise peut être supprimée ; une facture émise se "
            "corrige par un avoir")
    return _supprimer(facture_id, db_path=db_path)


def _supprimer(facture_id: str, *, db_path=None) -> dict[str, Any]:
    conn = get_db(db_path)
    try:
        def _table(nom):
            return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                                (nom,)).fetchone() is not None
        references = []
        for table, colonne in (("imputations_airbnb", "document_id"),
                               ("mouvements_tresorerie_proprietaires", "reference_metier"),
                               ("ecritures", "origine_id_opaque"),
                               ("factures_proprietaires", "facture_origine")):
            if _table(table):
                try:
                    n = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {colonne}=?",
                                     (facture_id,)).fetchone()[0]
                except Exception:   # noqa: BLE001 — colonne absente sur un schéma ancien
                    n = 0
                if n:
                    references.append(f"{table} ({n})")
        if references:
            raise svc.FactureProprietaireError(
                "suppression refusée, la facture est encore référencée : " + ", ".join(references))
        supprimees = {}
        for table in TABLES_PROPRES:
            if _table(table):
                supprimees[table] = conn.execute(
                    f"DELETE FROM {table} WHERE facture_id_opaque=?", (facture_id,)).rowcount
        conn.execute("DELETE FROM factures_proprietaires WHERE facture_id_opaque=?", (facture_id,))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "facture_id_opaque": facture_id, "lignes_supprimees": supprimees}


def supprimer_annulees(*, db_path=None) -> list[str]:
    """Supprime toutes les factures annulées jamais émises (bouton de la liste)."""
    faites = []
    for f in svc.lister(statut=svc.ST_ANNULE, db_path=db_path):
        if supprimable(f):
            supprimer_annulee(f["facture_id_opaque"], db_path=db_path)
            faites.append(f["facture_id_opaque"])
    return faites
