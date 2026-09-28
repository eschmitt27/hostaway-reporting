"""Bloqueurs financiers d'un mois de clôture — lus EN DIRECT dans Flux financiers (Mission 32).

La clôture ne se fie plus à `banque_mouvements.statut_classification` (ancien import Crédit Mutuel,
lu par le contrôle moteur `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE`) pour dire si l'argent du
mois est traité : la source canonique de l'état d'un mouvement est Flux financiers
(`flux_financiers_service.mouvements`), qui projette Qonto (GET-only), la caisse, les
rapprochements et les écritures. Ce module ne fait que LIRE : aucune qualification, aucun mapping,
aucune validation d'écriture ou de facture n'est jamais décidée ici.

POURQUOI CHAQUE ÉTAT BLOQUE, OU NE BLOQUE PAS (même vocabulaire que Flux, aucun statut parallèle) :

  BLOQUE —
  · mouvement bancaire du mois « À qualifier » : aucun objet ni aucune pièce ne l'explique ;
    une ligne bancaire non classée interdit la clôture (REGLES_METIER C4/C5, D017) ;
  · mouvement « À comptabiliser » : rapproché ou de nature connue (retrait d'espèces), mais sans
    écriture VALIDÉE — la comptabilité du mois serait incomplète ;
  · mouvement « Erreur » : rapprochement supérieur au mouvement ou écriture contrepassée ;
  · opération bancaire du mois encore en attente à la banque : elle ne peut pas être validée, or
    toutes les lignes bancaires de la période doivent l'être avant clôture (C4) ;
  · opération de caisse du mois à qualifier / à comptabiliser / en erreur — même règle ;
  · écriture du mois `PROPOSEE` : une écriture proposée n'entre pas dans la balance
    (`45_MODELE_ECRITURES_COMPTABLES.md` : « seule PROPOSEE reste exclue ») et la validation
    officielle précède la clôture (C3) — arrêter le mois figerait une balance incomplète ;
  · charge comptable du mois pas encore comptabilisée dont le compte est « à définir » (aucune
    règle de mapping VALIDÉE vers un compte actif de classe 6) : elle ne peut pas être
    comptabilisée. Une règle PROVISOIRE ne suffit pas (même contrat que Flux).

  NE BLOQUE PAS (affiché comme informatif, avec sa raison) —
  · opération sans effet (refusée ou contrepassée par la banque) : aucun argent n'a bougé ;
  · facture fournisseur à contrôler / brouillon / en litige : aucune règle ne rend la clôture
    dépendante de ce contrôle, et sa dette n'existe qu'après validation ;
  · dette fournisseur ouverte (facture validée non réglée) : une dette peut rester ouverte ;
  · charge comptable non encore comptabilisée dont le compte est défini : elle se comptabilise
    avec son paiement — si ce paiement est un mouvement du mois, c'est LUI qui bloque ;
  · charge hors comptabilité : sans effet sur la comptabilité du mois.

  Un retrait d'espèces apparaît côté Banque ET côté Caisse (transfert) : il n'est compté qu'une
  fois, sur le mouvement bancaire, qui porte le transfert 530 / 512.
"""
from __future__ import annotations

from typing import Any
from urllib.parse import quote

from app.db.connection import get_db
from app.services import flux_financiers_service as flux

# Familles de bloqueurs, dans l'ordre d'affichage de la synthèse.
F_BANQUE = "BANQUE"
F_CAISSE = "CAISSE"
F_ECRITURE = "ECRITURE"
F_COMPTE = "COMPTE_A_DEFINIR"
FAMILLES = {
    F_BANQUE: "Mouvements bancaires à traiter",
    F_CAISSE: "Mouvements de caisse à traiter",
    F_ECRITURE: "Écritures comptables à valider",
    F_COMPTE: "Comptes comptables à définir",
}

I_SANS_EFFET = "SANS_EFFET"
I_FACTURE = "FACTURE_FOURNISSEUR"
I_CHARGE = "CHARGE_A_COMPTABILISER"
I_HORS_COMPTA = "HORS_COMPTABILITE"
FAMILLES_INFO = {
    I_FACTURE: "Factures fournisseurs (non bloquantes)",
    I_CHARGE: "Charges qui se comptabiliseront avec leur paiement",
    I_SANS_EFFET: "Opérations sans effet",
    I_HORS_COMPTA: "Charges hors comptabilité",
}

JOURNAUX = {"ACHATS": "Achats", "VENTES": "Ventes", "BANQUE": "Banque", "CAISSE": "Caisse",
            "ODIVERSES": "Opérations diverses"}
FACTURES_A_CONTROLER = ("BROUILLON", "A_CONTROLER", "LITIGE")
FACTURES_DETTE_OUVERTE = ("VALIDEE", "PARTIELLEMENT_REGLEE")
LIBELLES_FACTURE = {"BROUILLON": "Brouillon", "A_CONTROLER": "À contrôler", "LITIGE": "En litige",
                    "VALIDEE": "Validée", "PARTIELLEMENT_REGLEE": "Partiellement réglée"}


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _tables(conn) -> set[str]:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def contexte(*, db_path=None) -> dict[str, Any]:
    """Tout ce que l'analyse d'un mois lit — chargé UNE fois, réutilisable pour plusieurs mois
    (liste des clôtures). Lecture seule."""
    conn = get_db(db_path)
    try:
        tables = _tables(conn)
        ecritures = [dict(r) for r in conn.execute(
            "SELECT ecriture_id_opaque, journal, periode, date_ecriture, libelle, origine_type, "
            "origine_id_opaque, statut, total_debit FROM ecritures")] if "ecritures" in tables else []
        rappro_mouvement = {r[0]: r[1] for r in conn.execute(
            "SELECT rapprochement_id_opaque, mouvement_id_opaque FROM banque_rapprochements")} \
            if "banque_rapprochements" in tables else {}
        charges = [dict(r) for r in conn.execute(
            "SELECT c.*, COALESCE(k.categorie_niveau_1 || ' · ' || k.categorie_niveau_2, "
            "c.categorie_charge_id) AS categorie_libelle FROM charges c "
            "LEFT JOIN ref_categories_charges k ON k.categorie_charge_id = c.categorie_charge_id "
            "WHERE c.statut = 'ACTIVE'")] if "charges" in tables else []
        factures = [dict(r) for r in conn.execute(
            "SELECT f.facture_id_opaque, f.facture_ref, f.date_facture, f.montant_ttc, f.statut, "
            "COALESCE(r.nom, '') AS fournisseur FROM factures f "
            "LEFT JOIN fournisseurs r ON r.fournisseur_id_opaque = f.fournisseur_id_opaque")] \
            if {"factures", "fournisseurs"} <= tables else []
    finally:
        conn.close()
    return {
        "mouvements": flux.mouvements(avec_propositions=False, db_path=db_path),
        "statuts_charges": flux.statuts_charges(db_path=db_path),
        "ecritures": ecritures, "rappro_mouvement": rappro_mouvement,
        "charges": charges, "factures": factures, "db_path": db_path,
    }


def _item(famille: str, *, type_: str, date: str, montant: float | None, origine: str,
          statut: str, raison: str, action: str, lien: str, lien_libelle: str) -> dict[str, Any]:
    return {"famille": famille, "type": type_, "date": date[:10], "date_fr": flux.date_fr(date),
            "montant": montant, "origine": origine, "statut": statut, "raison": raison,
            "action": action, "lien": lien, "lien_libelle": lien_libelle}


# ── Mouvements (Banque, Caisse) ───────────────────────────────────────────────────────────────

_QUOI = {flux.BANQUE: ("Mouvement bancaire", "Opération bancaire"),
         flux.CAISSE: ("Opération de caisse", "Opération de caisse")}


def _bloqueur_mouvement(m: dict) -> dict | None:
    source = m["source"]
    famille = F_BANQUE if source == flux.BANQUE else F_CAISSE
    lien_detail = (f"/flux-financiers/banque/{quote(m['id'])}" if source == flux.BANQUE
                   else f"/flux-financiers/caisse/{quote(m['id'])}")
    lien_rappro = f"/flux-financiers/rapprochement?m={quote(source + ':' + m['id'])}"
    origine = ("Qonto" if source == flux.BANQUE else "Caisse") + (
        f" — {m['libelle']}" if m.get("libelle") else "")
    commun = {"date": m["date"], "montant": m["montant"], "origine": origine}
    nom, _ = _QUOI[source]
    retrait = m.get("parcours_dedie") == "RETRAIT"
    if not m["definitif"]:
        return _item(famille, type_=f"{nom} en attente à la banque", statut="En attente",
                     raison="La banque n'a pas encore réglé cette opération : elle ne peut pas être "
                            "validée, et toutes les lignes bancaires du mois doivent l'être.",
                     action="Attendre son règlement, puis actualiser les mouvements Qonto.",
                     lien=lien_detail, lien_libelle="Voir le mouvement", **commun)
    code = m["statut_compta"]
    if code == flux.A_QUALIFIER:
        return _item(famille, type_=f"{nom} à qualifier", statut=m["compta"]["libelle"],
                     raison="Aucune charge, facture ou règlement ne l'explique encore "
                            f"(rapprochement : {m['rapprochement']['libelle'].lower()}).",
                     action="Le rapprocher d'une pièce, ou créer la charge correspondante.",
                     lien=lien_rappro, lien_libelle="Traiter dans Flux financiers", **commun)
    if code == flux.A_COMPTABILISER:
        if retrait:
            return _item(famille, type_="Retrait d'espèces à comptabiliser",
                         statut=m["compta"]["libelle"],
                         raison="Le transfert Banque → Caisse (530 / 512) n'est pas encore passé.",
                         action="Valider le retrait dans Flux financiers.",
                         lien=lien_detail, lien_libelle="Traiter le retrait", **commun)
        return _item(famille, type_=f"{nom} à comptabiliser", statut=m["compta"]["libelle"],
                     raison="Son rapprochement n'a pas encore d'écriture validée.",
                     action="Valider le rapprochement et sa comptabilisation.",
                     lien=lien_rappro, lien_libelle="Traiter dans Flux financiers", **commun)
    if code == flux.ERREUR:
        return _item(famille, type_=f"{nom} en anomalie", statut=m["compta"]["libelle"],
                     raison="Rapprochement supérieur au mouvement, ou écriture contrepassée "
                            "sans remplacement.",
                     action="Corriger ou annuler le rapprochement.",
                     lien=lien_detail, lien_libelle="Voir le mouvement", **commun)
    return None


# ── Analyse d'un mois ─────────────────────────────────────────────────────────────────────────

def analyser(mois: str, *, contexte_flux: dict | None = None, db_path=None) -> dict[str, Any]:
    """Bloqueurs et informatifs financiers du mois. Lecture seule, recalculée à chaque appel."""
    from app.services import flux_lettrage_service as lettrage

    ctx = contexte_flux or contexte(db_path=db_path)
    db_path = ctx.get("db_path", db_path)
    mois = _txt(mois)[:7]
    bloquants: list[dict] = []
    informatifs: list[dict] = []

    # 1. Mouvements Banque (Qonto) et Caisse du mois.
    mouvements_bloquants: set[str] = set()
    for m in ctx["mouvements"]:
        if m["mois"] != mois:
            continue
        if m["source"] == flux.CAISSE and m.get("nature_caisse") == "TRANSFERT":
            continue            # porté par le mouvement bancaire du retrait : compté une fois
        if m["sans_effet"]:
            informatifs.append(_item(
                I_SANS_EFFET, type_=f"{_QUOI[m['source']][1]} sans effet", date=m["date"],
                montant=m["montant"], origine=m.get("libelle", ""), statut="Sans effet",
                raison="Refusée ou contrepassée par la banque : aucun argent n'a bougé.",
                action="Aucune.", lien="", lien_libelle=""))
            continue
        b = _bloqueur_mouvement(m)
        if b:
            bloquants.append(b)
            mouvements_bloquants.add(m["id"])

    # 2. Écritures du mois encore PROPOSÉES.
    for e in ctx["ecritures"]:
        if _txt(e["periode"])[:7] != mois or e["statut"] != "PROPOSEE":
            continue
        if (e["origine_type"] == "RAPPROCHEMENT"
                and ctx["rappro_mouvement"].get(e["origine_id_opaque"]) in mouvements_bloquants):
            continue            # déjà dit par son mouvement « à comptabiliser »
        journal = JOURNAUX.get(e["journal"], e["journal"])
        bloquants.append(_item(
            F_ECRITURE, type_="Écriture proposée à valider", date=e["date_ecriture"] or mois,
            montant=e["total_debit"], origine=f"Journal {journal} — {e['libelle'] or ''}".strip(" —"),
            statut="Proposée",
            raison="Une écriture proposée n'entre pas dans la balance : le mois ne peut pas être "
                   "arrêté sans elle.",
            action="La vérifier, puis la valider — ou la contrepasser si elle est erronée.",
            lien=f"/comptabilite/ecritures/{quote(e['ecriture_id_opaque'])}",
            lien_libelle="Ouvrir l'écriture"))

    # 3. Charges du mois : compte comptable requis mais non défini.
    mois_des_mouvements = {m["id"]: m["mois"] for m in ctx["mouvements"]}
    nb_comptes = 0
    for c in ctx["charges"]:
        lien_mvt = _txt(c.get("lien_virement_banque"))
        dans_le_mois = (_txt(c["date_charge"])[:7] == mois
                        or mois_des_mouvements.get(lien_mvt) == mois)
        if not dans_le_mois:
            continue
        st = ctx["statuts_charges"].get(c["charge_id"], {})
        code = st.get("compta", {}).get("code")
        detail = _txt(c.get("commentaire"))
        categorie = c.get("categorie_libelle") or "Catégorie inconnue"
        origine_charge = f"{categorie} — {detail}" if detail else categorie
        if code == flux.CH_HORS_COMPTA:
            informatifs.append(_item(
                I_HORS_COMPTA, type_="Charge hors comptabilité", date=c["date_charge"],
                montant=abs(c["montant"] or 0), origine=c.get("categorie_libelle") or "",
                statut="Hors comptabilité", raison="Sans effet sur la comptabilité du mois.",
                action="Aucune.", lien="", lien_libelle=""))
            continue
        if code == flux.CH_COMPTABILISEE or _txt(c.get("statut_controle")).upper() == "REJETE" \
                or st.get("origine") == "Facture fournisseur":
            continue        # déjà comptabilisée, écartée, ou comptabilisée par sa facture
        compte, avertissement = lettrage.compte_de_charge(c, db_path=db_path)
        lien_charges = (f"/flux-financiers/charges?mois={quote(_txt(c['date_charge'])[:7])}"
                        f"&categorie_charge_id={quote(_txt(c['categorie_charge_id']))}")
        if not compte:
            nb_comptes += 1
            bloquants.append(_item(
                F_COMPTE, type_="Compte comptable à définir", date=c["date_charge"],
                montant=abs(c["montant"] or 0),
                origine=origine_charge,
                statut=st.get("compta", {}).get("libelle", "Comptable"),
                raison=avertissement.split(" : ", 1)[-1] if avertissement else
                "Aucune règle de mapping validée pour cette catégorie.",
                action="Ajouter le compte au plan comptable si besoin, puis valider une règle de "
                       "mapping pour cette catégorie.",
                lien=f"/comptabilite/mappings?categorie={quote(_txt(c['categorie_charge_id']))}"
                     "#nouvelle-regle",
                lien_libelle="Définir le compte"))
        else:
            informatifs.append(_item(
                I_CHARGE, type_="Charge comptable pas encore comptabilisée",
                date=c["date_charge"], montant=abs(c["montant"] or 0),
                origine=origine_charge,
                statut=st.get("compta", {}).get("libelle", ""),
                raison=f"Compte {compte} proposé : elle se comptabilise avec son paiement. Si ce "
                       "paiement est un mouvement du mois, c'est lui qui bloque.",
                action="Aucune pour la clôture.", lien=lien_charges, lien_libelle="Voir les charges"))

    # 4. Factures fournisseurs du mois : informatives (aucune règle ne les rend bloquantes).
    for f in ctx["factures"]:
        if _txt(f["date_facture"])[:7] != mois:
            continue
        statut = _txt(f["statut"])
        if statut in FACTURES_A_CONTROLER:
            raison = ("Aucune règle ne rend la clôture dépendante du contrôle d'une facture : sa "
                      "dette n'existe qu'après validation.")
        elif statut in FACTURES_DETTE_OUVERTE:
            raison = ("Dette fournisseur ouverte : elle peut le rester à la clôture. Son écriture "
                      "d'achat, si elle est encore proposée, figure parmi les bloquants.")
        else:
            continue
        informatifs.append(_item(
            I_FACTURE, type_=f"Facture fournisseur {LIBELLES_FACTURE.get(statut, statut).lower()}",
            date=f["date_facture"], montant=f["montant_ttc"],
            origine=" — ".join(x for x in (f.get("fournisseur"), f.get("facture_ref")) if x),
            statut=LIBELLES_FACTURE.get(statut, statut), raison=raison, action="Aucune pour la clôture.",
            lien=f"/factures/{quote(f['facture_id_opaque'])}", lien_libelle="Ouvrir la facture"))

    familles = [{"cle": k, "libelle": v, "items": [b for b in bloquants if b["famille"] == k]}
                for k, v in FAMILLES.items()]
    familles_info = [{"cle": k, "libelle": v, "items": [i for i in informatifs if i["famille"] == k]}
                     for k, v in FAMILLES_INFO.items()]
    return {
        "mois": mois,
        "bloquants": bloquants, "nb_bloquants": len(bloquants),
        "informatifs": informatifs, "nb_informatifs": len(informatifs),
        "familles": familles, "familles_info": [f for f in familles_info if f["items"]],
        "nb_comptes_a_definir": nb_comptes,
        "message_comptes": (f"{nb_comptes} opération(s) nécessitent encore un compte comptable."
                            if nb_comptes else ""),
    }


def mois_concernes(*, contexte_flux: dict | None = None, db_path=None) -> set[str]:
    """Mois portant au moins un mouvement, une écriture ou une charge : ceux qu'une clôture doit
    pouvoir examiner même si le moteur de contrôles ne les connaît pas encore."""
    ctx = contexte_flux or contexte(db_path=db_path)
    out = {m["mois"] for m in ctx["mouvements"] if m.get("mois")}
    out |= {_txt(e["periode"])[:7] for e in ctx["ecritures"] if _txt(e["periode"])}
    out |= {_txt(c["date_charge"])[:7] for c in ctx["charges"] if _txt(c["date_charge"])}
    return {m for m in out if len(m) == 7}
