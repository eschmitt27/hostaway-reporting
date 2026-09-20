"""Suggestions de rapprochement Qonto — des raisons, pas un score opaque.

Pour une transaction bancaire, ce module cherche à quoi elle pourrait correspondre parmi ce que
l'application connaît déjà : créances propriétaires, dettes fournisseurs, charges. Il **ne
rapproche rien** et **n'impute rien** : il propose, avec les raisons écrites en clair, et laisse
la décision à un humain.

CE QUI EST REFUSÉ D'EMBLÉE
  · le mauvais sens. Un encaissement ne solde pas une dette, un décaissement n'éteint pas une
    créance. Ce n'est pas un malus, c'est une exclusion : un candidat du mauvais sens n'est pas
    « moins probable », il est impossible.
  · un document déjà soldé.

LE MONTANT SEUL NE PROUVE RIEN. Deux factures à 240 € et un virement de 240 € : le montant ne
départage pas. Quand plusieurs candidats s'appuient sur la même preuve, le résultat est AMBIGU et
l'écran le dit, au lieu de désigner arbitrairement le premier de la liste. C'est le cœur de la
règle : une preuve partagée par plusieurs candidats n'est pas une preuve.

NIVEAUX : FORT (référence ou faisceau concordant) · MOYEN (deux signaux) · FAIBLE (un indice) ·
AMBIGU (plusieurs candidats également plausibles).
"""
from __future__ import annotations

import re
import unicodedata
from datetime import date

from app.db.connection import get_db

FORT = "FORT"
MOYEN = "MOYEN"
FAIBLE = "FAIBLE"
AMBIGU = "AMBIGU"
AUCUN = "AUCUN"

LIBELLES_NIVEAU = {
    FORT: "Correspondance forte",
    MOYEN: "Correspondance probable",
    FAIBLE: "Piste à vérifier",
    AMBIGU: "Plusieurs candidats équivalents",
    AUCUN: "Aucun candidat",
}

# Fenêtre de date : un paiement arrive rarement le jour même de la facture, mais au-delà de deux
# mois le rapprochement par date ne veut plus rien dire.
FENETRE_JOURS = 60

# Poids. Volontairement petits et lisibles : ce qui compte est la LISTE DES RAISONS affichée, le
# total ne sert qu'à ordonner.
P_REFERENCE = 4      # le numéro du document figure dans le libellé ou la référence bancaire
P_MONTANT_SOLDE = 3  # le montant correspond exactement au reste à payer
P_TIERS = 3          # le nom du tiers apparaît dans le libellé ou la contrepartie
P_MONTANT_TOTAL = 2  # le montant correspond au total du document (mais pas à son solde)
P_DATE = 1           # la date tombe dans la fenêtre

SEUIL_FORT = 6
SEUIL_MOYEN = 4


def _sans_accent(texte: str) -> str:
    decompose = unicodedata.normalize("NFKD", str(texte or ""))
    return "".join(c for c in decompose if not unicodedata.combining(c)).lower()


def _mots(texte: str) -> set[str]:
    return {m for m in re.split(r"[^a-z0-9]+", _sans_accent(texte)) if len(m) >= 4}


def _proche(montant_a, montant_b) -> bool:
    """Égalité au centime. Aucune tolérance inventée : un écart est une question, pas un détail."""
    try:
        return abs(round(float(montant_a) - float(montant_b), 2)) < 0.005
    except (TypeError, ValueError):
        return False


def _jours(date_a: str, date_b: str) -> int | None:
    try:
        a = date.fromisoformat(str(date_a)[:10])
        b = date.fromisoformat(str(date_b)[:10])
        return abs((a - b).days)
    except (TypeError, ValueError):
        return None


def _noms_tiers(db_path=None) -> dict[str, str]:
    """Nom lisible d'un propriétaire ou d'un fournisseur. Absence de table = dictionnaire partiel,
    pas une erreur : le moteur perd un signal, il ne tombe pas."""
    noms: dict[str, str] = {}
    conn = get_db(db_path)
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "ref_proprietaires" in tables:
            for r in conn.execute(
                    "SELECT proprietaire_id, nom_proprietaire, prenom_proprietaire "
                    "FROM ref_proprietaires"):
                noms[r["proprietaire_id"]] = " ".join(
                    x for x in (r["prenom_proprietaire"], r["nom_proprietaire"]) if x)
        if "fournisseurs" in tables:
            for r in conn.execute("SELECT fournisseur_id_opaque, nom FROM fournisseurs"):
                noms[r["fournisseur_id_opaque"]] = r["nom"] or ""
    finally:
        conn.close()
    return noms


def _texte_transaction(mouvement: dict) -> str:
    return " ".join(str(mouvement.get(c) or "") for c in
                    ("libelle", "contrepartie", "reference", "note", "categorie_flux"))


def _evaluer(mouvement: dict, document: dict, nom_tiers: str) -> dict | None:
    """Confronte une transaction à un document. `None` = candidat impossible, pas faible."""
    solde = document.get("solde")
    if solde is not None and abs(float(solde)) < 0.005:
        return None  # déjà soldé : il n'y a plus rien à rapprocher

    texte = _texte_transaction(mouvement)
    sans_accent = _sans_accent(texte)
    raisons: list[str] = []
    points = 0

    numero = str(document.get("numero") or "").strip()
    if numero and len(numero) >= 4 and _sans_accent(numero) in sans_accent:
        points += P_REFERENCE
        raisons.append(f"Référence « {numero} » présente dans le libellé bancaire")

    montant = mouvement.get("montant")
    if _proche(montant, solde):
        points += P_MONTANT_SOLDE
        raisons.append("Montant exactement égal au reste à payer")
    elif _proche(montant, document.get("total")):
        points += P_MONTANT_TOTAL
        raisons.append("Montant exactement égal au total du document")

    if nom_tiers:
        communs = _mots(nom_tiers) & _mots(texte)
        if communs:
            points += P_TIERS
            raisons.append(f"Contrepartie concordante ({nom_tiers})")

    date_banque = (mouvement.get("regle_le") or mouvement.get("emis_le") or "")[:10]
    ecart = _jours(date_banque, document.get("date_facture"))
    if ecart is not None and ecart <= FENETRE_JOURS:
        points += P_DATE
        raisons.append(f"Date proche du document ({ecart} j)")

    # Une date proche n'est PAS une preuve : sur un mois donné, tous les documents sont « proches »
    # de tous les virements. Sans au moins un signal de fond — référence, montant ou tiers — il
    # n'y a pas de candidat, il y a du bruit. Proposer trois factures au hasard parce qu'elles
    # datent de la même semaine ferait perdre du temps et userait la confiance dans l'écran.
    if points <= P_DATE:
        return None

    return {
        "type": document.get("type"),
        "numero": numero or "(sans numéro)",
        "tiers": nom_tiers or document.get("tiers_id") or "",
        "total": document.get("total"),
        "solde": solde,
        "date_document": document.get("date_facture"),
        "points": points,
        "raisons": raisons,
        # Sert à détecter l'ambiguïté : deux candidats appuyés sur exactement les mêmes preuves
        # ne se départagent pas.
        "empreinte_preuves": tuple(sorted(r.split(" (")[0] for r in raisons)),
    }


def _documents(sens: str, db_path=None) -> list[dict]:
    """Candidats possibles selon le SENS. Un crédit ne peut viser qu'une créance, un débit qu'une
    dette ou une charge. Le mauvais sens n'est pas filtré plus tard : il n'entre jamais."""
    from app.services import creances_dettes_service as cd

    if sens == "credit":
        lignes = cd.creances(db_path=db_path)
        return [dict(x, type="CREANCE") for x in (lignes if isinstance(lignes, list)
                                                  else lignes.get("lignes", []))]
    if sens == "debit":
        lignes = cd.dettes(db_path=db_path)
        return [dict(x, type="DETTE") for x in (lignes if isinstance(lignes, list)
                                                else lignes.get("lignes", []))]
    return []


def suggerer(mouvement: dict, *, db_path=None, documents=None, noms=None) -> dict:
    """Candidats classés, avec leurs raisons. N'écrit rien, ne décide rien."""
    sens = (mouvement.get("sens") or "").lower()
    documents = _documents(sens, db_path) if documents is None else documents
    noms = _noms_tiers(db_path) if noms is None else noms

    candidats = []
    for document in documents:
        evaluation = _evaluer(mouvement, document, noms.get(document.get("tiers_id"), ""))
        if evaluation:
            candidats.append(evaluation)

    if not candidats:
        return {"niveau": AUCUN, "candidats": [],
                "message": "Aucun document connu ne correspond à ce mouvement."}

    candidats.sort(key=lambda c: (-c["points"], c["numero"]))
    meilleur = candidats[0]
    ex_aequo = [c for c in candidats if c["points"] == meilleur["points"]]

    # Plusieurs candidats appuyés sur les mêmes preuves : rien ne les départage. Le dire est la
    # seule réponse honnête — en désigner un serait un tirage au sort déguisé en certitude.
    if len(ex_aequo) > 1:
        return {"niveau": AMBIGU, "candidats": candidats[:5],
                "message": f"{len(ex_aequo)} documents correspondent aussi bien : "
                           "contrôle humain nécessaire."}

    if meilleur["points"] >= SEUIL_FORT:
        niveau = FORT
    elif meilleur["points"] >= SEUIL_MOYEN:
        niveau = MOYEN
    else:
        niveau = FAIBLE

    # Le montant seul, même unique, ne suffit pas à affirmer : il reste une piste.
    if meilleur["empreinte_preuves"] == ("Montant exactement égal au reste à payer",):
        niveau = FAIBLE

    return {"niveau": niveau, "candidats": candidats[:5],
            "message": LIBELLES_NIVEAU[niveau]}
