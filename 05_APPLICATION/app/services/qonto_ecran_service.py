"""Ce que l'écran Banque montre de Qonto — et, tout aussi important, ce qu'il ne montre pas.

Ce module transforme la couche RAW en lignes lisibles. Il est la SEULE porte entre les tables
`qonto_*` et un gabarit : aucune vue ne lit la base directement, ce qui rend vérifiable ce qui
sort.

NE SORTENT JAMAIS D'ICI :
  · `QONTO_SECRET_KEY`, l'en-tête `Authorization`, quoi que ce soit d'authentification — rien de
    tout cela n'existe en base, et rien ici ne va le chercher ;
  · l'IBAN complet — masqué en `FR76 **** **** 1234` ;
  · `charge_utile_json`, l'empreinte, les identifiants de synchronisation : de la plomberie ;
  · les identifiants techniques (UUID, `transaction_id`) DÈS LORS qu'une information humaine
    existe. Une contrepartie et un libellé disent ce qu'est un mouvement ; un UUID ne dit rien à
    personne. Il n'est donc pas rendu — même pas en clé de boucle.

AUCUN EFFET COMPTABLE. Ce module lit. Il n'écrit rien, ne rapproche rien, ne règle rien.
"""
from __future__ import annotations

from app.adapters import qonto_client
from app.services import qonto_raw_service as raw
from app.services import qonto_statut_local_service as statut_local

# Libellés humains des états de Qonto. Le code brut reste disponible pour le style, mais c'est la
# phrase qui est lue.
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
    "direct_debit_hold": "Prélèvement en attente",
}

SENS = {"credit": "Crédit", "debit": "Débit"}


def _date_courte(valeur) -> str:
    """`2026-09-11T08:00:00.000Z` → `11/09/2026`. Valeur absente → tiret, jamais une date inventée."""
    if not valeur:
        return "—"
    texte = str(valeur)[:10]
    morceaux = texte.split("-")
    if len(morceaux) != 3:
        return texte
    annee, mois, jour = morceaux
    return f"{jour}/{mois}/{annee}"


def _reference_utile(mouvement: dict) -> str:
    """La référence n'est montrée que si elle APPREND quelque chose.

    Qonto recopie souvent le libellé dans `reference` : l'afficher deux fois ne renseigne
    personne et encombre la ligne. Une note saisie dans Qonto, en revanche, est de l'information
    humaine et prend le pas.
    """
    libelle = (mouvement.get("libelle") or "").strip()
    reference = (mouvement.get("reference") or "").strip()
    note = (mouvement.get("note") or "").strip()
    if note and note != libelle:
        return note
    if reference and reference.lower() != libelle.lower():
        return reference
    return ""


def ligne_transaction(mouvement: dict) -> dict:
    """Une ligne prête à afficher. Ne contient aucun identifiant technique."""
    statut_qonto = (mouvement.get("statut") or "").lower()
    definitif = bool(mouvement.get("comptabilisable"))
    contrepartie = (mouvement.get("contrepartie") or "").strip()
    libelle = (mouvement.get("libelle") or "").strip()
    return {
        # Pour une opération en attente, Qonto n'a pas encore de date de règlement : on montre la
        # date d'émission en le DISANT, plutôt qu'une case vide ou une date qui ferait croire à un
        # règlement.
        "date": _date_courte(mouvement.get("regle_le") or mouvement.get("emis_le")),
        "date_est_prevue": not mouvement.get("regle_le"),
        "montant": mouvement.get("montant"),
        "devise": mouvement.get("devise") or "EUR",
        "sens": SENS.get((mouvement.get("sens") or "").lower(), mouvement.get("sens") or "—"),
        "sens_code": (mouvement.get("sens") or "").lower(),
        "statut_qonto": STATUTS_QONTO.get(statut_qonto, mouvement.get("statut") or "—"),
        "statut_qonto_code": statut_qonto,
        "type": TYPES_QONTO.get((mouvement.get("type_operation") or "").lower(),
                                mouvement.get("type_operation") or "—"),
        # Le libellé porte l'information humaine ; à défaut, on l'assume au lieu d'afficher un UUID.
        "libelle": libelle or "(sans libellé)",
        "contrepartie": contrepartie or "",
        "reference": _reference_utile(mouvement),
        "statut_local": statut_local.LIBELLES.get(mouvement.get("statut_local") or "",
                                                  mouvement.get("statut_local") or "—"),
        "definitif": definitif,
        "motif_non_definitif": mouvement.get("motif_non_comptabilisable") or "",
    }


def _mouvements_avec_statut(db_path=None) -> list[dict]:
    """Jointure RAW ↔ statut applicatif, sans faire remonter les colonnes de plomberie."""
    from app.db.connection import get_db
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT t.montant, t.devise, t.sens, t.statut, t.type_operation, t.libelle, "
            "       t.reference, t.note, t.contrepartie, t.emis_le, t.regle_le, "
            "       s.statut_local, s.comptabilisable, s.motif_non_comptabilisable "
            "  FROM qonto_transactions_raw t "
            "  LEFT JOIN qonto_transactions_statut_local s "
            "    ON s.qonto_transaction_uuid = t.qonto_transaction_uuid "
            " ORDER BY COALESCE(t.regle_le, t.emis_le, t.cree_le) DESC")]
    finally:
        conn.close()


def tableau_de_bord(*, db_path=None) -> dict:
    """Tout ce dont l'écran Banque a besoin côté Qonto, déjà formaté et déjà masqué."""
    if not raw.tables_presentes(db_path=db_path):
        return {"disponible": False,
                "message": "Tables Qonto absentes : redémarrez l'application pour migrer la base."}

    comptes = raw.comptes(db_path=db_path)
    lignes = [ligne_transaction(m) for m in _mouvements_avec_statut(db_path)]
    derniere = raw.derniere_synchronisation(db_path=db_path)
    compteurs = statut_local.resume(db_path=db_path)

    total_solde = sum((c.get("solde") or 0) for c in comptes)
    total_dispo = sum((c.get("solde_autorise") or 0) for c in comptes)
    devise = comptes[0]["devise"] if comptes else "EUR"

    return {
        "disponible": True,
        "identifiants_presents": qonto_client.identifiants_presents(),
        "comptes": comptes,
        "solde": total_solde,
        "solde_disponible": total_dispo,
        "devise": devise,
        # L'écart entre les deux soldes n'est pas un détail : c'est exactement ce que les
        # opérations non encore réglées immobilisent.
        "ecart_soldes": round(total_solde - total_dispo, 2),
        "transactions": lignes,
        "nb_transactions": len(lignes),
        "a_rapprocher": compteurs["a_rapprocher"],
        "non_definitifs": compteurs["non_definitifs"],
        "derniere_synchronisation": {
            "horodatage": _horodatage_lisible(derniere.get("termine_le")
                                              or derniere.get("demarre_le")),
            "statut": derniere.get("statut") or "",
            "code_erreur": derniere.get("code_erreur") or "",
        } if derniere else {},
    }


def _horodatage_lisible(valeur) -> str:
    """`2026-09-20T15:57:13Z` → `20/09/2026 à 15:57`. Jamais de seconde : personne n'en a besoin."""
    if not valeur:
        return "jamais"
    texte = str(valeur)
    date = _date_courte(texte)
    heure = texte[11:16] if len(texte) >= 16 else ""
    return f"{date} à {heure}" if heure else date
