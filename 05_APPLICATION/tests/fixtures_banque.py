"""Fabrique de données Banque en SQLite, pour les tests.

Remplace les classeurs `BANQUE_LOT8_IMPORT.xlsx` que les tests fabriquaient. Les identifiants de
mouvement sont FOURNIS par l'appelant plutôt que tirés au hasard : un test qui affirme quelque chose
sur « MVT-004 » doit pouvoir le nommer.

Insertion en SQL direct, à dessein. Passer par `banque_mouvements_service.importer()` ferait dépendre
chaque test du bon fonctionnement de l'import, et interdirait de fabriquer un état incohérent — or
c'est précisément ce qu'il faut pour vérifier qu'un écran ne s'effondre pas devant.

Aucune donnée réelle : identifiants opaques, libellés inventés, propriétaires numérotés.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db
from app.services import banque_attentes_service as att
from app.services import banque_classification_service as cls
from app.services import banque_mouvements_service as bq

COMPTE = "CM_TEST_00000000001"
SENS_CREDIT = bq.SENS_CREDIT
IMPORT_ID = "IMP-BQ-TEST-001"
RUN_ID = "CLS-TEST00000001"

# Ce que la vue ne doit JAMAIS laisser sortir. Placé dans la contrepartie brute, qu'aucun écran
# n'affiche — le libellé, lui, est légitimement montré : la banque n'en fournit pas de version
# expurgée, et l'application n'en invente pas.
CONTREPARTIE_SENSIBLE = "SECRET-IBAN-FR7612345"


def mouvement(mid: str, date: str, libelle: str, montant: float, sens: str, *,
              statut_controle: str = cls.ST_VALIDE,
              statut_classification: str = cls.CLASS_CLASSE,
              type_flux: str = "TYPE_FLUX_017", compte: str = COMPTE,
              tiers: str = "", categorie: str = "", niveau_risque: str = "",
              regle_id: str = "R_001", codes_anomalie: str = "",
              niveau_anomalie: str = "", ligne_source: int | None = None,
              classe: bool = True, devise: str = "EUR", fingerprint: str | None = None,
              import_id: str | None = None, date_valeur: str | None = None,
              source_economique: str | None = None) -> dict[str, Any]:
    """Un mouvement et sa classification. `classe=False` en fabrique un non encore classé.

    `devise`, `fingerprint` et `date_valeur` sont réglables pour permettre de fabriquer des lignes
    ANORMALES — devise étrangère, empreinte absente. Les contrôles structurels existent précisément
    pour détecter celles-là : une fabrique incapable de produire autre chose que des lignes valides
    les rendrait intestables.
    """
    return {"mouvement_id": mid, "date_operation": date,
            "date_valeur": date if date_valeur is None else date_valeur, "libelle": libelle,
            "montant": montant, "sens": sens, "compte": compte, "ligne_source": ligne_source,
            "classe": classe, "statut_controle": statut_controle,
            "statut_classification": statut_classification, "type_flux": type_flux, "tiers": tiers,
            "categorie": categorie, "niveau_risque": niveau_risque, "regle_id": regle_id,
            "codes_anomalie": codes_anomalie, "niveau_anomalie": niveau_anomalie,
            "devise": devise, "fingerprint": fingerprint, "import_id": import_id,
            "source_economique": source_economique}


def attente(mid: str, montant: float, motif: str, *, tiers: str = "", reference: str = "",
            commentaire: str = "") -> dict[str, Any]:
    return {"mouvement_id": mid, "montant": montant, "motif": motif, "tiers": tiers,
            "reference": reference, "commentaire": commentaire}


def controle(mid: str, code: str, description: str, *, severite: str = cls.ST_A_CONTROLER,
             origine: str = cls.ORIGINE_CLASSIFICATION) -> dict[str, Any]:
    return {"mouvement_id": mid, "code": code, "description": description, "severite": severite,
            "origine": origine}


def _empreinte(m: dict[str, Any]) -> str:
    """Empreinte du mouvement. Une valeur FOURNIE est respectée, y compris vide.

    Distinguer « non précisée » (None → on calcule) de « absente » (chaîne vide → on la laisse
    absente) est nécessaire : le contrôle d'identifiant stable cherche précisément l'empreinte
    manquante, et la recalculer lui retirerait son objet.
    """
    if m.get("fingerprint") is not None:
        return m["fingerprint"]
    return bq.empreinte(m["compte"], m["date_operation"], m["montant"], m["libelle"],
                        date_valeur=m["date_valeur"], sens=m["sens"],
                        devise=m.get("devise") or "EUR")


def construire(db_path, *, mouvements: list[dict] = (), attentes: list[dict] = (),
               controles: list[dict] = (), date_import: str = "2026-03-15T08:00:00Z",
               run_id: str = RUN_ID) -> None:
    """Écrit un jeu Banque complet dans la base indiquée."""
    import json

    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO banque_import_source (import_id, bank_account_id, source_type, "
            "source_filename, source_sha256, date_min, date_max, nb_lignes, nb_inseres, "
            "nb_doublons, nb_a_controler, date_import, run_id, statut) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'IMPORTE')",
            (IMPORT_ID, COMPTE, bq.SOURCE_HISTORIQUE, "RELEVE_TEST.xlsx", "0" * 64,
             min((m["date_operation"] for m in mouvements), default=""),
             max((m["date_operation"] for m in mouvements), default=""),
             len(mouvements), len(mouvements), 0, 0, date_import, "RUN-TEST-001"))

        for i, m in enumerate(mouvements, start=1):
            conn.execute(
                "INSERT INTO banque_mouvements (mouvement_id_opaque, import_id, bank_account_id, "
                "external_transaction_id, date_operation, date_valeur, sens, montant, devise, "
                "libelle_brut, contrepartie_brute, fingerprint, ligne_source) "
                "VALUES (?,?,?,'',?,?,?,?,?,?,?,?,?)",
                (m["mouvement_id"], m.get("import_id") or IMPORT_ID, m["compte"],
                 m["date_operation"], m["date_valeur"], m["sens"], m["montant"],
                 m.get("devise") or "EUR", m["libelle"], CONTREPARTIE_SENSIBLE,
                 _empreinte(m), m["ligne_source"] if m["ligne_source"] is not None else i))

            if not m["classe"]:
                continue
            conn.execute(
                "INSERT INTO banque_classifications (mouvement_id_opaque, classification_run_id, "
                "regle_id, categorie, tiers_detecte, type_flux_id, code_impact, source_economique, "
                "statut_controle, statut_classification, niveau_risque, rapprochement_requis) "
                "VALUES (?,?,?,?,?,?,'',?,?,?,?,'')",
                (m["mouvement_id"], run_id, m["regle_id"], m["categorie"], m["tiers"],
                 m["type_flux"], m.get("source_economique") or cls.SOURCE_REGLE,
                 m["statut_controle"], m["statut_classification"], m["niveau_risque"]))
            conn.execute(
                "INSERT INTO banque_classification_signaux (mouvement_id_opaque, "
                "classification_run_id, niveau_anomalie, codes_anomalie) VALUES (?,?,?,?)",
                (m["mouvement_id"], run_id, m["niveau_anomalie"], m["codes_anomalie"]))

        for c in controles:
            conn.execute(
                "INSERT OR IGNORE INTO banque_controles (mouvement_id_opaque, "
                "classification_run_id, origine, code_controle, severite, description, "
                "statut_controle) VALUES (?,?,?,?,?,?,?)",
                (c["mouvement_id"], run_id, c["origine"], c["code"], c["severite"],
                 c["description"], cls.ST_A_CONTROLER))

        for a in attentes:
            type_objet = (att.TYPE_REVERSEMENT_PROPRIETAIRE
                          if a["motif"] == att.ATTENTE_SAISIE_ACOMPTE
                          else att.TYPE_PAYOUT_PLATEFORME)
            conn.execute(
                "INSERT INTO banque_rapprochements (rapprochement_id_opaque, mouvement_id_opaque, "
                "type_objet, objet_id, montant_rapproche, statut, source, criteres_json, "
                "commentaire) VALUES (?,?,?,NULL,?,?,?,?,?)",
                ("BRP-" + a["mouvement_id"], a["mouvement_id"], type_objet, a["montant"],
                 att.ST_PROPOSE, att.SOURCE_AUTO,
                 json.dumps({"motif_attente": a["motif"], "tiers_detecte": a["tiers"],
                             "reference": a["reference"]}, ensure_ascii=False),
                 a["commentaire"]))
        conn.commit()
    finally:
        conn.close()


def peupler_non_classes(db_path, mois: list[str] | None = None, *, par_mois: int = 2) -> list[str]:
    """Mouvements non classés, sur les mois demandés — pour les écrans de contrôle.

    Les contrôles agrégés du moteur portent un MOIS. Un mouvement fabriqué sur un autre mois n'ouvre
    aucun agrégat, et le détail semble vide à tort : les mois doivent donc venir de ce que le moteur a
    réellement produit, pas d'une date choisie au hasard.

    Retourne les identifiants créés.
    """
    from app.services import banque_classification_service as cls_svc

    mois = mois or ["2026-06"]
    mouvements = []
    for i, m in enumerate(mois, start=1):
        for j in range(1, par_mois + 1):
            mouvements.append(mouvement(
                f"MVT-NC-{i:02d}{j:02d}", f"{m}-0{j}", f"VIR A RAPPROCHER {i}{j}",
                100.0 * i + j, SENS_CREDIT, compte="CM_TEST", tiers="AIRBNB",
                statut_classification=cls_svc.CLASS_RAPPROCHEMENT_REQUIS,
                statut_controle=cls_svc.ST_A_CONTROLER))
    construire(db_path, mouvements=mouvements)
    return [m["mouvement_id"] for m in mouvements]


def mois_des_agregats_banque() -> list[str]:
    """Mois pour lesquels le moteur a produit un agrégat bancaire, triés."""
    from app.services import controles_cloture_service as base

    return sorted({v["mois"] for v in base._toutes_les_vues()
                   if v.get("module") == "BANQUE" and v.get("mois")})
