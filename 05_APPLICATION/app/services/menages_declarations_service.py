"""Saisie directe d'une déclaration de ménage interne — UI → SQLite, zéro Google Sheet.

`menages_declarations_internes` (migration 0038) était jusqu'ici alimentée uniquement par
`lot6b_m04_menages_internes.py` (Google Sheet M04). Ce module permet de créer une ligne
directement, avec EXACTEMENT les mêmes champs et la MÊME règle de coût que lot6b — reprise
verbatim de `lib_menage_costs.resolve_internal_cleaning_cost` (02_TRAVAIL), jamais réinventée.

La Google Sheet reste un chemin d'import possible (legacy), mais n'est plus nécessaire pour
qu'une nouvelle déclaration soit créée.
"""
from __future__ import annotations

import sys
from typing import Any

import app.config as cfg
from app.db.connection import get_db

_TRAVAIL_DIR = str(cfg.PROJECT_ROOT / "02_TRAVAIL")
if _TRAVAIL_DIR not in sys.path:
    sys.path.insert(0, _TRAVAIL_DIR)

SOURCE_UI = "UI_SAISIE"
SOURCE_APPLICATION = "APPLICATION"
SOURCE_GOOGLE_SHEET = "GOOGLE_SHEET"

E_LOGEMENT_INCONNU = "E_LOGEMENT_INCONNU"
E_INTERVENANT_INCONNU = "E_INTERVENANT_INCONNU"
E_MOIS_INVALIDE = "E_MOIS_INVALIDE"
E_DECLARATION_INCONNUE = "E_DECLARATION_INCONNUE"
E_JUSTIFICATION_REQUISE = "E_JUSTIFICATION_REQUISE"
E_MOIS_CLOTURE = "E_MOIS_CLOTURE"


def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def mois_cloture(mois: str, db_path=None) -> bool:
    return _mois_cloture(mois, db_path=db_path)


def _mois_cloture(mois: str, db_path=None) -> bool:
    """Vrai si le mois est CLOTURE (`ref_cloture_mensuelle`, alimentée par `clotures_service.
    archiver()` — migration 0064) : une déclaration d'un mois clos ne se modifie jamais en place ;
    la correction rétroactive existante (`assiette_correction_service`, migration 0065) est le seul
    chemin pour un mois clos, pas réinventée ici."""
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT statut_mois FROM ref_cloture_mensuelle WHERE mois = ?", (mois,)).fetchone()
        return bool(row and row["statut_mois"] == "CLOTURE")
    finally:
        conn.close()


def _extra(conn, mois: str, logement_id: str, intervenant_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM menages_declarations_extra WHERE mois=? AND logement_id=? AND intervenant_id=?",
        (mois, logement_id, intervenant_id),
    ).fetchone()
    return dict(row) if row else None


def _historiser(conn, *, mois: str, logement_id: str, intervenant_id: str, champ: str,
                ancienne_valeur, nouvelle_valeur, justification: str, auteur: str,
                source: str) -> None:
    conn.execute(
        "INSERT INTO menages_declarations_historique "
        "(mois, logement_id, intervenant_id, champ, ancienne_valeur, nouvelle_valeur, "
        "justification, auteur, source, date_heure) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (mois, logement_id, intervenant_id, champ,
         None if ancienne_valeur is None else str(ancienne_valeur),
         None if nouvelle_valeur is None else str(nouvelle_valeur),
         justification or None, auteur, source, _now()),
    )


def declaration_extra(mois: str, logement_id: str, intervenant_id: str,
                      db_path=None) -> dict[str, Any] | None:
    """Champs applicatifs (supplément/coût final/source/historique) d'une déclaration, ou None
    si aucune extension n'a jamais été créée (déclaration jamais éditée depuis l'application)."""
    conn = get_db(db_path)
    try:
        return _extra(conn, mois, logement_id, intervenant_id)
    finally:
        conn.close()


def historique(mois: str, logement_id: str, intervenant_id: str,
               db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM menages_declarations_historique WHERE mois=? AND logement_id=? "
            "AND intervenant_id=? ORDER BY id DESC", (mois, logement_id, intervenant_id),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def logements_actifs(db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT logement_id, COALESCE(nom_court, nom_logement_officiel) AS libelle "
            "FROM ref_logements WHERE actif = 'OUI' ORDER BY libelle"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def intervenants_actifs(db_path=None) -> list[dict[str, Any]]:
    """Intervenants INTERNES uniquement — un ménage externe se déclare par facture PDF (§11 : une
    facture externe ne devient jamais une déclaration interne)."""
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT intervenant_id, nom_intervenant AS libelle, type_intervenant "
            "FROM ref_intervenants WHERE actif = 'OUI' AND type_intervenant = 'INTERNE' "
            "ORDER BY libelle"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _logement(conn, logement_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT logement_id, COALESCE(nom_court, nom_logement_officiel) AS libelle, "
        "type_logement_id FROM ref_logements WHERE logement_id = ? AND actif = 'OUI'",
        (logement_id,),
    ).fetchone()
    return dict(row) if row else None


def _intervenant(conn, intervenant_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT intervenant_id, nom_intervenant AS libelle, type_intervenant "
        "FROM ref_intervenants WHERE intervenant_id = ? AND actif = 'OUI'",
        (intervenant_id,),
    ).fetchone()
    return dict(row) if row else None


def creer(*, mois: str, logement_id: str, intervenant_id: str, nb_menages: int,
          nb_heures: float | None, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Crée une déclaration interne réelle. Statut `A_CONTROLER` si le coût ne se résout pas
    (référentiel absent/ambigu) — jamais un coût inventé pour faire disparaître l'anomalie."""
    from lib_menage_costs import resolve_internal_cleaning_cost

    if not mois or len(mois) != 7 or mois[4] != "-":
        return {"ok": False, "code": E_MOIS_INVALIDE, "message": "Mois attendu au format AAAA-MM."}

    conn = get_db(db_path)
    try:
        logement = _logement(conn, logement_id)
        if logement is None:
            return {"ok": False, "code": E_LOGEMENT_INCONNU,
                    "message": "Logement inconnu ou inactif."}
        intervenant = _intervenant(conn, intervenant_id)
        if intervenant is None:
            return {"ok": False, "code": E_INTERVENANT_INCONNU,
                    "message": "Intervenant inconnu ou inactif."}
        if intervenant.get("type_intervenant") != "INTERNE":
            return {"ok": False, "code": E_INTERVENANT_INCONNU,
                    "message": "Une déclaration interne ne peut porter que sur un intervenant "
                               "INTERNE — un ménage externe se déclare par facture fournisseur."}

        hourly_ref = [dict(r) for r in conn.execute(
            "SELECT * FROM ref_taux_heures_menage").fetchall()]
        fixed_ref = [dict(r) for r in conn.execute(
            "SELECT * FROM ref_couts_menage_interne").fetchall()]

        cost = resolve_internal_cleaning_cost(
            ref_date=f"{mois}-01",
            intervenant_id=intervenant_id,
            logement_id=logement_id,
            type_logement_id=logement.get("type_logement_id"),
            nb_menages=nb_menages,
            nb_heures=nb_heures,
            hourly_rows=hourly_ref,
            fixed_rows=fixed_ref,
        )
        cout = cost.total if cost.status == "OK" else None
        statut_controle = "OK" if cost.status == "OK" else "A_CONTROLER"
        code_controle = None if cost.status == "OK" else f"COUT_INTERNE_{cost.status}"

        cur = conn.execute(
            "INSERT INTO menages_declarations_internes "
            "(mois, annee, mois_saisie, nom_appartement, logement_id, nom_intervenant, "
            "intervenant_id, type_intervenant, nb_menages, nb_heures, cout_lavage_attribue, "
            "statut_controle, code_controle, source_url, date_extraction, run_id) "
            "VALUES (?,?,strftime('%Y-%m-%dT%H:%M:%SZ','now'),?,?,?,?,?,?,?,?,?,?,?,"
            "strftime('%Y-%m-%dT%H:%M:%SZ','now'),?)",
            (mois, mois[:4], logement["libelle"], logement_id, intervenant["libelle"],
             intervenant_id, intervenant.get("type_intervenant"), nb_menages, nb_heures, cout,
             statut_controle, code_controle, SOURCE_UI, acteur),
        )
        conn.commit()
        return {"ok": True, "id": cur.lastrowid, "statut_controle": statut_controle,
                "cout_lavage_attribue": cout, "code_controle": code_controle}
    finally:
        conn.close()


def _declaration_interne(conn, mois: str, logement_id: str, intervenant_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM menages_declarations_internes WHERE mois=? AND logement_id=? "
        "AND intervenant_id=? ORDER BY id DESC LIMIT 1", (mois, logement_id, intervenant_id),
    ).fetchone()
    return dict(row) if row else None


def modifier(*, mois: str, logement_id: str, intervenant_id: str, nb_menages: int | None = None,
            supplement: float | None = None, justification_supplement: str = "",
            acteur: str = "", db_path=None) -> dict[str, Any]:
    """Édite nb_menages et/ou supplément d'une déclaration EXISTANTE (§A/A1/A2).

    Règles absolues :
    - justification obligatoire SI ET SEULEMENT SI le supplément final est != 0 (§A) ;
    - le supplément n'altère JAMAIS le coût standard calculé (§A2) — il s'AJOUTE ;
    - chaque champ modifié est historisé (auteur/date/ancienne/nouvelle valeur/justification/source),
      jamais silencieusement écrasé (§A1) ;
    - AUCUNE facture/charge/écriture/mouvement bancaire n'est créée ici (§A3) — cette fonction
      n'écrit que menages_declarations_internes/_extra/_historique.
    """
    from lib_menage_costs import resolve_internal_cleaning_cost

    if _mois_cloture(mois, db_path=db_path):
        return {"ok": False, "code": E_MOIS_CLOTURE,
                "message": "Mois clôturé : utiliser la correction rétroactive existante, "
                           "pas une modification directe."}

    conn = get_db(db_path)
    try:
        declaration = _declaration_interne(conn, mois, logement_id, intervenant_id)
        if declaration is None:
            return {"ok": False, "code": E_DECLARATION_INCONNUE,
                    "message": "Aucune déclaration existante pour cette clé — utiliser creer()."}
        logement = _logement(conn, logement_id)
        if logement is None:
            return {"ok": False, "code": E_LOGEMENT_INCONNU, "message": "Logement inconnu ou inactif."}

        extra = _extra(conn, mois, logement_id, intervenant_id) or {}
        ancien_nb = declaration.get("nb_menages")
        ancien_supplement = extra.get("supplement") or 0

        nouveau_nb = ancien_nb if nb_menages is None else nb_menages
        nouveau_supplement = ancien_supplement if supplement is None else supplement
        justification_supplement = (justification_supplement or "").strip()

        if nouveau_supplement and nouveau_supplement != 0 and not justification_supplement:
            return {"ok": False, "code": E_JUSTIFICATION_REQUISE,
                    "message": "Une justification est obligatoire dès que le supplément est différent de 0."}

        hourly_ref = [dict(r) for r in conn.execute("SELECT * FROM ref_taux_heures_menage").fetchall()]
        fixed_ref = [dict(r) for r in conn.execute("SELECT * FROM ref_couts_menage_interne").fetchall()]
        cost = resolve_internal_cleaning_cost(
            ref_date=f"{mois}-01", intervenant_id=intervenant_id, logement_id=logement_id,
            type_logement_id=logement.get("type_logement_id"), nb_menages=nouveau_nb,
            nb_heures=declaration.get("nb_heures"), hourly_rows=hourly_ref, fixed_rows=fixed_ref,
        )
        cout_standard_calcule = cost.total if cost.status == "OK" else None
        cout_final = (round(cout_standard_calcule + nouveau_supplement, 2)
                      if cout_standard_calcule is not None else None)

        champs_modifies = []
        if nouveau_nb != ancien_nb:
            _historiser(conn, mois=mois, logement_id=logement_id, intervenant_id=intervenant_id,
                       champ="nb_menages", ancienne_valeur=ancien_nb, nouvelle_valeur=nouveau_nb,
                       justification=justification_supplement, auteur=acteur, source=SOURCE_APPLICATION)
            champs_modifies.append("nb_menages")
        if nouveau_supplement != ancien_supplement:
            _historiser(conn, mois=mois, logement_id=logement_id, intervenant_id=intervenant_id,
                       champ="supplement", ancienne_valeur=ancien_supplement,
                       nouvelle_valeur=nouveau_supplement, justification=justification_supplement,
                       auteur=acteur, source=SOURCE_APPLICATION)
            champs_modifies.append("supplement")

        conn.execute(
            "UPDATE menages_declarations_internes SET nb_menages=? WHERE mois=? AND logement_id=? "
            "AND intervenant_id=?", (nouveau_nb, mois, logement_id, intervenant_id))
        conn.execute(
            "INSERT INTO menages_declarations_extra (mois, logement_id, intervenant_id, supplement, "
            "justification_supplement, cout_standard_calcule, cout_final, source, "
            "derniere_modification_app, date_modification) "
            "VALUES (?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(mois, logement_id, intervenant_id) DO UPDATE SET "
            "supplement=excluded.supplement, justification_supplement=excluded.justification_supplement, "
            "cout_standard_calcule=excluded.cout_standard_calcule, cout_final=excluded.cout_final, "
            "source='APPLICATION', derniere_modification_app=excluded.derniere_modification_app, "
            "date_modification=excluded.date_modification",
            (mois, logement_id, intervenant_id, nouveau_supplement,
             justification_supplement or None, cout_standard_calcule, cout_final,
             SOURCE_APPLICATION, _now(), _now()))
        conn.commit()
        return {"ok": True, "mois": mois, "nb_menages": nouveau_nb, "supplement": nouveau_supplement,
                "cout_standard_calcule": cout_standard_calcule, "cout_final": cout_final,
                "champs_modifies": champs_modifies}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Conflits Google Sheet <-> Application (§B2) — résolution humaine explicite
# ---------------------------------------------------------------------------

def lister_conflits(*, statut: str = "OUVERT", db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        q = "SELECT * FROM menages_declarations_conflits"
        params: tuple = ()
        if statut:
            q += " WHERE statut = ?"
            params = (statut,)
        rows = conn.execute(q + " ORDER BY date_detection DESC", params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def resoudre_conflit(conflit_id: int, *, choix: str, acteur: str = "",
                     db_path=None) -> dict[str, Any]:
    """`choix` = GARDER_APPLICATION | REPRENDRE_SHEET. Jamais de fusion silencieuse."""
    if choix not in ("GARDER_APPLICATION", "REPRENDRE_SHEET"):
        return {"ok": False, "message": "Choix invalide."}
    conn = get_db(db_path)
    try:
        c = conn.execute(
            "SELECT * FROM menages_declarations_conflits WHERE id = ? AND statut = 'OUVERT'",
            (conflit_id,)).fetchone()
        if c is None:
            return {"ok": False, "message": "Conflit introuvable ou déjà résolu."}
        c = dict(c)
        statut_final = "RESOLU_GARDE_APPLICATION" if choix == "GARDER_APPLICATION" else "RESOLU_REPRIS_SHEET"
        if choix == "REPRENDRE_SHEET":
            nouveau_nb = int(c["valeur_sheet"])
            conn.execute(
                "UPDATE menages_declarations_internes SET nb_menages=? WHERE mois=? AND logement_id=? "
                "AND intervenant_id=?", (nouveau_nb, c["mois"], c["logement_id"], c["intervenant_id"]))
            conn.execute(
                "INSERT INTO menages_declarations_extra (mois, logement_id, intervenant_id, source) "
                "VALUES (?,?,?,'GOOGLE_SHEET') "
                "ON CONFLICT(mois, logement_id, intervenant_id) DO UPDATE SET source='GOOGLE_SHEET'",
                (c["mois"], c["logement_id"], c["intervenant_id"]))
            _historiser(conn, mois=c["mois"], logement_id=c["logement_id"],
                       intervenant_id=c["intervenant_id"], champ="nb_menages",
                       ancienne_valeur=c["valeur_application"], nouvelle_valeur=c["valeur_sheet"],
                       justification="Conflit résolu : reprise valeur Google Sheet", auteur=acteur,
                       source=SOURCE_GOOGLE_SHEET)
        else:
            _historiser(conn, mois=c["mois"], logement_id=c["logement_id"],
                       intervenant_id=c["intervenant_id"], champ="nb_menages",
                       ancienne_valeur=c["valeur_application"], nouvelle_valeur=c["valeur_application"],
                       justification="Conflit résolu : valeur application conservée", auteur=acteur,
                       source=SOURCE_APPLICATION)
        conn.execute(
            "UPDATE menages_declarations_conflits SET statut=?, resolu_par=?, resolu_le=? WHERE id=?",
            (statut_final, acteur, _now(), conflit_id))
        conn.commit()
        return {"ok": True, "statut": statut_final}
    finally:
        conn.close()
