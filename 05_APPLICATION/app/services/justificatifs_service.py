"""Justificatifs — référence documentaire, dossier canonique, présence confirmée ou absence justifiée.

(Mission 36.) Chaque charge — et chaque facture fournisseur — reçoit dès sa création une référence
HUMAINE, distincte de son identifiant technique :

    CHG-2026-09-001      charge de septembre 2026, première de la série du mois
    FAF-2026-09-001      facture fournisseur

La pièce se range dans un dossier CANONIQUE, déterminé par la date de l'objet :

    01_SOURCES_BRUTES/Justificatifs/Charges/2026/09/CHG-2026-09-001__GIFI.pdf
    01_SOURCES_BRUTES/Justificatifs/FacturesFournisseurs/2026/09/FAF-2026-09-001__…pdf

Le nom commence par la référence : c'est ce qui permet de VÉRIFIER la présence du fichier, au lieu
de croire l'utilisateur sur parole. (`01_SOURCES_BRUTES` est la racine des sources brutes du
projet ; `Justificatifs/` est exclu du dépôt git : une pièce réelle n'y est jamais versionnée.)

Deux réponses, et seulement deux, closent la question :
    JUSTIFICATIF_ARCHIVE           le fichier est dans le dossier (vérifié) ;
    JUSTIFICATIF_ABSENT_JUSTIFIE   il n'y est pas, et une justification écrite dit pourquoi
                                   (« ticket perdu — duplicata demandé »). Sans elle : refus.
Chaque réponse est historisée (réponse, justification, fichier constaté, auteur, date).

Une référence n'est jamais rendue ni réattribuée (la séquence ne garde que son dernier numéro), et
la table refuse la suppression (migration 0114).
"""
from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db

OBJET_CHARGE = "CHARGE"
OBJET_FACTURE_FOURNISSEUR = "FACTURE_FOURNISSEUR"
PREFIXES = {OBJET_CHARGE: "CHG", OBJET_FACTURE_FOURNISSEUR: "FAF"}
SOUS_DOSSIERS = {OBJET_CHARGE: "Charges", OBJET_FACTURE_FOURNISSEUR: "FacturesFournisseurs"}

ST_A_CONFIRMER = "A_CONFIRMER"
ST_ARCHIVE = "JUSTIFICATIF_ARCHIVE"
ST_ABSENT_JUSTIFIE = "JUSTIFICATIF_ABSENT_JUSTIFIE"
LIBELLES_STATUT = {
    ST_A_CONFIRMER: "Justificatif à confirmer",
    ST_ARCHIVE: "Justificatif archivé",
    ST_ABSENT_JUSTIFIE: "Justificatif absent — absence justifiée",
}

E_OBJET = "J01_OBJET_INCONNU"
E_REPONSE = "J02_REPONSE_OBLIGATOIRE"
E_JUSTIFICATION = "J03_JUSTIFICATION_OBLIGATOIRE"
E_FICHIER = "J04_FICHIER_INTROUVABLE"
E_ACTEUR = "J05_AUTEUR_OBLIGATOIRE"
E_SANS_REFERENCE = "J06_SANS_REFERENCE"


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "code": code, "message": message}


def _mois(date_objet: Any) -> tuple[str, str]:
    texte = _txt(date_objet)[:10]
    try:
        d = date.fromisoformat(texte)
    except ValueError:
        d = date.today()
    return f"{d.year:04d}", f"{d.month:02d}"


def racine() -> Path:
    return Path(getattr(cfg, "JUSTIFICATIFS_ROOT", Path(cfg.PROJECT_ROOT) / "01_SOURCES_BRUTES"
                        / "Justificatifs"))


def dossier(objet_type: str, date_objet: Any) -> Path:
    annee, mois = _mois(date_objet)
    return racine() / SOUS_DOSSIERS[objet_type] / annee / mois


def dossier_affiche(chemin: Path | str) -> str:
    """Le dossier tel qu'on le montre : relatif au projet quand il y est, sinon absolu."""
    p = Path(chemin)
    try:
        return p.resolve().relative_to(Path(cfg.PROJECT_ROOT).resolve()).as_posix() + "/"
    except ValueError:
        return str(p)


def _serie(objet_type: str, date_objet: Any) -> str:
    annee, mois = _mois(date_objet)
    return f"{PREFIXES[objet_type]}-{annee}-{mois}"


def formater(serie: str, numero: int) -> str:
    return f"{serie}-{int(numero):03d}"


def prochaine_reference(objet_type: str, date_objet: Any, *, db_path=None) -> str:
    """La référence que recevra le prochain objet de ce mois — LECTURE SEULE (rien n'est réservé).

    Sert à l'écran de confirmation, pour que la pièce puisse être nommée avant l'enregistrement."""
    if objet_type not in PREFIXES:
        raise ValueError(objet_type)
    serie = _serie(objet_type, date_objet)
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT dernier_numero FROM justificatifs_sequence WHERE serie=?",
                         (serie,)).fetchone()
    finally:
        conn.close()
    return formater(serie, (r[0] if r else 0) + 1)


def preparer_dossier(objet_type: str, date_objet: Any) -> Path:
    """Crée le dossier canonique du mois s'il manque (système de fichiers, jamais la base)."""
    d = dossier(objet_type, date_objet)
    d.mkdir(parents=True, exist_ok=True)
    return d


def attribuer(conn, objet_type: str, objet_id: str, date_objet: Any, *, acteur: str) -> dict:
    """Attribue la référence de l'objet DANS la transaction de l'appelant. Idempotent : un objet
    garde sa référence. Retourne la ligne `justificatifs`."""
    if objet_type not in PREFIXES:
        raise ValueError(objet_type)
    existante = conn.execute("SELECT * FROM justificatifs WHERE objet_type=? AND objet_id=?",
                             (objet_type, objet_id)).fetchone()
    if existante:
        return dict(existante)
    serie = _serie(objet_type, date_objet)
    conn.execute("INSERT OR IGNORE INTO justificatifs_sequence (serie) VALUES (?)", (serie,))
    conn.execute("UPDATE justificatifs_sequence SET dernier_numero = dernier_numero + 1, "
                 "date_modification=? WHERE serie=?", (_now(), serie))
    numero = conn.execute("SELECT dernier_numero FROM justificatifs_sequence WHERE serie=?",
                          (serie,)).fetchone()[0]
    reference = formater(serie, numero)
    conn.execute("INSERT INTO justificatifs (reference, objet_type, objet_id, dossier, statut) "
                 "VALUES (?,?,?,?,?)",
                 (reference, objet_type, objet_id, dossier_affiche(dossier(objet_type, date_objet)),
                  ST_A_CONFIRMER))
    conn.execute("INSERT INTO justificatif_evenements (reference, type_evenement, statut, acteur) "
                 "VALUES (?,?,?,?)", (reference, "ATTRIBUTION", ST_A_CONFIRMER, acteur or "local"))
    return dict(conn.execute("SELECT * FROM justificatifs WHERE reference=?",
                             (reference,)).fetchone())


def attribuer_seul(objet_type: str, objet_id: str, date_objet: Any, *, acteur: str,
                   db_path=None) -> dict:
    """Attribution hors transaction appelante (objet antérieur à la Mission 36, sur demande)."""
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        ligne = attribuer(conn, objet_type, objet_id, date_objet, acteur=acteur)
        conn.commit()
    finally:
        conn.close()
    preparer_dossier(objet_type, date_objet)
    return ligne


def charger(objet_type: str, objet_id: str, *, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT * FROM justificatifs WHERE objet_type=? AND objet_id=?",
                         (objet_type, _txt(objet_id))).fetchone()
    finally:
        conn.close()
    if r is None:
        return None
    j = dict(r)
    j["libelle_statut"] = LIBELLES_STATUT.get(j["statut"], j["statut"])
    j["confirme"] = j["statut"] != ST_A_CONFIRMER
    return j


def historique(reference: str, *, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM justificatif_evenements WHERE reference=? ORDER BY id", (reference,))]
    finally:
        conn.close()


def fichiers_trouves(reference: str, dossier_: Path) -> list[str]:
    """Fichiers du dossier dont le nom commence par la référence (et pas par une référence plus
    longue : CHG-2026-09-001 ne reconnaît pas CHG-2026-09-0012)."""
    motif = re.compile(re.escape(reference) + r"(?!\d)")
    if not dossier_.is_dir():
        return []
    return sorted(p.name for p in dossier_.iterdir() if p.is_file() and motif.match(p.name))


def verifier_reponse(reference: str, dossier_: Path, *, present: str,
                     justification: str) -> dict[str, Any]:
    """Contrôle d'une réponse AVANT de l'enregistrer. {ok, statut, fichier} ou un refus."""
    present = _txt(present).upper()
    if present not in ("OUI", "NON"):
        return _refus(E_REPONSE, "Répondez à la question : le justificatif a-t-il bien été "
                                 "enregistré ? (oui / non)")
    if present == "NON":
        if not _txt(justification):
            return _refus(E_JUSTIFICATION, "Sans justificatif, une justification écrite est "
                                           "obligatoire (ex. « Ticket perdu — duplicata demandé "
                                           "au fournisseur »).")
        return {"ok": True, "statut": ST_ABSENT_JUSTIFIE, "fichier": None}
    trouves = fichiers_trouves(reference, dossier_)
    if not trouves:
        return _refus(E_FICHIER, f"Aucun fichier commençant par {reference} n'a été trouvé dans "
                                 f"{dossier_affiche(dossier_)}. Enregistrez-y le justificatif "
                                 f"(ex. {reference}__FOURNISSEUR.pdf), ou répondez « non » en "
                                 "justifiant l'absence.")
    return {"ok": True, "statut": ST_ARCHIVE, "fichier": trouves[0]}


def confirmer(objet_type: str, objet_id: str, *, present: str, justification: str = "",
              acteur: str, db_path=None) -> dict[str, Any]:
    """Enregistre la réponse « le justificatif a-t-il bien été enregistré ? ». Peut être rejouée
    (un duplicata reçu plus tard fait passer d'une absence justifiée à un justificatif archivé) ;
    chaque réponse reste dans l'historique."""
    if not _txt(acteur):
        return _refus(E_ACTEUR, "Indiquez votre nom : la réponse est tracée.")
    j = charger(objet_type, objet_id, db_path=db_path)
    if j is None:
        return _refus(E_SANS_REFERENCE, "Cet objet n'a pas encore de référence de justificatif.")
    d = Path(cfg.PROJECT_ROOT) / j["dossier"] if not Path(j["dossier"]).is_absolute() \
        else Path(j["dossier"])
    verif = verifier_reponse(j["reference"], d, present=present, justification=justification)
    if not verif["ok"]:
        return verif
    maintenant = _now()
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE justificatifs SET statut=?, justification_absence=?, "
                     "fichier_constate=?, confirme_par=?, confirme_le=? WHERE reference=?",
                     (verif["statut"], (_txt(justification) or None)
                      if verif["statut"] == ST_ABSENT_JUSTIFIE else None,
                      verif["fichier"], _txt(acteur), maintenant, j["reference"]))
        conn.execute("INSERT INTO justificatif_evenements (reference, type_evenement, statut, "
                     "justification, fichier, acteur) VALUES (?,?,?,?,?,?)",
                     (j["reference"], "CONFIRMATION_ARCHIVE" if verif["statut"] == ST_ARCHIVE
                      else "ABSENCE_JUSTIFIEE", verif["statut"], _txt(justification) or None,
                      verif["fichier"], _txt(acteur)))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "reference": j["reference"], "statut": verif["statut"],
            "fichier": verif["fichier"]}
