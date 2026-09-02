"""Import historique FIGÉ des coûts complets ménage (mission « backfill historique ménages figé »).

CE MODULE N'EST PAS UN MOTEUR DE CALCUL. Il ne rejoue aucune formule économique.

Contexte — pourquoi il existe
    Lot9 lisait TYPE_FLUX_018/019 dans `MASTER_CALC_CoutComplet_Menages.xlsx`. Basculer cette
    lecture vers `menages_cout_complet` (SQLite) supprimait le flux de 2026-05 : le classeur ne
    contient que ce mois-là, la table ne contenait que 2026-06/2026-07. Or 2026-05 est CLOTURÉ, et
    le recalculer est interdit — un mois arrêté ne se recalcule pas, il se conserve.

    D'où ce chemin : les valeurs déjà figées du classeur sont importées VERBATIM, telles qu'elles
    ont été arrêtées, avec leur provenance. Aucune ligne n'est recalculée, aucune règle n'est
    rejouée, `ref_cloture_mensuelle` n'est pas touchée, le mois reste CLOTURE.

Garde-fou central (`_mois_deja_calcule`)
    Un mois qui possède déjà des lignes CALCULÉES par le moteur n'est pas de l'historique : le
    backfiller écraserait un résultat courant par une valeur gelée, soit exactement la régression
    silencieuse que la mission interdit. Ces mois-là sont refusés, jamais importés.
"""
from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SOURCE_TYPE_FIGE = "LEGACY_IMPORT_FIGE"
FEUILLE = "DETAIL_COUT_COMPLET"

# Clé métier d'une ligne de coût complet (cf. index partiel unique, migration 0068).
CLE = ("mois", "logement_id", "intervenant_id")

E_CLASSEUR_ABSENT = "BACKFILL_CLASSEUR_ABSENT"
E_FEUILLE_ABSENTE = "BACKFILL_FEUILLE_ABSENTE"
E_MOIS_DEJA_CALCULE = "BACKFILL_MOIS_DEJA_CALCULE"
E_TABLE_ABSENTE = "BACKFILL_TABLE_ABSENTE"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _colonnes_table(conn: sqlite3.Connection) -> list[str]:
    return [r[1] for r in conn.execute("PRAGMA table_info(menages_cout_complet)")]


def lire_classeur(chemin) -> list[dict[str, Any]]:
    """Lignes brutes de la feuille DETAIL_COUT_COMPLET, sans aucune transformation.

    Lecture seule : le classeur n'est jamais réécrit, son empreinte reste inchangée.
    """
    import openpyxl

    path = Path(chemin)
    if not path.exists():
        raise FileNotFoundError(f"{E_CLASSEUR_ABSENT}: {path}")
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    try:
        if FEUILLE not in wb.sheetnames:
            raise KeyError(f"{E_FEUILLE_ABSENTE}: {FEUILLE}")
        ws = wb[FEUILLE]
        rows = [r for r in ws.iter_rows(values_only=True) if any(c is not None for c in r)]
    finally:
        wb.close()
    if not rows:
        return []
    entetes = [str(c) for c in rows[0]]
    return [dict(zip(entetes, r)) for r in rows[1:]]


def _mois_deja_calcule(conn: sqlite3.Connection, mois: str) -> bool:
    """Vrai si le mois porte des lignes produites par le moteur (donc pas de l'historique gelé).

    Une ligne de `menages_cout_complet` SANS entrée de provenance a été écrite par lot6f. Importer
    par-dessus reviendrait à remplacer un calcul courant par une valeur figée : refusé.
    """
    n = conn.execute(
        "SELECT COUNT(*) FROM menages_cout_complet c "
        "WHERE c.mois = ? AND NOT EXISTS ("
        "  SELECT 1 FROM menages_cout_complet_provenance p "
        "  WHERE p.mois = c.mois AND p.logement_id = c.logement_id "
        "    AND p.intervenant_id = c.intervenant_id)",
        (mois,),
    ).fetchone()[0]
    return n > 0


def importer_historique_fige(*, chemin_classeur, db_path, mois_autorises=None) -> dict[str, Any]:
    """Importe VERBATIM les lignes figées du classeur legacy dans `menages_cout_complet`.

    `mois_autorises` restreint l'import (None = tous les mois présents dans le classeur). Un mois
    déjà calculé par le moteur est refusé, jamais écrasé.

    Idempotent : un rejeu remplace la ligne figée de même clé au lieu d'en ajouter une seconde.
    """
    path = Path(chemin_classeur)
    lignes = lire_classeur(path)
    empreinte = _sha256(path)
    horodatage = _now()

    conn = sqlite3.connect(str(db_path))
    try:
        colonnes = _colonnes_table(conn)
        if not colonnes:
            return {"ok": False, "code": E_TABLE_ABSENTE,
                    "message": "menages_cout_complet absente : migrations non appliquées."}

        # Seules les colonnes réellement communes au classeur et à la table sont reprises : aucune
        # valeur n'est inventée pour combler une colonne absente du classeur (methode,
        # cout_interne_ref_id, run_id... restent NULL, ce qui est l'information exacte).
        communes = [c for c in colonnes if c in (lignes[0] if lignes else {})]
        for cle in CLE:
            if cle not in communes:
                return {"ok": False, "code": E_FEUILLE_ABSENTE,
                        "message": f"Colonne clé absente du classeur : {cle}"}

        mois_classeur = sorted({str(l.get("mois")) for l in lignes if l.get("mois")})
        cibles = [m for m in mois_classeur
                  if mois_autorises is None or m in set(mois_autorises)]

        refuses = [m for m in cibles if _mois_deja_calcule(conn, m)]
        if refuses:
            return {"ok": False, "code": E_MOIS_DEJA_CALCULE, "mois_refuses": refuses,
                    "message": f"Mois déjà calculés par le moteur, import figé refusé : {refuses}. "
                               "Un résultat courant ne se remplace pas par une valeur gelée."}

        insertions = [l for l in lignes if str(l.get("mois")) in set(cibles)]
        place = ", ".join("?" for _ in communes)

        nb = 0
        for l in insertions:
            cle = (l.get("mois"), l.get("logement_id"), l.get("intervenant_id"))
            # Idempotence : on retire la ligne figée de même clé avant réinsertion. La suppression
            # est bornée aux clés DÉJÀ enregistrées comme figées — une ligne calculée par le moteur
            # n'est jamais supprimée ici (et les mois calculés ont de toute façon été refusés
            # au-dessus).
            conn.execute(
                "DELETE FROM menages_cout_complet "
                "WHERE mois = ? AND logement_id = ? AND intervenant_id = ? AND EXISTS ("
                "  SELECT 1 FROM menages_cout_complet_provenance p "
                "  WHERE p.mois = ? AND p.logement_id = ? AND p.intervenant_id = ?)",
                cle + cle,
            )
            conn.execute(
                f"INSERT INTO menages_cout_complet ({', '.join(communes)}) VALUES ({place})",
                [l.get(c) for c in communes])
            conn.execute(
                "INSERT OR REPLACE INTO menages_cout_complet_provenance "
                "(mois, logement_id, intervenant_id, source_type, source_fichier, source_hash, "
                " date_import) VALUES (?,?,?,?,?,?,?)",
                cle + (SOURCE_TYPE_FIGE, path.name, empreinte, horodatage))
            nb += 1
        conn.commit()
    finally:
        conn.close()

    return {"ok": True, "nb_lignes": nb, "mois": cibles, "source_fichier": path.name,
            "source_hash": empreinte, "date_import": horodatage,
            "colonnes_reprises": communes}


def lignes_figees(*, db_path, mois=None) -> list[dict[str, Any]]:
    """Relit les lignes figées importées — utilisé par les contrôles de parité."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        sql = ("SELECT c.* FROM menages_cout_complet c "
               "JOIN menages_cout_complet_provenance p "
               "  ON p.mois = c.mois AND p.logement_id = c.logement_id "
               " AND p.intervenant_id = c.intervenant_id "
               "WHERE p.source_type = ?")
        args: list[Any] = [SOURCE_TYPE_FIGE]
        if mois:
            sql += " AND c.mois = ?"
            args.append(mois)
        return [dict(r) for r in conn.execute(sql, args)]
    finally:
        conn.close()
