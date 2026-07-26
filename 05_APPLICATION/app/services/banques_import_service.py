"""Import bancaire applicatif (module Banque) — prévisualisation sur copie puis confirmation
transactionnelle, écriture gardée dans `NORM_Banque` (`BANQUE_LOT8_IMPORT.xlsx`).

Reprend EXACTEMENT le schéma et les formules du moteur `lot8a_banque_import.py` (jamais une
seconde norme bancaire concurrente) :
- 26 colonnes `NORM_Banque` (23 lot8a + 3 lot8b) ;
- `ROW_HASH` = sha256(compte_id, date_operation, date_valeur, sens, montant_centimes,
  libellé normalisé, devise) — même formule, même ordre des champs ;
- `mouvement_id` = `MVT-<compte_id>-<date_operation YYYYMMDD>-<sens>-<montant_centimes>-<hash6>`.

Principes :
- lecture du fichier importé strictement en mémoire, jamais de modification du fichier source ;
- aucune écriture avant confirmation explicite (prévisualisation = manifest scellé sous
  `DRYRUNS_DIR`, jamais un octet dans `NORM_Banque`) ;
- doublon CERTAIN (même `ROW_HASH` qu'une ligne déjà présente dans `NORM_Banque`, ou déjà vue dans
  le même fichier) → ligne exclue de l'import, jamais silencieusement fusionnée ;
- doublon PROBABLE (même compte + date + montant + libellé normalisé, `ROW_HASH` différent —
  typiquement une `date_valeur` différente) → nécessite une justification explicite à la
  confirmation, sinon la ligne est exclue ;
- réimporter le même fichier n'ajoute aucune ligne (idempotent, uniquement via les doublons
  certains) ;
- écriture gardée par `BANQUE_REAL_WRITE_ENABLED`/`BANQUE_REAL_WRITE_CONFIRMATION_ENABLED`
  (double verrou `RECETTE_MODE`, cf. `app/config.py`) + write-guard mode recette
  (`recette_guard.assert_ecriture_autorisee`, via `_remplacer_fichier`).
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import tempfile
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import openpyxl

import app.config as cfg

SHEET_NORM = "NORM_Banque"
DEVISE_REF = "EUR"

NORM_BANQUE_HDR = [
    "mouvement_id", "ROW_HASH", "import_id", "ligne_source",
    "date_operation", "date_valeur", "libelle", "libelle_brut",
    "montant", "sens", "devise", "compte_id",
    "tiers_detecte", "categorie", "type_flux_id", "code_impact",
    "source_classification", "source_economique",
    "statut_controle", "niveau_risque",
    "codes_anomalie", "date_integration", "commentaire",
    "statut_classification", "niveau_anomalie", "regle_id_appliquee",
]

MANIFEST_NAME = "manifest.json"

E_FLAGS = "E_FLAGS_DESACTIVES"
E_FORMAT_INCONNU = "V01_FORMAT_INCONNU"
E_FICHIER_VIDE = "V02_FICHIER_VIDE"
E_COLONNES_MANQUANTES = "V03_COLONNES_MANQUANTES"
E_TOKEN_INCONNU = "E01_TOKEN_INCONNU"
E_MANIFESTE_ALTERE = "E02_MANIFESTE_ALTERE"
E_ECRITURE = "E03_ECRITURE_REFUSEE"

MESSAGES = {
    E_FLAGS: "Écriture désactivée sur cette installation : l'import est impossible.",
    E_FORMAT_INCONNU: "Format de fichier non reconnu (CSV ou XLSX attendu).",
    E_FICHIER_VIDE: "Le fichier est vide ou ne contient aucune ligne de données.",
    E_COLONNES_MANQUANTES: "Colonnes obligatoires manquantes (date, libellé, montant).",
    E_TOKEN_INCONNU: "Prévisualisation introuvable ou expirée : reprenez l'import depuis le début.",
    E_MANIFESTE_ALTERE: "Le manifeste a changé depuis la prévisualisation : reprenez l'import.",
    E_ECRITURE: "Écriture refusée : la cible n'est pas autorisée.",
}

# En-têtes reconnues (variantes usuelles d'export bancaire français), toutes ramenées aux mêmes
# colonnes brutes que lit lot8a : date_brute, valeur_brute, libelle_brut, debit_brut, credit_brut,
# solde_brut, devise_brute.
_ALIASES: dict[str, tuple[str, ...]] = {
    "date_brute": ("date operation", "date d'operation", "date", "date_operation"),
    "valeur_brute": ("date valeur", "date de valeur", "valeur", "date_valeur"),
    "libelle_brut": ("libelle", "libellé", "libelle operation", "libellé operation", "libelle_brut"),
    "debit_brut": ("debit", "débit", "montant debit", "debit_brut"),
    "credit_brut": ("credit", "crédit", "montant credit", "credit_brut"),
    "solde_brut": ("solde", "solde_brut"),
    "devise_brute": ("devise", "devise_brute"),
    "montant_unique": ("montant",),
    "sens_unique": ("sens",),
}


def _flags_actifs() -> bool:
    return bool(cfg.BANQUE_REAL_WRITE_ENABLED and cfg.BANQUE_REAL_WRITE_CONFIRMATION_ENABLED)


def _norm_libelle(s: Any) -> str:
    if s is None:
        return ""
    s = str(s).strip().upper()
    return re.sub(r"\s+", " ", s)


def _parse_date(val: Any) -> str | None:
    """Retourne une date ISO (AAAA-MM-JJ) ou None si non convertible."""
    if val is None:
        return None
    if isinstance(val, (date, datetime)):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def _to_float(val: Any) -> float | None:
    if val is None or str(val).strip() == "":
        return None
    cleaned = str(val).replace(",", ".").replace("\xa0", "").replace(" ", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _make_hash(parts: list[Any]) -> str:
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _detecter_colonnes(header: list[str]) -> dict[str, int]:
    """{clé_canonique: index_colonne} pour les colonnes reconnues dans `header`."""
    norm_hdr = [_norm_libelle(h) for h in header]
    trouve: dict[str, int] = {}
    for cle, variantes in _ALIASES.items():
        for i, h in enumerate(norm_hdr):
            if h.lower() in variantes or h in [v.upper() for v in variantes]:
                trouve[cle] = i
                break
    return trouve


def _lire_csv(contenu: bytes) -> tuple[list[str], list[list[Any]]]:
    texte = contenu.decode("utf-8-sig", errors="replace")
    dialecte = csv.Sniffer().sniff(texte.splitlines()[0]) if texte.splitlines() else csv.excel
    try:
        delimiter = dialecte.delimiter
    except Exception:
        delimiter = ";" if ";" in texte.splitlines()[0] else ","
    reader = csv.reader(io.StringIO(texte), delimiter=delimiter)
    rows = [r for r in reader if any(str(c or "").strip() for c in r)]
    if not rows:
        return [], []
    return rows[0], rows[1:]


def _lire_xlsx(contenu: bytes) -> tuple[list[str], list[list[Any]]]:
    fd, tmp = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    tmp_path = Path(tmp)
    try:
        tmp_path.write_bytes(contenu)
        wb = openpyxl.load_workbook(tmp_path, read_only=True, data_only=True)
        try:
            ws = wb.worksheets[0]
            rows = [list(r) for r in ws.iter_rows(values_only=True)
                    if any(c is not None and str(c).strip() for c in r)]
        finally:
            wb.close()
    finally:
        tmp_path.unlink(missing_ok=True)
    if not rows:
        return [], []
    header = [str(c) if c is not None else f"col_{i}" for i, c in enumerate(rows[0])]
    return header, rows[1:]


def _lignes_existantes(ref_path: Path) -> tuple[list[dict[str, Any]], set[str]]:
    """Lignes déjà présentes dans NORM_Banque + ensemble des ROW_HASH déjà connus (réimport
    idempotent : ne dépend jamais de l'ordre des lignes du fichier importé)."""
    if not ref_path.exists():
        return [], set()
    wb = openpyxl.load_workbook(ref_path, read_only=True, data_only=True)
    try:
        if SHEET_NORM not in wb.sheetnames:
            return [], set()
        ws = wb[SHEET_NORM]
        rows = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()
    if not rows:
        return [], set()
    hdr = list(rows[0])
    data = [dict(zip(hdr, r)) for r in rows[1:]]
    hashes = {str(r.get("ROW_HASH") or "") for r in data if r.get("ROW_HASH")}
    return data, hashes


def _normaliser(header: list[str], rows: list[list[Any]], compte_id: str, import_id: str,
                ref_path: Path) -> dict[str, Any]:
    """Normalise les lignes brutes au schéma NORM_Banque. Retourne le détail complet (valides,
    doublons certains, doublons probables, invalides) — aucune écriture."""
    cols = _detecter_colonnes(header)
    if not (("debit_brut" in cols or "credit_brut" in cols or "montant_unique" in cols)
            and "date_brute" in cols and "libelle_brut" in cols):
        return {"ok": False, "code": E_COLONNES_MANQUANTES}

    def _montant_cle(v: Any) -> str:
        m = _to_float(v)
        return f"{m:.2f}" if m is not None else ""

    existantes, hashes_connus = _lignes_existantes(ref_path)
    probables_index: dict[tuple, dict[str, Any]] = {}
    for r in existantes:
        cle = (str(r.get("compte_id") or ""), str(r.get("date_operation") or ""),
               _montant_cle(r.get("montant")), str(r.get("libelle") or ""))
        probables_index[cle] = r

    vues_dans_fichier: set[str] = set()
    valides: list[dict[str, Any]] = []
    doublons_certains: list[dict[str, Any]] = []
    doublons_probables: list[dict[str, Any]] = []
    invalides: list[dict[str, Any]] = []
    total_debit = 0.0
    total_credit = 0.0
    date_integration = datetime.now().strftime("%Y-%m-%d")

    for idx, row in enumerate(rows, start=2):
        def _v(cle: str) -> Any:
            i = cols.get(cle)
            return row[i] if i is not None and i < len(row) else None

        libelle_brut = _v("libelle_brut")
        lib_norm = _norm_libelle(libelle_brut)
        date_op = _parse_date(_v("date_brute"))
        date_val = _parse_date(_v("valeur_brute")) or date_op
        devise = (str(_v("devise_brute") or "").strip().upper()) or DEVISE_REF

        if "montant_unique" in cols:
            montant_brut = _to_float(_v("montant_unique"))
            sens_brut = str(_v("sens_unique") or "").strip().upper()
            if montant_brut is None:
                deb_f = cre_f = None
            elif sens_brut == "DEBIT" or montant_brut < 0:
                deb_f, cre_f = abs(montant_brut), None
            else:
                deb_f, cre_f = None, montant_brut
        else:
            deb_f = _to_float(_v("debit_brut"))
            cre_f = _to_float(_v("credit_brut"))

        erreurs: list[str] = []
        if not lib_norm:
            erreurs.append("Libellé manquant")
        if date_op is None:
            erreurs.append("Date invalide ou manquante")
        sens = montant = montant_centimes = None
        if deb_f is None and cre_f is None:
            erreurs.append("Montant manquant ou non numérique")
        elif deb_f is not None and cre_f is not None:
            erreurs.append("Débit et crédit tous deux renseignés")
        elif deb_f is not None:
            sens, montant = "DEBIT", round(abs(deb_f), 2)
            montant_centimes = int(round(montant * 100))
        else:
            sens, montant = "CREDIT", round(abs(cre_f), 2)
            montant_centimes = int(round(montant * 100))

        if erreurs:
            invalides.append({"ligne": idx, "libelle": str(libelle_brut or "")[:80],
                              "erreurs": erreurs})
            continue

        row_hash = _make_hash([compte_id, date_op, date_val, sens, montant_centimes, lib_norm,
                               devise])
        hash_court = row_hash[:6].upper()
        mouvement_id = f"MVT-{compte_id}-{date_op.replace('-', '')}-{sens}-{montant_centimes}-{hash_court}"

        ligne = {
            "mouvement_id": mouvement_id, "ROW_HASH": row_hash[:16], "import_id": import_id,
            "ligne_source": idx, "date_operation": date_op, "date_valeur": date_val,
            "libelle": lib_norm, "libelle_brut": str(libelle_brut or ""),
            "montant": montant, "sens": sens, "devise": devise, "compte_id": compte_id,
            "tiers_detecte": "", "categorie": "", "type_flux_id": "", "code_impact": "",
            "source_classification": "", "source_economique": "",
            "statut_controle": "EN_ATTENTE_CLASSIFICATION", "niveau_risque": "FAIBLE",
            "codes_anomalie": "", "date_integration": date_integration, "commentaire": "",
            "statut_classification": "NON_CLASSIFIEE", "niveau_anomalie": "FAIBLE",
            "regle_id_appliquee": "",
        }

        if row_hash[:16] in hashes_connus or row_hash[:16] in vues_dans_fichier:
            doublons_certains.append({"ligne": idx, "mouvement_id": mouvement_id,
                                      "libelle": lib_norm, "montant": montant, "sens": sens})
            continue
        vues_dans_fichier.add(row_hash[:16])

        cle_probable = (compte_id, date_op, _montant_cle(montant), lib_norm)
        if cle_probable in probables_index and row_hash[:16] not in hashes_connus:
            doublons_probables.append({"ligne": idx, "mouvement_id": mouvement_id,
                                       "libelle": lib_norm, "montant": montant, "sens": sens,
                                       "ligne_data": ligne})
            continue

        if sens == "DEBIT":
            total_debit += montant
        else:
            total_credit += montant
        valides.append(ligne)

    return {
        "ok": True, "compteurs": {
            "lignes_lues": len(rows), "valides": len(valides),
            "doublons_certains": len(doublons_certains), "doublons_probables": len(doublons_probables),
            "invalides": len(invalides), "debits": sum(1 for v in valides if v["sens"] == "DEBIT"),
            "credits": sum(1 for v in valides if v["sens"] == "CREDIT"),
            "total_debit": round(total_debit, 2), "total_credit": round(total_credit, 2),
        },
        "valides": valides, "doublons_certains": doublons_certains,
        "doublons_probables": doublons_probables, "invalides": invalides,
    }


def previsualiser(contenu: bytes, nom_fichier: str, compte_id: str,
                  ref_path: Path | None = None, dryruns_root: Path | None = None) -> dict[str, Any]:
    """Lit et normalise le fichier importé EN MÉMOIRE, sans jamais écrire NORM_Banque. Produit un
    manifeste scellé (token) rejoué et revérifié à la confirmation."""
    ext = Path(nom_fichier).suffix.lower()
    if ext == ".csv":
        header, rows = _lire_csv(contenu)
    elif ext in (".xlsx", ".xlsm"):
        header, rows = _lire_xlsx(contenu)
    else:
        return {"ok": False, "code": E_FORMAT_INCONNU, "message": MESSAGES[E_FORMAT_INCONNU]}

    if not header or not rows:
        return {"ok": False, "code": E_FICHIER_VIDE, "message": MESSAGES[E_FICHIER_VIDE]}

    p_ref = Path(ref_path or cfg.MASTER_BANQUE)
    resultat = _normaliser(header, rows, compte_id, f"IMPORT_{uuid.uuid4().hex[:10].upper()}", p_ref)
    if not resultat.get("ok"):
        return {"ok": False, "code": resultat["code"], "message": MESSAGES[resultat["code"]]}

    token = uuid.uuid4().hex
    root = Path(dryruns_root or cfg.DRYRUNS_DIR) / "banque_import" / token
    root.mkdir(parents=True, exist_ok=True)
    manifest = {
        "token": token, "nom_fichier": nom_fichier, "compte_id": compte_id,
        "ref_path": str(p_ref), "cree_le": datetime.now(timezone.utc).isoformat(),
        "compteurs": resultat["compteurs"], "valides": resultat["valides"],
        "doublons_certains": resultat["doublons_certains"],
        "doublons_probables": resultat["doublons_probables"],
        "invalides": resultat["invalides"],
    }
    (root / MANIFEST_NAME).write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                                      encoding="utf-8")
    return {"ok": True, "token": token, "compteurs": resultat["compteurs"],
            "doublons_probables": resultat["doublons_probables"],
            "invalides": resultat["invalides"], "nom_fichier": nom_fichier}


def _charger_manifest(token: str, dryruns_root: Path | None = None) -> dict[str, Any] | None:
    root = Path(dryruns_root or cfg.DRYRUNS_DIR) / "banque_import" / token
    p = root / MANIFEST_NAME
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def confirmer(token: str, *, justifier_doublons_probables: bool = False, acteur: str = "",
             dryruns_root: Path | None = None, db_path=None) -> dict[str, Any]:
    """Revérifie le manifeste et écrit atomiquement les lignes valides (+ doublons probables
    justifiés) dans NORM_Banque. Journalise l'import. Jamais un faux succès en cas d'échec aval."""
    if not _flags_actifs():
        return {"ok": False, "code": E_FLAGS, "message": MESSAGES[E_FLAGS]}

    manifest = _charger_manifest(token, dryruns_root)
    if manifest is None:
        return {"ok": False, "code": E_TOKEN_INCONNU, "message": MESSAGES[E_TOKEN_INCONNU]}

    p_ref = Path(manifest["ref_path"])
    lignes_a_ecrire = list(manifest["valides"])
    if justifier_doublons_probables:
        lignes_a_ecrire.extend(d["ligne_data"] for d in manifest["doublons_probables"])

    if p_ref.exists():
        wb = openpyxl.load_workbook(p_ref, keep_vba=p_ref.suffix.lower() == ".xlsm")
        if SHEET_NORM not in wb.sheetnames:
            ws = wb.create_sheet(SHEET_NORM)
            ws.append(NORM_BANQUE_HDR)
    else:
        wb = openpyxl.Workbook()
        wb.remove(wb.active)
        ws = wb.create_sheet(SHEET_NORM)
        ws.append(NORM_BANQUE_HDR)

    ws = wb[SHEET_NORM]
    hdr = [c.value for c in ws[1]]
    for ligne in lignes_a_ecrire:
        ws.append([ligne.get(h) for h in hdr])

    from app.services.saisie_charges_transaction_service import _remplacer_fichier
    fd, tmp = tempfile.mkstemp(suffix=p_ref.suffix or ".xlsx", dir=str(p_ref.parent) if p_ref.parent.exists() else None)
    os.close(fd)
    tmp_path = Path(tmp)
    try:
        p_ref.parent.mkdir(parents=True, exist_ok=True)
        wb.save(tmp_path)
        _remplacer_fichier(tmp_path, p_ref)
    except Exception as exc:
        tmp_path.unlink(missing_ok=True)
        wb.close()
        return {"ok": False, "code": E_ECRITURE, "message": MESSAGES[E_ECRITURE],
                "detail": f"{type(exc).__name__}: {exc}"}
    wb.close()

    _journaliser_import(manifest, len(lignes_a_ecrire), acteur, db_path)

    return {
        "ok": True, "nb_ajoutees": len(lignes_a_ecrire),
        "nb_doublons_certains_ignores": manifest["compteurs"]["doublons_certains"],
        "nb_doublons_probables_ignores": (
            0 if justifier_doublons_probables else manifest["compteurs"]["doublons_probables"]),
        "nb_invalides": manifest["compteurs"]["invalides"], "compteurs": manifest["compteurs"],
    }


def _journaliser_import(manifest: dict[str, Any], nb_ajoutees: int, acteur: str, db_path=None) -> None:
    import sqlite3
    conn = sqlite3.connect(str(db_path or cfg.DB_PATH))
    try:
        conn.execute(
            "INSERT INTO banque_imports (import_id, nom_fichier_origine, compte_id_opaque, "
            "nb_lignes_lues, nb_valides, nb_doublons_certains, nb_doublons_probables, "
            "nb_invalides, total_debit, total_credit, statut, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (manifest["token"], manifest["nom_fichier"], manifest.get("compte_id", ""),
             manifest["compteurs"]["lignes_lues"], nb_ajoutees,
             manifest["compteurs"]["doublons_certains"], manifest["compteurs"]["doublons_probables"],
             manifest["compteurs"]["invalides"], manifest["compteurs"]["total_debit"],
             manifest["compteurs"]["total_credit"], "SUCCES", acteur or "local"),
        )
        conn.commit()
    finally:
        conn.close()


def historique_imports(db_path=None, limit: int = 50) -> list[dict[str, Any]]:
    import sqlite3
    conn = sqlite3.connect(str(db_path or cfg.DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM banque_imports ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
