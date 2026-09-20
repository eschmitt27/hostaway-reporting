"""Lecture et validation d'un MD d'interprétation de facture (contrat FACTURE_FOURNISSEUR_MD_V1).

CE QUE CE MODULE FAIT, ET CE QU'IL NE FAIT PAS
Il LIT un fichier `.md` posé à côté du PDF, le valide contre le contrat, et rend des lignes prêtes
à être écrites. Il ne devine rien : un MD douteux n'est pas « à moitié utilisé », il est écarté
avec sa raison. Aucune IA n'est appelée ici — la transformation PDF → MD se fait à l'extérieur de
l'application.

LE SCHÉMA EST LA VÉRITÉ
La validation lit `app/contrats/FACTURE_FOURNISSEUR_MD_V1.schema.json` et applique CE fichier. Le
sous-ensemble de JSON Schema interprété ici (type, required, enum, const, pattern, minimum,
minLength, minItems, items, properties, additionalProperties) suffit au contrat et évite de
recopier les règles dans du code, où elles auraient divergé du schéma à la première évolution.

QUATRE ÉTATS, JAMAIS D'ENTRE-DEUX
  ABSENT    — pas de MD : le parseur PDF fait foi.
  VALIDE    — MD conforme, empreinte du PDF concordante : c'est LUI qui fait les lignes.
  OBSOLETE  — MD écrit pour une autre version du PDF (empreinte différente) : écarté.
  INVALIDE  — MD illisible ou non conforme au contrat : écarté, avec ses erreurs.
Un MD sans PDF est un ORPHELIN : refusé, car le PDF reste la pièce originale obligatoire.
"""
from __future__ import annotations

import hashlib
import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.services import referentiel_logements_export_service as ref_export

SCHEMA_NOM = "FACTURE_FOURNISSEUR_MD_V1"
CHEMIN_SCHEMA = Path(__file__).resolve().parent.parent / "contrats" / f"{SCHEMA_NOM}.schema.json"
ENTETE = f"# {SCHEMA_NOM}"

ETAT_ABSENT = "ABSENT"
ETAT_VALIDE = "VALIDE"
ETAT_INVALIDE = "INVALIDE"
ETAT_OBSOLETE = "OBSOLETE"
ETAT_ORPHELIN = "MD_ORPHELIN"

#: Natures du contrat → vocabulaire canonique de `ref_types_lignes_menage` (ce que la base stocke).
NATURES_CANONIQUES = {
    "MENAGE": "MENAGE_STANDARD",
    "REMISE_EN_ETAT": "REMISE_EN_ETAT",
    "AUTRE_PRESTATION": "AUTRE",
    "A_CLASSER": "AUTRE",
}
#: Une nature `A_CLASSER` dit explicitement « un humain doit trancher » : la ligne entre avec une
#: confiance AUCUN, exactement comme une ligne que le parseur PDF n'a pas comprise.
CONFIANCE_PAR_NATURE = {
    "MENAGE": "CERTAIN", "REMISE_EN_ETAT": "CERTAIN",
    "AUTRE_PRESTATION": "CERTAIN", "A_CLASSER": "AUCUN",
}
#: Confiance du logement dans le MD → vocabulaire du rapprochement applicatif.
CONFIANCE_LOGEMENT = {
    "CERTAIN": "CERTAIN", "A_CONFIRMER": "PROBABLE", "AMBIGU": "AUCUN",
    "NON_TROUVE": "AUCUN", "NON_APPLICABLE": "AUCUN",
}
#: Seules ces confiances affectent le logement à la ligne ; les autres laissent l'humain décider.
CONFIANCES_AFFECTANTES = ("CERTAIN", "A_CONFIRMER")

A_LOGEMENT_INCONNU = "LOGEMENT_INCONNU"
A_REFERENTIEL_ANCIEN = "REFERENTIEL_ANCIEN"


@lru_cache(maxsize=1)
def schema() -> dict[str, Any]:
    return json.loads(CHEMIN_SCHEMA.read_text(encoding="utf-8"))


# ── Validation : un sous-ensemble de JSON Schema, appliqué depuis le FICHIER de contrat ──────────

def _types_ok(valeur: Any, attendu: Any) -> bool:
    noms = attendu if isinstance(attendu, list) else [attendu]
    for nom in noms:
        if nom == "null" and valeur is None:
            return True
        if nom == "string" and isinstance(valeur, str):
            return True
        if nom == "integer" and isinstance(valeur, int) and not isinstance(valeur, bool):
            return True
        if nom == "number" and isinstance(valeur, (int, float)) and not isinstance(valeur, bool):
            return True
        if nom == "array" and isinstance(valeur, list):
            return True
        if nom == "object" and isinstance(valeur, dict):
            return True
        if nom == "boolean" and isinstance(valeur, bool):
            return True
    return False


def _valider_noeud(valeur: Any, regle: dict[str, Any], chemin: str, erreurs: list[str]) -> None:
    if "const" in regle and valeur != regle["const"]:
        erreurs.append(f"{chemin} : attendu « {regle['const']} », lu « {valeur} »")
        return
    if "enum" in regle and valeur not in regle["enum"]:
        erreurs.append(f"{chemin} : valeur « {valeur} » hors des valeurs autorisées "
                       f"({', '.join(map(str, regle['enum']))})")
        return
    if "type" in regle and not _types_ok(valeur, regle["type"]):
        erreurs.append(f"{chemin} : type {regle['type']} attendu, lu « {type(valeur).__name__} »")
        return
    if valeur is None:
        return
    if isinstance(valeur, str):
        if "pattern" in regle and not re.match(regle["pattern"], valeur):
            erreurs.append(f"{chemin} : « {valeur} » ne respecte pas le format attendu")
        if "minLength" in regle and len(valeur) < regle["minLength"]:
            erreurs.append(f"{chemin} : valeur vide")
    if isinstance(valeur, (int, float)) and not isinstance(valeur, bool):
        if "minimum" in regle and valeur < regle["minimum"]:
            erreurs.append(f"{chemin} : {valeur} inférieur au minimum {regle['minimum']}")
    if isinstance(valeur, list):
        if "minItems" in regle and len(valeur) < regle["minItems"]:
            erreurs.append(f"{chemin} : au moins {regle['minItems']} élément(s) attendu(s)")
        for i, element in enumerate(valeur):
            if "items" in regle:
                _valider_noeud(element, regle["items"], f"{chemin}[{i}]", erreurs)
    if isinstance(valeur, dict):
        proprietes = regle.get("properties", {})
        for requis in regle.get("required", []):
            if requis not in valeur:
                erreurs.append(f"{chemin}.{requis} : champ obligatoire absent")
        if regle.get("additionalProperties") is False:
            for cle in valeur:
                if cle not in proprietes:
                    erreurs.append(f"{chemin}.{cle} : champ inconnu du contrat")
        for cle, sous_regle in proprietes.items():
            if cle in valeur:
                _valider_noeud(valeur[cle], sous_regle, f"{chemin}.{cle}", erreurs)


def valider_contrat(donnees: Any) -> list[str]:
    """Erreurs de conformité au contrat. Liste vide = conforme."""
    erreurs: list[str] = []
    _valider_noeud(donnees, schema(), "md", erreurs)
    return erreurs


# ── Lecture du fichier ───────────────────────────────────────────────────────────────────────────

_RE_BLOC = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)


def chemin_md(pdf_path) -> Path:
    """Le MD d'un PDF porte le MÊME nom de base — règle stricte, aucune autre correspondance."""
    p = Path(pdf_path)
    return p.with_suffix(".md")


def lire(chemin) -> tuple[dict[str, Any] | None, list[str]]:
    """(données, erreurs) — le fichier doit porter l'en-tête du contrat et UN bloc JSON."""
    p = Path(chemin)
    try:
        texte = p.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        return None, [f"fichier illisible : {type(exc).__name__}"]
    if ENTETE not in texte:
        return None, [f"en-tête « {ENTETE} » absent : ce fichier ne déclare pas le contrat"]
    blocs = _RE_BLOC.findall(texte)
    if not blocs:
        debut = texte.find("{")
        blocs = [texte[debut:]] if debut >= 0 else []
    if not blocs:
        return None, ["aucun bloc JSON trouvé"]
    try:
        return json.loads(blocs[0]), []
    except json.JSONDecodeError as exc:
        return None, [f"JSON illisible : {exc.msg} (ligne {exc.lineno})"]


def sha256_fichier(chemin) -> str:
    return hashlib.sha256(Path(chemin).read_bytes()).hexdigest()


# ── Analyse complète : état du MD face à SON pdf et au référentiel ───────────────────────────────

def analyser(pdf_path, *, db_path=None, pdf_sha256: str | None = None) -> dict[str, Any]:
    """État du MD accompagnant ce PDF, et ce qu'il faut en faire.

    Un MD n'est retenu que s'il est conforme AU CONTRAT et écrit pour CE PDF (empreinte
    identique). Le référentiel logements, lui, ne disqualifie pas le MD : s'il a changé depuis, le
    MD reste exploitable tant que les identifiants existent encore — c'est signalé
    (`REFERENTIEL_ANCIEN`), pas rejeté. Un identifiant inconnu, en revanche, est une anomalie
    portée par la ligne : l'application ne crée jamais un logement depuis un MD.
    """
    pdf = Path(pdf_path)
    md = chemin_md(pdf)
    resultat: dict[str, Any] = {"md_present": md.exists(), "md_nom_fichier": md.name,
                                "etat": ETAT_ABSENT, "erreurs": [], "anomalies": [],
                                "donnees": None, "md_sha256": None,
                                "referentiel_version": None, "referentiel_actuel": None}
    if not md.exists():
        return resultat

    resultat["md_sha256"] = sha256_fichier(md)
    donnees, erreurs = lire(md)
    if erreurs or donnees is None:
        return {**resultat, "etat": ETAT_INVALIDE, "erreurs": erreurs}

    erreurs = valider_contrat(donnees)
    if erreurs:
        return {**resultat, "etat": ETAT_INVALIDE, "erreurs": erreurs[:10]}

    source = donnees.get("source", {})
    resultat["donnees"] = donnees
    resultat["referentiel_version"] = source.get("referentiel_version")

    attendu = pdf_sha256 or hashlib.sha256(pdf.read_bytes()).hexdigest()
    if str(source.get("pdf_sha256") or "").lower() != attendu.lower():
        return {**resultat, "etat": ETAT_OBSOLETE, "erreurs": [
            "l'empreinte du PDF déclarée par le MD ne correspond pas au PDF présent : "
            "le document a changé depuis la rédaction du MD"]}
    if str(source.get("pdf_filename") or "") != pdf.name:
        return {**resultat, "etat": ETAT_OBSOLETE, "erreurs": [
            f"le MD déclare le PDF « {source.get('pdf_filename')} », posé à côté de « {pdf.name} »"]}

    anomalies: list[str] = []
    connus = ref_export.logements_connus(db_path=db_path)
    inconnus = sorted({l["logement_id"] for ligne in donnees["lignes"]
                       for l in (ligne.get("logements") or [])
                       if l.get("logement_id") not in connus})
    if inconnus:
        anomalies.append(f"{A_LOGEMENT_INCONNU}:{','.join(inconnus)}")
    resultat["referentiel_actuel"] = ref_export.version(db_path=db_path)
    if (resultat["referentiel_version"]
            and resultat["referentiel_version"] != resultat["referentiel_actuel"]):
        anomalies.append(A_REFERENTIEL_ANCIEN)
    return {**resultat, "etat": ETAT_VALIDE, "anomalies": anomalies,
            "logements_inconnus": inconnus}


def lignes_canoniques(donnees: dict[str, Any], *, logements_connus: set[str]) -> list[dict[str, Any]]:
    """Les lignes du MD, traduites dans le vocabulaire des lignes de facture.

    Le MONTANT IMPRIMÉ (`montant_source`) est la donnée retenue — jamais `montant_calcule`, qui
    n'est qu'un contrôle. Une incohérence arithmétique reste donc visible, et la facture reste à
    contrôler : c'est au document d'être corrigé, pas à la lecture de le maquiller.
    """
    sorties = []
    for i, ligne in enumerate(donnees.get("lignes") or [], start=1):
        nature = ligne["nature"]
        logements = [l for l in (ligne.get("logements") or [])
                     if l.get("logement_id") in logements_connus
                     and str(l.get("confiance") or "CERTAIN") in CONFIANCES_AFFECTANTES]
        anomalies = list(ligne.get("anomalies") or [])
        for l in (ligne.get("logements") or []):
            if l.get("logement_id") not in logements_connus:
                anomalies.append(f"{A_LOGEMENT_INCONNU}:{l.get('logement_id')}")
        montant = round(float(ligne["montant_source"]), 2)
        calcule = ligne.get("montant_calcule")
        if calcule is not None and abs(round(float(calcule), 2) - montant) > 0.005:
            anomalies.append("INCOHERENCE_ARITHMETIQUE_DOCUMENT")
        dates = list(ligne.get("dates_prestation") or [])
        sorties.append({
            "numero": ligne.get("numero") or i,
            "page": ligne.get("page") or 1,
            "libelle_source": ligne["libelle_source"],
            "description": (ligne.get("libelle_metier") or ligne["libelle_source"]).strip(),
            "categorie": NATURES_CANONIQUES[nature],
            "categorie_confiance": CONFIANCE_PAR_NATURE[nature],
            "nature_md": nature,
            "quantite": (int(ligne["quantite_source"])
                         if ligne.get("quantite_source") is not None else None),
            "prix_unitaire": ligne.get("prix_unitaire_source"),
            "montant_ttc": montant,
            "montant_calcule": calcule,
            "logements": [{"logement_id": l["logement_id"],
                           "confiance": CONFIANCE_LOGEMENT.get(str(l.get("confiance") or "CERTAIN"),
                                                               "AUCUN"),
                           "quote_part": l.get("quote_part")} for l in logements],
            "dates_prestation": dates,
            "date_menage": dates[0] if len(dates) == 1 else None,
            "precision_date": ("DATE_PRECISE" if len(dates) == 1
                               else "MULTI_DATES" if len(dates) > 1 else "MOIS_FACTURE"),
            "anomalies": anomalies,
        })
    return sorties
