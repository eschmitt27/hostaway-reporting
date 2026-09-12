"""« Exporter les données » — le parcours utilisateur qui manquait (§16).

CE QUI EXISTAIT, ET CE QUI N'EXISTAIT PAS
Le moteur d'export produisait déjà treize fichiers et leur dictionnaire de colonnes, à l'identique
depuis SQLite. Mais aucun écran ne permettait de les DEMANDER ni de les RÉCUPÉRER : il fallait
lancer un lot en ligne de commande, puis aller chercher les fichiers dans un dossier du projet.
Une fonction qu'on ne peut pas atteindre n'est pas une fonction disponible.

CE QUE CET ÉCRAN AJOUTE
Un bouton qui produit l'export, la liste de ce qui a été produit avec sa volumétrie, et le
téléchargement — fichier par fichier ou en une archive. Rien du moteur n'est réécrit ici.

LE MOIS EN COURS EST MARQUÉ PROVISOIRE
Un export daté du mois courant contient un mois incomplet, qui bougera encore. La mention voyage
avec le fichier (nom de l'archive) et non seulement à l'écran : un fichier téléchargé, renommé et
transmis perdrait immédiatement un avertissement affiché ailleurs.
"""
import io
import zipfile
from datetime import date
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

import app.config as cfg
from app.services import lot13_export_service as lot13
from app.template_env import get_templates

router = APIRouter()
templates = get_templates()

SUFFIXE = ".csv"


def _fichiers() -> list[dict]:
    """Exports présents sur le disque, avec leur taille et leur date."""
    dossier = lot13.repertoire_exports()
    if not dossier.exists():
        return []
    from datetime import datetime, timezone

    resultat = []
    for chemin in sorted(dossier.glob(f"*{SUFFIXE}")):
        stat = chemin.stat()
        resultat.append({
            "nom": chemin.name,
            "taille_ko": round(stat.st_size / 1024, 1),
            "modifie_le": datetime.fromtimestamp(stat.st_mtime, timezone.utc)
                          .strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lignes": _compter_lignes(chemin),
        })
    return resultat


def _compter_lignes(chemin: Path) -> int:
    """Lignes de données, en-tête exclue. Un fichier illisible rend -1, jamais 0 : « je n'ai pas
    pu compter » n'est pas « il n'y a rien »."""
    try:
        with open(chemin, encoding="utf-8-sig") as f:
            return max(sum(1 for _ in f) - 1, 0)
    except OSError:
        return -1


@router.get("/exports", response_class=HTMLResponse)
def exports(request: Request, message: str = "", erreur: str = ""):
    fichiers = _fichiers()
    return templates.TemplateResponse(request, "exports.html", {
        "active_menu": "exports",
        "fichiers": fichiers,
        "dossier": str(lot13.repertoire_exports()),
        "mois_courant": date.today().strftime("%Y-%m"),
        "message": message,
        "erreur": erreur,
    })


@router.post("/exports/generer")
def generer():
    """Produit les exports. Le moteur ABANDONNE sans rien écrire s'il détecte une colonne
    sensible : mieux vaut aucun export qu'un export qui fuit — ce refus est remonté tel quel."""
    from urllib.parse import urlencode

    resultat = lot13.exporter()
    if not resultat.get("ok"):
        return RedirectResponse(
            "/exports?" + urlencode({"erreur": resultat.get("message", "Export refusé.")}),
            status_code=303)
    produits = sum(1 for r in resultat.get("rapport", []) if r.get("statut") == "OK")
    absents = [r["export"] for r in resultat.get("rapport", []) if r.get("statut") != "OK"]
    message = f"{produits} fichier(s) produit(s)."
    if absents:
        # Une source absente n'est pas une erreur, mais la taire ferait croire l'export complet.
        message += f" Sources absentes, non exportées : {', '.join(absents)}."
    return RedirectResponse("/exports?" + urlencode({"message": message}), status_code=303)


@router.get("/exports/fichier/{nom}")
def telecharger(nom: str):
    """Un fichier. Le nom est résolu DANS le dossier d'export et vérifié : une chaîne venue de
    l'URL ne doit jamais pouvoir désigner un fichier ailleurs sur le disque."""
    dossier = lot13.repertoire_exports().resolve()
    cible = (dossier / nom).resolve()
    if dossier not in cible.parents or cible.suffix != SUFFIXE or not cible.is_file():
        return Response("Fichier introuvable.", status_code=404, media_type="text/plain")
    return Response(cible.read_bytes(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{cible.name}"'})


@router.get("/exports/archive.zip")
def archive():
    """Tous les exports en une archive, assemblée en mémoire."""
    fichiers = _fichiers()
    if not fichiers:
        return Response("Aucun export disponible : générez-les d'abord.", status_code=404,
                        media_type="text/plain")
    dossier = lot13.repertoire_exports()
    tampon = io.BytesIO()
    with zipfile.ZipFile(tampon, "w", zipfile.ZIP_DEFLATED) as archive_zip:
        for fichier in fichiers:
            archive_zip.write(dossier / fichier["nom"], arcname=fichier["nom"])
    # PROVISOIRE dans le nom : l'export couvre TOUS les mois, donc le mois en cours, qui est
    # incomplet et bougera encore. Un fichier téléchargé puis transmis n'emporte pas les
    # avertissements de l'écran — son nom, si.
    nom = f"exports_{date.today().strftime('%Y%m%d')}_PROVISOIRE.zip"
    return Response(tampon.getvalue(), media_type="application/zip",
                    headers={"Content-Disposition": f'attachment; filename="{nom}"'})
