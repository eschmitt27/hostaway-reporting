"""« Exporter les données » — le parcours utilisateur qui manquait (§16).

CE QUI EXISTAIT, ET CE QUI N'EXISTAIT PAS
Le moteur d'export produisait déjà treize fichiers et leur dictionnaire de colonnes, à l'identique
depuis SQLite. Mais aucun écran ne permettait de les DEMANDER ni de les RÉCUPÉRER : il fallait
lancer un lot en ligne de commande, puis aller chercher les fichiers dans un dossier du projet.
Une fonction qu'on ne peut pas atteindre n'est pas une fonction disponible.

CHAQUE FICHIER EST CONSTRUIT AU MOMENT DU CLIC
L'écran servait auparavant les CSV écrits sur disque par un bouton « Générer l'export ». Personne
ne le relançait après une actualisation : les fichiers sont restés au 12/09/2026 pendant que
l'application, elle, avançait. Désormais rien n'est lu ni écrit sur disque : la liste, la
volumétrie et chaque téléchargement sont produits à la demande par `lot13.produire()`, depuis les
datasets ACTIFS de l'instant. Une actualisation normale suffit — il n'existe pas de second bouton
« actualiser les exports », et il ne doit pas en exister.

LE MOIS EN COURS EST MARQUÉ PROVISOIRE
Un export daté du mois courant contient un mois incomplet, qui bougera encore. La mention voyage
avec le fichier (nom de l'archive) et non seulement à l'écran : un fichier téléchargé, renommé et
transmis perdrait immédiatement un avertissement affiché ailleurs.
"""
import io
import zipfile
from datetime import date

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from app.services import lot13_export_service as lot13
from app.template_env import get_templates

router = APIRouter()
templates = get_templates()


@router.get("/exports", response_class=HTMLResponse)
def exports(request: Request):
    inventaire = lot13.inventaire()
    return templates.TemplateResponse(request, "exports.html", {
        "active_menu": "exports",
        "exports": inventaire["exports"],
        "dictionnaire": f"{lot13.NOM_DICTIONNAIRE}.csv",
        "mois_courant": date.today().strftime("%Y-%m"),
        # Filet anti-sensible déclenché : aucun fichier n'est servi, et l'écran dit pourquoi.
        "erreur": "" if inventaire["ok"] else inventaire["message"],
    })


@router.get("/exports/referentiel-logements.json")
def referentiel_logements():
    """Le référentiel des logements (`REFERENTIEL_LOGEMENTS_V1`), à donner à l'outil qui rédige les
    MD structurés des factures. Déplacé depuis l'écran Factures : c'est un export de données. Même
    générateur, inchangé — `referentiel_version` est l'empreinte du contenu."""
    from app.services import referentiel_logements_export_service as ref_export

    return Response(ref_export.exporter_json(), media_type="application/json", headers={
        "Content-Disposition": f'attachment; filename="{ref_export.NOM_FICHIER}"'})


def _refus(resultat: dict) -> Response:
    """Le moteur ABANDONNE s'il détecte une colonne sensible : mieux vaut aucun export qu'un export
    qui fuit — ce refus est remonté tel quel."""
    return Response(f"Export refusé : {resultat.get('message', '')}", status_code=409,
                    media_type="text/plain; charset=utf-8")


@router.get("/exports/fichier/{nom}")
def telecharger(nom: str):
    """Un fichier, construit à l'instant. Le nom n'est qu'une clé parmi les exports connus : il ne
    désigne jamais un chemin, donc aucune chaîne venue de l'URL ne peut atteindre le disque."""
    resultat = lot13.produire()
    if not resultat["ok"]:
        return _refus(resultat)
    contenu = resultat["fichiers"].get(nom)
    if contenu is None:
        return Response("Fichier introuvable.", status_code=404, media_type="text/plain")
    return Response(contenu, media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{nom}"'})


@router.get("/exports/archive.zip")
def archive():
    """Tous les exports en une archive, construits à l'instant et assemblés en mémoire — un seul
    passage, donc des fichiers cohérents entre eux."""
    resultat = lot13.produire()
    if not resultat["ok"]:
        return _refus(resultat)
    tampon = io.BytesIO()
    with zipfile.ZipFile(tampon, "w", zipfile.ZIP_DEFLATED) as archive_zip:
        for nom, contenu in resultat["fichiers"].items():
            archive_zip.writestr(nom, contenu)
    # PROVISOIRE dans le nom : l'export couvre TOUS les mois, donc le mois en cours, qui est
    # incomplet et bougera encore. Un fichier téléchargé puis transmis n'emporte pas les
    # avertissements de l'écran — son nom, si.
    nom = f"exports_{date.today().strftime('%Y%m%d')}_PROVISOIRE.zip"
    return Response(tampon.getvalue(), media_type="application/zip",
                    headers={"Content-Disposition": f'attachment; filename="{nom}"'})
