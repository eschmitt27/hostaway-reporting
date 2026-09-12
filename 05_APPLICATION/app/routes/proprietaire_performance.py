"""Relevé propriétaire — écran économique (§13-§15) et export utilisateur (§16)."""
import csv
import io

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from app.services import proprietaire_performance_service as svc
from app.template_env import get_templates

router = APIRouter()
templates = get_templates()


def _mois_retenu(mois: str, disponibles: list[str]) -> str:
    """Le mois demandé s'il est valide, sinon le dernier mois qui porte des données.

    Jamais le mois courant par défaut : sur une base dont le dernier calcul date d'août, ouvrir
    l'écran sur septembre afficherait un relevé vide et donnerait à croire qu'il n'y a rien.
    """
    if svc.mois_valide(mois):
        return mois
    return disponibles[0] if disponibles else svc.mois_courant()


@router.get("/releves-proprietaires", response_class=HTMLResponse)
def releves(request: Request, mois: str = "", proprietaire_id: str = ""):
    disponibles = svc.mois_disponibles()
    periode = _mois_retenu(mois, disponibles)
    proprietaires = svc.proprietaires_du_mois(periode)

    choisi = proprietaire_id if any(p["proprietaire_id"] == proprietaire_id
                                    for p in proprietaires) else ""
    releve = svc.releve(choisi, periode) if choisi else None
    synthese = [svc.releve(p["proprietaire_id"], periode) for p in proprietaires] \
        if not choisi else []

    return templates.TemplateResponse(request, "proprietaire_performance.html", {
        "active_menu": "releves_proprietaires",
        "mois": periode,
        "mois_disponibles": disponibles,
        "proprietaires": proprietaires,
        "proprietaire_id": choisi,
        "releve": releve,
        "synthese": synthese,
        "totaux": _totaux(synthese) if synthese else None,
        "provisoire": periode >= svc.mois_courant(),
        "formules": svc.FORMULES,
    })


def _totaux(releves: list[dict]) -> dict:
    """Somme du parc. Les moyennes sont RECALCULÉES sur les totaux, jamais moyennées entre
    propriétaires : une moyenne de moyennes donne un poids identique à un parc d'un logement et à
    un parc de six."""
    nuits = sum(r.get("nuits_occupees") or 0 for r in releves)
    percu = round(sum(r.get("total_percu") or 0 for r in releves), 2)
    net = round(sum(r.get("net_proprietaire") or 0 for r in releves), 2)
    commercialisables = sum(r.get("nuits_commercialisables") or 0 for r in releves)
    reservations = sum(r.get("nb_reservations") or 0 for r in releves)
    return {
        "nb_proprietaires": len(releves),
        "nb_reservations": reservations,
        "nuits_occupees": nuits,
        "nuits_commercialisables": commercialisables,
        "duree_moyenne": round(nuits / reservations, 1) if reservations else None,
        "taux_remplissage": round(100 * nuits / commercialisables, 1) if commercialisables else None,
        "total_percu": percu,
        "menages": round(sum(r.get("menages") or 0 for r in releves), 2),
        "commission": round(sum(r.get("commission") or 0 for r in releves), 2),
        "net_proprietaire": net,
        "adr": round(percu / nuits, 2) if nuits else None,
        "adr_net_proprietaire": round(net / nuits, 2) if nuits else None,
    }


# ── Export (§16) ────────────────────────────────────────────────────────────────────────────────

COLONNES = ("mois", "proprietaire", "logement", "reservations", "nuits",
            "nuits_commercialisables", "taux_remplissage_pct", "total_percu", "menages",
            "commission", "net_proprietaire", "adr", "adr_net_proprietaire", "provisoire")


@router.get("/releves-proprietaires/export.csv")
def export_csv(mois: str = "", proprietaire_id: str = ""):
    """Le relevé du mois, au grain logement. Généré en mémoire, aucun fichier écrit sur disque.

    `provisoire` est une COLONNE, pas une mention en en-tête : un fichier se découpe, se trie et se
    recolle, et la mention se perdrait au premier filtre.
    """
    disponibles = svc.mois_disponibles()
    periode = _mois_retenu(mois, disponibles)
    provisoire = "OUI" if periode >= svc.mois_courant() else "NON"

    cibles = [proprietaire_id] if proprietaire_id else \
        [p["proprietaire_id"] for p in svc.proprietaires_du_mois(periode)]

    tampon = io.StringIO()
    ecrivain = csv.writer(tampon, delimiter=";", lineterminator="\n")
    ecrivain.writerow(COLONNES)
    for identifiant in cibles:
        releve = svc.releve(identifiant, periode)
        if releve.get("status") != svc.ST_OK:
            continue
        for ligne in releve["logements"]:
            ecrivain.writerow([
                periode, releve["proprietaire"], ligne["logement"], ligne["reservations"],
                ligne["nuits"], ligne["nuits_commercialisables"],
                _nombre(ligne["taux_remplissage"]), _nombre(ligne["total_percu"]),
                _nombre(ligne["menages"]), _nombre(ligne["commission"]),
                _nombre(ligne["net_proprietaire"]), _nombre(ligne["adr"]),
                _nombre(ligne["adr_net_proprietaire"]), provisoire])

    nom = f"releve_proprietaires_{periode}{'_PROVISOIRE' if provisoire == 'OUI' else ''}.csv"
    return Response(
        # BOM utf-8 : sans lui, Excel en configuration française lit les accents de travers, et
        # l'utilisateur conclut que l'export est cassé.
        content="﻿" + tampon.getvalue(), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{nom}"'})


def _nombre(v) -> str:
    """Décimale FRANÇAISE : un tableur configuré en français lit « 44.62 » comme du texte."""
    return "" if v is None else str(v).replace(".", ",")
