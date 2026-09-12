"""Instance Jinja2Templates PARTAGÉE + filtres de libellés métier centralisés.

Règle UI transverse (mission « supprimer les IDs techniques de tous les écrans utilisateur ») :
un identifiant technique (PROP_*, LOG_*, TYPE_*, FOUR-*, INT_*, ASSOC_*…) ne s'affiche jamais tel
quel dans un écran opérationnel — seul le vrai libellé métier s'affiche, avec un texte explicite
« non résolu » si le référentiel ne connaît pas encore l'identifiant. L'ID reste disponible partout
où c'est réellement utile (base, jointures, routes, services, Administration des référentiels).

Chaque route créait jusqu'ici sa PROPRE `Jinja2Templates(directory=...)`, avec ses propres filtres
enregistrés en double (parfois oubliés) — fragile et non centralisé, exactement le problème que
cette mission demande de corriger. Toutes les routes doivent désormais utiliser `get_templates()`,
une seule instance, un seul endroit où les filtres de libellé sont posés.
"""
from __future__ import annotations

from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import referentiel_service as ref_svc

_templates: Jinja2Templates | None = None


def _nom_proprietaire(pid: str) -> str:
    return ref_svc.libelle_proprietaire(pid) if pid else ""


def _nom_logement(lid: str) -> str:
    return ref_svc.libelle_logement(lid) if lid else ""


def _nom_type_logement(tid: str) -> str:
    return ref_svc.libelle_type_logement(tid) if tid else ""


def _nom_fournisseur(fid: str) -> str:
    return ref_svc.libelle_fournisseur(fid) if fid else ""


def _nom_intervenant(iid: str) -> str:
    return ref_svc.libelle_intervenant(iid) if iid else ""


def _nom_associe(pid: str) -> str:
    return ref_svc.libelle_associe(pid) if pid else ""


def _nom_mode_paiement(mid: str) -> str:
    return ref_svc.libelle_mode_paiement(mid) if mid else ""


def _nom_categorie_charge(cid: str) -> str:
    return ref_svc.libelle_categorie_charge(cid) if cid else ""


def _nom_type_flux(tid: str) -> str:
    return ref_svc.libelle_type_flux(tid) if tid else ""


def _libelle_statut(code: str) -> str:
    """`PARTIELLEMENT_REGLEE` → `Partiellement réglée`. Purement typographique : le statut STOCKÉ
    reste le code canonique, seul son affichage change (mission « plus de codes à soulignés
    dans l'UI »)."""
    return ref_svc.humaniser_code(code) if code else ""


def _nom_canal(cid: str) -> str:
    return ref_svc.libelle_canal(cid) if cid else ""


# ── Formateurs français canoniques (recette utilisateur n°3, §7 et §63) ──────────────────────────
#
# UN seul formateur, ici, plutôt qu'un `strftime` recopié dans chaque template : l'écran Ménages
# affichait `2026-09-11T22:32:26Z` (ISO brut, T, Z, secondes) là où l'utilisateur attend
# `22h32 · 11/09/2026`. Et `15.0` réservations là où une quantité discrète s'écrit `15`.

def _datetime_fr(valeur) -> str:
    """`2026-09-11T22:32:26Z` → `22h32 · 11/09/2026`. Rend la valeur telle quelle si illisible.

    AUCUNE conversion de fuseau : l'heure affichée est celle qui est écrite dans la donnée. Ces
    horodatages sont produits et relus sur le même poste, et l'utilisateur les rapproche de ses
    propres actions (« j'ai actualisé à 22h32 »). Les décaler de deux heures au nom d'un `Z`
    rendrait chaque trace fausse à ses yeux, pour une exactitude dont personne n'a l'usage ici.
    """
    from datetime import datetime

    texte = str(valeur or "").strip()
    if not texte:
        return ""
    nu = texte.replace("Z", "").replace("T", " ").strip()
    if len(nu) <= 10:          # une date seule reste une date : pas de « 00h00 » inventé
        return _date_fr(nu)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(nu[:len(fmt) + 2].strip()[:19], fmt)
            break
        except ValueError:
            continue
    else:
        return texte
    return f"{dt:%Hh%M} · {dt:%d/%m/%Y}"


def _date_fr(valeur) -> str:
    """`2026-09-11` → `11/09/2026`. Rend la valeur telle quelle si illisible."""
    from datetime import datetime

    texte = str(valeur or "").strip()
    if not texte:
        return ""
    try:
        return f"{datetime.strptime(texte[:10], '%Y-%m-%d'):%d/%m/%Y}"
    except ValueError:
        return texte


def _entier(valeur) -> str:
    """`15.0` → `15`. Toute quantité discrète s'affiche en entier (§63)."""
    if valeur is None or valeur == "":
        return ""
    try:
        return str(int(round(float(valeur))))
    except (TypeError, ValueError):
        return str(valeur)


def get_templates() -> Jinja2Templates:
    """Instance Jinja2Templates unique, partagée par toutes les routes."""
    global _templates
    if _templates is None:
        t = Jinja2Templates(directory=str(TEMPLATES_DIR))
        t.env.filters["nom_proprietaire"] = _nom_proprietaire
        t.env.filters["nom_logement"] = _nom_logement
        t.env.filters["nom_type_logement"] = _nom_type_logement
        t.env.filters["nom_fournisseur"] = _nom_fournisseur
        t.env.filters["nom_prestataire"] = _nom_fournisseur
        t.env.filters["nom_intervenant"] = _nom_intervenant
        t.env.filters["nom_associe"] = _nom_associe
        t.env.filters["nom_mode_paiement"] = _nom_mode_paiement
        t.env.filters["nom_categorie_charge"] = _nom_categorie_charge
        t.env.filters["nom_type_flux"] = _nom_type_flux
        t.env.filters["libelle_statut"] = _libelle_statut
        t.env.filters["nom_canal"] = _nom_canal
        t.env.filters["datetime_fr"] = _datetime_fr
        t.env.filters["date_fr"] = _date_fr
        t.env.filters["entier"] = _entier
        # §45 — un numéro se STOCKE en E.164 et se LIT en groupes de deux. Le filtre est le seul
        # point de passage vers l'affichage : un numéro brut reste rendu tel quel, jamais inventé.
        t.env.filters["telephone"] = _telephone
        _templates = t
    return _templates


def _telephone(valeur) -> str:
    from app.services import telephone_service as tel
    return tel.afficher(valeur)
