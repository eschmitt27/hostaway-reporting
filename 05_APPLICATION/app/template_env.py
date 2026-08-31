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
        _templates = t
    return _templates
