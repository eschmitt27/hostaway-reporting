"""APP-5D — Export CSV du tableau de bord. Agrégation lecture seule, mêmes protections que APP-5C."""
from __future__ import annotations

import csv
import io
import re
from datetime import datetime

_RE_MOIS_SUR = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_AMORCES_FORMULE = ("=", "+", "-", "@", "\t", "\r")

_DISCLAIMER = ("Ce document ne constitue pas la clôture comptable réelle — synthèse agrégée en "
              "lecture seule, aucune donnée recalculée.")


def _cellule_sure(valeur) -> str:
    s = "" if valeur is None else str(valeur)
    if s and s[0] in _AMORCES_FORMULE:
        return "'" + s
    return s


def nom_fichier(filtre_mois: str = "") -> str:
    suffixe = filtre_mois if _RE_MOIS_SUR.match(str(filtre_mois or "")) else "tous"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"pilotage_mensuel_{suffixe}_{ts}.csv"


_COLONNES = [
    ("mois", "mois"), ("statut_moteur", "statut_reel_moteur"),
    ("statut_humain_libelle", "statut_suivi_humain_app5c"), ("indicateur_libelle", "indicateur_global"),
    ("nb_bloquants", "nb_controles_bloquants"), ("nb_exceptions", "nb_exceptions"),
    ("nb_mouvements_a_controler", "nb_mouvements_bancaires_a_controler"),
    ("nb_menages_a_controler", "nb_menages_a_controler"),
    ("nb_reglements_a_controler", "nb_reglements_proprietaires_a_controler"),
    ("nb_charges_mois", "nb_charges_du_mois"), ("nb_reservations_mois", "nb_reservations_du_mois"),
    ("derniere_action_humaine", "derniere_action_humaine_app5c"),
]


def exporter(tableau: dict) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\n")

    def row(*vals):
        w.writerow([_cellule_sure(v) for v in vals])

    row("AVERTISSEMENT", _DISCLAIMER)
    if tableau.get("indisponibles"):
        row("BLOCS INDISPONIBLES AU MOMENT DE L'EXPORT", ", ".join(tableau["indisponibles"]))
    w.writerow([])
    row(*[label for _, label in _COLONNES])
    for ligne in tableau["lignes"]:
        row(*[ligne.get(key) if ligne.get(key) is not None else "—" for key, _ in _COLONNES])
    return buf.getvalue()
