"""Périmètre des écrans Résultats — synthèse (`/resultats`) et analyse détaillée (`/resultats/pilotage`).

PRÉSENTATION UNIQUEMENT. Ce module lit les paramètres d'URL (période, propriétaire, logement,
plateforme, vision), les valide, et prépare ce que l'écran affiche AUTOUR des chiffres : libellé de
la période, raccourcis, badges des filtres actifs avec leur lien de retrait, lien de
réinitialisation, période précédente comparable. Aucune valeur économique n'est calculée ici,
aucune lecture ni écriture en base : les deux écrans partagent ainsi exactement les mêmes filtres,
avec les mêmes noms de paramètres et le même langage.

Paramètres d'URL (tous facultatifs, tous en lecture) :
    du, au          bornes incluses AAAA-MM. Présentes mais vides = période non bornée de ce côté
                    (`?du=&au=` = toute la période). Absentes toutes les deux = période par défaut.
    mois            ancien paramètre d'un mois unique, toujours accepté (liens existants).
    proprietaire_id, logement_id, canal, vision
"""
from __future__ import annotations

import datetime as _dt
import re
from typing import Any
from urllib.parse import urlencode

_RE_MOIS = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

MOIS_FR = ("janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août",
           "septembre", "octobre", "novembre", "décembre")

VISION_DEFAUT = "REEL"
VISIONS = {"REEL": "Réel", "COMPTABLE": "Comptable", "HORS_COMPTA": "Hors comptabilité"}

#: Période ouverte par défaut : le dernier mois disponible (synthèse) ou toute la période (analyse).
DEFAUT_DERNIER_MOIS = "dernier_mois"
DEFAUT_TOUT = "tout"


def mois_valide(valeur: Any) -> str:
    """`2026-09` si la valeur est un mois AAAA-MM valide, sinon chaîne vide."""
    texte = str(valeur or "").strip()[:7]
    return texte if _RE_MOIS.match(texte) else ""


def decaler(mois: str, n: int) -> str:
    """`decaler("2026-01", -1)` → `2025-12`."""
    idx = int(mois[:4]) * 12 + int(mois[5:7]) - 1 + n
    return f"{idx // 12:04d}-{idx % 12 + 1:02d}"


def nb_mois(du: str, au: str) -> int:
    return (int(au[:4]) * 12 + int(au[5:7])) - (int(du[:4]) * 12 + int(du[5:7])) + 1


def libelle_mois(mois: str) -> str:
    """`2026-09` → `Septembre 2026`."""
    if not mois_valide(mois):
        return mois or ""
    return f"{MOIS_FR[int(mois[5:7]) - 1].capitalize()} {mois[:4]}"


def libelle_periode(du: str, au: str) -> str:
    """Période lisible : « Septembre 2026 », « Juillet → Septembre 2026 », « Année 2026 »,
    « Novembre 2025 → Février 2026 », « Depuis mars 2026 », « Toute la période »."""
    if not du and not au:
        return "Toute la période"
    if du and not au:
        return f"Depuis {libelle_mois(du).lower()}"
    if au and not du:
        return f"Jusqu'à {libelle_mois(au).lower()}"
    if du == au:
        return libelle_mois(du)
    if du[:4] == au[:4]:
        if du[5:] == "01" and au[5:] == "12":
            return f"Année {du[:4]}"
        return f"{MOIS_FR[int(du[5:7]) - 1].capitalize()} → {libelle_mois(au)}"
    return f"{libelle_mois(du)} → {libelle_mois(au)}"


def _mois_courant(aujourdhui: _dt.date | None) -> str:
    d = aujourdhui or _dt.date.today()
    return f"{d.year:04d}-{d.month:02d}"


def raccourcis_periode(aujourdhui: _dt.date | None = None) -> list[dict[str, str]]:
    """Raccourcis calendaires, calculés depuis la date du jour (jamais inventés sur les données :
    un raccourci sans donnée mène à l'état vide, explicite)."""
    courant = _mois_courant(aujourdhui)
    annee = courant[:4]
    return [
        {"cle": "mois", "libelle": "Mois en cours", "du": courant, "au": courant},
        {"cle": "precedent", "libelle": "Mois précédent", "du": decaler(courant, -1), "au": decaler(courant, -1)},
        {"cle": "3m", "libelle": "3 derniers mois", "du": decaler(courant, -2), "au": courant},
        {"cle": "6m", "libelle": "6 derniers mois", "du": decaler(courant, -5), "au": courant},
        {"cle": "annee", "libelle": "Année en cours", "du": f"{annee}-01", "au": f"{annee}-12"},
        {"cle": "tout", "libelle": "Toute la période", "du": "", "au": ""},
    ]


def periode_par_defaut(mois_disponibles: list[str], defaut: str,
                       aujourdhui: _dt.date | None = None) -> tuple[str, str]:
    """Synthèse : le mois en cours s'il a des données, sinon le dernier mois disponible déjà
    commencé, sinon le dernier disponible. Analyse : toute la période."""
    if defaut == DEFAUT_TOUT or not mois_disponibles:
        return "", ""
    courant = _mois_courant(aujourdhui)
    passes = [m for m in mois_disponibles if m <= courant]
    m = passes[-1] if passes else mois_disponibles[-1]
    return m, m


def lire(*, chemin: str, mois_disponibles: list[str], du: str | None = None,
         au: str | None = None, mois: str | None = None, proprietaire_id: str = "",
         logement_id: str = "", canal: str = "", vision: str | None = None,
         defaut: str = DEFAUT_DERNIER_MOIS, avec_vision: bool = False,
         aujourdhui: _dt.date | None = None) -> dict[str, Any]:
    """Normalise les paramètres reçus et renvoie le périmètre prêt à afficher.

    `chemin` : l'écran qui construit ses liens (retrait d'un filtre, raccourcis, réinitialisation).
    `mois_disponibles` : mois réellement présents dans Lot10 — jamais un calendrier inventé.
    """
    dispo = sorted({m for m in mois_disponibles if mois_valide(m)})
    if du is None and au is None:
        if mois_valide(mois):
            d, a = mois_valide(mois), mois_valide(mois)
        elif mois is not None and not str(mois).strip():
            d, a = "", ""                      # ancien « Tous les mois » de l'analyse
        else:
            d, a = periode_par_defaut(dispo, defaut, aujourdhui)
    else:
        d, a = mois_valide(du), mois_valide(au)
    if d and a and d > a:
        d, a = a, d
    periode_defaut = periode_par_defaut(dispo, defaut, aujourdhui)

    vision = (vision or VISION_DEFAUT).upper()
    if vision not in VISIONS:
        vision = VISION_DEFAUT
    proprietaire_id = str(proprietaire_id or "").strip()
    logement_id = str(logement_id or "").strip()
    canal = str(canal or "").strip().upper()

    etat = {"du": d, "au": a, "proprietaire_id": proprietaire_id, "logement_id": logement_id,
            "canal": canal, "vision": vision}

    def url(chemin_cible: str | None = None, **changements: str) -> str:
        """Lien vers `chemin_cible` (défaut : cet écran) avec le périmètre courant, modifié."""
        p = {**etat, **changements}
        params: list[tuple[str, str]] = [("du", p["du"]), ("au", p["au"])]
        for cle in ("proprietaire_id", "logement_id", "canal"):
            if p[cle]:
                params.append((cle, p[cle]))
        if avec_vision and p["vision"] != VISION_DEFAUT:
            params.append(("vision", p["vision"]))
        return f"{chemin_cible or chemin}?{urlencode(params)}"

    mois_periode = [m for m in dispo if (not d or m >= d) and (not a or m <= a)]
    raccourcis = [{**r, "url": url(du=r["du"], au=r["au"]), "actif": (r["du"], r["au"]) == (d, a)}
                  for r in raccourcis_periode(aujourdhui)]

    precedente = None
    if d and a:
        n = nb_mois(d, a)
        p_du, p_au = decaler(d, -n), decaler(a, -n)
        # Comparaison seulement si les DEUX périodes sont entièrement couvertes par des données :
        # « Année en cours » (mois à venir sans données) face à une année pleine serait trompeur.
        couverts = set(dispo)
        if all(decaler(d, -k) in couverts for k in range(-n + 1, n + 1)):
            precedente = {"du": p_du, "au": p_au, "libelle": libelle_periode(p_du, p_au),
                          "nature": "Mois précédent" if n == 1 else "Période précédente"}

    return {
        **etat,
        "chemin": chemin,
        "mois_disponibles": dispo,
        # Options des listes « Du / Au » : un mois reçu hors liste (raccourci sans donnée) reste
        # affiché sélectionné plutôt que de basculer silencieusement sur un autre.
        "options_mois": sorted(set(dispo) | {m for m in (d, a) if m}),
        "libelle_periode": libelle_periode(d, a),
        "mois_unique": d if d and d == a else "",
        "mois_periode": mois_periode,
        "nb_mois_periode": nb_mois(d, a) if d and a else len(mois_periode),
        "raccourcis": raccourcis,
        "precedente": precedente,
        "periode_modifiee": (d, a) != periode_defaut,
        "vision_libelle": VISIONS[vision],
        "avec_vision": avec_vision,
        "url": url,
        "url_reinit": chemin,
    }


def badges(perimetre: dict[str, Any], *, nom_proprietaire: str = "", nom_logement: str = "",
           nom_canal: str = "") -> list[dict[str, str]]:
    """Filtres actifs, dans l'ordre de lecture. La période est toujours dite (elle n'a pas de
    retrait : elle se change) ; chaque autre filtre porte le lien qui le retire."""
    p = perimetre
    url = p["url"]
    out = [{"libelle": "Période", "valeur": p["libelle_periode"], "retrait": ""}]
    if p["proprietaire_id"]:
        out.append({"libelle": "Propriétaire", "valeur": nom_proprietaire or p["proprietaire_id"],
                    "retrait": url(proprietaire_id="")})
    if p["logement_id"]:
        out.append({"libelle": "Logement", "valeur": nom_logement or p["logement_id"],
                    "retrait": url(logement_id="")})
    if p["canal"]:
        out.append({"libelle": "Plateforme", "valeur": nom_canal or p["canal"],
                    "retrait": url(canal="")})
    if p["avec_vision"] and p["vision"] != VISION_DEFAUT:
        out.append({"libelle": "Vision", "valeur": p["vision_libelle"],
                    "retrait": url(vision=VISION_DEFAUT)})
    return out


def filtres_actifs(perimetre: dict[str, Any]) -> bool:
    """Vrai dès que l'écran ne montre plus sa vue par défaut — c'est là que « Réinitialiser » sert."""
    p = perimetre
    return bool(p["periode_modifiee"] or p["proprietaire_id"] or p["logement_id"] or p["canal"]
                or (p["avec_vision"] and p["vision"] != VISION_DEFAUT))
