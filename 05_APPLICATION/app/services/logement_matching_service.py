"""§23-25 — proposer le logement d'une ligne de facture fournisseur, sans jamais l'inventer.

CE QUE CE MODULE REMPLACE, ET POURQUOI
`02_TRAVAIL/lib_menages_externes_pdf.py` portait un dictionnaire `MAPPING_LOGEMENTS` de seize
entrées écrites à la main, chacune étant la transcription humaine d'un libellé vu un jour sur une
facture. Ce dictionnaire ne connaissait que ce qu'on y avait recopié : un logement nouveau, un
prestataire nouveau, ou simplement une ponctuation différente, et la ligne ressortait « non
affectée » alors que le référentiel, lui, contenait la réponse. `ref_mapping_logements` compte 86
correspondances déclarées, `ref_logements` porte nom officiel, nom court et adresse : la
connaissance existait déjà, elle n'était pas lue.

Ce module lit le RÉFÉRENTIEL et n'a aucune table de correspondance en dur.

LES CINQ VOIES, DE LA PLUS SÛRE À LA PLUS FAIBLE
1. `MAPPING_REFERENTIEL` — une correspondance déclarée dans `ref_mapping_logements` (actif=OUI).
   C'est une décision humaine enregistrée : elle prime sur tout raisonnement.
2. `NOM_OFFICIEL` / 3. `NOM_COURT` — égalité du libellé normalisé avec le nom du logement.
4. `ADRESSE` — l'adresse du logement se lit dans le libellé de la ligne.
5. `TOKENS` — recouvrement pondéré des mots significatifs.

CE QUI REND LA VOIE 5 HONNÊTE : LE POIDS VIENT DES DONNÉES
Aucune liste d'« mots vides » écrite à la main (« rue », « de », « appartement »…) : une telle
liste est un jugement arbitraire qu'il faut ensuite maintenir. Le poids d'un mot est
`1 / nombre de logements qui le contiennent`. « rue », présent partout, ne pèse presque rien ;
« galache », présent une fois, pèse 1. Le référentiel calibre lui-même ce qui distingue.

CE QUE LE MODULE NE FAIT JAMAIS
Choisir entre deux logements à égalité. Une ambiguïté reste une ambiguïté : elle ressort en
`AUCUN` avec ses candidats nommés, pour que l'humain tranche. Un quasi-match n'est jamais promu en
certitude : il ressort `PROBABLE`, prérempli mais marqué « à confirmer ».

Les logements techniques hors parc (`HORS_PARC_TECHNIQUE`, les bacs « divers ») sont exclus des
voies déductives : proposer tout seul un bac fourre-tout serait précisément l'invention à éviter.
Ils restent atteignables par une correspondance explicitement déclarée.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Any

from app.db.connection import get_db

#: Le libellé désigne ce logement sans ambiguïté — préremplissage sans réserve.
CERTAIN = "CERTAIN"
#: Un seul candidat sérieux, mais l'égalité n'est pas parfaite — prérempli ET marqué à confirmer.
PROBABLE = "PROBABLE"
#: Rien de sûr, ou plusieurs candidats à égalité — aucun préremplissage.
AUCUN = "AUCUN"

M_MAPPING = "MAPPING_REFERENTIEL"
M_NOM_OFFICIEL = "NOM_OFFICIEL"
M_NOM_COURT = "NOM_COURT"
M_ADRESSE = "ADRESSE"
M_TOKENS = "TOKENS"
M_AMBIGU = "AMBIGU"

#: Recouvrement pondéré minimal pour retenir un candidat par mots significatifs.
SEUIL_TOKENS = 0.60
#: Avance minimale du premier sur le second. En deçà, les deux sont candidats et aucun n'est choisi.
MARGE_TOKENS = 0.15

STATUT_HORS_PARC = "HORS_PARC_TECHNIQUE"

_APOSTROPHES = "’ʼ`´"


def normaliser(libelle: str) -> str:
    """Forme canonique d'un libellé : accents, casse, ponctuation et espaces neutralisés.

    Le type d'habitation est recollé (`T.4`, `T 4`, `t-4` → `t4`) parce que les prestataires
    l'écrivent de toutes les façons pour désigner la même chose ; sans cela `T.4-90 Blagnac` et
    `T4 - 90 Blagnac` seraient deux logements différents.
    """
    s = str(libelle or "")
    for a in _APOSTROPHES:
        s = s.replace(a, "'")
    s = "".join(c for c in unicodedata.normalize("NFD", s)
                if unicodedata.category(c) != "Mn")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\bt\s+(\d)\b", r"t\1", s)      # t 4 -> t4
    return re.sub(r"\s+", " ", s).strip()


def tokens(libelle: str) -> set[str]:
    """Mots de la forme normalisée. Les mots d'une seule lettre (`l'`, `d'`) ne distinguent rien."""
    return {t for t in normaliser(libelle).split(" ") if len(t) > 1}


def charger_referentiel(db_path=None) -> list[dict[str, Any]]:
    """Le référentiel de rapprochement, tel qu'il est en base — jamais une copie figée dans le code.

    Les logements RETIRÉS sont inclus : une facture d'un mois passé désigne légitimement un
    logement qui n'est plus géré aujourd'hui. Leur état est exposé (`actif`, `statut_parc`) pour
    que l'écran puisse le signaler, pas pour les écarter du rapprochement.
    """
    conn = get_db(db_path)
    try:
        logements = [dict(r) for r in conn.execute(
            "SELECT logement_id, nom_logement_officiel, nom_court, adresse, ville, actif, "
            "statut_parc FROM ref_logements ORDER BY logement_id").fetchall()]
        mappings = [dict(r) for r in conn.execute(
            "SELECT valeur_source, logement_id, niveau_confiance, champ_source, source "
            "FROM ref_mapping_logements WHERE UPPER(COALESCE(actif,'OUI')) = 'OUI'").fetchall()]
    finally:
        conn.close()

    par_id: dict[str, dict[str, Any]] = {}
    for l in logements:
        lid = str(l.get("logement_id") or "").strip()
        if not lid:
            continue
        l["aliases"] = []
        par_id[lid] = l
    for m in mappings:
        lid = str(m.get("logement_id") or "").strip()
        if lid in par_id:
            par_id[lid]["aliases"].append(m)
    return list(par_id.values())


def _deductible(logement: dict[str, Any]) -> bool:
    """Un bac technique « divers » ne se déduit pas : il se choisit."""
    return str(logement.get("statut_parc") or "").strip().upper() != STATUT_HORS_PARC


def _poids_tokens(referentiel: list[dict[str, Any]]) -> dict[str, float]:
    """Poids discriminant de chaque mot : l'inverse du nombre de logements qui l'emploient.

    Calibré sur le référentiel lui-même, donc toujours à jour — un mot qui devient banal parce que
    le parc s'agrandit perd son poids tout seul, sans que personne n'ait à modifier une liste.
    """
    freq: dict[str, int] = {}
    for l in referentiel:
        if not _deductible(l):
            continue
        for t in _tokens_logement(l):
            freq[t] = freq.get(t, 0) + 1
    return {t: 1.0 / n for t, n in freq.items()}


def _tokens_logement(logement: dict[str, Any]) -> set[str]:
    """Tout ce que le référentiel sait dire de ce logement : noms, adresse, ville, alias."""
    out: set[str] = set()
    for champ in ("nom_logement_officiel", "nom_court", "adresse", "ville"):
        out |= tokens(logement.get(champ) or "")
    for alias in logement.get("aliases") or []:
        out |= tokens(alias.get("valeur_source") or "")
    return out


def _candidat(logement: dict[str, Any], confiance: str, methode: str, raison: str,
              score: float = 1.0, signaux: list[str] | None = None) -> dict[str, Any]:
    return {
        "signaux": list(signaux or []),
        "logement_id": logement.get("logement_id") or "",
        "libelle": (logement.get("nom_logement_officiel") or logement.get("nom_court")
                    or logement.get("logement_id") or ""),
        "actif": str(logement.get("actif") or "").strip().upper() == "OUI",
        "statut_parc": logement.get("statut_parc") or "",
        "confiance": confiance, "methode": methode, "raison": raison,
        "score": round(float(score), 3),
    }


def _resultat(candidat: dict[str, Any] | None, *, candidats: list[dict[str, Any]] | None = None,
              raison: str = "") -> dict[str, Any]:
    if candidat is None:
        return {"logement_id": "", "confiance": AUCUN, "methode": "",
                "a_confirmer": False, "preremplir": False,
                "raison": raison or "Aucun logement du référentiel ne correspond à ce libellé.",
                "score": 0.0, "signaux": [],
                "candidats": candidats or []}
    return {
        "logement_id": candidat["logement_id"],
        "confiance": candidat["confiance"],
        "methode": candidat["methode"],
        "a_confirmer": candidat["confiance"] == PROBABLE,
        "preremplir": candidat["confiance"] in (CERTAIN, PROBABLE),
        "raison": candidat["raison"],
        "score": candidat["score"],
        "signaux": candidat["signaux"],
        "libelle": candidat["libelle"],
        "actif": candidat["actif"],
        "statut_parc": candidat["statut_parc"],
        "candidats": candidats if candidats is not None else [candidat],
    }


def proposer(libelle_source: str, referentiel: list[dict[str, Any]]) -> dict[str, Any]:
    """Propose un logement pour `libelle_source`. FONCTION PURE — aucun accès base.

    Les voies sont essayées dans l'ordre de fiabilité décroissante et la première qui conclut
    s'arrête là : une correspondance déclarée par un humain n'a pas à être confirmée par un calcul
    de mots.
    """
    cle = normaliser(libelle_source)
    if not cle:
        return _resultat(None, raison="Libellé vide : rien à rapprocher.")
    signaux = _signaux_libelle(libelle_source, referentiel)
    adresse = _voie_adresse(signaux)

    # 1 — correspondance déclarée au référentiel. Le libellé est essayé tel quel, puis sans son
    # adresse entre parenthèses : « T3 sept deniers (François) (4 rue Bardou) » est la même
    # désignation que l'alias « T3 - sept deniers (François) ». Un alias qui ne dit qu'une ville
    # ou un type (« BLAGNAC », « T3 ») ne désigne rien : il n'est jamais une correspondance.
    cles = {cle, normaliser(re.sub(r"\(\s*\d[^)]*\)", " ", str(libelle_source or "")))} - {""}
    neutres = _mots_neutres(referentiel)
    exacts = [(l, a) for l in referentiel for a in (l.get("aliases") or [])
              if normaliser(a.get("valeur_source") or "") in cles
              and not tokens(a.get("valeur_source") or "") <= neutres]
    ids = {l["logement_id"] for l, _ in exacts}
    if len(ids) == 1:
        logement, alias = exacts[0]
        if adresse and adresse["complete"] \
                and adresse["logement"]["logement_id"] != logement["logement_id"]:
            # Un humain a dit X, l'adresse écrite dit Y : aucune des deux ne gagne en silence.
            return _ambigu([
                _candidat(logement, AUCUN, M_MAPPING, "correspondance déclarée"),
                _candidat(adresse["logement"], AUCUN, M_ADRESSE, "adresse lue dans le libellé")])
        fort = str(alias.get("niveau_confiance") or "").strip().lower().startswith("fort")
        s = ["ALIAS_CONFIRME"] + (["ADRESSE_EXACTE"] if adresse and adresse["complete"] else [])
        return _resultat(_candidat(
            logement, CERTAIN if fort else PROBABLE, M_MAPPING,
            f"Correspondance déclarée au référentiel « {alias.get('source') or 'mapping'} » "
            f"(champ {alias.get('champ_source') or '?'}, confiance "
            f"{alias.get('niveau_confiance') or '?'}).", _score(s), s))
    if len(ids) > 1:
        return _ambigu([_candidat(l, AUCUN, M_MAPPING, "correspondance déclarée concurrente")
                        for l, _ in exacts])

    # 2 et 3 — égalité avec le nom officiel, puis le nom court.
    for champ, methode, etiquette in ((
            "nom_logement_officiel", M_NOM_OFFICIEL, "nom officiel"),
            ("nom_court", M_NOM_COURT, "nom court")):
        trouves = [l for l in referentiel
                   if _deductible(l) and normaliser(l.get(champ) or "") == cle]
        if len(trouves) == 1:
            return _resultat(_candidat(
                trouves[0], CERTAIN, methode,
                f"Le libellé est exactement le {etiquette} du logement, "
                f"accents et ponctuation mis à part."))
        if len(trouves) > 1:
            return _ambigu([_candidat(l, AUCUN, methode, f"même {etiquette}") for l in trouves])

    # 4 — l'adresse du logement se lit dans le libellé : numéro ET nom de la voie, mot à mot.
    if adresse is not None:
        return _resultat_adresse(adresse, signaux)
    if signaux["adresses_ambigues"]:
        return _ambigu([_candidat(l, AUCUN, M_ADRESSE, "même numéro et même voie")
                        for l in signaux["adresses_ambigues"]])

    # 4 bis — adresse contenue telle quelle, bornée aux mots (« 4 rue… » n'est pas « 14 rue… »).
    adresses = []
    for l in referentiel:
        adr = normaliser(l.get("adresse") or "")
        if _deductible(l) and adr and len(adr) > 4 and f" {adr} " in f" {cle} ":
            adresses.append((l, adr))
    if len({l["logement_id"] for l, _ in adresses}) == 1:
        logement, adr = adresses[0]
        return _resultat(_candidat(
            logement, PROBABLE, M_ADRESSE,
            f"L'adresse du logement au référentiel (« {logement.get('adresse')} ») se lit dans le "
            f"libellé de la ligne."))
    if len(adresses) > 1:
        # La plus longue adresse contenue l'emporte si elle est strictement plus précise.
        adresses.sort(key=lambda t: len(t[1]), reverse=True)
        if len(adresses[0][1]) > len(adresses[1][1]):
            logement = adresses[0][0]
            return _resultat(_candidat(
                logement, PROBABLE, M_ADRESSE,
                f"L'adresse du logement au référentiel (« {logement.get('adresse')} ») se lit dans "
                f"le libellé, et c'est la plus précise des adresses reconnues."))
        return _ambigu([_candidat(l, AUCUN, M_ADRESSE, "adresse également reconnue")
                        for l, _ in adresses])

    # 5 — recouvrement pondéré des mots significatifs.
    poids = _poids_tokens(referentiel)
    mots = tokens(cle)
    if not mots:
        return _resultat(None, raison="Ce libellé ne contient aucun mot exploitable.")
    # UN MOT INCONNU COMPTE CONTRE LE RAPPROCHEMENT, AU POIDS MAXIMAL.
    # Sans cela, « T5 - 200 avenue de Paris (Inconnu) » marquait 100 % sur un logement de Blagnac :
    # le seul mot commun était « avenue », et les quatre mots qui disaient justement qu'il s'agit
    # d'autre chose (t5, 200, paris, inconnu) étaient simplement ignorés du calcul. Un mot que le
    # référentiel n'a jamais vu est aussi distinctif qu'un mot propre à un seul logement — et le
    # candidat n'en explique rien.
    poids_libelle = {t: poids.get(t, 1.0) for t in mots}
    total = sum(poids_libelle.values())

    scores = []
    for l in referentiel:
        if not _deductible(l):
            continue
        communs = mots & _tokens_logement(l)
        if communs and _mots_designants(communs, referentiel):
            scores.append((sum(poids_libelle[t] for t in communs) / total, l, communs))
    scores.sort(key=lambda t: (-t[0], t[1]["logement_id"]))

    if not scores or scores[0][0] < SEUIL_TOKENS:
        meilleur = f" Le plus proche atteint {scores[0][0]:.0%}." if scores else ""
        type_ville = _candidats_type_ville(signaux, referentiel)
        if type_ville:
            return _resultat(None, candidats=type_ville, raison=(
                "Le type de logement et la ville ne suffisent pas à désigner un logement : "
                "les candidats sont proposés, il faut choisir."))
        return _resultat(None, raison=(
            f"Aucun logement ne partage assez de mots distinctifs avec ce libellé "
            f"(seuil {SEUIL_TOKENS:.0%}).{meilleur}"),
            candidats=[_candidat(l, AUCUN, M_TOKENS, "mots partagés insuffisants", s)
                       for s, l, _ in scores[:3]])

    if len(scores) > 1 and (scores[0][0] - scores[1][0]) < MARGE_TOKENS:
        return _ambigu([_candidat(l, AUCUN, M_TOKENS, "mots distinctifs partagés", s)
                        for s, l, _ in scores[:3] if s >= SEUIL_TOKENS - MARGE_TOKENS])

    score, logement, communs = scores[0]
    partages = ", ".join(sorted(communs, key=lambda t: -poids_libelle[t])[:4])
    return _resultat(_candidat(
        logement, PROBABLE, M_TOKENS,
        f"Mots distinctifs partagés avec ce logement ({partages}) : {score:.0%} du libellé. "
        f"Aucun autre logement n'en approche.", score),
        candidats=[_candidat(l, AUCUN, M_TOKENS, "autre candidat", s) for s, l, _ in scores[:3]])


#: Un type d'habitation : `t1`…`t9`, `studio`, `duplex`… Ce sont des CATÉGORIES, pas des adresses.
_RE_TYPE_HABITATION = re.compile(r"^(t\d|studio|duplex|loft|maison|villa|appartement|appart)$")


def _mots_designants(communs: set[str], referentiel: list[dict[str, Any]]) -> set[str]:
    """Parmi les mots partagés, ceux qui DÉSIGNENT un logement plutôt que de le décrire.

    « T3 » et « Toulouse » ne désignent rien : ils classent. Dans un parc de quatre logements où un
    seul est un T3 toulousain, le calcul de poids concluait pourtant à 100 % — mathématiquement
    unique, et métier faux : une ligne « T3 Toulouse » sur une facture ne dit pas QUEL T3, et
    imputer un ménage au seul T3 du parc parce qu'il se trouve être le seul serait exactement
    l'invention que §23 interdit. La distinction n'est pas une liste de mots écrite à la main : les
    villes viennent du champ `ville` du référentiel, les types d'un motif de forme.
    """
    villes = {v for l in referentiel for v in tokens(l.get("ville") or "")}
    return {t for t in communs if t not in villes and not _RE_TYPE_HABITATION.match(t)}


# ── Signaux structurés : adresse, type, propriétaire ─────────────────────────────────────────────
#
# Pas une liste de mots vides : la GRAMMAIRE d'une adresse française (type de voie, article) et
# d'une désignation de logement (type d'habitation, prénom du propriétaire entre parenthèses).
# Elle sert à lire la structure du libellé, jamais à décider seule.

_TYPES_VOIE = {"rue", "avenue", "av", "allee", "impasse", "place", "pl", "boulevard", "bd",
               "chemin", "route", "quai", "cour", "square", "residence", "lotissement", "voie",
               "sentier", "esplanade", "promenade", "faubourg"}
_ARTICLES = {"de", "du", "des", "la", "le", "les", "l", "d"}

#: Poids explicables de chaque signal : le score se lit, il ne se devine pas. La ville seule
#: vaut zéro — elle ne désigne jamais un logement.
POIDS_SIGNAUX = {"ADRESSE_EXACTE": 100, "NUMERO_RUE": 80, "ALIAS_CONFIRME": 70,
                 "PROPRIETAIRE_CONCORDANT": 15, "TYPE_CONCORDANT": 5,
                 "TYPE_DIFFERENT": -40, "PROPRIETAIRE_DIFFERENT": -40, "VILLE_SEULE": 0}


def _score(signaux: list[str]) -> float:
    return float(sum(POIDS_SIGNAUX.get(s, 0) for s in signaux))


def _mot(t: str) -> str:
    """Singulier grossier (« allées » → « allee », « verts » → « vert ») et numéro sans zéro de tête
    (« 04 » → « 4 ») : deux écritures d'une même voie se comparent mot à mot."""
    if t.isdigit():
        return t.lstrip("0") or "0"
    return t[:-1] if len(t) > 3 and t.endswith("s") else t


def _mots_liste(libelle: str) -> list[str]:
    return [_mot(t) for t in normaliser(libelle).split(" ") if t]


def _mots_neutres(referentiel: list[dict[str, Any]]) -> set[str]:
    """Mots qui classent sans désigner : villes du référentiel, types d'habitation."""
    villes = {v for l in referentiel for v in tokens(l.get("ville") or "")}
    types = {t for l in referentiel for c in ("nom_logement_officiel", "nom_court")
             for t in tokens(l.get(c) or "") if _RE_TYPE_HABITATION.match(t)}
    return villes | types


def _adresses_logement(logement: dict[str, Any], villes: set[str]) -> list[tuple[str, set[str]]]:
    """(numéro, mots du nom de la voie) de chaque adresse connue du logement."""
    valeurs = [logement.get("adresse") or ""] + [
        a.get("valeur_source") or "" for a in logement.get("aliases") or []
        if str(a.get("champ_source") or "").lower().startswith("adresse")]
    out = []
    for v in valeurs:
        mots = _mots_liste(v)
        numeros = [i for i, t in enumerate(mots) if t.isdigit()]
        if not numeros:
            continue
        i = numeros[0]
        nom = {t for t in mots[i + 1:] if t not in _TYPES_VOIE and t not in _ARTICLES
               and t not in villes and not t.isdigit()}
        if nom:
            out.append((mots[i], nom))
    return out


def _types_logement(logement: dict[str, Any]) -> set[str]:
    champs = [logement.get("nom_logement_officiel") or "", logement.get("nom_court") or ""] + [
        a.get("valeur_source") or "" for a in logement.get("aliases") or []]
    return {t for c in champs for t in tokens(c) if _RE_TYPE_HABITATION.match(t)}


def _proprietaires(texte: str) -> set[str]:
    """Prénoms entre parenthèses (« (François) ») ; une parenthèse qui commence par un chiffre est
    une adresse, pas un propriétaire."""
    out: set[str] = set()
    for p in re.findall(r"\(([^)]*)\)", str(texte or "")):
        if p.strip() and not p.strip()[0].isdigit():
            out |= {_mot(t) for t in tokens(p)}
    return out


def _proprietaires_logement(logement: dict[str, Any]) -> set[str]:
    champs = [logement.get("nom_logement_officiel") or ""] + [
        a.get("valeur_source") or "" for a in logement.get("aliases") or []]
    return {t for c in champs for t in _proprietaires(c)}


def _signaux_libelle(libelle: str, referentiel: list[dict[str, Any]]) -> dict[str, Any]:
    """Ce que le libellé dit de structuré, confronté à chaque logement déductible.

    Une adresse est « complète » quand le numéro du logement est écrit ET suivi (à quelques mots
    près) de TOUS les mots du nom de sa voie ; « partielle » quand au moins la moitié y est.
    Le numéro seul ne vaut rien : « 4 » se lit dans trop de libellés.
    """
    villes = {v for l in referentiel for v in tokens(l.get("ville") or "")}
    mots = _mots_liste(libelle)
    completes, partielles = [], []
    for l in referentiel:
        if not _deductible(l):
            continue
        niveau = None
        for numero, nom in _adresses_logement(l, villes):
            for i, t in enumerate(mots):
                if t != numero:
                    continue
                communs = nom & set(mots[i + 1:i + 1 + len(nom) + 4])
                if communs == nom:
                    niveau = "complete"
                elif communs and len(communs) / len(nom) >= 0.5 and niveau is None:
                    niveau = "partielle"
        if niveau == "complete":
            completes.append(l)
        elif niveau == "partielle":
            partielles.append(l)
    return {"types": {t for t in mots if _RE_TYPE_HABITATION.match(t)},
            "proprietaires": _proprietaires(libelle), "mots": set(mots), "villes": villes,
            "completes": completes, "partielles": partielles,
            "adresses_ambigues": completes if len(completes) > 1 else []}


def _voie_adresse(signaux: dict[str, Any]) -> dict[str, Any] | None:
    if len(signaux["completes"]) == 1:
        return {"logement": signaux["completes"][0], "complete": True}
    if not signaux["completes"] and len(signaux["partielles"]) == 1:
        return {"logement": signaux["partielles"][0], "complete": False}
    return None


def _resultat_adresse(adresse: dict[str, Any], signaux: dict[str, Any]) -> dict[str, Any]:
    """Numéro + nom de voie complets → CERTAIN, sauf si le type d'habitation ou le propriétaire
    écrits contredisent le logement (→ PROBABLE). Nom de voie partiel → PROBABLE."""
    l = adresse["logement"]
    s = ["ADRESSE_EXACTE" if adresse["complete"] else "NUMERO_RUE"]
    types_l, prop_l = _types_logement(l), _proprietaires_logement(l)
    if signaux["types"] and types_l:
        s.append("TYPE_CONCORDANT" if signaux["types"] & types_l else "TYPE_DIFFERENT")
    if signaux["proprietaires"] and prop_l:
        s.append("PROPRIETAIRE_CONCORDANT" if signaux["proprietaires"] & prop_l
                 else "PROPRIETAIRE_DIFFERENT")
    contradiction = "TYPE_DIFFERENT" in s or "PROPRIETAIRE_DIFFERENT" in s
    confiance = CERTAIN if adresse["complete"] and not contradiction else PROBABLE
    raison = (f"L'adresse du logement au référentiel (« {l.get('adresse') or '?'} ») se lit dans le "
              f"libellé : numéro et nom de voie{'' if adresse['complete'] else ' (en partie)'}.")
    if contradiction:
        raison += " Le type de logement ou le propriétaire écrit diffère : à confirmer."
    return _resultat(_candidat(l, confiance, M_ADRESSE, raison, _score(s), s))


def _candidats_type_ville(signaux: dict[str, Any], referentiel: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """« T4 blagnac » : ni une adresse, ni un nom. Les logements de ce type dans cette ville sont
    PROPOSÉS comme candidats, jamais retenus : la ville seule ne désigne pas un logement."""
    villes_citees = signaux["mots"] & signaux["villes"]
    if not signaux["types"] or not villes_citees:
        return []
    s = ["VILLE_SEULE", "TYPE_CONCORDANT"]
    return [_candidat(l, AUCUN, M_AMBIGU, "même type de logement dans la même ville", _score(s), s)
            for l in referentiel
            if _deductible(l) and tokens(l.get("ville") or "") & villes_citees
            and _types_logement(l) & signaux["types"]]


def _ambigu(candidats: list[dict[str, Any]]) -> dict[str, Any]:
    noms = ", ".join(c["libelle"] for c in candidats)
    return _resultat(None, candidats=candidats, raison=(
        f"Plusieurs logements correspondent également à ce libellé ({noms}). "
        f"Le rapprochement automatique s'arrête ici : il faut choisir."))


def proposer_pour_libelle(libelle_source: str, db_path=None) -> dict[str, Any]:
    """Version « avec base » de `proposer` — pour un appel isolé (une ligne, un écran)."""
    return proposer(libelle_source, charger_referentiel(db_path))


def enregistrer_correspondance(valeur_source: str, logement_id: str, *, acteur: str = "",
                               db_path=None) -> dict[str, Any]:
    """Grave dans `ref_mapping_logements` le choix que l'humain vient de faire.

    C'est ce qui fait que le logiciel APPREND : un libellé confirmé une fois est reconnu avec
    certitude la fois suivante, par la voie 1, sans repasser par la déduction. La correspondance
    est tracée avec sa source pour qu'on sache toujours d'où elle vient.
    """
    valeur = str(valeur_source or "").strip()
    lid = str(logement_id or "").strip()
    if not valeur or not lid:
        return {"ok": False, "code": "VALEUR_OU_LOGEMENT_MANQUANT"}
    conn = get_db(db_path)
    try:
        if conn.execute("SELECT 1 FROM ref_logements WHERE logement_id = ?", (lid,)).fetchone() \
                is None:
            return {"ok": False, "code": "LOGEMENT_INCONNU", "detail": lid}
        deja = conn.execute(
            "SELECT mapping_logement_id, logement_id FROM ref_mapping_logements "
            "WHERE champ_source = 'libelle_logement_source' AND valeur_source = ?",
            (valeur,)).fetchone()
        if deja is not None:
            if str(deja["logement_id"]) == lid:
                return {"ok": True, "deja_connue": True,
                        "mapping_logement_id": deja["mapping_logement_id"]}
            return {"ok": False, "code": "CORRESPONDANCE_CONTRADICTOIRE",
                    "detail": f"« {valeur} » est déjà rattaché à {deja['logement_id']}."}
        suivant = conn.execute(
            "SELECT COALESCE(MAX(CAST(SUBSTR(mapping_logement_id, 5) AS INTEGER)), 0) + 1 "
            "FROM ref_mapping_logements WHERE mapping_logement_id LIKE 'MAP_%'").fetchone()[0]
        mid = f"MAP_{int(suivant):04d}"
        conn.execute(
            # `import_id` est NOT NULL : une ligne de référentiel doit toujours dire d'où elle
            # vient. `SAISIE_APPLICATION` est la convention déjà en place pour ce que
            # l'application crée elle-même, par opposition à un import de fichier (`IMP-…`).
            "INSERT INTO ref_mapping_logements (mapping_logement_id, source, champ_source, "
            "valeur_source, logement_id, niveau_confiance, actif, commentaire, import_id) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (mid, "Facture ménage externe", "libelle_logement_source", valeur, lid, "Fort", "OUI",
             f"Confirmé sur une ligne de facture par {acteur or 'local'}.", "SAISIE_APPLICATION"))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "deja_connue": False, "mapping_logement_id": mid, "logement_id": lid}
