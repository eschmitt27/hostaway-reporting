"""Export du dossier de clôture (APP-5C) — CSV logique, sans donnée sensible.

Résumé, compteurs, bloquants/traités/résolus/exceptions/informatifs, historique, preuves (métadonnées
uniquement), aucun fichier métier réel, aucun chemin absolu, aucun IBAN/compte/mouvement brut.
"""
from __future__ import annotations

import csv
import io
import re
from datetime import datetime

from app.readers import controles_cloture_reader as ref_reader
from app.services import clotures_service as cs

_DISCLAIMER = "Ce document ne constitue pas la clôture comptable réelle — suivi humain uniquement."

# Caractères déclenchant l'exécution de formule dans Excel/LibreOffice si en tête de cellule.
_RE_MOIS_SUR = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_AMORCES_FORMULE = ("=", "+", "-", "@", "\t", "\r")


def _cellule_sure(valeur) -> str:
    """Neutralise l'injection CSV/formule : préfixe d'une apostrophe toute valeur dont le premier
    caractère déclencherait l'exécution d'une formule dans un tableur (=, +, -, @, tabulation, CR)."""
    s = "" if valeur is None else str(valeur)
    if s and s[0] in _AMORCES_FORMULE:
        return "'" + s
    return s


def nom_fichier(mois: str) -> str:
    """Nom horodaté, sans injection possible (mois déjà validé en amont — filet ici aussi)."""
    mois_sur = mois if _RE_MOIS_SUR.match(str(mois or "")) else "mois-invalide"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"dossier_cloture_{mois_sur}_{ts}.csv"


def _statut_moteur(mois: str) -> str:
    """Statut RÉEL (REF_Cloture_Mensuelle, moteur) — affiché à titre informatif, jamais recalculé
    ni fusionné avec le statut de suivi humain."""
    src = ref_reader.cloture_ref()
    if not src.etat.disponible:
        return "SOURCE_INDISPONIBLE"
    for r in src.lignes:
        if ref_reader.to_mois(r.get("mois")) == mois:
            return ref_reader.to_texte(r.get("statut_mois")).upper() or "INCONNU"
    return "INCONNU"


def exporter_dossier(cloture: dict, db_path=None) -> str:
    progression = cs.calcul_progression(cloture["mois"], db_path)
    hist = cs.historique(cloture["cloture_id_opaque"], db_path)
    docs = cs.documents(cloture["cloture_id_opaque"], db_path)

    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\n")

    def row(*vals):
        w.writerow([_cellule_sure(v) for v in vals])

    row("AVERTISSEMENT", _DISCLAIMER)
    w.writerow([])

    row("section", "champ", "valeur")
    row("resume", "mois", cloture["mois"])
    row("resume", "statut_suivi_humain", cs.STATUTS_LIBELLES.get(cloture["statut"], cloture["statut"]))
    row("resume", "statut_reel_moteur", _statut_moteur(cloture["mois"]))
    row("resume", "cloture_id_opaque", cloture["cloture_id_opaque"])
    row("resume", "date_creation", cloture.get("date_creation") or "—")
    row("resume", "date_preparation", cloture.get("date_preparation") or "—")
    row("resume", "date_validation", cloture.get("date_validation") or "—")
    row("resume", "date_reouverture", cloture.get("date_reouverture") or "—")
    row("resume", "commentaire_validation", cloture.get("commentaire_validation") or "—")
    row("resume", "justification_reouverture", cloture.get("justification_reouverture") or "—")
    row("compteur", "total_elements", progression["nb_total"])
    row("compteur", "anomalies", progression["nb_anomalies"])
    row("compteur", "bloqueurs", progression["nb_bloqueurs"])
    row("compteur", "a_traiter", progression["nb_a_traiter"])
    row("compteur", "en_cours", progression["nb_en_cours"])
    row("compteur", "resolus", progression["nb_resolus"])
    row("compteur", "exceptions", progression["nb_exceptions"])
    row("compteur", "reapparus", progression["nb_reapparus"])
    row("compteur", "informatifs", progression["nb_informatifs"])

    w.writerow([])
    row("bloqueurs", "code", "module", "entite_opaque")
    for b in progression["bloqueurs"]:
        row("bloqueur", b["code"], b["module"], b["entite_id"])

    w.writerow([])
    row("preuve", "nom_logique", "type", "date_creation")
    for d in docs:
        row("preuve", d["nom_logique"], d["type_document"], d["date_creation"])

    w.writerow([])
    row("historique", "type_evenement", "ancien_statut", "nouveau_statut", "commentaire", "date", "acteur")
    for h in hist:
        row("evenement", h["type_evenement"], h.get("ancien_statut") or "",
           h.get("nouveau_statut") or "", h.get("commentaire") or "", h["date_evenement"],
           h.get("acteur") or "")

    return buf.getvalue()
