"""APP-3D — Export CSV du relevé propriétaire. Mêmes protections que les exports APP-5C/5D."""
from __future__ import annotations

import csv
import io
import re
from datetime import datetime

_RE_MOIS_SUR = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_AMORCES_FORMULE = ("=", "+", "-", "@", "\t", "\r")

_DISCLAIMER = ("Document préparatoire — ne constitue pas une facture définitive tant que son "
              "émission n'a pas été validée.")


def _cellule_sure(valeur) -> str:
    s = "" if valeur is None else str(valeur)
    if s and s[0] in _AMORCES_FORMULE:
        return "'" + s
    return s


def nom_fichier(mois: str) -> str:
    mois_sur = mois if _RE_MOIS_SUR.match(str(mois or "")) else "mois-invalide"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"releve_proprietaire_{mois_sur}_{ts}.csv"


def exporter(releve: dict, detail: dict | None, eval_blocages: dict) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\n")

    def row(*vals):
        w.writerow([_cellule_sure(v) for v in vals])

    row("AVERTISSEMENT", _DISCLAIMER)
    w.writerow([])

    row("section", "champ", "valeur")
    row("resume", "mois", releve["mois"])
    row("resume", "releve_id_opaque", releve["releve_id_opaque"])
    row("resume", "statut_facturation", releve["statut_facturation"])
    row("resume", "date_creation", releve.get("date_creation") or "—")
    row("resume", "date_preparation", releve.get("date_preparation") or "—")
    row("resume", "date_validation", releve.get("date_validation") or "—")
    row("resume", "date_reouverture", releve.get("date_reouverture") or "—")
    row("resume", "commentaire_validation", releve.get("commentaire_validation") or "—")
    row("resume", "bloquants", ", ".join(eval_blocages.get("bloquants", [])) or "—")
    row("resume", "informatifs", ", ".join(eval_blocages.get("informatifs", [])) or "—")

    if detail and detail.get("status") == "OK":
        vue = detail.get("vue") or {}
        w.writerow([])
        row("exploitation", "champ", "valeur")
        for champ in ("ca_retenu", "menage", "base_commission", "commission", "net", "net_avant_charge"):
            row("exploitation", champ, vue.get(champ))

        w.writerow([])
        row("reglement", "champ", "valeur")
        for champ in ("reglement_acomptes", "reglement_paiement", "reste", "reglement_statut"):
            row("reglement", champ, vue.get(champ))

        w.writerow([])
        row("logements", "logement_id", "total_payout", "total_commission", "net_avant_charge", "reste")
        for lg in detail.get("logements", []):
            row("logement", lg.get("logement_id"), lg.get("total_payout"), lg.get("total_commission"),
               lg.get("net_avant_charge"), lg.get("reste"))

    return buf.getvalue()
