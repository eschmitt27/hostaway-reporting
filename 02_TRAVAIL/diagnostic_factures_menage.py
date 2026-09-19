# -*- coding: utf-8 -*-
"""Diagnostic du corpus des factures fournisseur ménage — une fiche par PDF, un tableau de qualité.

    python diagnostic_factures_menage.py [--dossier DOSSIER] [--db app.db] [--sortie rapport.md]

LECTURE SEULE. Les PDF sont lus, jamais modifiés. Le référentiel des logements est lu sur une
COPIE temporaire de la base : aucune facture n'est créée, aucune ligne écrite, aucune
correspondance apprise. C'est l'outil de mesure du parseur, pas un import.

Pour chaque facture : fichier, pages, fournisseur, numéro, date, totaux du document (base HT, TTC,
net à payer), mode de lecture, nombre de lignes, somme, écart, natures (ménages, remises en état,
autres prestations, lignes à classer), logements reconnus (certains / à confirmer / non trouvés),
anomalies, puis le détail ligne par ligne.
"""
from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

ICI = Path(__file__).resolve().parent
RACINE = ICI.parent
for p in (ICI, RACINE / "05_APPLICATION"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import lib_menages_externes_pdf as pdfex  # noqa: E402

FAMILLES = {pdfex.CAT_MENAGE_STANDARD: "MENAGE", pdfex.CAT_REMISE_EN_ETAT: "REMISE_EN_ETAT"}


def famille(ligne) -> str:
    if ligne.categorie_confiance == pdfex.CONF_AUCUN:
        return "A_CLASSER"
    return FAMILLES.get(ligne.categorie, "AUTRE_PRESTATION")


def _eur(v) -> str:
    return "—" if v is None else f"{v:,.2f} €".replace(",", " ").replace(".", ",")


def diagnostiquer(dossier: Path, referentiel) -> list[dict]:
    from app.services import logement_matching_service as lms

    fiches = []
    for pdf in sorted(dossier.glob("*.pdf")):
        fac = pdfex.extraire_pdf(pdf)
        lignes = []
        for l in fac.lignes:
            prop = lms.proposer(l.logement_source, referentiel) if referentiel is not None else None
            lignes.append({"ligne": l, "famille": famille(l), "proposition": prop})
        conf = [x["proposition"]["confiance"] for x in lignes if x["proposition"]]
        fiches.append({
            "fac": fac, "lignes": lignes,
            "natures": {f: sum(1 for x in lignes if x["famille"] == f)
                        for f in ("MENAGE", "REMISE_EN_ETAT", "AUTRE_PRESTATION", "A_CLASSER")},
            "logements": {c: conf.count(c) for c in ("CERTAIN", "PROBABLE", "AUCUN")},
        })
    return fiches


def rapport(fiches: list[dict]) -> str:
    out = ["# Diagnostic du corpus — factures fournisseur ménage", "",
           "Deux questions distinctes : le parseur a-t-il lu FIDÈLEMENT le document (anomalies "
           "parseur) ? Le document est-il mathématiquement COHÉRENT (anomalies document) ? La "
           "somme source est celle des montants IMPRIMÉS ; la somme théorique n'est qu'une "
           "suggestion, jamais substituée.", "",
           "| Facture | Pages | Lignes | Total document | Somme source | Écart source "
           "| Somme théorique | Correction suggérée | Ménages | Remises | Autres | À classer "
           "| Logements certains | À confirmer | Non trouvés | Anomalies document | Anomalies parseur |",
           "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    tot = {"lignes": 0, "ecart": 0.0, "CERTAIN": 0, "PROBABLE": 0, "AUCUN": 0, "ok": 0,
           "fideles": 0, "incoherents": 0}
    for f in fiches:
        fac, n, lg = f["fac"], f["natures"], f["logements"]
        out.append(
            f"| {fac.nom_fichier_source} | {fac.source_pages} | {len(fac.lignes)} "
            f"| {_eur(fac.montant_total_facture)} | {_eur(fac.somme_lignes)} "
            f"| {_eur(fac.ecart_reconciliation)} | {_eur(fac.somme_theorique)} "
            f"| {_eur(fac.correction_suggeree)} | {n['MENAGE']} | {n['REMISE_EN_ETAT']} "
            f"| {n['AUTRE_PRESTATION']} | {n['A_CLASSER']} | {lg.get('CERTAIN', 0)} "
            f"| {lg.get('PROBABLE', 0)} | {lg.get('AUCUN', 0)} "
            f"| {'<br>'.join(fac.anomalies_document) or '—'} "
            f"| {'<br>'.join(fac.anomalies_parseur) or '—'} |")
        tot["fideles"] += int(not fac.anomalies_parseur)
        tot["incoherents"] += int(bool(fac.anomalies_document))
        tot["lignes"] += len(fac.lignes)
        tot["ecart"] += abs(fac.ecart_reconciliation or 0)
        tot["ok"] += int(fac.ecart_reconciliation is not None and abs(fac.ecart_reconciliation) < 0.005)
        for c in ("CERTAIN", "PROBABLE", "AUCUN"):
            tot[c] += lg.get(c, 0)
    out += ["", f"**{len(fiches)} factures — {tot['lignes']} lignes — extraction fidèle (aucune "
                f"anomalie parseur) : {tot['fideles']}/{len(fiches)} — documents incohérents : "
                f"{tot['incoherents']} — sommes source = total document : {tot['ok']}/{len(fiches)} "
                f"— écart source absolu cumulé {_eur(tot['ecart'])} — logements : "
                f"{tot['CERTAIN']} certains, {tot['PROBABLE']} à confirmer, {tot['AUCUN']} non "
                f"trouvés.**", ""]

    for f in fiches:
        fac = f["fac"]
        out += [f"## {fac.nom_fichier_source}", "",
                f"- Fournisseur : {fac.nom_prestataire or '—'} ({fac.format_detecte}) — "
                f"facture n° {fac.numero_facture or '—'} du {fac.date_facture or '—'} — "
                f"{fac.source_pages} page(s) — lecture {fac.mode_lecture or '—'}",
                f"- Totaux du document : base HT {_eur(fac.base_ht)} · sous-total "
                f"{_eur(fac.sous_total)} · TTC {_eur(fac.total_ttc)} · net à payer "
                f"{_eur(fac.net_a_payer)}",
                f"- Lignes : {len(fac.lignes)} — somme des montants imprimés "
                f"{_eur(fac.somme_lignes)} — écart source {_eur(fac.ecart_reconciliation)}",
                f"- Contrôle arithmétique : somme théorique {_eur(fac.somme_theorique)} — écart "
                f"théorique {_eur(fac.ecart_theorique)} — correction suggérée "
                f"{_eur(fac.correction_suggeree)}",
                f"- Anomalies du document : {', '.join(fac.anomalies_document) or 'aucune'}",
                f"- Anomalies du parseur : {', '.join(fac.anomalies_parseur) or 'aucune'}", "",
                "| N° | Page | Qté | PU | Montant imprimé | Montant calculé | Nature | Logement "
                "| Confiance | Libellé du document |",
                "|---|---|---|---|---|---|---|---|---|---|"]
        for x in f["lignes"]:
            l, p = x["ligne"], x["proposition"] or {}
            libelle = l.libelle_source.replace("|", "/")
            if l.code_anomalie:
                libelle += f" ⚠ {l.code_anomalie}"
            out.append(
                f"| {l.numero_ligne or ''} | {l.source_page} | {l.quantite} | {_eur(l.prix_unitaire)} "
                f"| {_eur(l.montant_ligne)} | {_eur(l.montant_calcule)} | {x['famille']} "
                f"| {p.get('logement_id') or '—'} "
                f"| {p.get('confiance', '—')} | {libelle} |")
        out.append("")
    return "\n".join(out)


def main(argv=None) -> int:
    import app.config as cfg
    from app.services import logement_matching_service as lms

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dossier", type=Path, default=cfg.MENAGES_PDF_DIR)
    ap.add_argument("--db", type=Path, default=Path(cfg.DB_PATH))
    ap.add_argument("--sortie", type=Path, default=None)
    a = ap.parse_args(argv)

    referentiel = None
    if a.db.exists():
        with tempfile.TemporaryDirectory() as tmp:
            copie = Path(tmp) / "referentiel.db"
            shutil.copy2(a.db, copie)
            referentiel = lms.charger_referentiel(copie)
    texte = rapport(diagnostiquer(a.dossier, referentiel))
    if a.sortie:
        a.sortie.write_text(texte, encoding="utf-8")
        print(f"Rapport écrit : {a.sortie}")
    else:
        sys.stdout.reconfigure(encoding="utf-8")
        print(texte)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
