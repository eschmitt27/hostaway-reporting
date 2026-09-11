"""HR retiré du vocabulaire des CHARGES (migration 0078).

« HORS COMPTA + HORS RÉSULTAT n'a PAS de pertinence métier » est vrai pour une DÉPENSE : une charge
qui ne pèse ni sur le résultat ni sur la comptabilité n'est pas une charge.

CE FICHIER A CHANGÉ DE CAMP SUR L'AXE RÉSERVATIONS, ET C'EST VOLONTAIRE.
Il défendait auparavant le maintien de `HR` sur les réservations, pour une raison qui reste juste :
un séjour propriétaire ne doit pas devenir du chiffre d'affaires. Son propre commentaire prévoyait
la suite — « le jour où l'arbitrage tranche autrement, ce test doit être modifié EN MÊME TEMPS que
la règle, pas contourné ». L'arbitrage a été rendu le 2026-09-11 : `HR` disparaît aussi des
réservations, mais l'exclusion est CONSERVÉE — portée par `statut_controle` et un
`motif_exclusion` requêtable, et non plus par un code d'impact détourné de son rôle.

Les 125 réservations restent donc exclues, à 0,00 € d'écart sur le résultat. La couverture
détaillée de cet axe vit dans `test_hr_exclu_reservations.py` ; ce fichier ne garde ici que les
garde-fous croisés.

Détail et preuves : `00_CADRAGE/HR_SUPPRESSION_AUDIT.md`,
`00_CADRAGE/RAPPORT_ARBITRAGES_HR_ARRONDIS_LOT6B.md`.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import app.config as cfg
from app.moteurs.charges_engine import CODES_IMPACT_CHARGE, IMPACT_CHARGE, impact_charge
from app.services import charges_saisie_service as saisie

_TRAVAIL = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"


# ── Axe CHARGES : HR n'existe plus ───────────────────────────────────────────

def test_le_vocabulaire_des_charges_est_ic_et_hc():
    assert CODES_IMPACT_CHARGE == frozenset({"IC", "HC"})
    assert set(IMPACT_CHARGE) == CODES_IMPACT_CHARGE


def test_hr_n_est_plus_resolvable_comme_impact_de_charge():
    """`impact_charge("HR")` rend `None` — « inconnu », pas « neutre ».

    La nuance compte : rendre des drapeaux NON/NON reviendrait à réimplémenter HR sous le capot,
    exactement ce que la décision interdit (« NE PAS créer un remplaçant équivalent »)."""
    assert impact_charge("HR") is None
    assert impact_charge("IC") == IMPACT_CHARGE["IC"]
    assert impact_charge("hc") == IMPACT_CHARGE["HC"], "la casse ne doit pas décider"
    assert impact_charge(None) is None and impact_charge("") is None


@pytest.mark.parametrize("code", ["HR", "hr", " HR "])
def test_une_charge_hr_est_refusee_a_l_ecriture(code):
    """La SEULE porte d'écriture de `charges` refuse HR — pas seulement les formulaires.

    Retirer HR des écrans en le laissant passer par le service aurait été cosmétique : l'API et
    tout appelant interne auraient continué d'en créer."""
    res = saisie.valider({"date_charge": "2026-07-15", "montant": 120,
                          "categorie_charge_id": "CHG_002", "code_impact": code})
    assert res["ok"] is False
    assert res["code"] == saisie.E_IMPACT_INVALIDE
    assert "IC" in res["message"] and "HC" in res["message"]


def test_une_charge_sans_code_impact_reste_acceptee():
    """`code_impact` demeure FACULTATIF (D044) : ce service enregistre, il ne dérive pas.

    Le refus porte sur une valeur INCONNUE, jamais sur une valeur absente — sinon la suppression
    de HR se transformerait en durcissement non demandé du parcours de saisie."""
    for donnees in ({"code_impact": ""}, {"code_impact": None}, {}):
        res = saisie.valider({"date_charge": "2026-07-15", "montant": 120,
                              "categorie_charge_id": "CHG_002", **donnees})
        assert res["ok"] is True, res


def test_ic_et_hc_restent_acceptes():
    for code in ("IC", "HC"):
        res = saisie.valider({"date_charge": "2026-07-15", "montant": 120,
                              "categorie_charge_id": "CHG_002", "code_impact": code})
        assert res["ok"] is True, res


def test_le_mode_hr_n_est_plus_proposable_sur_une_facture():
    from app.services import factures_proprietaires_edition_service as edition

    assert "HR" not in edition.MODES_CHARGE
    assert {m["code_impact"] for m in edition.modes_charge()} == CODES_IMPACT_CHARGE


def test_le_filtre_de_l_ecran_de_controle_ne_propose_plus_hr():
    """La liste était écrite en dur dans le gabarit ; elle vient désormais du vocabulaire."""
    gabarit = (Path(cfg.PROJECT_ROOT) / "05_APPLICATION" / "app" / "templates"
               / "charges_controle.html").read_text(encoding="utf-8")
    assert "['IC','HC','HR']" not in gabarit
    assert "codes_impact" in gabarit


def test_lot3_ne_traduit_plus_hr_en_neutre():
    """Une charge portant encore HR doit ressortir A_CONTROLER, pas « sans impact ».

    C'est la différence entre un problème visible et un problème muet."""
    source = (_TRAVAIL / "lot3_generateur_charges.py").read_text(encoding="utf-8",
                                                                 errors="replace")
    code = "\n".join(l for l in source.splitlines() if not l.lstrip().startswith("#"))
    assert 'IMPACT_REEL: dict[str, str] = {"IC": "OUI", "HC": "OUI"}' in code
    assert 'IMPACT_COMPTA: dict[str, str] = {"IC": "OUI", "HC": "NON"}' in code


# ── Axe RÉSERVATIONS : HR supprimé, exclusion conservée ──────────────────────

def test_le_referentiel_ne_porte_plus_hr():
    """La ligne `HR` de `ref_codes_impact` a été supprimée (migration 0079).

    Elle était conservée tant que 125 réservations la référençaient. Ces réservations ne la
    référencent plus : leur exclusion est dite par leur statut et leur motif."""
    import tempfile

    from app.db.connection import apply_migrations

    db = Path(tempfile.mkdtemp()) / "app.db"
    apply_migrations(db)
    conn = sqlite3.connect(str(db))
    try:
        colonnes = {r[1] for r in conn.execute("PRAGMA table_info(ref_codes_impact)")}
        n = conn.execute("SELECT COUNT(*) FROM ref_codes_impact WHERE code_impact='HR'").fetchone()
    finally:
        conn.close()
    assert "code_impact" in colonnes, "le référentiel partagé subsiste"
    assert n[0] == 0, "HR ne doit plus y figurer"


def test_les_moteurs_reservations_ne_produisent_plus_hr_mais_excluent_toujours():
    """L'inverse exact de ce que ce test exigeait — et pour la MÊME raison métier.

    Ce qu'il défendait n'était pas `HR` : c'était que les séjours propriétaire ne deviennent pas du
    chiffre d'affaires. Cette garantie est intacte, exprimée autrement : `lot4bis` produit
    désormais une ligne SANS code d'impact, avec `statut_controle = EXCLU_RESULTAT` — le statut qui
    décidait déjà, puisqu'il forçait l'impact à NON/NON quel que soit le code."""
    lib = (_TRAVAIL / "lib_lot4a_reservations_hh.py").read_text(encoding="utf-8",
                                                                errors="replace")
    assert 'VALID_CODE_IMPACT = {"IC", "HC"}' in lib

    lot4bis = (_TRAVAIL / "lot4bis_charger_reservations.py").read_text(encoding="utf-8",
                                                                       errors="replace")
    assert "ownerStay" in lot4bis, "le cas métier doit toujours être traité"
    assert '"HR", "EXCLU_RESULTAT"' not in lot4bis, "plus aucun code HR produit"
    assert 'None, "EXCLU_RESULTAT"' in lot4bis, "l'exclusion doit rester posée, sans code d'impact"
    assert "motif_exclusion" in lot4bis, "le motif doit être écrit"


def test_l_arbitrage_de_l_axe_reservations_est_documente_comme_rendu():
    """La décision doit rester ÉCRITE, comme l'était l'arbitrage ouvert avant elle.

    Une décision non documentée se relit plus tard comme un accident, et se « corrige » à tort —
    c'est exactement le risque que courait `HR`, dont le blocage documenté avait survécu à sa
    propre levée ailleurs dans le dépôt."""
    doc = (Path(cfg.PROJECT_ROOT) / "00_CADRAGE" / "HR_SUPPRESSION_AUDIT.md").read_text(
        encoding="utf-8")
    assert "ARBITRAGE RENDU" in doc
    assert "séjours propriétaire" in doc, "le fait métier protégé doit rester nommé"


# ── Source unique ────────────────────────────────────────────────────────────

def test_le_vocabulaire_n_est_defini_qu_une_fois():
    """`STANDARD_CODES_IMPACT` doit ÊTRE la constante du moteur, pas une copie à jour par chance."""
    from app.services import charges_preview_service as preview

    assert preview.STANDARD_CODES_IMPACT is CODES_IMPACT_CHARGE


# ── DÉCISION 3 : les colonnes d'impact dérivées (migration 0078) ─────────────

def _colonnes_charges(tmp_path: Path) -> set[str]:
    from app.db.connection import apply_migrations

    db = tmp_path / "app.db"
    apply_migrations(db)
    conn = sqlite3.connect(str(db))
    try:
        return {r[1] for r in conn.execute("PRAGMA table_info(charges)")}
    finally:
        conn.close()


def test_les_colonnes_d_impact_derivees_ont_disparu(tmp_path):
    """`charges` ne porte plus de copie dénormalisée de `code_impact`.

    Les deux colonnes ne portaient aucune information propre — elles se déduisaient de
    `code_impact` par une table fixe. Deux représentations du même fait, donc deux occasions de
    diverger : elles divergeaient déjà (4 charges réelles sur 5 avaient un `code_impact` renseigné
    et ces colonnes à NULL)."""
    colonnes = _colonnes_charges(tmp_path)
    assert "impact_resultat_reel" not in colonnes
    assert "impact_resultat_comptable" not in colonnes
    assert "code_impact" in colonnes, "la source de vérité, elle, doit rester"


def test_la_saisie_n_ecrit_plus_ces_colonnes(tmp_path):
    """Elles ne doivent pas non plus revenir par la liste des champs modifiables."""
    assert "impact_resultat_reel" not in saisie.CHAMPS_SAISIE
    assert "impact_resultat_comptable" not in saisie.CHAMPS_SAISIE
    assert "code_impact" in saisie.CHAMPS_SAISIE


def test_les_projections_de_lecture_ne_les_demandent_plus(tmp_path):
    """Une colonne supprimée qu'un SELECT demande encore casse la lecture entière.

    Deux listes de projection les nommaient : le lecteur applicatif et lot10."""
    from app.readers import charges_reader

    assert "impact_resultat_reel" not in charges_reader._COLONNES
    assert "impact_resultat_comptable" not in charges_reader._COLONNES

    lot10 = (_TRAVAIL / "lot10_calculer_resultats.py").read_text(encoding="utf-8",
                                                                 errors="replace")
    bloc = lot10.split("_CHARGES_COLS_SQL = (")[1].split(")")[0]
    assert "impact_resultat_reel" not in bloc
    assert "impact_resultat_comptable" not in bloc


def test_les_tables_de_reservations_gardent_leurs_colonnes(tmp_path):
    """0078 ne touche QUE `charges`.

    Les colonnes de même nom sur les réservations sont LUES pour décider de l'inclusion au
    résultat (`reservations_adaptateur_moteur`, lot4bis, lot4quater) : les supprimer casserait le
    calcul économique. Même nom, rôle opposé."""
    from app.db.connection import apply_migrations

    db = tmp_path / "app.db"
    apply_migrations(db)
    conn = sqlite3.connect(str(db))
    try:
        for table in ("reservations_resolues", "reservations_hh"):
            presente = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,)).fetchone()
            if not presente:
                continue
            colonnes = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            assert "impact_resultat_reel" in colonnes, table
            assert "impact_resultat_comptable" in colonnes, table
    finally:
        conn.close()


def test_l_impact_reste_derivable_pour_chaque_code():
    """Rien n'est perdu : c'est ce qui rend la suppression sûre."""
    assert impact_charge("IC")["impact_resultat_reel"] == "OUI"
    assert impact_charge("IC")["impact_resultat_comptable"] == "OUI"
    assert impact_charge("HC")["impact_resultat_reel"] == "OUI"
    assert impact_charge("HC")["impact_resultat_comptable"] == "NON"
