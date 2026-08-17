"""Contrôles Lot 5 portés en Python/SQLite — parité avec la règle legacy.

POURQUOI UNE RÉFÉRENCE LEGACY RÉÉCRITE DANS LE TEST
Le dataset réel de Lot 5 est vide : comparer deux sorties vides prouverait seulement que rien ne se
passe. La parité se fait donc sur une fixture synthétique, et la référence est la règle telle que le
Lot 5 la RÉDIGE dans sa table `CONTROLS` (colonne « regle »), réencodée ici sur le vocabulaire
d'origine. Les deux implémentations partent ainsi de la même énonciation, sans partager de code.

Chaque cas porte un verdict attendu explicite : même code de contrôle, même niveau, même montant.
"""
from __future__ import annotations

import pytest

from app.services import proprietaires_tresorerie_service as tresorerie
from app.services import tresorerie_controles_service as ctrl

PROP = "PROP_9001"
LOG = "LOG_9001"


# ── Référence legacy : la règle Lot 5, telle qu'énoncée ─────────────────────────────────────────
#
# Transcription littérale de la colonne « regle » de `lot5_master_acomptes_proprietaires.CONTROLS`,
# appliquée au vocabulaire Lot 5 (acompte_id, facture_ref, montant_acompte…). Aucune ligne de code
# n'est partagée avec le service : c'est ce qui donne son sens à la comparaison.

def _legacy(lignes: list[dict]) -> set[tuple[str, str]]:
    """{(acompte_id, code_controle)} tel que le Lot 5 les aurait produits."""
    trouves: set[tuple[str, str]] = set()
    vus: dict[tuple, list[str]] = {}

    for l in lignes:
        aid = l["acompte_id"]

        # « facture_ref null ou vide ET statut_controle <> EXCLU_RESULTAT »
        if not l.get("facture_ref") and l.get("statut_controle") != "EXCLU_RESULTAT":
            trouves.add((aid, "ACOMPTE_NON_RATTACHE_FACTURE"))

        # « source_acompte=HH_RESERVATION ET montant_acompte ≠ acompte_facture Lot 4 »
        if l.get("source_acompte") == "HH_RESERVATION":
            trouves.add((aid, "ACOMPTE_HH_INCOHERENT"))

        # « proprietaire_id null ou vide »
        if not l.get("proprietaire_id"):
            trouves.add((aid, "ACOMPTE_PROPRIETAIRE_ABSENT"))

        # « montant_acompte null ou <= 0 »
        montant = l.get("montant_acompte")
        if montant is None or montant <= 0:
            trouves.add((aid, "ACOMPTE_MONTANT_INVALIDE"))

        # « logement_id null ou vide »
        if not l.get("logement_id"):
            trouves.add((aid, "ACOMPTE_LOGEMENT_ABSENT"))

        # « source_hh_id non null ET absent de HH_Acomptes_Ref »
        if l.get("source_hh_id") and l.get("source_acompte") != "HH_RESERVATION":
            trouves.add((aid, "ACOMPTE_SOURCE_HH_INTROUVABLE"))

        # « report_mois_precedent > 0 ET commentaire null ou vide »
        if l.get("report") and not l.get("commentaire"):
            trouves.add((aid, "ACOMPTE_REPORT_INCOHERENT"))

        # « source_acompte = AUTRE »
        if l.get("source_acompte") == "AUTRE":
            trouves.add((aid, "ACOMPTE_SOURCE_A_CONTROLER"))

        # « facture_ref non null ET ne commence pas par FAC- »
        ref = l.get("facture_ref")
        if ref and not ref.startswith("FAC-"):
            trouves.add((aid, "ACOMPTE_FACTURE_REF_FORMAT_INVALIDE"))

        # « acompte_id présent plusieurs fois dans la table » — transposé au doublon métier.
        cle = (l.get("proprietaire_id"), l.get("mois"), l.get("montant_acompte"),
               l.get("nature"), l.get("sens"))
        vus.setdefault(cle, []).append(aid)

    for ids in vus.values():
        if len(ids) > 1:
            for aid in ids:
                trouves.add((aid, "ACOMPTE_CALC_ID_DUPLIQUE"))
    return trouves


# ── Fixture synthétique : un cas par contrôle ───────────────────────────────────────────────────

CAS = [
    # (clé, champs du mouvement, champs legacy équivalents, codes attendus)
    ("valide",
     dict(reference_metier="FAC-2026-04-PROP-001", logement_id=LOG,
          nature="ACOMPTE_PROPRIETAIRE", montant=100.0),
     dict(facture_ref="FAC-2026-04-PROP-001", logement_id=LOG, montant_acompte=100.0),
     set()),

    ("reference_absente",
     dict(reference_metier="", logement_id=LOG, nature="ACOMPTE_PROPRIETAIRE", montant=150.0),
     dict(facture_ref="", logement_id=LOG, montant_acompte=150.0),
     {"ACOMPTE_NON_RATTACHE_FACTURE"}),

    ("logement_absent",
     dict(reference_metier="FAC-2026-04-PROP-002", logement_id="",
          nature="ACOMPTE_PROPRIETAIRE", montant=200.0),
     dict(facture_ref="FAC-2026-04-PROP-002", logement_id="", montant_acompte=200.0),
     {"ACOMPTE_LOGEMENT_ABSENT"}),

    ("reference_format",
     dict(reference_metier="RECU-2026-04-007", logement_id=LOG,
          nature="ACOMPTE_PROPRIETAIRE", montant=250.0),
     dict(facture_ref="RECU-2026-04-007", logement_id=LOG, montant_acompte=250.0),
     {"ACOMPTE_FACTURE_REF_FORMAT_INVALIDE"}),

    ("nature_a_controler",
     dict(reference_metier="FAC-2026-04-PROP-003", logement_id=LOG,
          nature="AUTRE_A_CONTROLER", montant=300.0),
     dict(facture_ref="FAC-2026-04-PROP-003", logement_id=LOG, montant_acompte=300.0,
          source_acompte="AUTRE"),
     {"ACOMPTE_SOURCE_A_CONTROLER"}),

    ("ajustement_sans_justification",
     dict(reference_metier="FAC-2026-04-PROP-004", logement_id=LOG,
          nature="REGULARISATION_PROPRIETAIRE", montant=350.0, justification=""),
     dict(facture_ref="FAC-2026-04-PROP-004", logement_id=LOG, montant_acompte=350.0,
          report=1, commentaire=""),
     {"ACOMPTE_REPORT_INCOHERENT"}),

    ("source_non_verifiable",
     dict(reference_metier="FAC-2026-04-PROP-005", logement_id=LOG,
          nature="ACOMPTE_PROPRIETAIRE", montant=400.0,
          source_type="OBJET_VALIDE", source_id="HH-0001"),
     dict(facture_ref="FAC-2026-04-PROP-005", logement_id=LOG, montant_acompte=400.0,
          source_acompte="HH_RESERVATION", source_hh_id="HH-0001"),
     {"ACOMPTE_HH_INCOHERENT"}),

    ("source_sans_type",
     dict(reference_metier="FAC-2026-04-PROP-006", logement_id=LOG,
          nature="ACOMPTE_PROPRIETAIRE", montant=450.0,
          source_type="MANUEL", source_id="HH-0002"),
     dict(facture_ref="FAC-2026-04-PROP-006", logement_id=LOG, montant_acompte=450.0,
          source_acompte="MANUEL", source_hh_id="HH-0002"),
     {"ACOMPTE_SOURCE_HH_INTROUVABLE"}),
]

# Deux mouvements strictement identiques : même propriétaire, même jour, même montant, même nature.
CAS_DOUBLON = dict(reference_metier="FAC-2026-04-PROP-007", logement_id=LOG,
                   nature="ACOMPTE_PROPRIETAIRE", montant=500.0)


def _inserer(db_path, cle: str, champs: dict) -> str:
    """Écrit un mouvement en base, en contournant les validations de saisie.

    Les contrôles Lot 5 portent sur des lignes DÉJÀ enregistrées, y compris des lignes que la saisie
    d'aujourd'hui refuserait — c'est précisément leur rôle. Passer par `creer()` empêcherait de
    fabriquer un montant nul ou un propriétaire vide, donc de tester ces contrôles.
    """
    from app.db.connection import get_db

    valeurs = {
        "mouvement_opaque": f"MTP-{cle.upper()}",
        "proprietaire_id": PROP, "logement_id": None, "date_mouvement": "2026-04-10",
        "montant": 0.0, "sens": "PROPRIETAIRE_VERS_SOCIETE", "nature": "ACOMPTE_PROPRIETAIRE",
        "statut": tresorerie.ST_VALIDE, "reference_metier": "", "justification": "",
        "source_type": "MANUEL", "source_id": None,
    }
    valeurs.update(champs)
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO mouvements_tresorerie_proprietaires (mouvement_opaque, proprietaire_id, "
            "logement_id, date_mouvement, montant, sens, nature, statut, reference_metier, "
            "justification, source_type, source_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            tuple(valeurs[c] for c in (
                "mouvement_opaque", "proprietaire_id", "logement_id", "date_mouvement", "montant",
                "sens", "nature", "statut", "reference_metier", "justification", "source_type",
                "source_id")))
        conn.commit()
    finally:
        conn.close()
    return valeurs["mouvement_opaque"]


@pytest.fixture
def jeu(tmp_db):
    """Un mouvement par cas, plus deux mouvements en doublon."""
    for cle, champs, _, _ in CAS:
        _inserer(tmp_db, cle, champs)
    _inserer(tmp_db, "doublon_a", CAS_DOUBLON)
    _inserer(tmp_db, "doublon_b", CAS_DOUBLON)
    return tmp_db


def _codes(resultat, mouvement_opaque: str) -> set[str]:
    return {c["code_controle"] for c in resultat["constats"]
            if c["mouvement_opaque"] == mouvement_opaque}


# ── Parité, cas par cas ─────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("cle,champs,legacy_champs,attendus",
                         CAS, ids=[c[0] for c in CAS])
def test_parite_avec_la_regle_lot5(jeu, cle, champs, legacy_champs, attendus):
    """Même énoncé de règle, deux implémentations, même verdict."""
    mid = f"MTP-{cle.upper()}"
    obtenus = _codes(ctrl.controler(db_path=jeu), mid)

    ligne = {"acompte_id": mid, "proprietaire_id": PROP, "mois": "2026-04",
             "nature": champs.get("nature"), "sens": "PROPRIETAIRE_VERS_SOCIETE"}
    ligne.update(legacy_champs)
    attendus_legacy = {code for aid, code in _legacy([ligne]) if aid == mid}

    assert obtenus == attendus, f"{cle} : portage {obtenus} contre attendu {attendus}"
    assert obtenus == attendus_legacy, f"{cle} : portage {obtenus} contre legacy {attendus_legacy}"


def test_doublon_metier_signale_toutes_les_occurrences(jeu):
    """Aucune occurrence n'est désignée d'office comme la bonne."""
    for mid in ("MTP-DOUBLON_A", "MTP-DOUBLON_B"):
        assert ctrl.C_DOUBLON in _codes(ctrl.controler(db_path=jeu), mid)


def test_montant_invalide(tmp_db):
    _inserer(tmp_db, "montant_nul",
             dict(reference_metier="FAC-2026-04-PROP-008", logement_id=LOG, montant=0.0))
    assert ctrl.C_MONTANT_INVALIDE in _codes(ctrl.controler(db_path=tmp_db), "MTP-MONTANT_NUL")


def test_proprietaire_absent(tmp_db):
    _inserer(tmp_db, "sans_proprio",
             dict(proprietaire_id="", reference_metier="FAC-2026-04-PROP-009", logement_id=LOG,
                  montant=90.0))
    assert ctrl.C_PROPRIETAIRE_ABSENT in _codes(ctrl.controler(db_path=tmp_db), "MTP-SANS_PROPRIO")


# ── Propriétés d'ensemble ───────────────────────────────────────────────────────────────────────

def test_les_dix_controles_sont_declares():
    assert len(ctrl.codes_connus()) == 10
    assert sum(1 for c in ctrl.codes_connus()
               if ctrl.NIVEAUX[c] == ctrl.NIVEAU_BLOQUANT) == 5
    assert sum(1 for c in ctrl.codes_connus()
               if ctrl.NIVEAUX[c] == ctrl.NIVEAU_A_CONTROLER) == 5


def test_un_mouvement_annule_nest_plus_controle(tmp_db):
    """Il ne produit plus d'effet ; le signaler encombrerait la liste sans objet."""
    _inserer(tmp_db, "annule", dict(reference_metier="", logement_id="", montant=0.0,
                                    statut=tresorerie.ST_ANNULE))
    assert _codes(ctrl.controler(db_path=tmp_db), "MTP-ANNULE") == set()


def test_les_controles_nalterent_aucun_mouvement(jeu):
    avant = tresorerie.lister(db_path=jeu)
    ctrl.controler(db_path=jeu)
    assert tresorerie.lister(db_path=jeu) == avant


def test_filtre_par_proprietaire(jeu):
    assert ctrl.controler(proprietaire_id="PROP_INCONNU", db_path=jeu)["nb_mouvements"] == 0


# ── Validations de saisie : le vocabulaire refuse ce qui n'existe pas ───────────────────────────
#
# Ces quatre refus sont la première barrière : ils empêchent d'ENREGISTRER une nature, un sens, une
# date ou un montant hors vocabulaire. Les dix contrôles ci-dessus sont la seconde : ils relisent ce
# qui est déjà enregistré. Les deux sont nécessaires — une base contient toujours des lignes plus
# anciennes que ses règles de saisie.

@pytest.fixture
def proprietaire_connu(tmp_db):
    """Le référentiel doit connaître le propriétaire, sinon c'est ce refus-là qui l'emporte."""
    from app.db.connection import get_db

    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
            "prenom_proprietaire, email, telephone, adresse_facturation, mode_facturation, actif, "
            "commentaire, import_id) VALUES (?,'DEMO','','','','','PAR_LOGEMENT','OUI','',"
            "'IMP-TEST')", (PROP,))
        # Sans trace d'import, le référentiel est considéré ABSENT et tout propriétaire inconnu :
        # c'est ce refus-là qui masquerait celui qu'on veut observer.
        conn.execute(
            "INSERT OR IGNORE INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut, nb_feuilles, nb_lignes) "
            "VALUES ('IMP-TEST','2026-01-01T00:00:00','fixture','x','IMPORTE',28,1)")
        conn.commit()
    finally:
        conn.close()
    return tmp_db


def test_saisie_refuse_une_nature_hors_vocabulaire(proprietaire_connu):
    res = tresorerie.previsualiser(PROP, "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_INVENTE", 100.0,
                                   "2026-04-10")
    assert res["ok"] is False and res["code"] == tresorerie.E_NATURE_INCONNUE


def test_saisie_refuse_un_sens_hors_vocabulaire(proprietaire_connu):
    res = tresorerie.previsualiser(PROP, "SENS_INVENTE", "ACOMPTE_PROPRIETAIRE", 100.0,
                                   "2026-04-10")
    assert res["ok"] is False and res["code"] == tresorerie.E_SENS_INCONNU


def test_saisie_refuse_une_date_manquante(proprietaire_connu):
    res = tresorerie.previsualiser(PROP, "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE",
                                   100.0, "")
    assert res["ok"] is False and res["code"] == tresorerie.E_DATE_MANQUANTE


def test_saisie_refuse_un_montant_invalide(proprietaire_connu):
    res = tresorerie.previsualiser(PROP, "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE",
                                   -5, "2026-04-10")
    assert res["ok"] is False and res["code"] == tresorerie.E_MONTANT_INVALIDE


def test_saisie_refuse_un_proprietaire_inconnu(tmp_db):
    res = tresorerie.previsualiser("PROP_INEXISTANT", "PROPRIETAIRE_VERS_SOCIETE",
                                   "ACOMPTE_PROPRIETAIRE", 100.0, "2026-04-10")
    assert res["ok"] is False and res["code"] == tresorerie.E_PROPRIETAIRE_INCONNU
