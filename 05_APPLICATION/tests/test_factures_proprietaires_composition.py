"""Composition d'une facture propriétaire : extras, réductions, charges rattachées, formule,
prévisualisation, PDF de marque et comptabilisation.

Aucun appel réseau, aucune vraie base : `db` isole `cfg.DB_PATH` dans `tmp_path`.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import factures_proprietaires_composition_service as compo
from app.services import factures_proprietaires_pdf as pdfsvc
from app.services import factures_proprietaires_service as svc

# Identité RÉELLE, reprise du Kbis (2026-09-10). Ni SIRET ni TVA intracommunautaire n'y figurent :
# les deux restent volontairement vides ici, et les tests vérifient qu'ils ne sont JAMAIS imprimés.
EMETTEUR = {"nom": "CHOUETTE PATRIMOINE", "forme_juridique": "SAS", "capital": "200,00 €",
            "adresse": "48E Route de Larnavey, 33650 Saint-Selve",
            "siren": "109624767", "rcs": "R.C.S. Bordeaux", "siret": "", "tva_intra": ""}
DESTINATAIRE = {"nom": "Proprietaire Fixture", "adresse": "2 rue de Test, 31000 Toulouse"}


@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    apply_migrations(chemin)
    # Un acompte passe par `proprietaires_tresorerie_service`, qui refuse un propriétaire absent du
    # référentiel (`V01_PROPRIETAIRE_INCONNU`). C'est la bonne règle : on l'alimente plutôt que de
    # la contourner, sinon le test prouverait un chemin que la production n'emprunte jamais.
    conn = get_db(chemin)
    try:
        # Le référentiel n'est « disponible » que s'il porte un import abouti
        # (`ref_setup_repo.est_disponible`) : un schéma vide n'est pas un référentiel.
        conn.execute("INSERT OR IGNORE INTO ref_setup_imports "
                     "(import_id, horodatage, chemin_source, empreinte_source, statut, "
                     " nb_feuilles, nb_lignes) "
                     "VALUES ('IMP-TEST','2026-06-01T00:00:00Z','fixture','0'*64,'IMPORTE',1,1)")
        for pid in ("PROP_FIXT_1", "PROP_0", "PROP_1", "PROP_2", "PROP_AUTRE"):
            conn.execute(
                "INSERT OR IGNORE INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
                "prenom_proprietaire, adresse_facturation, actif, import_id) "
                "VALUES (?,?,?,?,'OUI','IMP-TEST')",
                (pid, f"Nom {pid}", "Prenom", "2 rue de Test, 31000 Toulouse"))
        conn.commit()
    finally:
        conn.close()
    return chemin


@pytest.fixture()
def ecritures_actives(monkeypatch):
    for flag in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                 "COMPTABILITE_REAL_WRITE_ENABLED",
                 "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED",
                 "CHARGES_REAL_WRITE_ENABLED", "CHARGES_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, flag, True, raising=False)


def _source(**kw):
    base = {"mois": "2026-06", "proprietaire_id": "PROP_FIXT_1", "logement_id": "LOG_FIXT_1",
            "source_calcul": "PREF-2026-06-PROP_FIXT_1-LOG_FIXT_1-001",
            "COMMISSION_CONCIERGERIE": 300.0, "MENAGE_FACTURE": 150.0, "CHARGE_FIXE": 50.0,
            "montant_du_conciergerie": 500.0}
    base.update(kw)
    return base


@pytest.fixture()
def facture(db):
    return svc.creer(_source(), acteur="test", db_path=db)["facture_id_opaque"]


def _charge(db, **kw):
    from app.services import charges_saisie_service as charges
    donnees = {"date_charge": "2026-06-12", "montant": 42.90, "categorie_charge_id": "CHG_004",
               "code_impact": "IC", "refacturable": "OUI", "proprietaire_id": "PROP_FIXT_1",
               "logement_id": "LOG_FIXT_1", "commentaire": "Remplacement bouilloire"}
    donnees.update(kw)
    return charges.creer(donnees, acteur="test", db_path=db)["charge_id"]


def _position(db, charge_id):
    """La position de refacturation créée par `charges_saisie_service.creer()`.

    C'est elle, et non la charge, qu'une facture référence : passer par ce détour dans les tests
    reproduit exactement ce que fait l'interface. `None` si la charge n'est pas refacturable.
    """
    conn = get_db(db)
    try:
        r = conn.execute("SELECT position_id FROM charges_refacturation_positions "
                         "WHERE charge_id = ? AND actif = 1", (charge_id,)).fetchone()
    finally:
        conn.close()
    return r[0] if r else None


def _charge_rattachee(db, facture_id, **kw):
    """Crée une charge refacturable et la rattache à la facture. Renvoie (charge_id, résultat)."""
    cid = _charge(db, **kw)
    return cid, compo.rattacher_charge(facture_id, _position(db, cid), acteur="t", db_path=db)


# ── 1-9 : brouillon, période, formule de base ───────────────────────────────────────────────────

def test_brouillon_cree_avec_ses_postes(db, facture):
    d = compo.decomposition(facture, db_path=db)
    assert d["par_cle"]["commissions"] == 300.0
    assert d["par_cle"]["menages"] == 150.0
    assert d["par_cle"]["forfait"] == 50.0
    assert d["total_facture"] == 500.0
    assert d["montant_du"] == 500.0


def test_forfait_est_une_seule_ligne_logiciel_et_consommables(db, facture):
    """Le référentiel (`REC_001`) porte « Forfait client logiciel et consommables » d'un seul
    tenant : la facture ne doit pas inventer deux postes séparés."""
    poste = next(p for p in compo.decomposition(facture, db_path=db)["postes"]
                 if p["cle"] == "forfait")
    assert poste["nb"] == 1
    assert "CHARGE_FIXE" in [l["type_ligne"] for l in poste["lignes"]]
    assert not any(p["cle"] == "consommables"
                   for p in compo.decomposition(facture, db_path=db)["postes"])


def test_periode_expose_les_bornes_reelles_du_mois(db, facture):
    p = compo.periode("2026-06")
    assert (p["debut"], p["fin"]) == ("2026-06-01", "2026-06-30")
    assert compo.periode("2026-02")["fin"] == "2026-02-28"


def test_periode_illisible_ne_leve_pas():
    assert compo.periode("")["debut"] == ""


# ── 10-15 : extras ──────────────────────────────────────────────────────────────────────────────

def test_ajout_extra_augmente_le_total(db, facture):
    compo.ajouter_extra(facture, libelle="Serrurier", montant=60, acteur="t", db_path=db)
    d = compo.decomposition(facture, db_path=db)
    assert d["par_cle"]["extras"] == 60.0
    assert d["total_facture"] == 560.0


def test_extra_negatif_refuse(db, facture):
    with pytest.raises(svc.FactureProprietaireError, match="strictement positif"):
        compo.ajouter_extra(facture, libelle="X", montant=-5, acteur="t", db_path=db)


def test_retrait_extra_retablit_le_total(db, facture):
    r = compo.ajouter_extra(facture, libelle="Serrurier", montant=60, acteur="t", db_path=db)
    svc.supprimer_ligne(facture, r["ligne_id_opaque"], acteur="t", db_path=db)
    assert compo.decomposition(facture, db_path=db)["total_facture"] == 500.0


# ── 16-20 : réductions, et leur différence avec un acompte ──────────────────────────────────────

def test_reduction_diminue_le_total_facture(db, facture):
    compo.ajouter_reduction(facture, libelle="Remise", montant=25, acteur="t", db_path=db)
    d = compo.decomposition(facture, db_path=db)
    assert d["par_cle"]["reductions"] == -25.0
    assert d["total_reductions"] == 25.0
    assert d["total_facture"] == 475.0


def test_reduction_saisie_positive_stockee_negative(db, facture):
    compo.ajouter_reduction(facture, libelle="Remise", montant=25, acteur="t", db_path=db)
    ligne = next(l for l in svc.lire(facture, db_path=db)["lignes"]
                 if l["type_ligne"] == compo.TYPE_REDUCTION)
    assert ligne["montant"] == -25.0


def test_reduction_ne_peut_pas_rendre_la_facture_negative(db, facture):
    with pytest.raises(svc.FactureProprietaireError, match="depasse le montant facturable"):
        compo.ajouter_reduction(facture, libelle="Remise", montant=10_000, acteur="t", db_path=db)


def test_reduction_et_acompte_ne_sont_pas_la_meme_chose(db, facture, monkeypatch):
    """Invariant central : la réduction entre dans le total facturé, l'acompte non."""
    from app.services import factures_proprietaires_edition_service as edition
    compo.ajouter_reduction(facture, libelle="Remise", montant=25, acteur="t", db_path=db)
    edition.ajouter_acompte(facture, montant=100, date_mouvement="2026-06-05", acteur="t",
                            db_path=db)
    d = compo.decomposition(facture, db_path=db)
    assert d["total_facture"] == 475.0, "l'acompte ne doit PAS diminuer le total facture"
    assert d["total_acomptes"] == 100.0
    assert d["montant_du"] == 375.0


# ── 21 : la formule complète ────────────────────────────────────────────────────────────────────

def test_formule_totale_exacte(db, facture, ecritures_actives):
    from app.services import factures_proprietaires_edition_service as edition
    _charge_rattachee(db, facture)                                           # +42.90
    compo.ajouter_extra(facture, libelle="Extra", montant=60, acteur="t", db_path=db)   # +60
    compo.ajouter_reduction(facture, libelle="Remise", montant=25, acteur="t", db_path=db)  # -25
    edition.ajouter_acompte(facture, montant=100, date_mouvement="2026-06-05", acteur="t",
                            db_path=db)
    d = compo.decomposition(facture, db_path=db)
    attendu_sous_total = 300.0 + 150.0 + 50.0 + 42.90 + 60.0
    assert d["sous_total"] == pytest.approx(attendu_sous_total)
    assert d["total_facture"] == pytest.approx(attendu_sous_total - 25.0)
    assert d["montant_du"] == pytest.approx(attendu_sous_total - 25.0 - 100.0)


# ── 22-26 : charges refacturables rattachées ────────────────────────────────────────────────────

def test_charge_eligible_proposee_puis_retiree_de_la_liste(db, facture, ecritures_actives):
    cid = _charge(db)
    assert [c["charge_id"] for c in compo.charges_eligibles(facture, db_path=db)] == [cid]
    compo.rattacher_charge(facture, _position(db, cid), acteur="t", db_path=db)
    assert compo.charges_eligibles(facture, db_path=db) == []


def test_selection_deleguee_au_selecteur_canonique(db, facture, ecritures_actives):
    """`charges_eligibles` doit proposer des POSITIONS, pas des charges : c'est ce que la
    validation impute. Deux sélecteurs concurrents produiraient des factures invalidables."""
    from app.services import charges_refacturation_service as refac
    cid = _charge(db)
    proposees = compo.charges_eligibles(facture, db_path=db)
    canoniques = refac.proposer_pour_facture("PROP_FIXT_1", logement_id="LOG_FIXT_1", db_path=db)
    assert [p["position_id"] for p in proposees] == [p["position_id"] for p in canoniques]
    assert proposees[0]["position_id"] == _position(db, cid)
    assert proposees[0]["montant_restant"] == 42.90


def test_rattachement_reference_la_position_sans_recreer_la_charge(db, facture, ecritures_actives):
    cid, r = _charge_rattachee(db, facture)
    ligne = next(l for l in svc.lire(facture, db_path=db)["lignes"]
                 if l["ligne_id_opaque"] == r["ligne_id_opaque"])
    assert ligne["objet_source_type"] == compo.SOURCE_POSITION == svc.SOURCE_POSITION_REFAC
    assert ligne["objet_source_ref"] == _position(db, cid), \
        "la ligne reference la POSITION : c'est elle que valider() impute"
    assert ligne["montant"] == 42.90, "le montant vient du solde de la position, jamais ressaisi"
    conn = get_db(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM charges WHERE charge_id=?", (cid,)).fetchone()[0] == 1
    finally:
        conn.close()


def test_charge_rattachee_est_validable_et_impute_sa_position(db, facture, ecritures_actives):
    """LE test de non-régression : une facture composée par l'interface doit pouvoir être validée.
    Référencer la charge au lieu de sa position produisait « IMPUTATION_REFUSEE ... Position
    introuvable » — un brouillon impossible à émettre, découvert seulement au dernier clic."""
    cid, _ = _charge_rattachee(db, facture)
    svc.valider(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, acteur="t", db_path=db)
    assert svc.lire(facture, db_path=db)["statut"] == svc.ST_VALIDE
    conn = get_db(db)
    try:
        impute, statut = conn.execute(
            "SELECT montant_impute_total, statut FROM charges_refacturation_positions "
            "WHERE charge_id=?", (cid,)).fetchone()
    finally:
        conn.close()
    assert impute == 42.90 and statut == "IMPUTEE", "la validation consomme la position, une fois"


def test_ligne_refacturee_de_mauvaise_source_refusee_a_la_validation(db, facture,
                                                                     ecritures_actives):
    """Garde de contrat : une ligne CHARGE_REFACTUREE qui ne référence pas une position est
    refusée avec un message qui désigne la ligne, pas la position."""
    cid, r = _charge_rattachee(db, facture)
    conn = get_db(db)
    try:
        conn.execute("UPDATE factures_proprietaires_lignes SET objet_source_type='CHARGE', "
                     "objet_source_ref=? WHERE ligne_id_opaque=?", (cid, r["ligne_id_opaque"]))
        conn.commit()
    finally:
        conn.close()
    with pytest.raises(svc.FactureProprietaireError, match="doit referencer sa position"):
        svc.valider(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, acteur="t", db_path=db)
    assert svc.lire(facture, db_path=db)["statut"] == svc.ST_BROUILLON


def test_meme_element_refuse_deux_fois_sur_la_meme_facture(db, facture, ecritures_actives):
    """Deux fois le même élément sur LA MÊME facture, c'est un double-clic — jamais une intention.
    (Sur deux factures différentes, c'est au contraire le partage légitime : test suivant.)"""
    cid, _ = _charge_rattachee(db, facture)
    with pytest.raises(svc.FactureProprietaireError, match="deja porte par cette facture"):
        compo.rattacher_charge(facture, _position(db, cid), acteur="t", db_path=db)


def test_refacturation_partielle_sur_deux_factures(db, facture, ecritures_actives):
    """Recette utilisateur n°2 (§8, §11) : une dépense se récupère comme l'utilisateur le décide.

    L'ancienne garde « une charge = une seule ligne de facture » interdisait ce cas métier normal
    en même temps que le double comptage. C'est le MONTANT CUMULÉ qui est plafonné, pas le nombre
    de factures.
    """
    from app.services import charges_refacturation_service as refac
    cid = _charge(db, montant=100.0)
    pos = _position(db, cid)
    autre = svc.creer(_source(mois="2026-07", source_calcul="PREF-2026-07"),
                      acteur="t", db_path=db)["facture_id_opaque"]

    compo.rattacher_charge(facture, pos, montant=60, justification="solde reporte",
                           acteur="t", db_path=db)
    assert refac.montant_disponible(pos, db_path=db) == 40.0, "un brouillon RÉSERVE le montant"
    compo.rattacher_charge(autre, pos, montant=40, acteur="t", db_path=db)  # solde exact
    assert refac.montant_disponible(pos, db_path=db) == 0.0

    # Solde épuisé : une TROISIÈME facture ne peut plus rien en tirer. (Sur les deux premières, le
    # refus porterait un autre nom — « déjà porté par cette facture » — et ne prouverait pas
    # l'épuisement du montant.)
    troisieme = svc.creer(_source(mois="2026-08", source_calcul="PREF-2026-08"),
                          acteur="t", db_path=db)["facture_id_opaque"]
    with pytest.raises(svc.FactureProprietaireError, match="non proposable|solde"):
        compo.rattacher_charge(troisieme, pos, montant=1, acteur="t", db_path=db)


def test_le_cumul_ne_peut_pas_depasser_le_montant_source(db, facture, ecritures_actives):
    """700 réparti 500 + 300 = 800 : le second ajout doit être refusé, pas « arrondi »."""
    cid = _charge(db, montant=700.0)
    pos = _position(db, cid)
    autre = svc.creer(_source(mois="2026-07", source_calcul="PREF-2026-07"),
                      acteur="t", db_path=db)["facture_id_opaque"]
    compo.rattacher_charge(facture, pos, montant=500, justification="reste a repartir",
                           acteur="t", db_path=db)
    with pytest.raises(svc.FactureProprietaireError, match="superieur au solde disponible"):
        compo.rattacher_charge(autre, pos, montant=300, justification="depassement",
                               acteur="t", db_path=db)


@pytest.mark.parametrize("montant", [0, -50, 700.01])
def test_montants_invalides_refuses(db, facture, ecritures_actives, montant):
    cid = _charge(db, montant=700.0)
    with pytest.raises(svc.FactureProprietaireError):
        compo.rattacher_charge(facture, _position(db, cid), montant=montant, acteur="t", db_path=db)


def test_charge_mauvais_proprietaire_refusee(db, facture, ecritures_actives):
    cid = _charge(db, proprietaire_id="PROP_AUTRE", logement_id="LOG_AUTRE")
    assert cid not in [c["charge_id"] for c in compo.charges_eligibles(facture, db_path=db)]
    with pytest.raises(svc.FactureProprietaireError, match="proprietaire"):
        compo.rattacher_charge(facture, _position(db, cid), acteur="t", db_path=db)


def test_charge_non_refacturable_na_pas_de_position(db, facture, ecritures_actives):
    """Une charge non refacturable ne produit AUCUNE position : elle est donc inatteignable, et
    pas seulement filtrée à l'affichage."""
    cid = _charge(db, refacturable="NON")
    assert _position(db, cid) is None
    assert cid not in [c["charge_id"] for c in compo.charges_eligibles(facture, db_path=db)]
    with pytest.raises(svc.FactureProprietaireError, match="non proposable"):
        compo.rattacher_charge(facture, "POSREF-INEXISTANTE", acteur="t", db_path=db)


def test_detachement_libere_la_charge_sans_l_annuler(db, facture, ecritures_actives):
    cid, r = _charge_rattachee(db, facture)
    compo.detacher_charge(facture, r["ligne_id_opaque"], acteur="t", db_path=db)
    assert [c["charge_id"] for c in compo.charges_eligibles(facture, db_path=db)] == [cid]
    conn = get_db(db)
    try:
        statut = conn.execute("SELECT statut FROM charges WHERE charge_id=?", (cid,)).fetchone()[0]
        impute = conn.execute("SELECT montant_impute_total FROM "
                              "charges_refacturation_positions WHERE charge_id=?",
                              (cid,)).fetchone()[0]
    finally:
        conn.close()
    assert statut == "ACTIVE", "une charge preexistante n'est jamais annulee par le document"
    assert impute == 0.0, "un brouillon n'a rien consomme : il n'y a rien a desimputer"


def test_index_unique_interdit_structurellement_le_double_rattachement(db, facture,
                                                                       ecritures_actives):
    """Ceinture ET bretelles : même en contournant la garde applicative, SQLite refuse d'inscrire
    DEUX FOIS la même position sur LA MÊME facture (index partiel, migration 0075).

    L'index d'origine (0072) portait sur `charge_id` seul : il interdisait aussi le partage entre
    deux factures, donc le cas métier normal. Il a été remplacé, pas supprimé.
    """
    import sqlite3
    cid, r = _charge_rattachee(db, facture)
    pos = _position(db, cid)
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO factures_proprietaires_lignes "
                "(ligne_id_opaque, facture_id_opaque, numero_ligne, type_ligne, libelle, montant, "
                " objet_source_type, objet_source_ref) "
                "VALUES ('FPRL-DOUBLON', ?, 99, 'CHARGE_REFACTUREE', 'doublon', 10, ?, ?)",
                (facture, svc.SOURCE_POSITION_REFAC, pos))
            conn.commit()
    finally:
        conn.close()


def test_ancienne_contrainte_trop_large_retiree(db, facture, ecritures_actives):
    """L'index unique sur `charge_id` (0072) ne doit plus exister : il interdisait la
    refacturation partagée entre deux factures, que la migration 0075 rend possible."""
    conn = get_db(db)
    try:
        noms = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'")}
    finally:
        conn.close()
    assert "idx_fprlc_charge_unique" not in noms
    assert "idx_fprl_position_par_facture" in noms


def test_lien_charge_accepte_deux_lignes_pour_une_meme_charge(db, facture, ecritures_actives):
    """La table de lien (0071) doit désormais accepter plusieurs lignes pour une même charge :
    c'est ce que produit une refacturation partagée entre deux factures."""
    cid, _ = _charge_rattachee(db, facture)
    conn = get_db(db)
    try:
        conn.execute("INSERT INTO factures_proprietaires_lignes_charge "
                     "(ligne_id_opaque, facture_id_opaque, charge_id, code_impact) "
                     "VALUES ('FPRL-SECONDE-LIGNE', ?, ?, 'IC')", (facture, cid))
        conn.commit()
        n = conn.execute("SELECT COUNT(*) FROM factures_proprietaires_lignes_charge "
                         "WHERE charge_id = ?", (cid,)).fetchone()[0]
    finally:
        conn.close()
    assert n == 2


# ── 27-30 : document, prévisualisation, PDF ─────────────────────────────────────────────────────

def test_document_brouillon_et_pdf_utilisent_la_meme_structure(db, facture):
    doc = compo.document(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    assert doc["fige"] is False
    assert {"lignes", "reservations", "decomposition", "montant_total"} <= set(doc)
    octets = pdfsvc.rendre(doc)
    assert octets[:4] == b"%PDF"


def test_pdf_groupes_alignes_sur_le_service():
    """Le PDF redéfinit la table des groupes pour rester sans dépendance applicative : ce test
    empêche les deux de diverger en silence."""
    assert [(c, t) for c, _, t in pdfsvc._GROUPES_PDF] == [(c, t) for c, _, t in compo.GROUPES]


def test_pdf_porte_le_logo_officiel():
    assert pdfsvc.LOGO.exists(), "le logo officiel doit etre present dans les assets"
    assert pdfsvc.LOGO.name.endswith(".png")


class _Sonde:
    """Lit les mentions de pied de page sans rendre le PDF entier."""
    def __init__(self, snapshot):
        objet = pdfsvc._Facture(snapshot)
        self.mentions = objet._mentions_pied()


# ── Identité légale issue du Kbis ───────────────────────────────────────────────────────────────

def test_pied_de_page_reprend_la_presentation_legale_du_kbis(db, facture):
    """Les trois lignes légales, dans la présentation usuelle d'une facture française."""
    doc = compo.document(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    mentions = _Sonde(doc).mentions
    assert "CHOUETTE PATRIMOINE — SAS au capital de 200,00 €" in mentions
    assert "48E Route de Larnavey — 33650 Saint-Selve" in mentions
    assert "109 624 767 R.C.S. Bordeaux" in mentions


def test_siren_formate_mais_jamais_presente_comme_siret(db, facture):
    doc = compo.document(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    texte = " | ".join(_Sonde(doc).mentions)
    assert "109 624 767" in texte, "le SIREN doit etre lisible, en trois groupes de trois"
    assert "SIRET" not in texte, "aucun SIRET n'est connu : il ne doit jamais apparaitre"
    assert "TVA" not in texte, "aucune TVA intracommunautaire n'est connue"


def test_siret_et_tva_s_affichent_des_qu_ils_sont_connus(db, facture):
    """L'omission est CONDITIONNELLE, pas codée en dur : le jour où les numéros existent, ils
    s'impriment — chacun sous sa propre étiquette."""
    emetteur = {**EMETTEUR, "siret": "10962476700012", "tva_intra": "FR00109624767"}
    doc = compo.document(facture, emetteur=emetteur, destinataire=DESTINATAIRE, db_path=db)
    texte = " | ".join(_Sonde(doc).mentions)
    assert "SIRET 10962476700012" in texte
    assert "TVA FR00109624767" in texte


def test_siren_lisible():
    assert pdfsvc._siren_lisible("109624767") == "109 624 767"
    assert pdfsvc._siren_lisible("109 624 767") == "109 624 767"
    # Un numéro inattendu est rendu tel quel plutôt que regroupé au hasard.
    assert pdfsvc._siren_lisible("12345") == "12345"
    assert pdfsvc._siren_lisible("") == ""


def test_forme_et_capital_forment_une_seule_mention(db, facture):
    """« SAS — 200,00 € » ne veut rien dire : la mention légale est « SAS au capital de … »."""
    doc = compo.document(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    ligne = _Sonde(doc).mentions[0]
    assert "au capital de" in ligne
    assert "SAS — 200,00" not in ligne


def test_aucune_mention_fabriquee_quand_tout_manque(db, facture):
    """Un émetteur vide ne produit ni ligne vide, ni formule plausible inventée."""
    doc = compo.document(facture, emetteur={"nom": "X"}, destinataire=DESTINATAIRE, db_path=db)
    mentions = _Sonde(doc).mentions
    assert mentions == ["X"], mentions


def test_pdf_reel_porte_les_mentions_du_kbis(db, facture):
    """Bout en bout : les mentions doivent réellement atteindre les octets du PDF."""
    doc = compo.document(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    octets = pdfsvc.rendre(doc)
    assert octets[:4] == b"%PDF"
    # Le rendu translittère vers latin-1 : « — » devient « - » et « € » devient « EUR ».
    for attendu in (b"CHOUETTE PATRIMOINE", b"SAS au capital de 200,00 EUR",
                    b"48E Route de Larnavey", b"33650 Saint-Selve", b"109 624 767",
                    b"R.C.S. Bordeaux"):
        assert attendu in octets, f"{attendu!r} absent du PDF"
    assert b"SIRET" not in octets


def test_pdf_multi_pages_repete_les_entetes(db, facture):
    doc = compo.document(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    doc["reservations"] = [
        {"reservation_id": f"HA-{i}", "check_in": "2026-06-01", "check_out": "2026-06-03",
         "nights": 2, "plateforme": "Airbnb", "payout": 200.0,
         "assiette_commission": 180.0, "taux_commission": 0.19, "commission": 34.2}
        for i in range(40)]
    octets = pdfsvc.rendre(doc)
    assert octets.count(b"/Type /Page\n") >= 2, "40 sejours doivent produire plusieurs pages"


def test_pdf_deterministe(db, facture):
    doc = compo.document(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    assert pdfsvc.rendre(doc) == pdfsvc.rendre(doc)


def test_pdf_sans_pii_voyageur(db, facture):
    doc = compo.document(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    doc["reservations"] = [{"reservation_id": "HA-1", "guest_name": "Jean SECRET",
                            "check_in": "2026-06-01", "check_out": "2026-06-03", "nights": 2,
                            "plateforme": "Airbnb", "payout": 200.0}]
    assert b"SECRET" not in pdfsvc.rendre(doc), "aucun nom de voyageur ne doit atteindre le PDF"


def test_taux_affiche_en_pourcent():
    assert pdfsvc._taux(0.19).startswith("19,00")
    assert pdfsvc._taux(19).startswith("19,00")
    assert pdfsvc._taux(None) == ""


# ── 31-36 : émission, numérotation, comptabilité ────────────────────────────────────────────────

def _emettre(db, facture, tmp_path, ecritures=True):
    svc.valider(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, acteur="t", db_path=db)
    return svc.emettre(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE,
                       date_facture="2026-07-01",
                       generer_pdf=pdfsvc.fabrique(Path(tmp_path) / "docs"), acteur="t",
                       exiger_conformite=False, db_path=db)


def test_identite_acceptee_avec_siren_sans_siret(db, facture):
    svc.valider(facture, emetteur=EMETTEUR, destinataire=DESTINATAIRE, acteur="t", db_path=db)
    assert svc.lire(facture, db_path=db)["statut"] == svc.ST_VALIDE


def test_identite_refusee_sans_aucun_identifiant(db, facture):
    sans = {**EMETTEUR, "siren": "", "siret": ""}
    with pytest.raises(svc.FactureProprietaireError, match="siret ou siren"):
        svc.valider(facture, emetteur=sans, destinataire=DESTINATAIRE, acteur="t", db_path=db)


def test_emission_fige_reservations_et_decomposition(db, facture, tmp_path):
    emise = _emettre(db, facture, tmp_path)
    import json
    snap = json.loads(svc.lire(facture, db_path=db)["snapshot_json"])
    assert "reservations" in snap and "decomposition" in snap
    assert snap["decomposition"]["montant_du"] == emise["montant_total"]


def test_document_emis_rend_le_snapshot_fige(db, facture, tmp_path):
    _emettre(db, facture, tmp_path)
    doc = compo.document(facture, db_path=db)
    assert doc["fige"] is True
    assert doc["numero_facture"]


def test_comptabilisation_idempotente(db, facture, tmp_path, ecritures_actives):
    from app.services import comptabilite_ecritures_service as compta
    emise = _emettre(db, facture, tmp_path)
    e1 = compta.generer_ecriture_vente_facture(emise, acteur="t", db_path=db)
    e2 = compta.generer_ecriture_vente_facture(emise, acteur="t", db_path=db)
    assert e1["ok"] and e2["ok"]
    assert e1["ecriture_id_opaque"] == e2["ecriture_id_opaque"]
    assert e2.get("deja_generee") is True
    conn = get_db(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM ecritures WHERE origine_id_opaque=?",
                            (facture,)).fetchone()[0] == 1
    finally:
        conn.close()


def test_ecriture_liee_a_la_facture_et_equilibree(db, facture, tmp_path, ecritures_actives):
    from app.services import comptabilite_ecritures_service as compta
    emise = _emettre(db, facture, tmp_path)
    e = compta.generer_ecriture_vente_facture(emise, acteur="t", db_path=db)
    conn = get_db(db)
    try:
        row = conn.execute("SELECT origine_type, total_debit, total_credit FROM ecritures "
                           "WHERE ecriture_id_opaque=?", (e["ecriture_id_opaque"],)).fetchone()
    finally:
        conn.close()
    assert row["origine_type"]
    assert row["total_debit"] == row["total_credit"] == emise["montant_total"]


def test_brouillon_ne_produit_aucune_ecriture(db, facture, ecritures_actives):
    from app.services import comptabilite_ecritures_service as compta
    r = compta.generer_ecriture_vente_facture(svc.lire(facture, db_path=db), acteur="t",
                                              db_path=db)
    assert r["ok"] is False, "un BROUILLON ne constate aucune vente"


def test_facture_emise_non_editable(db, facture, tmp_path):
    _emettre(db, facture, tmp_path)
    for action in (
            lambda: compo.ajouter_extra(facture, libelle="X", montant=10, acteur="t", db_path=db),
            lambda: compo.ajouter_reduction(facture, libelle="R", montant=10, acteur="t",
                                            db_path=db),
    ):
        with pytest.raises(svc.FactureProprietaireError):
            action()


def test_numero_unique_par_serie(db, tmp_path):
    ids = [svc.creer(_source(proprietaire_id=f"PROP_{i}"), acteur="t",
                     db_path=db)["facture_id_opaque"] for i in range(3)]
    numeros = [_emettre(db, fid, tmp_path)["numero_facture"] for fid in ids]
    assert len(set(numeros)) == 3, f"numeros dupliques : {numeros}"
