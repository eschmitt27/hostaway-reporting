"""§26/§27 — la quantité vient du document, la nature de la ligne décide de qui paie.

TROIS RÈGLES, ET LE CAS RÉEL QUI LES A IMPOSÉES

§27 « une quantité extraite du PDF est une donnée source » — la corriger reste possible, mais la
valeur lue est conservée pour toujours et le motif est obligatoire.

§27 « ne jamais répartir silencieusement une quantité ambiguë » — une ligne « 4 ménages » couvrant
deux logements ne se devine pas (2+2 ? 3+1 ?). La répartition est fournie, jamais inventée, et la
somme des quantités doit retomber sur celle du document.

§26 « ménage O/N » — le type de la ligne décide si le montant entre dans le coût ménage d'un
logement, donc dans ce qui sera refacturé à son propriétaire. Le parseur se trompe : l'utilisateur
doit pouvoir le dire.

LE CAS RÉEL. La facture 0005 portait une ligne à quantité 0, prix unitaire 32,00 € et montant
36,00 € — trois valeurs incompatibles. C'est ce +36,00 € qui empêchait la facture de boucler.
`diagnostic_ecart` le nomme désormais pour ce qu'il est (une extraction incorrecte) au lieu
d'annoncer un écart sans cause, et le distingue du cas opposé — la facture 2026-40, dont toutes
les lignes sont cohérentes et où il manque simplement 89,00 €.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import facture_lignes_menage_service as flm
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs_svc


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    conn = get_db(p)
    try:
        for lid in ("LOG_0001", "LOG_0002", "LOG_0003"):
            conn.execute(
                "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, "
                "adresse, ville, actif, statut_parc, import_id) VALUES (?,?,?,?,?,?,?,?)",
                (lid, f"Logement {lid}", lid, "", "TOULOUSE", "OUI", "GERE", "TEST"))
        conn.commit()
    finally:
        conn.close()
    return p


def _facture(db, montant_ttc: float, ref: str = "F-1") -> str:
    frs = frs_svc.creer(f"Presta {ref}", "MENAGE", db_path=db)["fournisseur_id_opaque"]
    return fact.creer({"fournisseur_id_opaque": frs, "facture_ref": ref,
                       "date_facture": "2026-07-31", "montant_ttc": montant_ttc},
                      db_path=db)["facture_id_opaque"]


# ── Les valeurs du document sont photographiées à l'import ──────────────────────────────────────

def test_les_valeurs_extraites_sont_conservees_telles_quelles(db):
    f = _facture(db, 260.0)
    flm.ajouter_ligne(f, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=260.0,
                      logement_id="LOG_0001", description="T4 (PDF)", quantite=4,
                      prix_unitaire=65.0, db_path=db)
    conn = get_db(db)
    try:
        r = conn.execute(
            "SELECT l.montant_ttc, l.montant_ttc_source, l.logement_id_source, d.quantite, "
            "d.quantite_source, d.prix_unitaire_source FROM facture_lignes_menage l "
            "JOIN facture_lignes_menage_detail d ON d.ligne_id_opaque = l.ligne_id_opaque "
            "WHERE l.facture_id_opaque=?", (f,)).fetchone()
    finally:
        conn.close()
    assert (r["montant_ttc_source"], r["quantite_source"], r["prix_unitaire_source"]) == \
        (260.0, 4, 65.0)
    assert r["logement_id_source"] == "LOG_0001"


# ── §27 : cohérence interne d'une ligne ─────────────────────────────────────────────────────────

def test_coherence_ligne_detecte_le_cas_reel_de_la_facture_0005():
    """quantité 0 × 32,00 € ne fait pas 36,00 € : la ligne se contredit elle-même."""
    c = flm.coherence_ligne({"quantite": 0, "prix_unitaire": 32.0, "montant_ttc": 36.0})
    assert c["verifiable"] is True and c["coherente"] is False
    assert c["attendu"] == 0.0 and c["ecart"] == 36.0
    assert "32.00" in c["message"] and "36.00" in c["message"]


def test_coherence_ligne_accepte_une_ligne_juste():
    assert flm.coherence_ligne(
        {"quantite": 4, "prix_unitaire": 65.0, "montant_ttc": 260.0})["coherente"] is True


def test_quantite_absente_n_est_pas_une_incoherence():
    """Un document qui ne détaille pas la quantité n'est pas un document faux."""
    c = flm.coherence_ligne({"quantite": None, "prix_unitaire": None, "montant_ttc": 50.0})
    assert c["verifiable"] is False and c["coherente"] is True


# ── §27 : l'écart de la facture est QUALIFIÉ, pas seulement chiffré ──────────────────────────────

def test_diagnostic_distingue_une_ligne_mal_lue(db):
    """Reproduit la facture 0005 : total 520 €, lignes 556 €, une seule ligne incohérente."""
    f = _facture(db, 520.0, "F-0005")
    for montant, log, q, pu in [(260.0, "LOG_0001", 4, 65.0), (52.0, "LOG_0002", 1, 52.0),
                                (208.0, "LOG_0003", 4, 52.0), (36.0, "LOG_0001", 0, 32.0)]:
        flm.ajouter_ligne(f, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=montant,
                          logement_id=log, description=f"ligne {montant}", quantite=q,
                          prix_unitaire=pu, db_path=db)

    d = flm.diagnostic_ecart(f, db_path=db)
    assert d["montant_lignes"] == 556.0 and d["ecart"] == -36.0
    assert d["diagnostic"] == flm.D_LIGNES_INCOHERENTES
    assert len(d["lignes_incoherentes"]) == 1
    assert d["somme_recalculee"] == 520.0
    assert "extraction incorrecte" in d["message"]


def test_diagnostic_distingue_une_ligne_manquante(db):
    """Reproduit la facture 2026-40 : toutes les lignes justes, et 89 € absents."""
    f = _facture(db, 1056.0, "F-2026-40")
    for montant, q, pu in [(87.0, 3, 29.0), (385.0, 7, 55.0), (232.0, 8, 29.0),
                           (29.0, 1, 29.0), (55.0, 1, 55.0), (69.0, 1, 69.0), (110.0, 2, 55.0)]:
        flm.ajouter_ligne(f, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=montant,
                          logement_id="LOG_0001", description=f"ligne {montant}", quantite=q,
                          prix_unitaire=pu, db_path=db)

    d = flm.diagnostic_ecart(f, db_path=db)
    assert d["montant_lignes"] == 967.0 and d["ecart"] == 89.0
    assert d["diagnostic"] == flm.D_LIGNE_MANQUANTE
    assert d["lignes_incoherentes"] == []
    assert "n'ont pas été extraites" in d["message"]


def test_diagnostic_coherent_quand_tout_boucle(db):
    f = _facture(db, 100.0, "F-OK")
    flm.ajouter_ligne(f, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=100.0,
                      logement_id="LOG_0001", description="x", quantite=2, prix_unitaire=50.0,
                      db_path=db)
    assert flm.diagnostic_ecart(f, db_path=db)["diagnostic"] == flm.D_COHERENT


def test_neutraliser_la_ligne_fautive_fait_boucler_la_facture(db):
    """Le geste §30 appliqué au diagnostic §27 : la facture tombe sur son total."""
    f = _facture(db, 520.0, "F-FIX")
    ids = []
    for montant, q, pu in [(260.0, 4, 65.0), (52.0, 1, 52.0), (208.0, 4, 52.0), (36.0, 0, 32.0)]:
        ids.append(flm.ajouter_ligne(
            f, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=montant, logement_id="LOG_0001",
            description=f"ligne {montant}", quantite=q, prix_unitaire=pu,
            db_path=db)["ligne_id_opaque"])

    flm.marquer_extraction_incorrecte(
        ids[-1], motif="Montant 36,00 € incompatible avec quantité 0 × 32,00 €", db_path=db)
    d = flm.diagnostic_ecart(f, db_path=db)
    assert d["montant_lignes"] == 520.0
    assert d["diagnostic"] == flm.D_COHERENT


# ── §27 : corriger une quantité ─────────────────────────────────────────────────────────────────

def test_corriger_une_quantite_exige_un_motif(db):
    f = _facture(db, 100.0, "F-Q1")
    lid = flm.ajouter_ligne(f, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=100.0,
                            logement_id="LOG_0001", description="x", quantite=2,
                            prix_unitaire=50.0, db_path=db)["ligne_id_opaque"]
    assert flm.corriger_quantite(lid, quantite=3, motif="  ", db_path=db)["code"] == \
        "MOTIF_OBLIGATOIRE"


def test_corriger_une_quantite_conserve_la_valeur_du_document(db):
    f = _facture(db, 100.0, "F-Q2")
    lid = flm.ajouter_ligne(f, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=100.0,
                            logement_id="LOG_0001", description="x", quantite=2,
                            prix_unitaire=50.0, db_path=db)["ligne_id_opaque"]
    res = flm.corriger_quantite(lid, quantite=4, motif="Le prestataire a confirmé 4 passages",
                                acteur="test", db_path=db)
    assert res["ok"] is True and res["quantite"] == 4
    assert res["quantite_source"] == 2, "la valeur lue sur le document ne bouge jamais"

    ligne = next(l for l in flm.lignes(f, db_path=db) if l["ligne_id_opaque"] == lid)
    assert ligne["quantite"] == 4
    assert "Le prestataire a confirmé 4 passages" in (ligne["commentaire"] or "")


def test_quantite_negative_refusee(db):
    f = _facture(db, 100.0, "F-Q3")
    lid = flm.ajouter_ligne(f, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=100.0,
                            logement_id="LOG_0001", description="x", quantite=2,
                            prix_unitaire=50.0, db_path=db)["ligne_id_opaque"]
    assert flm.corriger_quantite(lid, quantite=-1, motif="essai", db_path=db)["code"] == \
        "QUANTITE_INVALIDE"


# ── §26 : ménage oui / non ──────────────────────────────────────────────────────────────────────

def test_marquer_une_ligne_comme_non_menage_la_sort_du_cout(db):
    """Un « frais de déplacement » typé ménage gonflerait le coût d'un logement."""
    f = _facture(db, 100.0, "F-N1")
    lid = flm.ajouter_ligne(f, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=40.0,
                            logement_id="LOG_0001", description="Frais de déplacement",
                            quantite=1, prix_unitaire=40.0, db_path=db)["ligne_id_opaque"]
    assert flm.cout_menages_par_logement(f, db_path=db) == {"LOG_0001": 40.0}

    res = flm.marquer_menage(lid, menage=False,
                             motif="Déplacement facturé, ce n'est pas un ménage", db_path=db)
    assert res["ok"] is True and res["type_ligne"] == flm.TYPE_FRAIS_NON_AFFECTE
    assert flm.cout_menages_par_logement(f, db_path=db) == {}


def test_marquer_menage_exige_un_motif(db):
    f = _facture(db, 100.0, "F-N2")
    lid = flm.ajouter_ligne(f, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=40.0,
                            logement_id="LOG_0001", description="x", db_path=db)["ligne_id_opaque"]
    assert flm.marquer_menage(lid, menage=False, motif="", db_path=db)["code"] == \
        "MOTIF_OBLIGATOIRE"


def test_passer_en_menage_sans_logement_refuse(db):
    """Un coût ménage sans logement ne veut rien dire : on ne sait pas qui le paie."""
    f = _facture(db, 100.0, "F-N3")
    lid = flm.ajouter_ligne(f, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE, montant_ttc=40.0,
                            description="Frais divers", db_path=db)["ligne_id_opaque"]
    res = flm.marquer_menage(lid, menage=True, motif="c'est bien un ménage", db_path=db)
    assert res["ok"] is False and res["code"] == "LOGEMENT_MANQUANT"


# ── §27 : répartir une ligne sur plusieurs logements ────────────────────────────────────────────

def _ligne_multi(db):
    f = _facture(db, 260.0, "F-R1")
    lid = flm.ajouter_ligne(f, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=260.0,
                            logement_id="LOG_0001", description="4 ménages (2 logements)",
                            quantite=4, prix_unitaire=65.0, db_path=db)["ligne_id_opaque"]
    return f, lid


def test_repartition_respecte_la_quantite_du_document(db):
    f, lid = _ligne_multi(db)
    res = flm.repartir_ligne(lid, [{"logement_id": "LOG_0001", "quantite": 3},
                                   {"logement_id": "LOG_0002", "quantite": 2}],
                             motif="relecture du détail", db_path=db)
    assert res["ok"] is False and res["code"] == "QUANTITE_NON_CONSERVEE"
    assert "n'ajuste pas tout seul" in res["detail"]


def test_repartition_exige_un_motif_et_deux_logements(db):
    f, lid = _ligne_multi(db)
    assert flm.repartir_ligne(lid, [{"logement_id": "LOG_0001", "quantite": 2},
                                    {"logement_id": "LOG_0002", "quantite": 2}],
                             motif=" ", db_path=db)["code"] == "MOTIF_OBLIGATOIRE"
    assert flm.repartir_ligne(lid, [{"logement_id": "LOG_0001", "quantite": 4}],
                              motif="x", db_path=db)["code"] == "REPARTITION_INSUFFISANTE"


def test_repartition_somme_exactement_au_centime(db):
    """100,00 € sur 3 parts : 33,34 + 33,33 + 33,33 — jamais 99,99 €."""
    f = _facture(db, 100.0, "F-R2")
    lid = flm.ajouter_ligne(f, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=100.0,
                            logement_id="LOG_0001", description="3 ménages", quantite=3,
                            prix_unitaire=33.33, db_path=db)["ligne_id_opaque"]
    res = flm.repartir_ligne(lid, [{"logement_id": "LOG_0001", "quantite": 1},
                                   {"logement_id": "LOG_0002", "quantite": 1},
                                   {"logement_id": "LOG_0003", "quantite": 1}],
                             motif="un ménage par logement", db_path=db)
    assert res["ok"] is True
    assert res["somme_parts"] == 100.0
    assert sorted(p["montant_ttc"] for p in res["parts"]) == [33.33, 33.33, 33.34]


def test_repartition_neutralise_la_ligne_source_sans_la_supprimer(db):
    f, lid = _ligne_multi(db)
    res = flm.repartir_ligne(lid, [{"logement_id": "LOG_0001", "quantite": 3},
                                   {"logement_id": "LOG_0002", "quantite": 1}],
                             motif="3 au T4, 1 au studio (confirmé par le prestataire)",
                             acteur="test", db_path=db)
    assert res["ok"] is True
    assert sorted(p["montant_ttc"] for p in res["parts"]) == [65.0, 195.0]

    toutes = flm.lignes(f, db_path=db)
    source = next(l for l in toutes if l["ligne_id_opaque"] == lid)
    assert source["statut_ligne"] == flm.STATUT_LIGNE_REPARTIE
    assert source["montant_ttc"] == 260.0, "le document disait 260,00 € : cela reste lisible"
    parts = [l for l in toutes if l["ligne_parente_id_opaque"] == lid]
    assert len(parts) == 2
    assert all(p["source"] == flm.SOURCE_REPARTITION for p in parts)

    # Le total de la facture ne compte la dépense qu'une fois.
    assert flm.controler_total(f, db_path=db)["coherent"] is True
    assert flm.cout_menages_par_logement(f, db_path=db) == {"LOG_0001": 195.0, "LOG_0002": 65.0}


def test_repartition_refuse_un_logement_inconnu(db):
    f, lid = _ligne_multi(db)
    res = flm.repartir_ligne(lid, [{"logement_id": "LOG_0001", "quantite": 2},
                                   {"logement_id": "LOG_9999", "quantite": 2}],
                             motif="x", db_path=db)
    assert res["ok"] is False and res["code"] == "LOGEMENT_INCONNU"


def test_repartition_refuse_un_logement_en_double(db):
    f, lid = _ligne_multi(db)
    res = flm.repartir_ligne(lid, [{"logement_id": "LOG_0001", "quantite": 2},
                                   {"logement_id": "LOG_0001", "quantite": 2}],
                             motif="x", db_path=db)
    assert res["ok"] is False and res["code"] == "LOGEMENT_EN_DOUBLE"


def test_une_ligne_deja_repartie_ne_se_repartit_pas_deux_fois(db):
    f, lid = _ligne_multi(db)
    flm.repartir_ligne(lid, [{"logement_id": "LOG_0001", "quantite": 2},
                             {"logement_id": "LOG_0002", "quantite": 2}],
                       motif="moitié-moitié", db_path=db)
    res = flm.repartir_ligne(lid, [{"logement_id": "LOG_0001", "quantite": 1},
                                   {"logement_id": "LOG_0003", "quantite": 3}],
                             motif="encore", db_path=db)
    assert res["ok"] is False and res["code"] == "LIGNE_NON_ACTIVE"


# ── §23 : affecter le logement et apprendre du geste ────────────────────────────────────────────

def test_affecter_un_logement_enregistre_la_correspondance(db):
    from app.services import logement_matching_service as lms
    f = _facture(db, 50.0, "F-A1")
    lid = flm.ajouter_ligne(f, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE, montant_ttc=50.0,
                            description="Le studio de Didier, 2e étage",
                            db_path=db)["ligne_id_opaque"]
    assert lms.proposer_pour_libelle("Le studio de Didier, 2e étage",
                                     db_path=db)["logement_id"] == ""

    res = flm.affecter_logement(lid, logement_id="LOG_0001", acteur="test", db_path=db)
    assert res["ok"] is True
    assert res["correspondance_apprise"]["ok"] is True

    apres = lms.proposer_pour_libelle("Le studio de Didier, 2e étage", db_path=db)
    assert (apres["logement_id"], apres["confiance"]) == ("LOG_0001", lms.CERTAIN)


def test_affecter_un_logement_inconnu_refuse(db):
    f = _facture(db, 50.0, "F-A2")
    lid = flm.ajouter_ligne(f, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE, montant_ttc=50.0,
                            description="x", db_path=db)["ligne_id_opaque"]
    assert flm.affecter_logement(lid, logement_id="LOG_9999", db_path=db)["code"] == \
        "LOGEMENT_INCONNU"
