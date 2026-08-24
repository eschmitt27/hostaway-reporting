"""Mission 6 bis — répartition des charges communes de facture (remplace toute mention de
« groupes de logements », qui n'existe pas : voir DECISIONS_METIER.md D-REF-HIST-01).

Règle réellement trouvée dans le code (`charges_impact_service.py`) : une charge dont le périmètre
de logements est directement sélectionné (`logements_directs`) — ou dérivé des logements ACTIFS
d'un propriétaire sélectionné à la date de la charge (`compute_perimetre_logements`, déjà datée via
`gestion_active_pour_mois`, réutilisant `ref_gestion_logements_hist`) — est répartie également et
de façon déterministe entre ces logements (`repartir_egal`, centimes au premier logement trié).

Il n'existe PAS de groupe permanent : le périmètre vient de LA CHARGE (assimilable à une facture,
`factures.charge_id` étant unique), jamais d'un ensemble de logements mémorisé ailleurs.

Ces tests caractérisent (§27/§28 de la mission) des propriétés déjà garanties par l'architecture
existante (répartition calculée UNE FOIS à la création de la charge, jamais recalculée depuis la
formule courante — `lot3_generateur_charges.py` ne rappelle jamais `repartir_egal`, il relit les
quotes déjà persistées) : aucune modification de `repartir_egal`/`compute_perimetre_logements`
n'a été faite dans cette mission.
"""
import fixtures_referentiel as fx
from app.services import charges_impact_service as impact

GESTION = [
    {"logement_id": "LOG_A", "proprietaire_id": "PROP_1", "statut_gestion": "ACTIF",
     "date_debut": "2026-01-01", "date_fin": None},
    {"logement_id": "LOG_B", "proprietaire_id": "PROP_1", "statut_gestion": "ACTIF",
     "date_debut": "2026-01-01", "date_fin": None},
    {"logement_id": "LOG_C", "proprietaire_id": "PROP_1", "statut_gestion": "ACTIF",
     "date_debut": "2026-01-01", "date_fin": None},
    {"logement_id": "LOG_D", "proprietaire_id": "PROP_1", "statut_gestion": "ACTIF",
     "date_debut": "2026-01-01", "date_fin": None},
]


def test_facture_synthetique_A_B_C_charge_commune_repartie_sur_les_trois():
    """§27 — Facture F-100 : lignes directes A, B, C, un frais commun non attribuable. La règle
    V1 (repartir_egal) valable à la date de la facture répartit également entre A/B/C."""
    perimetre = impact.compute_perimetre_logements(
        logements_directs=["LOG_A", "LOG_B", "LOG_C"], proprietaires=[],
        mois="2026-06", gestion_rows=GESTION)
    assert perimetre["logements_finaux"] == ["LOG_A", "LOG_B", "LOG_C"]

    quotes = impact.repartir_egal(90.0, perimetre["logements_finaux"])
    assert impact.somme_quotes_parts(quotes) == 90.0
    assert {q["logement_id"]: q["quote_part"] for q in quotes} == {
        "LOG_A": 30.0, "LOG_B": 30.0, "LOG_C": 30.0}


def test_perimetre_dune_seconde_facture_najamais_les_logements_de_la_premiere():
    """§28 — Facture F-200 : A + D uniquement. La charge commune de F-200 ne doit jamais toucher
    B ou C (qui appartenaient au périmètre de F-100) : le périmètre vient de LA FACTURE, pas d'un
    groupe permanent partagé entre factures."""
    perimetre_f100 = impact.compute_perimetre_logements(
        logements_directs=["LOG_A", "LOG_B", "LOG_C"], proprietaires=[],
        mois="2026-06", gestion_rows=GESTION)
    perimetre_f200 = impact.compute_perimetre_logements(
        logements_directs=["LOG_A", "LOG_D"], proprietaires=[],
        mois="2026-06", gestion_rows=GESTION)

    assert perimetre_f200["logements_finaux"] == ["LOG_A", "LOG_D"]
    assert "LOG_B" not in perimetre_f200["logements_finaux"]
    assert "LOG_C" not in perimetre_f200["logements_finaux"]

    quotes_f200 = impact.repartir_egal(50.0, perimetre_f200["logements_finaux"])
    logements_crediteurs = {q["logement_id"] for q in quotes_f200}
    assert logements_crediteurs == {"LOG_A", "LOG_D"}
    assert "LOG_B" not in logements_crediteurs and "LOG_C" not in logements_crediteurs
    # Le périmètre de F-100 (calculé indépendamment) reste inchangé par le calcul de F-200.
    assert perimetre_f100["logements_finaux"] == ["LOG_A", "LOG_B", "LOG_C"]


def test_recalcul_dune_facture_historique_est_deterministe_et_inchange():
    """§27 (fin) — rejouer le même calcul (même périmètre, même montant) à un instant ultérieur
    rend exactement le même résultat : aucune notion d'horloge courante n'entre dans la fonction
    (pas d'appel à `datetime.now()` dans `repartir_egal`/`compute_perimetre_logements`)."""
    perimetre = impact.compute_perimetre_logements(
        logements_directs=["LOG_A", "LOG_B", "LOG_C"], proprietaires=[],
        mois="2026-06", gestion_rows=GESTION)
    premier_calcul = impact.repartir_egal(100.0, perimetre["logements_finaux"])
    second_calcul_plus_tard = impact.repartir_egal(100.0, perimetre["logements_finaux"])
    assert premier_calcul == second_calcul_plus_tard


def test_perimetre_proprietaire_reste_date_du_mois_de_la_charge():
    """La résolution du périmètre via un propriétaire (`logements_actifs_proprietaire`) est déjà
    datée par `gestion_active_pour_mois`, réutilisant `ref_gestion_logements_hist` — un logement
    retiré de la gestion après le mois de la charge reste dans le périmètre historique de cette
    charge, jamais recalculé sur la composition ACTUELLE du parc."""
    gestion_avec_retrait = GESTION + [
        {"logement_id": "LOG_A", "proprietaire_id": "PROP_1", "statut_gestion": "RETIRE",
         "date_debut": "2027-01-01", "date_fin": None},
    ]
    perimetre_juin_2026 = impact.compute_perimetre_logements(
        logements_directs=[], proprietaires=["PROP_1"], mois="2026-06",
        gestion_rows=gestion_avec_retrait)
    assert "LOG_A" in perimetre_juin_2026["logements_finaux"]
