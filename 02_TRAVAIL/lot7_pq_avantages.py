"""Lot7 — M-code Power Query DOCUMENTAIRE (preuve complémentaire optionnelle).

IMPORTANT : le pipeline RÉEL de Lot7 est le générateur Python `lot7_generateur_avantages.py`
(Option A). Ce module ne s'exécute pas et n'est PAS nécessaire au pipeline principal : il porte le
M-code documentaire, cohérent avec le générateur Python, utilisé uniquement comme preuve
complémentaire OPTIONNELLE via Excel COM (tests/test_pq_avantage_lot7_reel.py, skip si Excel
indisponible). Le classeur Lot7 ne contient aucun Power Query vivant ; POWER_QUERY_CODE est documentaire.

Correction majeure vs l'existant :
- L'ancien Q5 mettait `avantage_brut_depenses_perso` à 0.0 (Q4a défini mais JAMAIS joint).
- Le nouveau branchement lit SAISIE_Charges_Flux (Lot3) et attribue l'avantage ainsi :
  * avantage_associe_id renseigné → totalité du montant de la charge attribuée à cet associé,
    quel que soit le moyen de paiement (PAY_001 banque pro inclus) ;
  * sinon → règle historique TYPE_FLUX_002 par associe_id (traitement inchangé) ;
  * jamais les deux voies pour une même charge (Hist exclut les charges à flag explicite) ;
  * une même charge_id n'est jamais comptée deux fois ; l'actualisation est idempotente.
Les traitements TYPE_FLUX_004 / TYPE_FLUX_008 (charges_payees_pour_societe) restent inchangés.
JAMAIS d'écriture dans SOURCE_SAISIE pour une charge déjà présente en Lot3.
"""

# Bloc « avantage brut depenses_perso » (à joindre dans Q5, remplace le placeholder 0.0).
# Path3 = MASTER_FACT_MAN_Charges.xlsx (Lot3). Attribution déterministe par bénéficiaire.
MCODE_DEPENSES_PERSO_LOT3 = (
    'let\n'
    '    Path3 = "..\\Lot3_Charges\\MASTER_FACT_MAN_Charges.xlsx",\n'
    '    WB3 = Excel.Workbook(File.Contents(Path3), null, true),\n'
    '    MASTER3 = WB3{[Name="MASTER"]}[Data],\n'
    '    // Voie 1 : avantage explicite porté par la charge (avantage_associe_id renseigné)\n'
    '    Explicit = Table.SelectRows(MASTER3, each ([avantage_associe_id] <> null) and ([avantage_associe_id] <> "")),\n'
    '    AggExplicit = Table.Group(Explicit, {"mois","avantage_associe_id"}, {{"avantage_brut_depenses_perso", each List.Sum([montant]), type number}}),\n'
    '    RenExplicit = Table.RenameColumns(AggExplicit, {{"avantage_associe_id","associe_id"}}),\n'
    '    // Voie 2 : règle historique TYPE_FLUX_002 par associe_id, EXCLUANT les charges à flag explicite\n'
    '    Hist = Table.SelectRows(MASTER3, each ([type_flux_id] = "TYPE_FLUX_002") and (([avantage_associe_id] = null) or ([avantage_associe_id] = ""))),\n'
    '    AggHist = Table.Group(Hist, {"mois","associe_id"}, {{"avantage_brut_depenses_perso", each List.Sum([montant]), type number}}),\n'
    '    // Combinaison sans double comptage (une charge = une seule voie)\n'
    '    Combined = Table.Combine({RenExplicit, AggHist}),\n'
    '    AggDP = Table.Group(Combined, {"mois","associe_id"}, {{"avantage_brut_depenses_perso", each List.Sum([avantage_brut_depenses_perso]), type number}})\n'
    'in\n'
    '    AggDP\n'
)

# Version auto-contenue (table CHARGES du classeur) — utilisée pour la preuve réelle sur scratch.
MCODE_AVANTAGES_CHARGES = (
    'let\n'
    '    Source = Excel.CurrentWorkbook(){[Name="CHARGES"]}[Content],\n'
    '    Typed = Table.TransformColumnTypes(Source, {{"montant", type number}}),\n'
    '    Explicit = Table.SelectRows(Typed, each ([avantage_associe_id] <> null) and ([avantage_associe_id] <> "")),\n'
    '    AggExplicit = Table.Group(Explicit, {"mois","avantage_associe_id"}, {{"avantage_brut", each List.Sum([montant]), type number}}),\n'
    '    RenExplicit = Table.RenameColumns(AggExplicit, {{"avantage_associe_id","associe_id"}}),\n'
    '    Hist = Table.SelectRows(Typed, each ([type_flux_id] = "TYPE_FLUX_002") and (([avantage_associe_id] = null) or ([avantage_associe_id] = ""))),\n'
    '    AggHist = Table.Group(Hist, {"mois","associe_id"}, {{"avantage_brut", each List.Sum([montant]), type number}}),\n'
    '    Combined = Table.Combine({RenExplicit, AggHist}),\n'
    '    Final = Table.Group(Combined, {"associe_id","mois"}, {{"avantage_brut", each List.Sum([avantage_brut]), type number}})\n'
    'in\n'
    '    Final\n'
)
