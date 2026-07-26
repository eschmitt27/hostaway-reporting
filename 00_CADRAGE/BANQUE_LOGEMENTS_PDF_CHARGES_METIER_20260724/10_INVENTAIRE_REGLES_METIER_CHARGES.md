# 10 — Inventaire des règles métier des charges

Source : lecture du code du worktree (`app/services/charges_impact_service.py`,
`charges_preview_service.py`, `charges_affectations_service.py`, readers `saisie_charges_reader.py`)
+ tests `test_charges_*`. Chaque règle est reliée à une preuve (fichier/fonction/constante).

## R1 — Une charge reste UNE seule charge économique
Preuve : docstring `charges_impact_service` + `somme_quotes_parts()`. Les ventilations
(multi-logements, multi-propriétaires, ménage) sont des **quotes-parts analytiques** ; leur somme
égale toujours le montant réel. **Jamais** de seconde charge réelle/comptable. → pas de double
comptage.

## R2 — Code d'impact : IC / HC / HR
Preuve : `STANDARD_CODES_IMPACT = {IC, HC}` ; `PRISE_EN_COMPTA_BY_IMPACT = {IC:OUI, HC:NON}` ;
`build_effet_saisie` : `impacte_resultat_comptable = "Oui" si IC sinon "Non"`.
- **IC** = impacte résultat **réel ET comptable** (prise_en_compta = OUI).
- **HC** = impacte résultat réel, **hors comptabilité** (prise_en_compta = NON) → c'est le « hors
  comptabilité » du cahier des charges (réel mais pas comptable).
- **HR** = hors résultat, **exclu du formulaire standard** (parcours dédié). L'« informative sans
  impact » relève de HR ou d'une catégorie neutralisée, hors Nouvelle charge.

## R3 — Sens du flux (5 valeurs canoniques)
Preuve : `CANONICAL_SENS_FLUX = {DEPENSE, RECUPERATION, REMBOURSEMENT, REFACTURATION, NEUTRE}`.
`DEFAULT_SENS_FLUX = DEPENSE`. → supporte les scénarios remboursement / récupération / refacturation.

## R4 — Type d'affectation
Preuve : `CANONICAL_AFFECTATION = {LOGEMENT, PROPRIETAIRE, GLOBAL, NON_AFFECTABLE}`.
- Sans sélection → **global conciergerie** (`compute_perimetre_logements` → `global_conciergerie`).
- Un propriétaire → **élargit à ses logements actifs** (`logements_actifs_proprietaire`).
- Logements précis → périmètre restreint.

## R5 — Répartition égale, aucun centime perdu
Preuve : `repartir_egal(montant, logements)` + `somme_quotes_parts()` ; test
`test_charges_affectations`. La somme des quotes-parts = montant exact (gestion du reste au centime).

## R6 — Catégorie → comportement ménage + avantage
Preuve : `CATEGORY_CATALOG` (15 catégories). Chaque catégorie porte :
- `menage` ∈ {FORCE, CHOIX, INTERDIT} ; `avantage` ∈ {True, False}.
- **FORCE** (CHG_003 Blanchisserie, CHG_004 Achat ménage, CHG_027 Supplément ménage) : impact ménage
  forcé, **jamais refacturable**, alimente coût complet ménage + gain/perte.
- **CHOIX** (CHG_009 Déplacement, CHG_025 Repas, CHG_018 Achat divers, CHG_026 Prestation) : ménage
  Oui/Non proposé, **avantage associé** possible.
- **INTERDIT** (logiciels, frais bancaires, assurance, maintenance, charge générale, personnalisée).

## R7 — Catégories hors formulaire
Preuve : `CATEGORIES_HORS_FORMULAIRE_EXPLICITE = {CHG_016, CHG_023}`.
- **CHG_016 Forfait client** = ligne de **facturation propriétaire**, PAS une charge réelle.
- **CHG_023 Forfait cave** = charge **récurrente** (REF_Charges_Recurrentes) — c'est la « cave 50 € »
  ; gérée en récurrent, pas en saisie manuelle.

## R8 — Refacturable → réserve de facturation
Preuve : `build_reserve_refacturation()` → quotes-parts réservées, `montant_total_refacturable`.
Une charge ménage n'est **jamais** refacturable (`build_effet_saisie` : « non applicable »).
La réserve alimente la **préfacture** propriétaire (ligne à refacturer), sans créer de facture.

## R9 — Impact ménage : mode INTERVENANT vs LOGEMENT
Preuve : `MENAGE_MODE_INTERVENANT` / `MENAGE_MODE_LOGEMENT` ; `menage_perimetre()`. Le ménage
alimente coût complet + gain/perte, reste **analytique**, ne crée pas de 2ᵉ charge réelle. Interne
vs externe = comportements distincts (mode + source du coût).

## R10 — ASSOC_MODE + identifiant de charge
Preuve : `resolve_assoc_mode()` (REF_Assoc_Mode) ; `charge_id = CHG-{AAAA}-{MM}-{IMPACT}-{ASSOC_MODE}-{NNN}`.
ASSOC_MODE ∈ {BANQUE, GLOBAL, …}. Trace le mode de rattachement dans l'identifiant.

## R11 — Avantage associé
Preuve : `build_effet_saisie` (`avantage_associe`, `associe_id`) ; `avantage_possible(cat)`.
Pour les catégories CHOIX, une charge peut être fléchée comme **avantage d'un associé** (ex. repas).

## R12 — Validation (gardes V01→V10)
Preuve : `validate_charge()` — V06 code_impact obligatoire ∈ {IC,HC} ; V10 ASSOC_MODE résolvable ;
montant, dates, mois ouvert, catégorie active, etc. → refus propre avant toute écriture.

## R13 — Prise en compte différée (Excel = vérité)
Preuve : `charges_post_write_service.executer_post_ecriture` (subprocess moteur) régénère
MASTER_FACT_MAN_Charges (Lot3) + contrôles Lot11 **après** écriture SAISIE. Tant que ce recalcul
n'a pas tourné, une charge saisie n'impacte pas encore les résultats affichés.

## Champs déterminant chaque effet (synthèse)

| Question métier | Champ(s) déterminant(s) |
|---|---|
| Qui supporte la charge ? | affectation + refacturable + supporteur (conciergerie/propriétaire) |
| Qui a payé ? | mode de paiement (PAY_001..004) + sens_flux |
| À rembourser ? | sens_flux ∈ {REMBOURSEMENT, RECUPERATION} |
| Diminue le net propriétaire ? | refacturable=Oui OU affectation propriétaire |
| Diminue le résultat conciergerie ? | code_impact réel + supporteur conciergerie + non refacturable |
| Entre en comptabilité ? | code_impact = IC (sinon HC/HR = hors compta) |
| Entre en analytique/gestion ? | toujours (réel), sauf HR |
| Rattachée à un logement / répartie ? | affectation LOGEMENT + périmètre + repartir_egal |
| Concerne un ménage ? | catégorie FORCE, ou CHOIX+ménage=Oui |
| Apparaît en préfacture ? | refacturable=Oui → réserve de facturation |
| Affecte rapprochement bancaire ? | mode paiement BANQUE + ASSOC_MODE=BANQUE |
| Affecte tableau de bord / clôture ? | après recalcul (R13) |
