# RUNBOOK OPÉRATEUR — Pilotage_Conciergerie

> Mode d'emploi pour faire tourner le projet **sans IA**. Le système calcule par règles
> déterministes : il ne devine jamais. Une donnée ambiguë/incomplète produit un contrôle
> `A_CONTROLER` ou un **blocage explicite** — jamais un montant inventé.

---

## 1. Prérequis avant lancement
| Élément | Nécessaire pour | Fourni par |
|---|---|---|
| `.env` (token API Hostaway) | lot1 extraction | opérateur (jamais versionné) |
| Export banque Crédit Mutuel | lot8 banque | opérateur |
| Google Sheet « Suivi ménage » accessible (réseau) | lot6b/lot6f | auto (URL dans REF_Setup SRC_011) |
| `REF_Setup.xlsm` à jour | tous | opérateur |
| Factures PDF ménage externe | lot6c | opérateur (si applicable) |
| Python + `pip install pandas openpyxl requests python-dotenv` | tout | poste opérateur |

## 2. Fichiers humains à remplir
- `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm` — référentiels (logements, propriétaires, taux, mapping, clôture…).
- `01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx` — réservations hors Hostaway.
- `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx` — charges/courses (+ `affectable_menage`, `intervenant_concerne`).
- Google Sheet « Suivi ménage » (en ligne) — déclarations ménages internes.

## 3. Où déposer
- Banque brute → `01_SOURCES_BRUTES/Banque/`
- Factures ménage externe → `01_SOURCES_BRUTES/MenagesExternes/Factures_PDF/`
- Saisies → chemins ci-dessus (§2). Ne pas renommer les fichiers.

## 4. Fichiers à NE JAMAIS modifier ni versionner
- Sorties générées `MASTER_*.xlsx` (recalculées par les scripts).
- `.env`, `01_SOURCES_BRUTES/Banque/`, `02_TRAVAIL/Lot8_Banque/`, `04_LOGS/`,
  `03_EXPORTS/PowerBI/*.csv`, `02_DONNEES_NORMALISEES/menages/_cache_google_sheet/`.
- **Ne jamais corriger une sortie à la main** : corriger la SOURCE ou le RÉFÉRENTIEL, puis relancer.

## 5. Ordre réel des opérations
```
lot1_hostaway_extract            (API Hostaway, .env requis)
lot6a_cleaning_tasks_comptage    (tâches ménage Hostaway)
lot8a/8b/8c                      (banque : import, règles, rapprochement)
lot4bis_charger_reservations     (table commune live)
lot4ter_historiser_...           (historise mois CLOTURE)
lot4quater_resoudre_source_...   (résout open/closed)
lot6b/lot6d/lot6e/lot6f          (ménages : run_menages_pipeline.py)
lot3 / lot5 / lot7               (charges / acomptes / IK — si saisies)
lot9_construire_flux
lot10_calculer_resultats
lot11_controles_coherence
lot12_generer_factures
lot13_export_powerbi
```

## 6. Commandes exactes
```bash
python 02_TRAVAIL/lot1_hostaway_extract.py
python 02_TRAVAIL/lot8a_banque_import.py && python 02_TRAVAIL/lot8b_banque_regles.py && python 02_TRAVAIL/lot8c_rapprochement_banque.py
python 02_TRAVAIL/lot4bis_charger_reservations.py
python 02_TRAVAIL/lot4ter_historiser_reservations_cloturees.py
python 02_TRAVAIL/lot4quater_resoudre_source_reservations.py
python 02_TRAVAIL/run_menages_pipeline.py      # lot6b -> 6d -> 6e -> 6f (ordre garanti)
python 02_TRAVAIL/lot9_construire_flux.py
python 02_TRAVAIL/lot10_calculer_resultats.py
python 02_TRAVAIL/lot11_controles_coherence.py
python 02_TRAVAIL/lot12_generer_factures.py
python 02_TRAVAIL/lot13_export_powerbi.py
```

## 7. Mois ouvert / futur / clôturé
- **Ouvert / futur** : données vivantes (API + saisies). Peuvent évoluer.
- **Clôturé** : `REF_Setup.xlsm > REF_Cloture_Mensuelle.statut_mois = CLOTURE`. Source figée = HIST_Reservations_Cloturees.
- **Ne jamais marquer CLOTURE un mois non réellement validé** (pas de clôture artificielle pour faire tourner un calcul).

## 8. Générer résultats / contrôles / préfactures / exports
- Résultats/commissions/net : `lot10`. Contrôles : `lot11`. Préfactures : `lot12`. Exports Power BI : `lot13` → `03_EXPORTS/PowerBI/*.csv`.

## 9. Si une ligne est `A_CONTROLER`
1. Lire `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` (onglet A_CONTROLER_OUVERTS).
2. Corriger la **source/référentiel** (jamais la sortie).
3. Relancer la chaîne. L'écart doit disparaître ou être justifié.

## 10. Si un script échoue
- Lire le message `[BLOQUANT ...]` en terminal. Corriger le prérequis indiqué. Relancer (les scripts sont **idempotents** : relancer ne crée pas de doublon).

## 11. Source indisponible
- **Google Sheet ménages KO** : lot6b/lot6f réessaient (3×), puis basculent sur le **cache local ≤ 72h** → contrôle `SOURCE_SHEET_CACHE_UTILISE` (A_CONTROLER). Cache > 72h ou absent → **BLOQUANT `SOURCE_SHEET_INDISPONIBLE`** (aucune sortie ménage). **Règle : ne pas clôturer un mois tant que `SOURCE_SHEET_CACHE_UTILISE` est ouvert** — rafraîchir la Sheet puis relancer.
- **API Hostaway KO** : relancer lot1 plus tard. **Banque KO** : fournir l'export, relancer lot8.

## 12. Vérifier la cohérence
- `REEL = COMPTABLE + HORS_COMPTA` (lot10 CTR-LOT10-20).
- 0 BLOQUANT en lot11. Totaux stables entre deux relances (idempotence).

## 13. Distinguer les statuts
- **Blocage technique** (`SOURCE_SHEET_INDISPONIBLE`, `JOINTURE_*_MANQUANTE`) : prérequis absent → corriger + relancer.
- **`A_CONTROLER`** : écart métier à expliquer/corriger en source → puis relancer.
- **Correction de source** : la seule façon de résoudre ; jamais éditer une sortie générée.

## 14. Règles Git
- Brancher : **`master`** → upstream **`origin/pilotage-conciergerie`**. Pousser : `git push`.
- **JAMAIS** pousser sur **`origin/main`** (= pipeline d'extraction Hostaway, GitHub Action, autre projet).
- Ne jamais committer : `.env`, banque brute, `04_LOGS/`, exports CSV, cache Google Sheet.

---
*Procédure fin de mois (future) : valider les contrôles (0 BLOQUANT, A_CONTROLER traités/justifiés, pas de `SOURCE_SHEET_CACHE_UTILISE` ouvert), puis marquer le mois `CLOTURE` dans REF_Cloture_Mensuelle, puis relancer la chaîne.*
