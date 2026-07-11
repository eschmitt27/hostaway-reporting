# RECETTE AUTONOMIE — Pilotage_Conciergerie — 2026-06-19

## 1. État Git initial
HEAD 8bdd39b ; branche master -> origin/pilotage-conciergerie ; working tree propre (seul 04_LOGS/ non suivi) ; origin/main NON touché.

## 2. Environnement de recette
git worktree détaché HEAD à C:/Users/Ewan/_recette_pilotage_20260619 (HORS OneDrive).
- Lus (tracked) : scripts lot*, REF_Setup.xlsm, sorties MASTER_* committées, M04, VRBO csv, Lot1_Hostaway/*, REF.
- Ignorés ABSENTS (= prérequis opérateur) : .env, 01_SOURCES_BRUTES/Banque, 02_TRAVAIL/Lot8_Banque, 04_LOGS, 03_EXPORTS/PowerBI.
- Copié localement pour test lot9 : BANQUE_LOT8_IMPORT.xlsx (jamais commité/exporté).
- Suppression : git worktree remove (aucun impact prod).

## 3. Prérequis opérationnels
| Étape | Fichier/accès | Obligatoire | Source | Sensibilité | Si absent | Script |
|---|---|---|---|---|---|---|
| Extraction Hostaway | API + .env (token) | OUI | auto | secret | BLOQUE | lot1_hostaway_extract |
| Référentiel | REF_Setup.xlsm | OUI | humain | normal | BLOQUE | tous |
| Réservations HH | SAISIE_ReservationsHorsHostaway | optionnel | humain | normal | 0 HH | lot4 |
| Charges | SAISIE_Charges_Flux | optionnel | humain | normal | pools 0 | lot3/lot9 |
| Ménages internes | Google Sheet (REF SRC_011) | OUI ménage | auto réseau | normal | BLOQUE lot6b | lot6b |
| Factures ménage ext | PDF MenagesExternes (ignoré) | optionnel | humain | normal | extract figée | lot6c |
| Banque | export CM + BANQUE_LOT8_IMPORT | OUI flux banque | humain/auto | sensible | BLOQUE lot9 (CTR-9-001) | lot8a/b/c |
| Acomptes | SAISIE_Acomptes | optionnel | humain | normal | 0 | lot5 |
| IK/avantages | SAISIE IK | optionnel | humain | normal | 0 | lot7 |
| Tâches Hostaway | CleaningTasks Discovery | OUI ménage | auto | normal | rappro partiel | lot6a |
| Préfactures | sorties lot10/11 | auto | auto | normal | — | lot12 |
| Exports PBI | sorties MASTER_* | auto | auto | normal | — | lot13 |

## 4. Chaîne d'exécution réelle (vérifiée)
lot1(API) / lot8(banque) / lot6c(PDF) / lot3(saisie) = EXTRACTION (prérequis opérateur, non rejouables en isolation).
TRANSFORMATION rejouée en recette (autonome) :
lot4bis -> lot4ter -> lot4quater -> lot6b -> lot6d -> lot6e -> lot6f -> lot9 -> lot10 -> lot11 -> lot12 -> lot13.

## 5. Résultats run 1 : 12/12 scripts OK, zéro intervention manuelle.

## 6. Idempotence (run 2, sources inchangées)
Totaux identiques, row counts stables (Flux 1393, Resolues 1391, HIST 1269, préfactures 270, A_CONTROLER 7, 12 CSV) -> aucun doublon.
INCIDENT : lot6b + lot6f ont échoué au run 2 (blip réseau fetch Google Sheet curl). Abort propre (pas d'invention), relance OK. -> dépendance réseau = fragilité.

## 7. Cohérence vs baseline
REEL 291779.67 = COMPTABLE 281255.51 + HORS_COMPTA 10524.16 (OK) ; 0 BLOQUANT / 7 A_CONTROLER ; 270 préfactures / 0 finale. == baseline. (commission/net/charge fixe inchangés vu identité globale + 0 diff.)

## 8. Sécurité/confidentialité
.env, banque brute, BANQUE_LOT8_IMPORT, 04_LOGS, 03_EXPORTS/PowerBI/*.csv : tous ignorés (check-ignore OK). Aucun fichier suivi sensible (lot8*.py = code sans secret). Exports PBI = whitelist (sans email/tel/IBAN/voyageur).

## 9. Documentation opérateur — DÉFAUT
Présents : CLAUDE.md, PLAN_CONSTRUCTION, README_PROJET, run_menages_pipeline.py.
MANQUE : runbook opérateur unique (séquence complète des commandes lot1->lot13, quoi remplir/déposer, quoi faire si A_CONTROLER, comment générer préfactures + exports, quoi ne jamais versionner). Info dispersée -> dépend d'une connaissance implicite.

## 10. Défauts trouvés
- DEF-1 (MINEUR) : dépendance réseau Google Sheet (lot6b/6f) sans retry/cache -> échec transitoire. Mitigation : retry + fallback CSV local.
- DEF-2 (MINEUR) : pas de runbook opérateur global.
- DEF-3 (INFO) : extraction (lot1 API, lot8 banque, lot6c PDF) non rejouable sans prérequis -> normal mais à documenter comme prérequis.
- 7 A_CONTROLER métier (attendus, identifiés, non bloquants).

## 11. VERDICT : AUTONOME_AVEC_RESERVES
Le projet PEUT être utilisé sans intervention IA pour la couche transformation -> reporting (déterministe, idempotent, blocages explicites, aucune invention), à condition que les prérequis soient présents :
- accès API Hostaway (.env) + extraction lot1 ;
- export banque + lot8 ;
- Google Sheet ménages accessible (réseau) ;
- REF_Setup à jour ;
- factures PDF ménage externe si applicable.
Réserves : ajouter retry/cache sur le fetch Google Sheet ; rédiger un runbook opérateur.
