# APP-5C — Clôtures mensuelles : suivi humain (PRÊT POUR INTÉGRATION)

**Statut : `APP5C_PRET_POUR_INTEGRATION`**

Suivi humain de la préparation/validation d'un mois avant clôture. **La clôture RÉELLE reste
exclusivement `REF_Cloture_Mensuelle` (REF_Setup.xlsm, moteur, D024)** — jamais écrite ici. Ce module
ne fait que journaliser la préparation, la revue, la décision humaine et l'historique, dans une base
SQLite isolée (`clotures_mensuelles`, `cloture_evenements`, `cloture_elements`, `cloture_documents`,
migration `0008_clotures.sql`).

## Écrans

- `/clotures` — liste filtrable (statut, année, bloqueurs).
- `/clotures/{CLO-opaque}` — fiche : état, progression, contrôles (snapshot figé une fois en
  A_VALIDER, bannière si dérive vs état courant), preuves.
- `/clotures/{CLO-opaque}/preparation`, `/validation` (résumé recalculé, confirmation JS avant
  validation), `/historique` (append-only), `/reouvrir` (justification requise, confirmation JS).
- Export CSV sécurisé (injection formule neutralisée, disclaimer explicite).

## Machine à états

`NON_DEMARREE → EN_PREPARATION → A_VALIDER → VALIDEE → ROUVERTE`. Garde de version optimiste
atomique au niveau SQL (`WHERE version=?` + `rowcount`), transaction unique pour transition+snapshot,
mois validé au format strict `AAAA-MM` (normalisé après validation — corrige un bug réel de crash sur
mois avec espace).

## Preuves (checkpoint `APP5CD_FINAL_INTEGRABLE_20260720_141530`)

Campagne complète : 1344 collectés = 1344 exécutés, 0 échec, 0 erreur, 65 skips justifiés. Migrations
12/12 scénarios. Recette visuelle 28/28 captures (4 largeurs × 7 écrans) à 0px de débordement
horizontal. Restauration N/N 124/124. Réel intact, flags `False`.

## Limites connues

Tests de charge concurrente réelle multi-thread non exécutés (garde SQL prouvée par construction +
test séquentiel). 5 tests de recette réelle (comptages exacts sur données bancaires réelles) sautent
proprement en l'absence de `BANQUE_LOT8_IMPORT.xlsx` dans l'environnement courant — non falsifiables
sans données réelles.
