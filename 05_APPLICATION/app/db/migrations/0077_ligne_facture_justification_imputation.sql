-- Migration 0077 — justification d'une refacturation PARTIELLE, portée par la ligne de facture.
--
-- LE DÉFAUT QU'ELLE CORRIGE
-- `charges_refacturation_service.imputer()` exige une justification dès que le montant imputé
-- diffère du solde de la position. La règle est juste : ne récupérer qu'une partie d'une dépense
-- est une décision, pas une saisie approximative. Mais elle n'était contrôlée qu'à la VALIDATION,
-- où plus personne ne pouvait fournir ce motif — l'interface ne le demandait nulle part et
-- `valider()` ne recevait aucun `decisions_charges`. Conséquence : une facture portant une
-- refacturation partielle se composait normalement puis refusait indéfiniment de se valider
-- (« Justification obligatoire : montant impute different du solde propose »). Découvert par le
-- banc d'essai de l'arborescence des charges, sur le scénario 500/200.
--
-- POURQUOI UNE COLONNE ET NON UN COMMENTAIRE DÉTOURNÉ
-- `ajouter_ligne(commentaire=…)` n'écrit pas sur la ligne : il alimente le journal d'événements.
-- Ranger la justification dans un champ libre existant l'aurait rendue indistinguable d'une note
-- quelconque, et `valider()` aurait pu prendre n'importe quel commentaire pour une justification
-- d'imputation. Une colonne dédiée dit ce qu'elle contient.
--
-- La colonne reste NULL pour l'immense majorité des lignes : seules les refacturations partielles
-- la renseignent. Son absence n'est donc pas une anomalie, c'est le cas normal.

ALTER TABLE factures_proprietaires_lignes ADD COLUMN justification_imputation TEXT;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0077');
