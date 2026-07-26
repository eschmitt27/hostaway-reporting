# 16 — Tests transactionnels

- **Écriture réussie** : chaque scénario (A-I) écrit la charge dans SAISIE_Charges_Flux + les
  affectations dans SAISIE_Charges_Impacts (transaction), puis Lot3 régénère le MASTER. Constaté :
  charge_id, montant, quotes-parts présents après confirmation.
- **Recalcul aval** : Lot3 OK + Lot11 OK pour les 6 scénarios (après ajout du Lot7 fictif).
- **Persistance** : les charges confirmées restent lues après relecture des fichiers (le MASTER est
  sur disque) ; sur la précédente instance 8016 (mission antérieure) la persistance après
  redémarrage a été prouvée pour le journal SQLite.
- **Intégrité sources réelles** : `git status` sur 01_SOURCES_BRUTES/02_TRAVAIL du worktree = vide
  après tous les scénarios → aucune écriture n'a fui hors data_recette (write-guard + flags recette).
- **Rollback / erreur en cours** : le service de transaction charges conserve le mécanisme
  sauvegarde/restauration (`_sauvegarder`/`_rollback`/`_restaurer_fichier`) couvert par
  `test_saisie_charges_transaction_service` (53/53). Un test dédié « erreur au milieu → aucun état
  partiel » sur data_recette reste à ajouter (bloc suivant).
