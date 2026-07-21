# APP-3F — Rapprochement déclaratif des règlements avec les mouvements bancaires

## Objet
Contrôler si un mouvement bancaire (lecture seule) semble correspondre à un règlement propriétaire
déclaré payé dans APP-3E. **Jamais** de paiement, virement, modification/écriture bancaire, API
bancaire, IBAN stocké, preuve bancaire certifiée.

## Architecture (ne duplique rien — cf. `APP3F_AUDIT_BANCAIRE.md`)
- `rapprochement_bancaire_reader.py` : contrat de lecture masqué délégant à `banques_reader`
  (jamais dupliqué, jamais écrit). Expose `MVT-opaque`, date, montant, sens, libellé masqué,
  référence normalisée, empreinte, état. Jamais IBAN/RIB/BIC/compte/libellé brut/chemin/id SQLite.
- `rapprochement_candidats_service.py` : candidats à critères **explicables** (sens sortant, montant
  exact, mois compatible, date dans la fenêtre, référence compatible) — jamais de score opaque,
  jamais de confirmation auto. Montant : tolérance **exacte** par défaut (constante documentée).
  Fenêtre de date : `FENETRE_JOURS_DEFAUT = 7` (constante explicite, configurable, jamais en template).
  18 codes de contrôle (INFO ne bloque jamais).
- `rapprochement_reglements_service.py` : machine à états + journal SQLite (migration 0014).

## Machine à états
`NON_RAPPROCHE → PROPOSITION_DISPONIBLE → A_CONTROLER → RAPPROCHE / ECARTE / ANOMALIE → ROUVERT /
ANNULE`. Confirmation HUMAINE uniquement, refusée si : règlement non payé, mouvement disparu,
mouvement entrant, double confirmation, version obsolète, transition rejouée. Index d'unicité : un
rapprochement actif par règlement ; un mouvement confirmé une seule fois. Motif obligatoire pour
écarter/anomalie/rouvrir/annuler. Aucune suppression physique ; historique append-only.

## Interface
Complète `/proprietaires-reglements` (aucun menu parallèle). `/a-payer` : colonnes statut de
rapprochement + nb candidats + lien « Contrôler le mouvement ». Fiche
`/proprietaires-reglements/{REG-opaque}/rapprochement` : règlement déclaré, candidat **masqué**
(opaque/date/montant/sens/libellé masqué/écarts/critères), contrôles, actions, historique.
Confirmation avec `confirm()` navigateur. Aucun bouton payer/virer. Export
`/a-payer/rapprochement-export.csv` (opaques + montants + écart + statut, mention de non-preuve,
anti-injection CSV).

## Sécurité (prouvée par tests)
Aucune donnée bancaire sensible stockée/affichée/exportée ; aucun appel réseau/API bancaire/SEPA/
bénéficiaire/virement ; aucune écriture fichier ; `BANQUE_REAL_WRITE_ENABLED=False` ; autoescape ;
CSV neutralisé ; identifiants opaques ; version optimiste + rollback. Mention partout :
« Rapprochement déclaratif interne — ne constitue ni un ordre de paiement ni une preuve bancaire
certifiée. »

## Clarification du rapport APP-3E (Mission 0)
« 0 mention bancaire visible » (recette Playwright APP-3E) = aucune **donnée bancaire sensible**
visible (aucun IBAN, RIB, numéro de compte, bouton de virement). Les **mentions de sécurité**
restent au contraire visibles et sont vérifiées présentes : « Déclaration humaine interne — ne
constitue pas une confirmation bancaire. » et « Fichier préparatoire interne — ne constitue pas un
ordre bancaire. » (tests `test_securite_app3e`). Aucune réécriture de l'historique APP-3E.

## Livrables
Migration 0014 ; 3 services/reader ; interface (routes + `rapprochement_fiche.html`) ; export ;
matrice 63 cas (`APP3F_MATRICE_63_CAS.md`) ; recette fonctionnelle 20/20 (`APP3F_RECETTE_FONCTIONNELLE.md`)
+ script reproductible ; recette Playwright ; campagne complète ; checkpoint formel. Voir le rapport
final de session.
