# 69 — Guide de recette utilisateur (parcours court)

Parcours pas-à-pas reproductible par l'utilisateur, sur l'environnement de copies. Ne duplique pas
la documentation technique (`54`-`68`) — renvoie vers elle pour le détail.

| # | Écran | Objectif | Action humaine | Résultat attendu | Anomalie acceptable | Anomalie bloquante |
|---|---|---|---|---|---|---|
| 1 | Accueil `/` | Confirmer le mode recette actif | Consulter le bandeau | « MODE RECETTE » affiché, environnement nommé (jamais un chemin complet depuis le fix `c65f891`) | — | bandeau absent en recette |
| 2 | `/logements` | Vérifier le parc | Parcourir 2-3 fiches | Historique et statut cohérents | — | 404 sur un logement existant |
| 3 | `/proprietaires` | Vérifier propriétaires | Ouvrir une fiche | Résultat conciergerie ≠ net propriétaire, jamais additionnés | — | confusion des deux montants |
| 4 | `/reservations` | Consulter réservations | Filtrer par mois | Statuts variés (VALIDE/A_CONTROLER) | des lignes `A_CONTROLER` | zéro fabriqué au lieu d'absence |
| 5 | `/menages/cycle` | Consulter cycle ménage | Ouvrir une fiche | Statut PREVU→REGLE cohérent | — | 404 sur un ménage existant |
| 6 | `/resultats/categories` | Consulter charges | Ouvrir une catégorie | Détail des charges de la catégorie | `NON_DISPONIBLE` si saisie vide | tableau à zéro fabriqué |
| 7 | `/factures` | Consulter factures fournisseurs | Ouvrir une facture | Statut, ventilation, historique | — | 404 sur une facture existante |
| 8 | `/reglements` | Consulter règlements | Ouvrir un règlement | Lien vers facture(s) réglée(s) | — | perte du lien facture↔règlement |
| 9 | `/banques-caisse` | Consulter mouvements bancaires | Filtrer par mois, ouvrir un mouvement | Compte masqué, classification affichée | statut `A_CONTROLER` majoritaire (attendu, cf. `68`) | compte en clair, RIB complet |
| 10 | fiche mouvement | Revoir une classification | Consulter proposition (règle, score) | Jamais confirmé automatiquement | — | confirmation automatique observée |
| 11 | rapprochements | Revoir propositions Airbnb/propriétaires | Décider (confirmer/refuser/reporter) | Décision journalisée, jamais silencieuse | file d'attente non vide (166+56) | confirmation sans action humaine |
| 12 | `/calculs` | Lancer le pipeline aval | Prévisualiser puis lancer, mois au choix | 6/6 lots SUCCÈS | — | faux succès (sortie manquante déclarée SUCCES) |
| 13 | `/comptabilite` | Consulter écritures | Ouvrir une écriture | Origine tracée (facture/règlement) | 0 écriture réelle (attendu, jamais exercé en réel) | 404 sur écriture existante |
| 14 | `/resultats` | Consulter résultats | Vérifier REEL=COMPTABLE+HC | Écart affiché = 0,00 € | — | écart non nul non expliqué |
| 15 | `/resultats/reconciliation` | Consulter les 8 réconciliations | Comparer par mois | A/B/D/H `OK`, C/E/F/G attendues `A_CONTROLER`/`NON_DISPONIBLE` | statuts attendus ci-dessus | statut `BLOQUANT` inexpliqué |
| 16 | `/comptabilite/controles` puis `/clotures-mensuelles` | Consulter contrôles et clôture | Vérifier le statut de période | Période refuse toute écriture directe si clôturée | volume élevé de contrôles A_CONTROLER (cf. `68`) | écriture acceptée sur période clôturée |
| 17 | exports (`/resultats/export.csv`, Power BI) | Vérifier les exports | Télécharger un export | 0 PII, opaque, cohérent avec l'écran | — | PII ou chemin absolu dans l'export |

## Ce que l'utilisateur doit décider (jamais Claude)

- Les arbitrages Banque (`68_MATRICE_ARBITRAGES_BANQUE.md`).
- Les arbitrages comptables (`70_MATRICE_ARBITRAGES_COMPTABLES.md`).
- La fiche de signature (`71_...` — à venir, `CHECKLIST_GO_NO_GO_MODE_REEL.md` porte les domaines).

## Rappel

Ce parcours se fait uniquement sur l'environnement de copies. Aucune donnée réelle n'est modifiée
à aucune étape. Le mode réel n'est jamais activé par ce guide.
