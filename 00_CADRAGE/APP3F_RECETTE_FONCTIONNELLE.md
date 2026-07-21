# Recette fonctionnelle APP-3F — 20 scénarios

Généré le 2026-07-21T17:55:31 — base isolée `C:\Users\Ewan\AppData\Local\Temp\recette_app3f_w21vkl7m\recette.db`.
**Résultat : 20/20 OK.**

| N° | Scénario | Données | Action | Attendu | Obtenu | Statut | Preuve |
| -: | -------- | ------- | ------ | ------- | ------ | ------ | ------ |
| 1 | candidat exact | 1 mouvement -120 le 05/01 | chercher candidats | `1` | `1` | OK | ['sens_sortant', 'montant_exact', 'mois_compatible', 'date_dans_fenetre'] |
| 2 | aucun candidat | montant ne correspond pas | chercher | `0` | `0` | OK | 0 candidat |
| 3 | deux candidats | 2 mouvements -120 | chercher | `2` | `2` | OK | 2 candidats |
| 4 | mouvement entrant | crédit +120 | chercher | `0` | `0` | OK | entrant jamais proposé |
| 5 | montant différent | -130 vs 120 | chercher | `0` | `0` | OK | tolérance exacte |
| 6 | date éloignée | mouvement en mars | chercher fenêtre 7j | `True` | `True` | OK | date hors fenêtre signalée |
| 7 | sélection manuelle | candidat exact proposé | enregistrer proposition | `PROPOSITION_DISPONIBLE` | `PROPOSITION_DISPONIBLE` | OK | MVT-fb05c2af1e |
| 8 | confirmation humaine | à contrôler | confirmer | `RAPPROCHE` | `RAPPROCHE` | OK | CONFIRME |
| 9 | écartement | proposition disponible | écarter avec motif | `ECARTE` | `ECARTE` | OK | pas le bon mouvement |
| 10 | anomalie | non rapproché | signaler anomalie | `ANOMALIE` | `ANOMALIE` | OK | ambigu |
| 11 | réouverture | rapproché | rouvrir avec motif | `ROUVERT` | `ROUVERT` | OK | erreur détectée |
| 12 | mouvement disparu | mouvement proposé puis absent | recharger mouvement | `False` | `False` | OK | charger_mouvement -> None |
| 13 | source absente | fichier bancaire absent | chercher | `RAPPROCHEMENT_SOURCE_ABSENTE` | `RAPPROCHEMENT_SOURCE_ABSENTE` | OK | code source |
| 14 | source vide | onglet vide | chercher | `RAPPROCHEMENT_SOURCE_VIDE` | `RAPPROCHEMENT_SOURCE_VIDE` | OK | code source |
| 15 | export préparatoire | rapprochements en base | générer une cellule sûre | `True` | `True` | OK | amorce neutralisée |
| 16 | injection CSV | commentaire =CMD() | neutraliser | `'=CMD()` | `'=CMD()` | OK | quote de tête |
| 17 | double confirmation | déjà rapproché | confirmer à nouveau | `True` | `True` | OK | refusé |
| 18 | redémarrage | rapproché en base | relire | `RAPPROCHE` | `RAPPROCHE` | OK | persisté |
| 19 | concurrence | objet modifié entretemps | action version périmée | `True` | `True` | OK | refusé |
| 20 | intégrité fichier bancaire | modules APP-3F | chercher toute écriture fichier | `True` | `True` | OK | aucun open()/load_workbook |
