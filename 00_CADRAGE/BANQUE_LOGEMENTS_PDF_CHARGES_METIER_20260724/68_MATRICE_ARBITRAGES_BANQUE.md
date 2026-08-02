# 68 — Matrice d'arbitrages Banque (541 mouvements, cycle Lot8a→8b→8c)

Aucune confirmation automatique effectuée. Toutes les décisions ci-dessous restent à trancher par
l'utilisateur, dans l'application (`/banques-caisse`), jamais par ce document.

## Synthèse des 517 mouvements `A_CONTROLER`

Répartition par motif dominant (classification Lot8b, règles seed génériques — pas encore
arbitrées finement) :

| Motif | Nombre | Montant absolu (indicatif) | Période | Action recommandée |
|---|---:|---:|---|---|
| `RAPPROCHEMENT_REQUIS` (statut classification) | 222 | non totalisé ici (cf. export CSV local) | 03/11/2025→01/08/2026 | Revoir un par un via `/banques-caisse`, prioriser les montants les plus élevés |
| `A_ENVOYER_IA` (catch-all, aucune règle déterministe) | 83 | idem | idem | Étudier pour créer de nouvelles règles déterministes dans `REF_Banque_Regles`, plutôt que de dépendre durablement de l'IA |
| `CLASSE` mais niveau de risque `ELEVE` | 74 (sous-ensemble des 236 `CLASSE`) | idem | idem | Revue prioritaire même si déjà classées |
| Niveau de risque `MOYEN` | 215 | idem | idem | Revue secondaire |
| Niveau de risque `FAIBLE` | 252 | idem | idem | Revue de routine, peut être groupée |
| Doublon probable (empreinte SHA256 partagée) | 1 | — | — | Vérifier manuellement s'il s'agit d'un vrai doublon ou d'une transaction légitimement répétée |
| Période incohérente (nom de fichier vs contenu) | 1 signalement global | — | 9 mois dans un fichier nommé mono-mois | Informationnel — le fichier est un consolidé assumé, pas une anomalie de données |

Le total exact par motif nécessite un export local (`/banques-caisse/export.csv`) — non recopié
ici pour éviter de dupliquer des libellés bancaires dans la documentation Git.

## 166 propositions de rapprochement Airbnb (Lot8c)

| Élément | Valeur agrégée |
|---|---|
| Nombre proposé | 166 |
| Statut | `EN_ATTENTE_EXPORT_AIRBNB` (aucun confirmé) |
| Total bancaire Airbnb (informatif) | 14 467,27 € |
| Ajustement 4,97 € détecté | 0 ligne |

Pour chaque proposition, l'écran `/banques-caisse/mouvements/{id}` affiche : mouvement opaque,
objet proposé (réservation/versement plateforme), montant mouvement, montant objet, écart, date,
règle appliquée (`R_001` notamment), score/justification, doublon potentiel — **décision humaine
requise pour chacune** (`CONFIRMER`/`REFUSER`/`REPORTER`/`A_CONTROLER`), aucune n'est exécutée
automatiquement par ce document ni par l'application.

## 56 propositions de rapprochement propriétaires (Lot8c)

| Élément | Valeur agrégée |
|---|---|
| Nombre proposé | 56 |
| Statut | `EN_ATTENTE_SAISIE_ACOMPTE` (aucun confirmé) |
| Total virements propriétaires | 27 069,18 € |
| Prérequis manquant signalé | `LOT5_PREREQUIS_MANQUANT` (56 virements en attente — Lot5 Acomptes Propriétaires non encore intégré à ce cycle) |

## Rapport agrégé (confiance / écart)

| Type | Proposés | Confiance haute | Moyenne | Faible | Écart > 0,01 € |
|---|---:|---:|---:|---:|---:|
| Airbnb | 166 | non calculé par Lot8c (score qualitatif par ligne, pas un indice numérique agrégé) | — | — | à vérifier ligne par ligne dans l'écran dédié |
| Propriétaires | 56 | idem | — | — | idem |

Lot8c ne produit pas aujourd'hui un score de confiance numérique agrégé exportable tel quel — il
produit une liste priorisée par ligne (déjà consultable dans `/banques-caisse`). Construire un tel
agrégat serait une nouvelle fonctionnalité, hors mandat de cette mission (« ne commence aucune
fonctionnalité métier différée sans décision explicite »).

## Décisions humaines requises avant mode réel (Banque)

1. Arbitrer les règles de classification déterministes (`REF_Banque_Regles`) pour réduire le volume
   `A_ENVOYER_IA`/`A_CONTROLER`.
2. Décider du traitement des 166 propositions Airbnb et 56 propositions propriétaires (confirmer,
   refuser ou reporter, mouvement par mouvement — jamais en masse sans revue).
3. Décider si le prérequis Lot5 (Acomptes Propriétaires) doit être intégré avant d'activer les
   rapprochements propriétaires en réel.
4. Vérifier le doublon détecté (1 mouvement) — confirmer s'il s'agit d'une vraie duplication ou
   d'une transaction légitimement répétée.

Aucune de ces décisions n'a été prise par cette mission. Aucun rapprochement n'a été confirmé.
