# 29 — Module Logements : couche HTTP, fiche centrale, recette navigateur

Suite de `28_CYCLE_DE_VIE_LOGEMENT.md` (service testé mais pas exposé). Ce tour construit toute la
couche applicative manquante : routes, templates, formulaires, navigation, protections, et une
vraie recette navigateur du parcours complet.

## Ce qui a été construit

### Routes (`app/routes/logements.py`)
| Route | Effet |
|---|---|
| `GET /logements/nouveau` | Formulaire de création (référentiels chargés via `logements_creation_service.referentiels()`) |
| `POST /logements` | Création (`logements_creation_service.creer()`) |
| `GET /logements/{id}` | Fiche — voir ci-dessous |
| `POST /logements/{id}/modifier` | Champs descriptifs |
| `POST /logements/{id}/archiver` | Archivage |
| `POST /logements/{id}/reactiver` | Réactivation |
| `POST /logements/{id}/changer-proprietaire` | Changement de propriétaire (historisé) |
| `POST /logements/{id}/changer-taux-commission` | Changement de taux (historisé, grain logement) |

Toutes les routes d'écriture suivent le motif POST-Redirect-GET déjà utilisé par
`charges_controle.py` : redirection vers la fiche avec `?message=...` (succès) ou `?erreur=...`
(échec), jamais de payload traité en double par rafraîchissement.

### Fiche logement = écran central (`app/templates/logements_detail.html`)
Trois informations affichées, deux sources distinctes et explicitement annoncées :
- **« État actuel (temps réel) »** + **« Historique — gestion/taux »** : lus directement dans
  `REF_Setup.xlsm` par `logements_gestion_service.etat_actuel()`/`historique()` (nouvelles fonctions
  de lecture, cf. `28_CYCLE_DE_VIE_LOGEMENT.md`). Toujours à jour, y compris juste après une action.
- **« Informations principales »/« Historique des taux » (bloc existant)** : basé sur le CSV PBI
  (Lot13) — respecte l'arbitrage APP-1 (`logements_service.py` ne lit jamais
  `REF_Gestion_Logements_Hist`/`REF_Proprietaires`). Peut être en retard tant que le pipeline n'a pas
  tourné ; un texte l'annonce clairement sous le bloc « État actuel ».

Depuis cette fiche : modifier, archiver, réactiver, changer de propriétaire, changer de taux,
consulter l'historique complet, voir l'état actuel — **aucune manipulation Excel**.

### Fiche minimale pour un logement tout juste créé
Un logement créé via l'application n'existe pas encore dans le CSV PBI (Lot13 ne l'a pas encore
repris) : au lieu d'un 404, la route sert une **fiche minimale** construite directement depuis
`REF_Setup` (même état actuel + historique + actions), avec un bandeau explicite. Jamais de 404
pour un logement qui existe réellement.

### Formulaire de création (`app/templates/logements_nouveau.html`)
Champs alignés sur `logements_creation_service.valider()`. Bouton désactivé + bandeau si écriture
désactivée sur l'installation.

### Liste (`app/templates/logements_list.html`)
Bouton « + Ajouter un logement ». Le sous-titre « lecture seule » a été retiré (obsolète : la
création/modification passe maintenant par l'application) et remplacé par une mention neutre sur
l'origine Lot13 de la liste.

## Correction en cours de route
`{% block title %}` de la fiche testait uniquement `detail` — un logement en fiche minimale
affichait « Logement introuvable » dans l'onglet malgré un contenu correct. Corrigé
(`detail or fiche_minimale`).

## Vérification cross-module (point 4 de la consigne)
`charges_preview_service.load_form_refs()` et `saisie_hh_service.load_form_refs()` lisent tous deux
`REF_Logements`/`REF_Gestion_Logements_Hist` **directement dans `REF_Setup`** — la même feuille que
`logements_gestion_service` écrit. **Aucun cache à invalider** : l'exclusion d'un logement archivé
des nouveaux traitements (réservations, charges) est immédiate, sans attendre Lot13. Vérifié par
`tests/test_logements_cross_module.py` (reproduit fidèlement les filtres réels des deux services :
`actif == 'OUI'` pour les charges, `actif == 'OUI' ET statut_parc == 'GERE'` pour les réservations).
Ménages et Banque ne construisent pas de sélecteur de logement pour de nouvelles entrées
(réconciliation/tâches externes) : non concernés par ce point.

Un logement archivé reste visible dans `REF_Logements` (jamais supprimé) et dans
`REF_Gestion_Logements_Hist` (ligne clôturée, jamais supprimée) — vérifié par test.

## Vérification de la résolution historisée à toutes les dates (point 5)
`resolve_commission_rate()` (Lot10, `lib_ref_history.py`) exécutée en direct contre les données
produites par la recette navigateur (voir plus bas) : taux résolu correctement avant/après chaque
changement, jamais de régression sur une ligne déjà close.

## Recette navigateur complète (point 6)
Serveur recette lancé (`PROJECT_ROOT=data_recette`, port 8019), parcours réalisé **dans le
navigateur réel** (Chrome piloté), pas par appels HTTP :

1. **Création** — `/logements/nouveau` : logement `LOG_D1` créé (PROP_A, 2026-07-01).
2. **Consultation** — fiche minimale affichée immédiatement (PBI pas encore régénéré), état actuel
   correct.
3. **Modification** — adresse changée ("12 rue de la Recette"), message de confirmation affiché,
   valeur relue correcte.
4. **Changement de propriétaire** — PROP_A → PROP_B au 2026-08-01 : ligne PROP_A clôturée au
   2026-07-31, nouvelle ligne PROP_B ouverte, historique affiche les deux lignes.
5. **Changement de taux** — 0.20 au 2026-08-01, message « pensez à relancer Lot10 ».
6. **Archivage** — date de sortie 2026-09-30 : `actif=NON`, `statut_parc=RETIRE`, gestion clôturée,
   bouton bascule sur « Réactiver ».
7. **Réactivation** — PROP_B, 2026-10-01 : `actif=OUI`, `statut_parc=GERE`, **nouvelle** ligne de
   gestion ouverte (l'ancienne, clôturée, reste dans l'historique — 3 lignes au total).
8. **Nouveau calcul / contrôle des résultats** — `/sources-calculs` de l'application est en mode
   **dry-run uniquement** (Lot APP-0 : aucun script réellement exécuté, débloqué au Lot APP-5). Le
   moteur Lot10 complet (`lot10_calculer_resultats.py`) nécessite `pandas`, **absent de cet
   environnement** (limite d'environnement, pas de l'application). Contrôle réalisé en exécutant
   directement `resolve_commission_rate()` (le cœur du moteur Lot10, sans dépendance) contre le
   classeur `REF_Setup.xlsm` produit par la session navigateur :
   - `LOG_D1` : `MISSING` avant le 2026-08-01 (cohérent, aucun taux avant la création), `0.20`
     résolu à partir du 2026-08-01 (la ligne créée par l'action navigateur).
   - `LOG_A1` (donnée fictive préexistante, non touchée) : `0.19` en janvier/février, `0.15` à partir
     de mars — confirme que la résolution temporelle reste correcte sur un cas à changement de taux
     déjà en place.

### Incident d'outillage rencontré et résolu
Les clics automatisés sur les `<summary>` de `<details>` (panneaux dépliables) échouaient
silencieusement à plusieurs reprises (aucune requête HTTP envoyée, formulaire jamais soumis) sans
erreur visible dans l'outil de clic. Contourné en ouvrant le `<details>` visé via
`details.open = true` (JavaScript, dans la page réelle) avant de remplir/soumettre le formulaire —
le clic sur le bouton **réel** et la soumission HTTP restent authentiques, seul le clic sur le
triangle d'ouverture était contourné. N'affecte pas la validité du test : chaque action a été
confirmée par le message de succès **et** par la relecture de l'état affiché.

Le jeu de données recette a été régénéré (`recette/build_data_recette.py`) après la session pour
repartir d'un état propre.

## Tests écrits ce tour
- `tests/test_logements_routes.py` (13 tests) : fiche = écran central, boutons désactivés si écriture
  coupée, création → fiche minimale, modification, changement propriétaire/taux (succès + erreurs),
  archivage/réactivation, toutes les écritures refusées si flags off.
- `tests/test_logements_cross_module.py` (5 tests) : exclusion immédiate des nouveaux traitements
  (charges + réservations), visibilité conservée dans l'historique, réactivation, résolution du taux
  à plusieurs dates.
- Total module Logements (création + gestion + routes + cross-module) : **49 tests**, tous verts.

## Suite de tests complète
1642 passent / 70 skip / **1 échec pré-existant** (`test_appsec1_diagnostic.py`, fuite du nom
d'utilisateur Windows dans un chemin temporaire pytest — indépendant de ce module, non touché par ce
tour, déjà signalé dans `24_ETAT_FINAL_CHARGES.md`/session précédente).

## État du module Logements

**Terminé** : création, modification, changement de propriétaire, changement de taux, archivage,
réactivation — service + route + template + navigation + protections + historisation vérifiée +
cross-module vérifié + recette navigateur réelle passée de bout en bout.

**Peut être déclaré terminé** pour le périmètre couvert par la consigne initiale (le cycle de vie
complet d'un logement, pilotable depuis le navigateur, sans jamais toucher les fichiers Excel).

**Hors périmètre, explicitement non traité** (signalé dès l'audit du tour précédent, non requis
cette fois) :
- Forfait logiciel/consommables historisé — nécessite une modification du moteur Lot10
  (`build_charge_fixe()`), traitée séparément par consigne explicite de l'utilisateur.
- Exécution réelle du pipeline (Lot9/Lot10) depuis l'application — bloquée par conception jusqu'au
  Lot APP-5 (dry-run uniquement), indépendante du module Logements.
