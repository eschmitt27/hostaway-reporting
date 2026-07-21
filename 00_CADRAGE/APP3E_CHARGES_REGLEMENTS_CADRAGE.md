# APP-3E — Charges, cycle de préparation des relevés, préparation des règlements

## 1. Audit d'écart (préalable à cette branche)

Le rapport précédent (Mission 1+2 APP-3D) livrait réellement : fusion `/proprietaires-reglements`,
référentiel fournisseur minimal, contrats des 4 sources complémentaires, 12 règles de blocage,
1359 tests. Il ne livrait PAS (vérifié par audit de code avant cette branche, aucune trace trouvée) :

| Mission demandée | État avant cette branche | Preuve |
|---|---|---|
| Affectation charge → fournisseur/logement/propriétaire | Absente | `grep fournisseur_id app/services/charges_*.py` → aucun résultat |
| Contrôles des charges (16 codes) | Absente | aucun fichier `charges_controles_service.py` |
| Cycle de préparation du relevé (machine à états dédiée) | Absente | seul `proprietaires_suivi_service.py` existait (statut de FACTURATION, pas de préparation) |
| Snapshot + détection de dérive | Absente | `grep snapshot app/services/proprietaires*.py` → aucun résultat |
| Vue `/a-payer` | Absente | `grep a-payer app/routes/*.py` → aucun résultat |
| Statuts PRET_A_PAYER / MARQUE_COMME_PAYE | Absents | aucune occurrence |
| Export préparatoire sans IBAN | Absent | pas de route associée |

Conclusion : ces missions ont été correctement identifiées comme non livrées par l'audit initial
de cette session, d'où la création de la présente branche.

## 2. Ce qui a été construit dans `integration/app3e-charges-reglements`

### Charges et affectations (commit 1)
- Migration 0011 : `charges_affectations` + `charges_affectation_evenements` (append-only).
- `charges_affectations_service.py` : affecter/modifier/historique, opaque `CHA-`, version optimiste.
- `charges_controles_service.py` : 16 codes (`CHARGE_SOURCE_ABSENTE` … `AJUSTEMENT_SANS_MOTIF`),
  détection pure, aucun recalcul financier (preuve structurelle : test dédié grep sur le code source).
- Affichage dans la fiche propriétaire existante (section « 4b · Charges affectées »), aucun écran
  parallèle créé.
- Le lien fournisseur↔charge est réellement fonctionnel (testé) ; le lien fournisseur↔ménage
  préexistant (`intervenant_id`) reste un concept distinct, non fusionné (décision documentée dans
  le cadrage APP-3D précédent, non remise en cause ici) ; le lien fournisseur↔logement n'est pas
  historisé séparément (le logement est un simple champ de l'affectation, pas une relation
  temporelle) — limite documentée.

### Cycle de préparation du relevé (commit 2)
- Migration 0012 : `proprietaires_releve_cycle`, `proprietaires_paiement` (tables séparées plutôt
  qu'`ALTER TABLE` sur `proprietaires_releves` — le runner de migrations réexécute chaque script à
  chaque appel, un `ADD COLUMN` casserait l'idempotence).
- `proprietaires_releve_cycle_service.py` : machine à états NON_DEMARRE→EN_PREPARATION→BLOQUE/
  A_VALIDER→VALIDE→ROUVERT/ANNULE. Snapshot figé (champs whitelistés, jamais de chemin/IBAN/id
  SQLite) à la validation, immuable ensuite. Détection de dérive par comparaison texte (jamais un
  recalcul).

### Bannière de dérive, relevé, préparation des règlements (commit 3)
- Route `/releve` affiche l'état du cycle, la bannière de dérive, les actions démarrer/valider/rouvrir.
- Route statique `/proprietaires-reglements/a-payer` (déclarée avant `{identifiant}` — testée,
  aucune collision).
- `proprietaires_paiement_service.py` : machine à états NON_PREPARE→A_CONTROLER→PRET_A_PAYER→
  MARQUE_COMME_PAYE→ROUVERT/ANNULE. **MARQUE_COMME_PAYE est une déclaration humaine, jamais une
  preuve bancaire** (mention affichée sur l'écran et dans l'export).
- Aucun champ IBAN/RIB/compte bancaire dans le modèle ; une référence interne contenant un motif
  suspect est refusée à l'écriture (`marquer_paye`).
- Export CSV préparatoire (`/a-payer/export.csv`) : mention « ne constitue pas un ordre bancaire »,
  protection anti-injection CSV réutilisée de l'export relevé existant.

## 3. Recette Playwright (4 largeurs)

12 captures (3 écrans × 4 largeurs : `/a-payer`, `/releve` avec bannière cycle, fiche de suivi).
0 débordement, 0 double menu, 0 mention bancaire visible. Serveur isolé port 8011 (port 8000 jamais
touché).

## 4. Finalisation (session de finalisation)

Les limites reconnues dans la version initiale de ce document ont été traitées :

- **Historique Git** : commit `e543535` (bannière dérive + préparation règlements) séparé en deux
  commits atomiques (`fc9f7e4` interface/dérive, `223054c` préparation règlements). Historique final
  à 7 commits.
- **Lien fournisseur↔logement historisé** : implémenté (migration 0013, table `fournisseur_rattachements`
  avec période date_debut/date_fin, type_prestation, statut, version, historique append-only),
  distinct de l'intervenant ménage du moteur et de l'affectation flat par charge. Exposé dans la
  fiche fournisseur (aucun nouvel item de menu). Voir commit `ad39d8a`.
- **Matrice exacte des cas demandés** : `APP3E_MATRICE_60_CAS.md` — 56 cas + 8 transverses rattachés
  chacun à un test **nommé** (jamais par similarité). 4 tests ajoutés pour combler les cas 33/34/45/46.
- **Recette fonctionnelle séparée** : `recette_fonctionnelle_app3e.py` (26 scénarios reproductibles
  sur base isolée), rapport `APP3E_RECETTE_FONCTIONNELLE.md` — 26/26 OK.
- **Validation approfondie des migrations** : `test_migrations_app3e_validation.py` (10 tests : base
  vierge, partielle, double, concurrence, idempotence, aucun doublon table/index, ordre déterministe,
  nombre découvert par glob).
- **Audit sécurité consolidé** : `test_securite_app3e.py` (aucun champ bancaire, aucun appel réseau,
  aucune écriture fichier, flags False, autoescape, neutralisation CSV, version optimiste, opaques).
- **Recette Playwright rejouée depuis les commits finaux** et **checkpoint formel SHA256** : voir le
  rapport final de la session de finalisation.

## 5. Invariants de sécurité (rappel)

Aucune comptabilité générale, aucun virement, aucun IBAN/RIB, aucun appel API bancaire, aucun writer
réel, tous les flags d'écriture à False. « MARQUE_COMME_PAYE » = déclaration humaine, jamais une
confirmation bancaire (mention affichée en liste, fiche, historique, export).
