# 30 — Module Banque : import et rapprochement des mouvements

Suite du module Logements (terminé, cf. `29_MODULE_LOGEMENTS_COUCHE_HTTP.md`). Construit un module
Banque réellement exploitable : import d'un relevé, normalisation, détection de doublons,
confirmation transactionnelle, catégorisation (existante, réutilisée), rapprochement multi-objets,
vue non-rapprochés, contrôles ciblés, vérification d'impact Lot9.

## Audit ciblé (avant écriture)

| Élément | État constaté |
|---|---|
| Source bancaire attendue | Crédit Mutuel, un seul fichier/compte/mois codé en dur dans `lot8a_banque_import.py` (`01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_CreditMutuel.xlsx`) — **absent de ce worktree** |
| Formats supportés (avant ce tour) | XLSX uniquement, batch, aucune UI d'upload |
| Fichier normalisé | `BANQUE_LOT8_IMPORT.xlsx` onglet `NORM_Banque`, 26 colonnes (23 lot8a + 3 lot8b) |
| Clé mouvement | `mouvement_id` = `MVT-<compte>-<date YYYYMMDD>-<sens>-<centimes>-<hash6>` ; `ROW_HASH` = sha256(compte, date_op, date_valeur, sens, montant_centimes, libellé normalisé, devise) |
| Détection doublon (avant ce tour) | `ROW_HASH` déjà vu dans le même run → `DOUBLON_BANCAIRE_POTENTIEL` (dans le batch uniquement, jamais entre imports puisqu'aucun import répété n'existait) |
| Catégorisation | **Déjà construite** (APP-4B) : `banques_controle_service.py`/`banques_controle_writer.py`, décisions journalisées SQLite (`banque_overrides`, versionné, historique), écriture sur COPIE isolée |
| Rapprochement (avant ce tour) | **Déclaratif uniquement** : `lot8c` pose des statuts d'attente (`EN_ATTENTE_EXPORT_AIRBNB`, `EN_ATTENTE_SAISIE_ACOMPTE`) dans des onglets séparés ; aucun lien persistant mouvement↔objet, `nb_rapproches` forcé à 0 côté appli |
| Consommateur Lot9 | `lot9_construire_flux.py:126` — **uniquement** `TYPE_FLUX_016` + `statut_controle=VALIDE` (frais bancaires) ; payouts/virements propriétaires n'entrent jamais dans Lot9 par ce chemin |
| Consommateur Lot10 | Aucun accès direct à NORM_Banque — l'impact ne passe que par les flux `BNQ` déjà construits par Lot9 |
| Contrôles Lot11 | Section « Banque adaptative » (D-LOT11-01/03), lit NORM_Banque si présent, rapprochement payout indicatif non bloquant |
| Interface existante | Dashboard filtrable (`/banques-caisse`), écran « à rapprocher » (lecture, placeholder « prochain lot » pour le rapprochement manuel), fiche mouvement + contrôle/catégorisation (APP-4B), export CSV |
| Manques identifiés | Aucune route d'import ; `BANQUE_REAL_WRITE_ENABLED` codé `False` en dur sans mécanisme d'activation (contrairement à Charges/Logements) ; aucun rapprochement persistant multi-objets ; aucun dossier `01_SOURCES_BRUTES/Banque` dans ce worktree |

## Ce qui a été construit ce tour

### Sécurité (§4 consigne)
`app/config.py` : `BANQUE_REAL_WRITE_ENABLED`/`BANQUE_REAL_WRITE_CONFIRMATION_ENABLED` suivent
désormais exactement le patron Charges/Logements — `RECETTE_MODE and _env_flag(...)`, double
verrou, jamais activables hors recette. Écriture atomique via `_remplacer_fichier` (write-guard
partagé), jamais de modification des sources réelles.

### Import bancaire (§5-9) — `app/services/banques_import_service.py`
Reprend **exactement** le schéma et les formules de `lot8a_banque_import.py` (26 colonnes,
`ROW_HASH`, `mouvement_id`) : pas de seconde norme concurrente. Parcours : upload (CSV/XLSX,
en-têtes françaises usuelles reconnues) → lecture en mémoire (jamais de modification du fichier
importé) → normalisation → prévisualisation scellée (manifest JSON sous `DRYRUNS_DIR/banque_import`,
jamais un octet écrit) → confirmation qui revérifie le token, écrit atomiquement dans `NORM_Banque`
(crée le fichier/onglet s'il est absent), journalise l'import (`banque_imports`, migration 0015).

- **Doublon certain** : même `ROW_HASH` qu'une ligne déjà dans `NORM_Banque` ou déjà vue dans le
  fichier → ligne exclue, jamais fusionnée. Réimporter le même fichier n'ajoute aucune ligne (vérifié
  par test **et** en navigateur : deuxième import → 0 ajoutée, 3 doublons certains).
- **Doublon probable** : même compte/date/montant/libellé, `ROW_HASH` différent (typiquement
  `date_valeur` différente) → exclu par défaut, ajouté seulement si la case « Confirmer malgré les
  doublons probables » est cochée à la confirmation.
- **Lignes invalides** : libellé manquant, date inexploitable, montant non numérique, débit et
  crédit tous deux renseignés → détaillées, jamais écrites, jamais silencieusement ignorées.
- Toute ligne fraîchement importée entre en `EN_ATTENTE_CLASSIFICATION` (jamais `VALIDE`) : **aucun
  impact financier** avant classification/validation humaine — vérifié directement contre le filtre
  réel de Lot9 (`test_banques_impact_lot9.py`).

### Rapprochement (§12-14) — `app/services/banques_rapprochement_service.py` + migration 0015
Généralise le patron déjà éprouvé de `rapprochements_reglements` (migration 0014 : identifiants
opaques, critères explicables, historique append-only) à N types d'objets
(`RESERVATION, PAYOUT_PLATEFORME, CHARGE_FOURNISSEUR, REGLEMENT_CHARGE, REVERSEMENT_PROPRIETAIRE,
REMBOURSEMENT_ASSOCIE, REMBOURSEMENT_VOYAGEUR, MOUVEMENT_INTERNE, NON_IDENTIFIE`) :
- **jamais de relation 1-1 forcée** : plusieurs liens actifs possibles par mouvement (couvre
  plusieurs objets) et par objet (réglé par plusieurs mouvements) — contrôle sur la **somme** des
  montants actifs, jamais sur un couple unique ;
- dépassement du montant du mouvement → refusé (`V03_DEPASSEMENT_MONTANT`) ;
- mouvement déjà rapproché à 100 % → nouveau lien refusé (`V04_MOUVEMENT_DEJA_RAPPROCHE_INTEGRALEMENT`)
  tant que le lien existant n'est pas annulé ;
- statuts `PROPOSE → CONFIRME/REFUSE/ANNULE`, historique append-only
  (`banque_rapprochement_evenements`) — une proposition automatique n'est **jamais** validée
  silencieusement ;
- `proposer()` : score explicable (référence exacte, montant exact, date proche) — jamais un
  matching opaque, toujours une raison lisible.

### Vue « non rapprochés » (§14)
`/banques-caisse/a-rapprocher` (existante) enrichie de compteurs applicatifs (non rapprochés,
partiellement rapprochés, rapprochés, bloquants moteur), sans dupliquer une seconde page — le
placeholder « rapprochement manuel — prochain lot » a été retiré (capacité réelle désormais).

### Interface (§10-11, §17)
- `GET/POST /banques-caisse/importer`, `GET .../previsualisation/{token}`,
  `POST .../confirmer/{token}` — templates `banques_importer.html`,
  `banques_importer_previsualisation.html`, `banques_importer_resultat.html`.
- Fiche mouvement (`banques_mouvement.html`) enrichie d'un bloc « Rapprochement » : état
  (NON_RAPPROCHE/PARTIEL/RAPPROCHE), liens existants avec actions confirmer/refuser/annuler,
  formulaire de proposition.
- Catégorisation : **réutilisée telle quelle** (APP-4B, déjà mature — décisions versionnées,
  historique, référentiels lisibles) — non reconstruite.
- Liste/filtres/détail/export : **réutilisés tels quels** (`banques_service.py`,
  `banques_reader.py`) — déjà conformes à la demande (période, compte, sens, montant, texte,
  statut, non-rapproché).

### Impacts Lot9/Lot10/Lot11 (§16)
Vérifié par lecture de code + test exécutable (pas de pandas dans cet environnement, cf.
limite déjà rencontrée sur le module Logements) :
- `test_banques_impact_lot9.py` reproduit **verbatim** le filtre de `lot9_construire_flux.py:126`
  et prouve qu'un import brut n'y entre jamais, et que seuls les frais bancaires `TYPE_FLUX_016`
  `VALIDE` y entrent — jamais un payout ni un reversement propriétaire (pas de double comptage avec
  Hostaway/HH/règlements propriétaire).
- Lot10 ne lit pas directement NORM_Banque (confirmé par l'audit) : aucun impact direct à vérifier.
- Lot11 : section banque adaptative inchangée, non touchée par ce tour.
- Aucun rapprochement ne modifie un fichier moteur : le journal est **additif et séparé**
  (SQLite), jamais une réécriture rétroactive d'un résultat.

## Recette navigateur réelle (Chrome piloté, pas de simples appels HTTP)

Serveur recette (`PROJECT_ROOT=data_recette`, flags Banque + Charges actifs, port 8020) :

1. **Ouvrir Banque** — dashboard affiche les 8 mouvements fictifs du jeu de recette (encaissement,
   payout, reversement propriétaire, paiement fournisseur, remboursement associé, frais bancaires,
   doublon certain, mouvement inconnu).
2. **Importer un relevé fictif** — CSV 3 lignes uploadé via le vrai formulaire (payout, reversement
   propriétaire, paiement fournisseur).
3. **Prévisualiser** — 3 lignes lues, 3 valides, totaux corrects (445,00 € débit / 925,50 € crédit).
4. **Confirmer** — 3 mouvements ajoutés, dashboard passe de 8 à 11 mouvements.
5. **Réimporter le même fichier** — prévisualisation : 0 valide, 3 doublons certains ; confirmation :
   0 ajoutée. **Idempotence prouvée en conditions réelles**, pas seulement en test.
6. **Catégorisation** — écran existant (APP-4B), non re-testé en navigateur ce tour (fonctionnalité
   pré-existante, hors périmètre de régression de ce chantier).
7. **Rapprocher une réservation** — mouvement « VIR HOSTAWAY PAYOUT » 850 € → proposé puis confirmé,
   état `RAPPROCHE`, 850,00 € / 0,00 € restant.
8. **Rapprocher une charge** — mouvement « VIR FOURNISSEUR MENAGE B » 120 € → `CHARGE_FOURNISSEUR`
   proposé, état `RAPPROCHE`.
9. **Laisser un mouvement inconnu** — « PRLV INCONNU DIVERS » non touché.
10. **Vue non rapprochés** — compteurs applicatifs affichés (1 non rapproché, 0 partiel, 0
    rapproché sur le périmètre moteur de cette page — les deux liens créés portent sur des
    mouvements `VALIDE` hors périmètre de cette page spécifique, cohérent avec sa portée).
11. **Redémarrer le serveur puis revérifier** — dashboard (11 mouvements), fiche du mouvement
    rapproché (`RAPPROCHE`, `CONFIRME`, 850,00 €) : **persistance confirmée** après redémarrage
    (Excel + SQLite).

### Incident d'outillage (non applicatif)
Les clics automatisés sur certains boutons `<button type="submit">` échouaient silencieusement à
plusieurs reprises (aucune requête HTTP envoyée, aucune erreur visible) — cliquer une seconde fois
au bon endroit, ou recalculer la coordonnée après un changement de mise en page, résolvait
systématiquement le problème. Chaque étape listée ci-dessus a été confirmée par le message de
succès affiché **et** la relecture de l'état réel (dashboard, fiche), jamais par une supposition.

## Tests ajoutés ce tour

| Fichier | Nb tests | Couvre |
|---|--:|---|
| `test_banques_import.py` | 16 | import CSV/XLSX, formats/colonnes/fichier vide, doublons certain/probable, idempotence, normalisation débit/crédit/hash stable, sécurité (flags off, hors racine recette, token inconnu, fichier jamais modifié) |
| `test_banques_import_routes.py` | 7 | page import, upload→prévisualisation→confirmation HTTP, erreurs (fichier manquant, format inconnu), flags off |
| `test_banques_rapprochement.py` | 16 | rapprochement par type d'objet, partiel/multiple, dépassement refusé, double rapprochement refusé, annulation, transitions, historique, propositions automatiques |
| `test_banques_rapprochement_routes.py` | 5 | bloc rapprochement sur la fiche, proposer→confirmer HTTP, partiel visible, dépassement refusé HTTP, flags off |
| `test_banques_impact_lot9.py` | 2 | filtre Lot9 réel : import brut jamais repris, seuls les frais bancaires VALIDE y entrent |
| **Total module Banque (ce tour)** | **46** | |

Corrections de non-régression (causées par ce tour, corrigées) :
- `test_no_metier_calc.py::test_no_bidirectional_sync` — `banques_import_service.py` ajouté à la
  liste des writers « copie/write-guard + journal one-way » déjà établie pour Charges/Logements.
- `test_sqlite_migrations.py` — 3 nouvelles tables (migration 0015) ajoutées à `EXPECTED_TABLES`.

## Suite de tests complète
1712 passent / 65 skip / **1 échec pré-existant** (`test_appsec1_diagnostic.py`, fuite du nom
d'utilisateur Windows dans un chemin temporaire pytest — indépendant de ce module, déjà signalé au
tour précédent).

## État du module Banque

**Terminé** : import (upload → prévisualisation → normalisation → détection doublons →
confirmation transactionnelle → journal), liste/filtres/détail (réutilisés), catégorisation
(réutilisée), rapprochement multi-objets persistant (proposer/confirmer/refuser/annuler, partiel et
multiple, contrôles de dépassement et de double rapprochement), vue non-rapprochés enrichie,
vérification d'impact Lot9 (pas de double comptage), recette navigateur réelle bout en bout avec
persistance vérifiée après redémarrage.

**Partiel / hors périmètre explicitement signalé** :
- Formats supportés : CSV et XLSX uniquement (OFX non ajouté — aucun besoin réel constaté à
  l'audit, conformément à la consigne « ne pas ajouter un format sans besoin réel »).
- Suggestions automatiques de rapprochement (`proposer()`) : moteur de score construit et testé,
  **non branché sur un écran dédié** (pas de bouton « voir les suggestions » sur la fiche ou la vue
  non-rapprochés) — l'utilisateur saisit la proposition manuellement pour l'instant.
- Catalogue de contrôles §15 : seuls doublon bancaire (existant, moteur), dépassement de montant et
  double rapprochement (nouveaux, ce tour) sont réellement implémentés. Les autres codes listés
  (devise inattendue, paiement propriétaire sans propriétaire, objet rapproché pour un montant
  excessif au-delà du simple dépassement, mouvement rejeté présent dans Lot9...) ne sont **pas**
  construits ce tour — gap réel, à traiter si le module doit couvrir l'exhaustivité de la consigne.
- Impact Lot10/Lot11 : vérifié par lecture de code et test ciblé (pas d'exécution réelle du
  pipeline complet — `pandas` absent de cet environnement, même limite que pour le module
  Logements).
- Import OFX, multi-devise, multi-compte simultané : non traités (aucune donnée réelle multi-compte
  disponible pour cadrer le besoin).

**Peut être déclaré PARTIEL** : le cœur (import, dédoublonnage, confirmation transactionnelle,
rapprochement persistant multi-objets, non-régression, recette navigateur réelle) est terminé et
réellement utilisable depuis l'application. Le catalogue de contrôles étendu et le branchement UI
des suggestions automatiques restent à compléter pour couvrir l'intégralité de la consigne.
