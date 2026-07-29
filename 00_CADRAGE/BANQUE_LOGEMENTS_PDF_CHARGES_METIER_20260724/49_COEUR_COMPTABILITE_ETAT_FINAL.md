# 49 — Cœur Comptabilité : état final de cette mission (2026-07-29)

Suite de `43`/`45`/`46`/`47` (premier socle ACHATS/BANQUE). Cette mission complète le cœur
opérationnel : VENTES, CAISSE, OD, auxiliaires, périodes et clôture, rapprochement comptable,
contrôles, interface, recette. N'a PAS construit : analytique exploité, écrans Résultats,
facturation propriétaire comme objet applicatif, plan de comptes détaillé.

## Migration

`0023_comptabilite_coeur.sql` — additive, ne modifie aucune table de `0021`/`0022`. Nouvelles
tables : `mapping_categorie_compte`, `operations_caisse`, `operations_diverses`, `od_lignes`,
`periodes_comptables`, `periode_evenements`. Plan comptable étendu : `530000` (Caisse), `467000`
(Associés), `706000` (Ventes — commissions).

## Journaux — état par journal

| Journal | État | Source | Fichier |
|---|---|---|---|
| ACHATS | TERMINÉ (mission précédente) | Facture fournisseur validée | `comptabilite_ecritures_service.generer_ecriture_achat` |
| BANQUE | TERMINÉ (mission précédente) | Rapprochement bancaire confirmé | `comptabilite_ecritures_service.generer_ecriture_banque` |
| VENTES | TERMINÉ, **source provisoire** | Adaptateur Lot12 (`montant_du_conciergerie`, jamais recalculé) | `ventes_lot12_adapter_service.py` |
| CAISSE | TERMINÉ | Règlement fournisseur (moyen CAISSE) + `operations_caisse` (encaissement, remboursement associé) | `operations_caisse_service.py` + générateurs dans `comptabilite_ecritures_service.py` |
| ODIVERSES | TERMINÉ | `operations_diverses` — objet BROUILLON à lignes libres, équilibrées avant insertion | `operations_diverses_service.py` |

Chaque générateur respecte le contrat déjà établi par ACHATS/BANQUE : ne crée jamais l'objet source,
équilibre imposé en code, idempotence par `(journal, origine_type, origine_id_opaque)`, jamais de
suppression (contrepassation uniquement).

## VENTES — ce que « SOURCE_PROVISOIRE_LOT12 » veut dire précisément

L'écriture VENTES traduit un montant déjà calculé par Lot12 (`montant_du_conciergerie`, par
propriétaire et par mois) en débit/crédit. Ce n'est ni une facture propriétaire émise comme objet
applicatif, ni une migration de Lot12 vers SQLite : Lot12 reste l'unique moteur, l'adaptateur ne
fait que lire (`proprietaires_reglements_service.load_owners`) et transmettre. Si Lot12 est
indisponible, `lignes_du_mois()` renvoie une liste vide sans lever — aucune écriture n'est générée,
ce qui n'est pas une anomalie. Le libellé de chaque écriture porte explicitement
`SOURCE_PROVISOIRE_LOT12`.

## Auxiliaires

`comptabilite_auxiliaires_service.py` ajoute une vue consolidée (solde + éléments ouverts) pour
trois familles — `FOURNISSEUR` (401000), `PROPRIETAIRE` (411000), `ASSOCIE` (467000) — sans
recalculer le solde (`comptabilite_ecritures_service.solde_auxiliaire`, déjà générique, réutilisé
tel quel). « Éléments ouverts » : factures ouvertes pour un fournisseur ; écritures encore
`PROPOSEE` (non validées) pour un propriétaire/associé.

## Périodes comptables et clôture

Table `periodes_comptables`, distincte de la clôture APPLICATIVE du pilotage des calculs
(`clotures_mensuelles`, migration `0008` — celle-ci porte sur l'exécution des lots de calcul,
celle-ci sur les écritures). Statuts : `OUVERTE → EN_CONTROLE → VALIDEE → CLOTUREE → ROUVERTE`,
transitions contrôlées, chacune journalisée (`periode_evenements`) avec acteur et, pour la clôture,
un résumé (nb écritures, totaux débit/crédit, nb anomalies).

Une période `CLOTUREE` fait refuser **toute** écriture directe : le contrôle est câblé au plus bas
niveau, dans `_inserer_ecriture` (fonction interne partagée par tous les générateurs), pas
dupliqué par journal — impossible à contourner en passant par un générateur qui l'aurait oublié.
Seule la contrepassation reste possible (elle crée une écriture datée du jour, sur la période
courante, jamais sur la période clôturée elle-même). La réouverture exige une justification non
vide (`E04_REOUVERTURE_SANS_JUSTIFICATION` sinon).

La clôture elle-même est gardée par `comptabilite_controles_service.controler()` : refusée si au
moins une anomalie `BLOQUANT` subsiste sur la période (écriture déséquilibrée, compte absent/
inactif, doublon d'écriture — défenses en profondeur, ces cas devraient déjà être impossibles par
construction).

## Contrôles comptables — catalogue

15 codes, 4 niveaux (BLOQUANT/CRITIQUE/AVERTISSEMENT/INFO cf. `comptabilite_controles_service.py`).
Notables : `CTRL_CPT_FACTURE_VALIDEE_SANS_ECRITURE`, `CTRL_CPT_REGLEMENT_CAISSE_SANS_ECRITURE`,
`CTRL_CPT_RAPPROCHEMENT_CONFIRME_SANS_ECRITURE_BANQUE`, `CTRL_CPT_MAPPING_CATEGORIE_NON_ARBITRE`,
`CTRL_CPT_CAISSE_NEGATIVE`, `CTRL_CPT_CONTREPASSATION_INCOMPLETE`. `CTRL_CPT_TVA_NON_ARBITREE` est
toujours présent en INFO (limite affichée en permanence, jamais masquée).

## Rapprochement comptable

Réutilise `banques_rapprochement_service.py` (aucun second moteur) : la page
`/comptabilite/rapprochement` liste les rapprochements `CONFIRME` de type `REGLEMENT_CHARGE`, avec
lien vers mouvement, règlement, facture, fournisseur et écriture BANQUE — signale explicitement
« absente » quand l'écriture BANQUE n'a pas encore été générée pour un rapprochement confirmé
(le contrôle `CTRL_CPT_RAPPROCHEMENT_CONFIRME_SANS_ECRITURE_BANQUE` détecte le même cas).

## Interface

Routes ajoutées : `/comptabilite/journaux/ventes` (+ `POST .../generer`),
`/comptabilite/journaux/caisse` (+ `POST .../operations`, `POST .../operations/{id}/generer-ecriture`),
`/comptabilite/journaux/od` (+ `POST` création, `POST .../{id}/valider`), `/comptabilite/periodes`
(+ `/{periode}` détail et 5 routes de transition), `/comptabilite/rapprochement`,
`/comptabilite/auxiliaires/{opaque}` (détail par famille). Plus : `POST /reglements/{opaque}/
generer-ecriture-caisse` depuis la fiche facture, pour un règlement en espèces.

## Recette

**Automatisée** (HTTP réel, mêmes routes qu'un navigateur) : `test_comptabilite_coeur_recette.py`
exerce le parcours complet — facture → ACHATS → validation → dette → règlement → rapprochement →
BANQUE → solde à 0 → VENTES → CAISSE → OD → contrepassation → période EN_CONTROLE → VALIDEE →
CLOTUREE → écriture refusée → réouverture justifiée → persistance → idempotence.

**Navigateur réel** (Chrome, port 8091, `data_recette` avec `PROJECT_ROOT` isolé) : opération de
caisse créée puis écriture générée (25,00 €, équilibrée) ; OD créée puis validée (écriture
ODIVERSES 12,00 €) ; période 2026-07 : `OUVERTE → EN_CONTROLE → VALIDEE → CLOTUREE`, tentative
d'écriture refusée avec le message exact, réouverture avec justification tracée dans l'historique ;
auxiliaire `PERS_NAVIGATEUR` : solde à 0 tant que l'écriture est `PROPOSEE`, passe à −25,00 € après
validation ; VENTES testé sur `data_recette` isolé (Lot12 absent de ce jeu → 0 propriétaire, 0
écriture — comportement attendu, pas une anomalie). Incident et correction : cf.
`JOURNAL_ANOMALIES.md`, leçon du 2026-07-29 (PROJECT_ROOT oublié lors d'un premier essai,
corrigé avant que l'incident ne se reproduise, base de recette régénérée et vérifiée sans PII).

## Tests

104 tests ajoutés/modifiés ce tour, répartis : `test_comptabilite_journaux_ventes_caisse_od.py`
(36), `test_comptabilite_periodes.py` (9), `test_comptabilite_controles.py` (7),
`test_comptabilite_auxiliaires.py` (4), `test_ventes_lot12_adapter.py` (4),
`test_comptabilite_coeur_recette.py` (1, parcours complet), plus corrections de
`test_sqlite_migrations.py` (tables attendues).

## Estimation honnête

Le cœur technique (5 journaux, équilibre, idempotence, auxiliaires, périodes, clôture,
réouverture, contrôles, recette) est TERMINÉ pour le périmètre défini par cette mission. Le plan de
comptes reste **PROVISOIRE** (affiché comme tel partout, jamais présenté comme validé) : le mapping
catégorie→compte existe comme table mais n'est pas relié à la génération réelle des écritures
ACHATS. Aucun pourcentage de projet supérieur à 85 % ne peut être annoncé tant que l'Analytique et
les Résultats ne sont pas construits (règle déjà actée dans `48`).

## Prochaine mission suggérée (non commencée ici)

Analytique et Résultats — règle de ventilation, peuplement des dimensions, écrans `/resultats/*`.
Explicitement hors périmètre de cette mission, sur instruction.
