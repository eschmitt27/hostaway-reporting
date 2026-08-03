# 74 — Audit des prérequis du rapprochement bancaire (Airbnb, Lot5, Lot8c)

Mission d'audit ciblé (2026-08-03), pas un audit général du projet. Objectif : déterminer
précisément ce qui bloque un rapprochement bancaire réellement vérifiable pour les 222 propositions
(166 Airbnb + 56 propriétaires), et ne construire que ce qui est sûr, testé et nécessaire.

## Tableau des blocages

| Blocage | Cause exacte | Composant existant | Correction nécessaire | Source requise |
|---|---|---|---|---|
| Candidats RESERVATION jamais générés | `banques_candidats_service._reservations()` cherchait les colonnes `montant_paye`/`montant_total`/`date_checkin`/`date_reservation`, qui n'existent dans **aucune** version de `MASTER_CALC_Reservations_Resolues.xlsx` (schéma réel : `montant_retenu`, `date_arrivee`) — retournait silencieusement 0 candidat sur les 1391 réservations réelles | `app/services/banques_candidats_service.py`, `app/config.MASTER_CALC_RESERVATIONS_RESOLUES` | **CORRIGÉ ce tour** (test rouge→vert, commit ci-dessous) | Aucune — bug de code, pas de donnée manquante |
| Aucun export Airbnb détaillé (paiement par paiement) | Aucun fichier de ce type n'existe dans `01_SOURCES_BRUTES`, `02_TRAVAIL`, les archives ou l'environnement de recette. Hostaway (`Lot1_Hostaway`) ne contient qu'un `airbnbExpectedPayout` par réservation (montant attendu, pas le virement bancaire réel), aucun `payout_id`, aucun code `G-xxxxxxx` | Le G-code est aujourd'hui extrait **du libellé bancaire lui-même** par `lot8c_rapprochement_banque.extract_gcode()`, jamais d'un export Airbnb | Import à construire (contrat ci-dessous), **non codé ce tour** — aucun fichier réel pour le tester, un import non testé contre une vraie forme serait spéculatif | Export Airbnb détaillé (paiement par paiement), à fournir par l'utilisateur |
| Paiements Airbnb groupés non matchables 1-pour-1 | Un virement bancaire Airbnb agrège plusieurs réservations ; vérifié par recherche directe (25 montants bancaires testés contre `montant_retenu`/`payout_calcule` : 1/25 correspond exactement) | Aucun algorithme de somme partielle/groupée dans `banques_candidats_service.py` ni `banques_rapprochement_service.py` | Algorithme de correspondance par sous-ensemble (somme de N réservations ≈ 1 virement, tolérance 0,01 €) — **non construit ce tour**, nécessite une conception dédiée et des tests combinatoires | Aucune (algorithme, pas une source) |
| Lot5 vide (0 ligne) | Confirmé par lecture directe : `MASTER_FACT_MAN_AcomptesProprietaires.xlsx` a 22 colonnes, 0 ligne de données | `lot5_master_acomptes_proprietaires.py`, source `SAISIE_AcomptesProprietaires.xlsx` | Aucune saisie n'a jamais été faite — attendu, pas un défaut | Saisie utilisateur dans `SAISIE_AcomptesProprietaires.xlsx` |
| **Lot5 ne peut de toute façon pas répondre au besoin du rapprochement Banque** | Le message d'attente de Lot8c ("Saisir acomptes/factures dans Lot 5") suppose que `SAISIE_AcomptesProprietaires.xlsx` peut enregistrer *"un propriétaire a versé un acompte à la société"*. Or le contrat réel de Lot5 (`ARCHITECTURE_DONNEES.md` §10.4, `DECISIONS_METIER.md`) est entièrement différent : `AcompteFacture = TotalPercu − Menage − Commission − MontantReverseProprietaire`, calculé **par réservation hors Hostaway côté voyageur**, pas un mouvement de trésorerie propriétaire↔société | Aucun objet "acompte/remboursement/régularisation/compensation propriétaire" n'existe dans l'application — vérifié dans toutes les migrations SQL (`app/db/migrations/*.sql`) : seuls `proprietaires_releves` (statut de facturation mensuel) et `rapprochements_reglements`/`rapprochement_evenements` (migration 0014, rapprochement déclaratif règlement↔mouvement) existent, aucun n'est un grand livre de trésorerie associé/propriétaire | **Décision d'architecture requise avant tout code** : soit créer un nouvel objet métier (migration + service + écran, comme le prévoit la mission), soit décider que ces 56 virements restent `A_CONTROLER` indéfiniment tant qu'aucune saisie manuelle n'existe. **Non construit ce tour** — c'est une décision produit, pas un bug, elle ne se prend pas à la place de l'utilisateur | Décision utilisateur sur la nature de l'objet à créer |
| Lot8c (script Excel) ne fait aucun matching réel | Lecture complète : `lot8c_rapprochement_banque.py` ne fait que router en attente (`EN_ATTENTE_EXPORT_AIRBNB`, `EN_ATTENTE_SAISIE_ACOMPTE`), aucun score, aucune tolérance montant/date, aucune gestion partiel/groupé/doublon | — | Le vrai moteur de matching existe déjà **côté application**, pas dans ce script (voir décision d'architecture) | — |

## Décision d'architecture (documentée, durable)

**Ne pas construire un second moteur.** L'application FastAPI possède déjà l'infrastructure cible :

- `app/db/migrations/0015_banque_import_rapprochement.sql` : table `banque_rapprochements` générique,
  `type_objet` incluant déjà `RESERVATION`, `PAYOUT_PLATEFORME`, `CHARGE_FOURNISSEUR`,
  `REGLEMENT_CHARGE`, `REVERSEMENT_PROPRIETAIRE`, `REMBOURSEMENT_ASSOCIE`, `REMBOURSEMENT_VOYAGEUR`,
  `MOUVEMENT_INTERNE`, `NON_IDENTIFIE` — montant rapproché en somme (supporte déjà le partiel et le
  multiple, jamais une relation 1-1 forcée), statuts `PROPOSE|CONFIRME|REFUSE|ANNULE`, historique
  append-only (`banque_rapprochement_evenements`).
- `app/services/banques_candidats_service.py` : point d'extension unique pour ajouter de nouveaux
  générateurs de candidats (aujourd'hui : `_charges()` et `_reservations()`, ce dernier corrigé ce
  tour). Un futur générateur `_reversements_proprietaires()` ou `_remboursements_associes()`
  s'ajouterait ici, jamais ailleurs.
- `app/services/rapprochement_reglements_service.py` / `rapprochement_candidats_service.py` :
  patron déjà éprouvé (fenêtre de jours, tolérance stricte, contrôles bloquants/informatifs) —
  modèle à suivre pour tout nouveau générateur, pas à dupliquer.

La Banque reste une preuve de mouvement financier ; elle ne crée jamais seule l'objet économique
(cohérent avec les décisions D-8c du script Lot8c et avec `banques_rapprochement_service.py`).

## Contrat cible — source Airbnb détaillée (documenté, non implémenté)

Si un export Airbnb détaillé est un jour fourni, le contrat canonique minimal est :

`transaction_id`, `payout_id`, `reference_airbnb` (G-code), `reservation_id`, `date_transaction`,
`date_payout`, `montant_brut`, `frais`, `montant_net`, `devise`, `listing_id`, `statut`,
`source_fichier`, `source_ligne`, `source_hash`, `cle_dedoublonnage`.

Fonctions requises à la construction : détection de format, prévisualisation, confirmation
explicite, import transactionnel idempotent, détection de doublons, hash, rapport d'erreurs,
aucune suppression silencieuse, aucune écriture dans la source. **Non codé ce tour** — construire
un importeur sans un seul fichier réel pour le valider serait spéculatif (forme de fichier,
encodage, noms de colonnes réels inconnus).

## Correction livrée ce tour

`app/services/banques_candidats_service.py::_reservations()` — colonnes corrigées
(`reservation_calc_id`, `montant_retenu`, `date_arrivee`, `canal` au lieu de `reservation_id`,
`montant_paye`/`montant_total`, `date_checkin`/`date_reservation`, `canal_id`). Procédure
test-rouge→correction→test-vert respectée : `tests/test_banques_candidats_service.py` (7 tests,
4 rouges avant correction confirmant le bug, 7/7 verts après). Régression : `pytest -k banque`,
219 passés / 29 ignorés (pré-existants) / 0 échec.

**Effet mesuré (information, sur copies, aucune écriture)** : même après correction, un test de
correspondance directe montant-à-montant entre les 25 crédits Banque déjà examinés en validation
humaine (groupes 4/5) et les 1391 réservations réelles ne trouve qu'1 correspondance exacte sur 25
— confirme que le blocage principal n'est plus le bug de lecture, mais l'absence d'un algorithme de
paiement groupé (voir tableau ci-dessus).

## Verdict de cette mission

**NO GO — SOURCE MÉTIER ET DÉCISION PRODUIT REQUISES POUR RECETTE RÉELLE.**

Le seul élément corrigible sans décision métier (le bug de lecture des candidats réservation) est
corrigé et testé. Les trois blocages restants (export Airbnb, algorithme de paiement groupé, objet
métier propriétaire) nécessitent soit une source externe (export Airbnb), soit une décision
produit explicite de l'utilisateur (créer un nouvel objet propriétaire, ou accepter que ces 56
virements restent `A_CONTROLER` indéfiniment), avant qu'un nouveau code ou une nouvelle migration
ne soit écrit. Aucune fonctionnalité cosmétique ou facultative n'a été entreprise.
