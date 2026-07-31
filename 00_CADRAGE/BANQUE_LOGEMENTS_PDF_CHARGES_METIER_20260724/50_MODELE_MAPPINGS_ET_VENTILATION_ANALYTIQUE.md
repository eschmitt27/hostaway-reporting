# 50 — Mappings comptables branchés et ventilation analytique (Phase 1)

Suite de `49` (cœur Comptabilité). Première phase de la mission Analytique/Résultats : brancher
les mappings comptables sur la génération réelle des écritures (gap explicitement laissé ouvert par
`46`), et peupler les dimensions déjà présentes sur `ecriture_lignes` depuis `0021`
(`logement_id`/`proprietaire_id`), jamais exploitées jusqu'ici (`47`).

## Audit ciblé — état avant cette phase

| Flux | Mapping actuel | Dimension disponible | Manque | Action |
|---|---|---|---|---|
| ACHATS (mono-charge) | `606000` en dur | `charges_reader.find_charge().logement_id/proprietaire_id` (déjà dans MASTER Lot3, jamais lu par le générateur) | résolution de compte, lecture de la dimension | branché ce tour |
| ACHATS (`facture_lignes`) | `606000` en dur, une seule ligne pour toute la facture | `facture_lignes.logement_id` (déjà saisi par l'utilisateur, migration `0022`) | une ligne d'écriture par ligne de facture | branché ce tour |
| VENTES (Lot12) | `706000` fixe | `proprietaire_id` déjà passé en paramètre, jamais posé sur la ligne | poser `proprietaire_id` sur les deux lignes | branché ce tour |
| BANQUE | `512000`/`401000` fixes | aucune (règlement, pas de logement) | — | hors périmètre, pas de dimension pertinente à ce grain |
| CAISSE | `530000`/`467000` fixes | aucune (opération de caisse, pas de logement) | — | hors périmètre, pas de dimension pertinente à ce grain |
| OD | comptes saisis librement | aucune par nature (objet libre) | — | hors périmètre, l'utilisateur choisit déjà le compte |
| Charge liée à un ménage sans logement propre | — | `menages.logement_id`/`proprietaire_id` (NOT NULL, migration `0019`), `menages.charge_id` (FK unique) | résoudre via le ménage lié | branché ce tour (case E) |
| Charge multi-logements (pool) | — | **aucune source de poids** — la ventilation des pools (`REC_002`) reste un gap ouvert et documenté ailleurs (`41` §7bis) | inventer une clé de répartition serait improviser | **non traité, honnêtement** — case F, `A_CONTROLER` |

## 1. Mappings comptables branchés

Nouvelle table `mapping_comptable_regles` (migration `0024`), historisée, à trois portées :

1. `CATEGORIE` — clé = `categorie_charge_id` (colonne déjà présente dans le MASTER Lot3, jamais
   exploitée par la Comptabilité avant ce tour) ;
2. `TYPE_FLUX` — clé = `type_flux_id`, utilisé si aucun mapping catégorie ne s'applique ;
3. `PROVISOIRE_GENERIQUE` — filet explicitement autorisé (seedé à `606000`), jamais un fallback
   silencieux : la résolution renvoie toujours `statut` (`VALIDE`/`PROVISOIRE`) et `regle_id_opaque`
   (ou `None` avec `regle="AUCUNE_REGLE"` si la table est vide).

`comptabilite_mappings_service.resoudre_compte(categorie_charge_id, type_flux_id, date_reference)`
implémente l'ordre de résolution exact du brief : exact catégorie actif à la date → type de flux →
provisoire catégorie-spécifique → provisoire générique → filet absolu. Un mapping dont
`date_fin_validite` est dépassée, ou dont `date_debut_validite` n'est pas encore atteinte, n'est
JAMAIS utilisé — vérifié par test (`test_mapping_expire_ignore`,
`test_mapping_pas_encore_actif_ignore`), et deux mappings sur la même catégorie à des périodes
différentes donnent chacun le bon compte selon la date de référence
(`test_historisation_deux_periodes_meme_categorie`) — même principe que `REF_Taux_Commission`/
`REF_Gestion_Logements_Hist` dans les sources réelles.

L'ancienne table `mapping_categorie_compte` (`0023`) reste en place, inchangée, toujours utilisée
par le contrôle `CTRL_CPT_MAPPING_CATEGORIE_NON_ARBITRE` — les deux tables coexistent
délibérément (une pour le signalement d'arbitrage en attente, une pour la règle réellement
appliquée), comme `REF_Logements`/`REF_Gestion_Logements_Hist` dans les sources réelles.

Écran : `/comptabilite/mappings` (liste + création de règle).

## 2. Ventilation analytique — `generer_ecriture_achat` refondu

Le générateur ACHATS ne produit plus une seule ligne de débit générique : il construit une ligne de
débit **par charge source**, résolue ainsi (ordre de préférence) :

- **Case A/B — affectation directe** : la charge (mono-charge historique) ou la ligne de facture
  (`facture_lignes`, case D) porte déjà `logement_id`/`proprietaire_id` — utilisés tels quels. Le
  logement saisi sur la ligne de facture **prime** sur celui du MASTER charge (précision humaine
  explicite au moment de la facturation), vérifié par test.
- **Case D — facture multi-lignes** : une ligne d'écriture par `facture_lignes`, montant = celui de
  la ligne (déjà équilibré par construction, `0022`) — pas de recalcul, pas de répartition inventée.
- **Case E — répartition Ménages** : si la charge n'a pas de `logement_id` propre mais est liée à un
  ménage (`menages.charge_id`), le ménage (NOT NULL sur ses deux dimensions) fournit
  `logement_id`/`proprietaire_id`.
- **Case F — sans dimension** : aucune des sources ci-dessus ne fournit de logement → la ligne se
  génère quand même (jamais bloquant), `statut_ventilation='A_CONTROLER'`, dimensions `NULL`.
- **Case C — répartition multi-logements par pool** : **non traitée**. Aucune clé de poids
  n'existe dans le schéma actuel pour une charge qui couvrirait plusieurs logements SANS passer par
  `facture_lignes` (c'est exactement le gap déjà documenté `41` §7bis, pools de courses/`REC_002`).
  Une telle charge tombe en case F, `A_CONTROLER` — honnête, pas une répartition arbitraire.

Chaque ligne de débit résout aussi son compte via `comptabilite_mappings_service.resoudre_compte`
(catégorie de la charge sous-jacente), jamais `606000` en dur — sauf absence totale de règle, auquel
cas le filet `PROVISOIRE_GENERIQUE` s'applique et est visible comme tel.

## 3. Traçabilité — `ecriture_ligne_ventilation`

Table satellite (migration `0024`), une ligne par ligne d'écriture débitrice, portant :
`methode` (les 5 valeurs ci-dessus), `montant_non_arrondi`/`montant_affiche` (identiques ici, la
distinction existe pour un futur besoin d'arrondi intermédiaire), `statut_ventilation`
(`VALIDE`/`A_CONTROLER`), `origine_type`/`origine_id` (vers la `facture_ligne` ou la `charge`
d'origine), `mapping_regle_id_opaque` (vers la règle utilisée, traçable jusqu'à l'arbitrage).
Jamais recréée à la régénération idempotente d'une écriture déjà existante (vérifié par test).

Visible sur la fiche écriture (`/comptabilite/ecritures/{id}`), nouveau bloc « Ventilation
analytique ».

## 4. Réconciliation — montant source = somme des ventilations

Garantie structurellement, pas vérifiée après coup : chaque ligne de débit porte exactement le
montant de sa source (`facture_lignes.montant_ttc` ou la facture entière pour le cas mono-charge),
et l'équilibre débit=crédit reste imposé par `_inserer_ecriture` (inchangé). La somme des
`montant_non_arrondi` de `ecriture_ligne_ventilation` égale donc toujours le débit total de
l'écriture — vérifié par test (`test_case_d_facture_multi_lignes`), tolérance 0,01 € (aucun écart
possible ici puisqu'aucun recalcul n'a lieu, seule une redistribution de montants déjà exacts).

## Ce qui n'est pas fait, honnêtement

- **Ventilation par pool (case C)** : aucune clé de poids n'existe. Reste liée au gap Ménages/pools
  déjà documenté, non traité ici.
- **BANQUE/CAISSE/OD** : pas de dimension logement/propriétaire ajoutée — ces journaux n'ont pas de
  grain pertinent à ce niveau (règlement, caisse, ajustement libre), pas un oubli.
- **Plan de comptes détaillé** : toujours non arbitré. Le mécanisme de résolution est prêt à recevoir
  des règles `VALIDE` dès qu'un arbitrage est pris — aucune règle `VALIDE` n'est créée par défaut
  (seul le filet `PROVISOIRE_GENERIQUE` existe).

## Tests

`test_comptabilite_mappings.py` (10), `test_comptabilite_mappings_routes.py` (3),
`test_comptabilite_ventilation_analytique.py` (9) — cases A/D/E/F, historisation, idempotence,
réconciliation, dimension VENTES. Suite ciblée Comptabilité rejouée : 142 passés, 0 échec.
