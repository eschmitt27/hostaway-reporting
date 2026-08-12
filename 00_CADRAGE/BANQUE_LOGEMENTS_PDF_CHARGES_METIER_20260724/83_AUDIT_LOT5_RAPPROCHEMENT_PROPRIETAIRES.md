# 83 — Audit Lot 5 et rapprochement des 56 mouvements propriétaires (2026-08-13)

Objectif : déterminer combien des 56 mouvements Banque propriétaires peuvent être résolus par une
**preuve métier déjà existante** (Lot 5 / trésorerie propriétaires), et non par supposition.

**Réponse courte : 0.** La source métier est totalement vide. Détail et conséquences ci-dessous.

## 1. Architecture Lot 5 — identifiée

| Élément | Chemin / valeur |
|---|---|
| Script | `02_TRAVAIL/lot5_master_acomptes_proprietaires.py` |
| Source de saisie | `01_SOURCES_BRUTES/AcomptesProprietaires/SAISIE_AcomptesProprietaires.xlsx` (onglets `SAISIE`, `REF_LOCALE`, `CONTROLES_SAISIE`, `README`) |
| Sortie | `02_TRAVAIL/Lot5_AcomptesProprietaires/MASTER_FACT_MAN_AcomptesProprietaires.xlsx` (onglets `MASTER`, `VUE_ACTIVE`, `POWER_QUERY_CODE`) |
| Clé | `acompte_id` (+ `ROW_HASH`) |
| Rattachement propriétaire | `proprietaire_id`, `logement_id`, `facture_ref` |
| Montant / sens | `montant_acompte`, `report_mois_precedent`, `mode_paiement_id`, `code_impact` |
| Contrôle propre au lot | `ACOMPTE_NON_RATTACHE_FACTURE` (BLOQUANT si `facture_ref` vide hors `EXCLU_RESULTAT`) |

**Objet distinct** : la migration `0025` (`mouvements_tresorerie_proprietaires`) est explicitement
documentée comme un objet **différent** de Lot 5 (« Lot5 conserve son rôle historique, non modifié,
non renommé »). Elle porte les natures `ACOMPTE_/REMBOURSEMENT_/REGULARISATION_/COMPENSATION_/
AVANCE_/RESTITUTION_PROPRIETAIRE|AUTRE_A_CONTROLER`, les sens `PROPRIETAIRE_VERS_SOCIETE` /
`SOCIETE_VERS_PROPRIETAIRE`, les statuts `BROUILLON|A_CONTROLER|VALIDE|ANNULE`, avec historique
append-only et rattachement au rapprochement bancaire générique (`banque_rapprochements`,
migration `0015`) via `type_objet = REVERSEMENT_PROPRIETAIRE`. **Ce moteur n'a pas été reconstruit.**

## 2. Source Lot 5 disponible — mesure exacte

| Source | Contenu |
|---|---:|
| `SAISIE_AcomptesProprietaires.xlsx` onglet `SAISIE` (source réelle, lue en read-only) | **0 ligne** |
| `MASTER_FACT_MAN_AcomptesProprietaires.xlsx` onglet `MASTER` (sortie réelle) | **0 ligne** |
| `mouvements_tresorerie_proprietaires` (base de recette, migration 26) | **0 ligne** |
| `mouvements_tresorerie_proprietaires` (base réelle `05_APPLICATION/data/app.db`) | **table absente** — la base réelle est en migration `0016`, la table est créée en `0025` |
| `proprietaires_releves`, `proprietaires_paiement`, `proprietaires_releve_cycle`, `proprietaires_releve_evenements` (base réelle) | 0 ligne chacune |

`REF_LOCALE` du classeur de saisie contient bien 16 lignes de référentiel (propriétaires, logements,
modes de paiement) — mais c'est du référentiel, **pas un objet économique**.

**Conclusion factuelle : aucun objet métier propriétaire n'existe nulle part dans le dépôt.** Le
contrôle moteur `LOT5_PREREQUIS_MANQUANT` émis par Lot 8c disait déjà exactement cela ; il est
confirmé, pas contourné.

## 3. Les 56 mouvements — structure mesurée

Tous issus de `BANQUE_LOT8_IMPORT.xlsx` onglet `RAPPROCH_PROPRIETAIRES_ATTENTE`, simulation
canonique fraîche (Banque réelle reconstruite sur copies).

| Caractéristique | Valeur |
|---|---|
| Nombre | 56 |
| Sens | **100 % CREDIT** (entrants, propriétaire → société) |
| Montant total | 27 069,18 € |
| Période | 2025-11-10 → 2026-07-06 |
| `nature_presumee` moteur | `ENCAISSEMENT_PROPRIETAIRE_A_VENTILER` (56/56) — un libellé d'attente, **pas** une nature établie |
| Statut moteur | `EN_ATTENTE_SAISIE_ACOMPTE` (56/56) |
| Montants répétés à l'identique | **0** — chaque montant est unique, aucune récurrence exploitable |

### Ventilation par identité candidate

| Identité (opaque) | Nb | Montant cumulé | Période |
|---|---:|---:|---|
| PROP_0009 | 16 | 8 839,74 € | 2025-11 → 2026-07 |
| PROP_0002 | 9 | 5 702,02 € | 2025-11 → 2026-07 |
| PROP_0006 | 9 | 4 040,24 € | 2025-11 → 2026-07 |
| PROP_0008 | 8 | 4 577,88 € | 2025-11 → 2026-07 |
| `FAMILLE_UZON_A_CONTROLER` | 7 | 1 161,69 € | 2025-11 → 2026-05 |
| PROP_0010 | 5 | 1 547,61 € | 2025-11 → 2026-06 |
| PROP_0005 | 2 | 1 200,00 € | 2026-04 → 2026-05 |
| **TOTAL** | **56** | **27 069,18 €** | |

## 4. Identité : candidate ≠ prouvée

Les `proprietaire_id` ci-dessus proviennent de **règles de libellé bancaire** (`lot8b_banque_regles.py`,
règles `R_071`+ : « CONTIENT <nom> → PROP_xxxx »). C'est une **IDENTITÉ_CANDIDATE**, obtenue par
correspondance de chaîne sur le libellé du virement — **pas une IDENTITÉ_PROUVÉE** par un objet
métier. Le moteur lui-même ne les valide pas : il les laisse en `RAPPROCHEMENT_REQUIS` /
`A_CONTROLER`, jamais en `VALIDE`.

Le groupe `FAMILLE_UZON_A_CONTROLER` (7 mouvements) est explicitement marqué à contrôler par le
moteur : le libellé ne permet pas de trancher entre plusieurs personnes d'un même foyer. **Identité
non résolue, à confirmer par l'utilisateur.**

## 5. Niveaux de preuve — résultat

| Statut de preuve | Nombre | Justification |
|---|---:|---|
| PREUVE_A (objet métier existant, compatible, sans contradiction) | **0** | Aucun objet Lot 5 / trésorerie n'existe (§2) |
| PREUVE_B (présomption forte, reconstruction indirecte) | **0** | Aucune reconstruction possible sans objet source ; le montant, la date, la récurrence et le nom sont explicitement exclus comme preuves de nature (§9 du cadrage) |
| AMBIGU (plusieurs objets/interprétations) | **0** | Il faudrait au moins deux objets candidats ; il y en a zéro |
| **ABSENT (aucun objet disponible)** | **56** | |
| **TOTAL** | **56** | |

| Résultat de rapprochement | Nombre |
|---|---:|
| EXACT | 0 |
| PARTIEL | 0 |
| GROUPE | 0 |
| AMBIGU | 0 |
| **AUCUN** | **56** |

Somme cohérente : 0+0+0+0+56 = 56.

**Aucun test EXACT/PARTIEL/GROUPE/AMBIGU n'a pu être exercé sur données réelles** — non par échec du
moteur, mais parce qu'il n'existe aucun objet à rapprocher. Le moteur de rapprochement (exact,
partiel, groupé, ambigu, annulation, historique append-only) reste couvert par sa suite de tests sur
fixtures, exécutée cette mission (§8).

## 6. Effet sur les 56

```
56 décisions initiales
→ X résolubles par PREUVE_A : 0
→ Y restant réellement humaines : 56
X + Y = 56 ✔
```

**Aucune réduction déterministe n'est possible.** Chercher à réduire ce chiffre reviendrait à
inventer la nature économique d'un encaissement, ce qui est explicitement interdit.

### Ce que l'utilisateur doit faire (par ordre de levier)

Le travail réel n'est **pas** 56 enquêtes indépendantes. Il se décompose en :

1. **7 confirmations d'identité** (une par ligne du tableau §3), dont une seule réellement ouverte
   (`FAMILLE_UZON_A_CONTROLER`, 7 mouvements, 1 161,69 €) — les 6 autres sont des identités
   candidates à confirmer ou infirmer en bloc.
2. **Décision de nature** : pour chaque identité confirmée, indiquer la nature applicable
   (`ACOMPTE_PROPRIETAIRE`, `REMBOURSEMENT_`, `AVANCE_`, etc.). Si une même nature s'applique à tous
   les mouvements d'un propriétaire, **une seule décision couvre le bloc entier** — mais cette
   uniformité doit être affirmée par l'utilisateur, elle n'est pas déduite ici.
3. **Création des objets** dans Lot 5 (`SAISIE_AcomptesProprietaires.xlsx`) ou en trésorerie
   propriétaires, puis relance de Lot 8c pour le rapprochement automatique.

**Plancher réaliste : 7 décisions** (1 par identité, si nature uniforme par propriétaire).
**Plafond : 56** (si chaque mouvement a une nature propre). L'utilisateur tranche ; aucune
hypothèse n'est prise ici.

## 7. Prérequis technique découvert

La base applicative réelle (`05_APPLICATION/data/app.db`) est en migration **`0016`** ; la table
`mouvements_tresorerie_proprietaires` est créée en migration **`0025`**. La base réelle ne contient
d'ailleurs aucune donnée métier (seulement 49 événements d'audit et quelques artefacts de recette).

**Ce n'est pas un bug** : le mode réel n'a jamais été activé, la base réelle n'a jamais servi. Mais
c'est un **prérequis explicite** : avant toute exploitation réelle de la trésorerie propriétaires,
les migrations `0017`→`0026` devront être appliquées à la base réelle (opération standard, gatée,
non entreprise ici). Consigné pour la checklist mode réel.

## 8. Tests

0 code modifié. Tests métier ciblés sur fixtures (aucune donnée réelle touchée) :
`test_proprietaires_tresorerie_service.py`, `test_proprietaires_tresorerie_routes.py`,
`test_banques_rapprochement.py`, `test_banques_rapprochement_groupe_routes.py`,
`test_rapprochement_contrat_candidats.py`, `test_banques_candidats_service.py` — couvrent création
d'objet, validation, immutabilité du VALIDE, annulation justifiée, historique append-only,
rapprochement exact/partiel/groupé/ambigu, exclusion des candidats `PAYOUT_PLATEFORME`.

## 9. Idempotence

Pipeline Banque (`lot8a` → `lot8b` → `lot8c`) relancé une seconde fois sur la copie : sortie
**strictement identique** — 236 `CLASSE`, 222 `RAPPROCHEMENT_REQUIS`, 83 `A_ENVOYER_IA`,
166 `PAYOUT_PLATEFORME`, 56 `VIREMENT_PROPRIETAIRE_A_RAPPROCHER`. Aucun doublon créé, mêmes statuts,
mêmes montants, même file humaine.

## 10. Question comptable

Les 56 encaissements propriétaires **ne créent aucune question comptable nouvelle**. La trésorerie
propriétaires était déjà identifiée comme un mapping non arbitré (réserve connue, non bloquante), et
la migration `0025` précise qu'un mouvement **ne crée jamais automatiquement une écriture
comptable** — il devient seulement candidat au rapprochement bancaire. La question du compte
définitif se posera au moment de l'arbitrage comptable global, pas ici.

## 11. Verdict de cette partie

- **LOT 5 : FONCTIONNEL** (script, source, sortie, contrôles présents et cohérents) mais
  **NON ALIMENTÉ** (0 ligne).
- **MOUVEMENTS PROPRIÉTAIRES : 56 → 56 décisions humaines restantes** (0 résoluble par preuve),
  compressibles à **7 décisions** si l'utilisateur confirme une nature uniforme par propriétaire.
- **Aucune donnée réelle modifiée. Aucun objet créé. Aucune nature inférée.**
