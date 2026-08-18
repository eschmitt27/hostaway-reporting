# 95 — Migration Excel → SQLite : inventaire et état

Inventaire **mesuré**, pas déclaré : produit en croisant les fichiers présents, les scripts qui les
écrivent, et les modules applicatifs qui les nomment.

**54 fichiers** Excel/CSV hors archives, données de recette et artefacts runtime.

---

## 1. Synthèse

| Classe | Nombre | Cible d'architecture |
|---|---|---|
| `SOURCE_EXTERNE` | 1 | **Reste** — import ponctuel autorisé |
| `TABLE_SQLITE_SOURCE` | 1 | Migré en base (0029), **encore lu en Excel** |
| `SAISIE_INTERNE` | 7 | À migrer — saisie doit passer par l'application |
| `CALCUL_INTERMEDIAIRE` | 29 | À migrer — tables dérivées SQLite |
| `EXPORT_OPTIONNEL` | 14 | Doivent devenir terminaux |
| `A_CLASSER` | 2 | À qualifier |

---

## 2. Le constat qui compte

La migration du référentiel est faite **au niveau du stockage** et pas encore **au niveau de la
consommation**.

`REF_Setup.xlsm` est importable dans les 28 tables `ref_*` depuis la migration 0029. Il reste
pourtant lu par **18 modules applicatifs**. Le repository `ref_setup_repo`, lui, n'a **qu'un seul**
consommateur : l'écran d'import qui l'a créé.

Autrement dit : la base contient le référentiel, personne ne s'en sert.

Même écart côté exports. `03_EXPORTS/PowerBI/` est censé être terminal (§19). Cinq de ces CSV sont
lus par l'application :

| Export | Modules applicatifs qui le lisent |
|---|---|
| `PBI_Referentiel_Logements.csv` | 4 |
| `PBI_Referentiel_Gestion_Logements.csv` | 2 |
| `PBI_Referentiel_Proprietaires.csv` | 1 |
| `PBI_Controles_Ouverts.csv` | 1 |
| `PBI_Flux.csv` | 1 |

C'est la boucle interdite par le §39 : **SQLite → Excel/CSV → application**. Elle est transitoire et
documentée depuis le correctif `/logements`, mais elle existe toujours.

---

## 3. `SOURCE_EXTERNE` — conforme à la cible

| Fichier | Producteur | Git |
|---|---|---|
| `01_SOURCES_BRUTES/Banque/BANQUE_ACTUELLE_HISTORIQUE_2025-11-03_2026-08-01.xlsx` | tiers (banque) | non |

Seul fichier légitimement Excel dans l'architecture cible. Aucun module applicatif ne le lit
directement : il passe par `lot8a`.

---

## 4. `SAISIE_INTERNE` — 7 fichiers, tous à migrer

| Fichier | Lu par l'app |
|---|---|
| `SAISIE_Charges_Flux.xlsx` | 10 modules |
| `SAISIE_Charges_Impacts.xlsx` | 5 |
| `SAISIE_AirCover.xlsx` | 4 |
| `SAISIE_Ajustements_PostCloture.xlsx` | 4 |
| `SAISIE_ImputationsAirbnb.xlsx` | 4 |
| `SAISIE_AcomptesProprietaires.xlsx` | 2 |
| `SAISIE_ReservationsHorsHostaway.xlsx` | 2 |

Ce sont des **interfaces de saisie Excel**, explicitement interdites par la règle d'architecture.
`SAISIE_AcomptesProprietaires` est le cas le plus lourd : son passage vers le master se fait par
**Power Query**, actualisation manuelle dans Excel, 192 lignes de code M.

---

## 5. `CALCUL_INTERMEDIAIRE` — 29 fichiers

Les masters. Tous **suivis par Git** sauf `BANQUE_LOT8_IMPORT.xlsx`.

Les plus consommés, et donc les plus structurants à migrer :

| Master | Lu par l'app | Lu par des lots |
|---|---|---|
| `BANQUE_LOT8_IMPORT.xlsx` | 9 | 0 |
| `MASTER_CALC_Flux.xlsx` (Lot9) | 7 | 2 |
| `MASTER_CALC_Resultats.xlsx` (Lot10) | 7 | 2 |
| `MASTER_CALC_NetProprietaire.xlsx` (Lot10) | 7 | 2 |
| `MASTER_CTRL_Coherence.xlsx` (Lot11) | 7 | 2 |
| `MASTER_CALC_Commissions.xlsx` (Lot10) | 6 | 2 |
| `MASTER_NORM_Declarations_Internes.xlsx` | 6 | 0 |
| `MASTER_FACT_MEN_MenagesExternes.xlsx` | 6 | 0 |
| `MASTER_FACT_Proprietaires.xlsx` (Lot12) | 5 | 2 |

La colonne « lu par des lots » est décisive : un master lu par un autre lot est une **dépendance
inter-traitements**, exactement ce que la règle d'architecture interdit. Neuf masters sont dans ce
cas.

---

## 6. `A_CLASSER`

| Fichier | Nature probable |
|---|---|
| `00_CADRAGE/APPLICATION_LOCALE/Catalogue_operations_navigation_conciergerie.xlsx` | document de cadrage, aucun consommateur — sans effet |
| `01_SOURCES_BRUTES/VRBO/IMPORT_UNIQUE_Revenus_*.csv` | import historique ponctuel — conforme au §1 alinéa 1 |

Aucun des deux n'est lu par un module applicatif ni par un lot. Ni l'un ni l'autre n'est une
dépendance opérationnelle.

---

## 7. État par lot

| Lot | Entrée | Sortie | État migration |
|---|---|---|---|
| lot8a | Excel externe | Excel | **non migré** |
| lot8b | **SQLite** (règles) + Excel | Excel | **partiellement migré** |
| lot8c | Excel | Excel | non migré |
| lot1 Hostaway | API | Excel | non migré |
| lot3 charges | Excel | Excel | non migré |
| lot4bis/4ter/4quater | Excel | Excel | non migré |
| lot5 acomptes | Excel + **Power Query** | Excel | non migré |
| lot6a→6f ménages | Excel + Google | Excel | non migré |
| lot9 flux | Excel | Excel | non migré |
| lot10 résultats | Excel | Excel | non migré |
| lot11 contrôles | Excel | Excel | non migré |
| lot12 relevés | Excel | Excel | non migré |
| lot13 export | Excel | CSV | **conforme** (export terminal) — mais l'app lit ses CSV |

**1 lot sur 13 partiellement migré** : `lot8b`, qui lit ses règles de classification en SQLite
depuis cette mission et n'écrit plus dans le référentiel.

---

## 8. Ce qui a été fait dans cette mission

| Point | État |
|---|---|
| PII réelle dans `SEED_RULES` | **corrigé** — seed 100 % synthétique, HEAD nettoyé, historique Git intact |
| `FAMILLE_UZON` dans le code | **corrigé** — disparaît avec le seed |
| `FAMILLE_UZON` dans la **donnée** réelle | **subsiste** — `REF_Setup.xlsm` → `ref_banque_regles`, règle `R_074` |
| Chargement des règles `lot8b` | **fail-closed** — sans référentiel SQLite, le lot refuse de tourner |
| Isolation des snapshots | **corrigé** — chemins lus à l'appel, plus figés à l'import |
| Inventaire Excel complet | **fait** — ce document |

---

## 9. Ce qui n'a pas été fait

Les phases §43 B à N n'ont pas été entreprises : orchestrateur, registre de runs, fraîcheur,
administration du référentiel, chaîne Banque complète en SQLite, Lot5 hors Power Query,
réservations, ménages, Lot9, Lot10, Lot11, Lot12, détachement de l'application vis-à-vis des CSV
Power BI, retrait des masters.

Aucun test de parité n'a été mené, puisqu'aucun chemin SQLite alternatif n'existe pour ces lots.

---

## 10. Ordre recommandé pour la suite

Il découle de l'inventaire, pas d'une préférence :

1. **Détacher l'application des CSV Power BI** — 5 fichiers, 9 points de lecture. C'est le plus
   petit chantier et il supprime la boucle interdite `SQLite → CSV → app`. Les données sont déjà
   en base (`ref_logements`, `ref_gestion_logements_hist`, `ref_proprietaires`).
2. **Faire consommer `ref_setup_repo` par les 18 modules** qui lisent encore `REF_Setup.xlsm`.
3. **Orchestrateur + registre de runs + fraîcheur**, sans quoi rien n'est pilotable.
4. Puis les chaînes, dans l'ordre de leur couplage : Banque, Lot5, réservations, ménages, Lot9,
   Lot10, Lot11, Lot12.

Le critère du §44 s'applique à chaque retrait : producteur SQLite, consommateur SQLite, parité
vérifiée, tests verts, UI fonctionnelle, redémarrage, environnement neuf.


---

## 11. Mise à jour — l'application est détachée des exports Power BI

### 11.1 Ce qui a changé

| Consommateur | Lisait | Lit désormais |
|---|---|---|
| `logements_service` (liste) | `PBI_Referentiel_Logements.csv` | `ref_logements` |
| `logements_service` (propriétaire) | `PBI_Referentiel_Gestion_Logements.csv` | `ref_gestion_logements_hist` |
| `logements_service` (fiche) | `REF_Setup.xlsm` | `ref_logements`, `ref_types_logements`, `ref_taux_commission`, `ref_couts_*` |
| `home` — logements | `PBI_Referentiel_Logements.csv` | `ref_logements` (parc, hors lignes techniques) |
| `home` — propriétaires | `PBI_Referentiel_Proprietaires.csv` | `ref_proprietaires` |
| `home` — réservations | `PBI_Flux.csv` | `MASTER_CALC_Flux` via `flux_unifie_reader` |
| `home` — contrôles | `PBI_Controles_Ouverts.csv` | `MASTER_CTRL_Coherence` via `controles_cloture_reader` |
| `proprietaires_reader` | `REF_Setup.xlsm` / `REF_Proprietaires` | `ref_proprietaires` |

**Lectures d'export Power BI par l'application : 9 → 0.**

Les deux compteurs du tableau de bord adossés à des masters ne sont pas un recul : ils quittent un
**export** pour une **sortie de moteur**. Ils basculeront quand le moteur écrira en SQLite. La
différence est explicite dans le code (`_compteur_referentiel` / `_compteur_moteur`).

### 11.2 Nouvelle couche

`referentiel_service` s'intercale entre les services d'écran et `ref_setup_repo` :

    route → service métier → referentiel_service → ref_setup_repo → SQLite

Chaque fonction reproduit la sémantique du lecteur Excel qu'elle remplace, y compris l'absence de
tri des taux et la notion volontairement absente de taux « actuel ».

### 11.3 Fail-closed

Sans référentiel importé, les écrans affichent `REFERENTIEL_NON_INITIALISE` et renvoient vers
l'écran d'import. Aucun repli sur le classeur. « Référentiel absent » et « référentiel vide »
restent deux messages distincts.

### 11.4 Parité vérifiée

| Contrôle | Ancien chemin | Nouveau chemin |
|---|---|---|
| Propriétaires réels | 12 (Excel) | 12 (SQLite) |
| Logements du parc | 17 | 17 |
| Rattachements de gestion | 17 | 17 |
| Compteur logements du tableau de bord | **19** | **17** |

Le dernier écart est **assumé** : l'ancien compteur incluait les deux lignes techniques
(`APPARTEMENT_DIVERS`, `LOGEMENT_DIVERS`) que l'écran Logements a toujours exclues. Les deux
affichages disaient des chiffres différents pour la même chose.

Les tests de `test_proprietaires.py` importent le **vrai** référentiel dans une base isolée : si
l'import perdait ou déformait une ligne, leurs compteurs tomberaient.

### 11.5 Consommateurs de `REF_Setup.xlsm` restants

18 modules au début de la mission, **16 à la fin** (`logements_service` et `proprietaires_reader`
migrés). La liste est figée dans `test_non_dependance_fichiers.py` : elle doit décroître, jamais
croître.

Ils se répartissent en deux familles, à traiter par deux missions distinctes :

- **Administration du référentiel** — services qui ÉCRIVENT dans le classeur
  (`logements_creation_service`, `logements_gestion_service`, `ref_assoc_mode_prepare_service`,
  la famille `saisie_hh_*`).
- **Moteur** — services qui pilotent ou contrôlent des lots
  (`calculs_executeur_service`, `calculs_pipeline_service`, `controles_runner_service`,
  `menages_chaine_service`, `menages_recalcul_service`, `file_registry`,
  `charges_preview_service`, `charges_controles_integrite_service`,
  `controles_cloture_reader`, `proprietaires_reglements_reader`).

### 11.6 Services lisant encore un MASTER du moteur (§27)

Frontière d'entrée de la mission suivante :

`flux_unifie_reader`, `controles_cloture_reader`, `controles_detail_reader`, `menages_reader`,
`banques_reader`, `charges_reader`, `proprietaires_reader` (parties `MASTER_*`),
`proprietaires_reglements_reader`, `rapprochement_bancaire_reader`, `reservations_hh_reader`,
`run_log_reader`.

Ces lectures sont **hors périmètre** de la présente mission : elles ne concernent ni un export ni
le référentiel, mais des résultats de calcul.

### 11.7 Bugs trouvés et corrigés

1. **Fiche logement en erreur 500** — le template supposait `etat.fiche` toujours défini. Un
   logement présent en base mais absent du classeur produisait une `UndefinedError`. Corrigé, avec
   la garde placée au niveau du bloc : un `set` Jinja déclaré dans une branche `{% if %}` n'existe
   pas dans les autres, ce qui avait provoqué un second échec.
2. **Liaisons figées à l'import** — `routes/proprietaires_tresorerie.py` et
   `proprietaires_tresorerie_service.py` faisaient `from ... import find_proprietaire`. Le nom
   étant lié au chargement, toute redirection du référentiel restait sans effet. Même famille que
   le défaut `snapshot_service` corrigé précédemment.

## 12. Mise à jour — Banque et Lot 5 sont sortis d'Excel

### 12.1 Classement des sources concernées

| Fichier | Classement | Lu par le runtime |
|---|---|---|
| `BANQUE_ACTUELLE_HISTORIQUE_*.xlsx` (relevé reçu de la banque) | `SOURCE_EXTERNE` | à l'import uniquement |
| `BANQUE_LOT8_IMPORT.xlsx` | `LEGACY_PARITE_TEMPORAIRE` / `À_RETIRER` | **non** |
| `SAISIE_AcomptesProprietaires.xlsx` | `LEGACY_PARITE_TEMPORAIRE` / `À_RETIRER` | **non** |
| `MASTER_FACT_MAN_AcomptesProprietaires.xlsx` | `LEGACY_PARITE_TEMPORAIRE` / `À_RETIRER` | **non** |
| Power Query (5 requêtes Lot 5) | `REMPLACÉ` | — |

Le relevé bancaire reste un fichier, et c'est normal : il vient de la banque, l'application ne le
fabrique pas. Ce qui a changé, c'est qu'il n'est plus *stocké* comme vérité — il est importé, puis la
base fait foi.

### 12.2 Ce qui lit quoi, désormais

La Banque vit dans `banque_mouvements` (brut immuable), `banque_classifications` +
`banque_classification_signaux` (interprétation, par exécution), `banque_controles` (constats) et
`banque_rapprochements` (files d'attente et décisions). Les acomptes propriétaires vivent dans
`mouvements_tresorerie_proprietaires`, avec leur historique d'événements.

Une couche de vues (`banque_vues_service`) rend depuis la base les mêmes jeux de colonnes que les
onglets du classeur produisaient. Le vocabulaire du moteur est conservé volontairement : une dizaine
de routes, services et gabarits lisent ces clés, et les renommer en même temps qu'on change de source
rendrait impossible de savoir laquelle des deux modifications casse un affichage.

### 12.3 L'adaptateur moteur — ce qu'il est, ce qu'il n'est pas

`lot8c_rapprochement_banque.py` et `lot11_controles_coherence.py` lisent encore un classeur. Les
migrer relève du chantier Lot 9/10/11 ; réécrire leurs règles dans l'application produirait deux
moteurs de contrôle divergents. `banque_adaptateur_moteur` fabrique donc ce classeur **à la demande**,
depuis la base, dans un workspace jetable.

Ce fichier n'est source de vérité pour rien, n'est lu que par le sous-processus moteur, et disparaît
avec le workspace. **Il ne doit jamais devenir un MASTER.** S'il se met à être lu ailleurs, c'est que
la frontière a bougé : il faut la remettre en place, pas l'élargir.

### 12.4 Ce que la migration a révélé

Quatre défauts trouvés en portant le code, et qui existaient avant :

1. **Lot 11 conclut « Banque non disponible » avec un code retour 0** s'il manque un seul de ses
   quatre onglets bancaires — il les lit dans un même bloc protégé. Un onglet oublié ne casse rien de
   visible : il fait simplement disparaître les contrôles bancaires du rapport. La liste des onglets
   attendus est désormais relevée un par un dans la source des moteurs, et un test la vérifie.

2. **Deux définitions d'empreinte bancaire coexistaient**, sur les mêmes champs mais avec un format de
   montant différent (centimes d'un côté, décimal de l'autre). Elles ne coïncidaient jamais : un
   réimport n'était plus reconnu comme doublon dès que la comparaison changeait de côté. Une seule
   subsiste.

3. **L'empreinte ignorait le sens et la date de valeur.** Un débit et un crédit de 120 € le même jour
   sous le même libellé portaient la même empreinte — l'un signalé comme doublon de l'autre alors
   qu'ils s'annulent. Et deux prélèvements identiques valorisés à des dates différentes devenaient
   indistinguables, ce qui produisait un constat de doublon de trop sur le relevé réel.

4. **Les tables ajoutées après 0016 faisaient échouer la lecture sur la base réelle**, qui est restée
   en 0016. « no such table » transformait un état parfaitement normal en écran illisible. Les lectures
   Banque et trésorerie répondent maintenant « non initialisée », ce qui est un état, pas une panne.

### 12.5 Parité vérifiée

Banque, sur le relevé réel : 541 mouvements importés ; classification 222 `RAPPROCHEMENT_REQUIS` /
236 `CLASSE` / 83 `A_ENVOYER_IA` ; 24 `VALIDE` / 517 `A_CONTROLER` ; risques 74 / 215 / 252 ;
169 constats de contrôle avec les mêmes codes qu'au classeur ; files d'attente plateforme
166 lignes / 14 467,27 € et propriétaires 56 lignes / 27 069,18 € ; **0 rapprochement de type
`RESERVATION`** ; écart financier 0,00 €.

Lot 5 : le dataset réel est vide, donc la parité se mesure sur une fixture synthétique, contre la
règle **telle que le Lot 5 la rédige** (colonne `regle` de sa table `CONTROLS`), réencodée dans le test
sur le vocabulaire d'origine et sans partager de code avec le portage. Comparer deux sorties vides
n'aurait rien prouvé.

### 12.6 Trois contrôles Lot 5 qui ont dû être traduits

Les dix contrôles gardent leurs codes et leurs niveaux (5 bloquants, 5 à contrôler). Trois ne se
transposent pas littéralement, et le dire vaut mieux que le masquer :

- `ACOMPTE_CALC_ID_DUPLIQUE` cherchait un identifiant présent deux fois — un risque propre à une
  feuille de calcul. En base l'identifiant est UNIQUE : ce risque a disparu. Celui qui reste est le
  doublon **métier**, le même versement saisi deux fois, et c'est lui qui fausse un compte.
- `ACOMPTE_HH_INCOHERENT` comparait le montant à une réservation du Lot 4, pas encore en base. Le
  contrôle signale l'impossibilité de vérifier, au lieu de conclure « cohérent » faute de pouvoir
  comparer.
- `ACOMPTE_REPORT_INCOHERENT` visait un report sans justification ; le modèle porte une nature, pas un
  report. Même exigence, exprimée dans son vocabulaire.

Aucune nature nouvelle n'a été créée : les sept existantes suffisent. Aucune règle de cohérence
nature ↔ sens n'a été inventée — personne ne l'a décidée, et la déduire d'exemples aurait fabriqué
une règle métier.

### 12.7 Ce qui reste à faire

- Retirer les trois classeurs `LEGACY_PARITE_TEMPORAIRE` une fois la période de parité close.
- Migrer Lot 8c et Lot 11 vers SQLite, ce qui supprimera `banque_adaptateur_moteur`.
- Migrer les trois autres sources de `proprietaires_extras_reader` (AirCover, imputations Airbnb,
  ajustements post-clôture), restées des classeurs.
- Chaîne suivante : Hostaway / réservations.

## 13. Mise à jour — Hostaway et réservations sont sortis d'Excel

### 13.1 Classement des sources concernées

| Source | Classement | Lue par le runtime |
|---|---|---|
| API Hostaway | `SOURCE_API` | oui — chemin normal |
| `hostaway_extractions` / `_reservations` / `_payouts` / `_listings` / `_fees` / `_finance_fields` / `_anomalies` | `TABLE_SQLITE_SOURCE` | oui |
| `reservations_calculees` / `reservations_resolues` / `reservations_historique_cloture` | `TABLE_SQLITE_DERIVEE` | oui |
| `MASTER_FACT_HA_*.xlsx`, `MASTER_CALC_HA_Payout.xlsx`, `MASTER_REF_HA_Listings.xlsx` | `LEGACY_PARITE_TEMPORAIRE` / `À_RETIRER` | **non** |
| `MASTER_CALC_Reservations.xlsx` (Lot 4bis) | `LEGACY_PARITE_TEMPORAIRE` / `À_RETIRER` | **non** |
| `MASTER_CALC_Reservations_Resolues.xlsx` (Lot 4quater) | `LEGACY_PARITE_TEMPORAIRE` / `À_RETIRER` | **non** |
| `HIST_Reservations_Cloturees.xlsx` (Lot 4ter) | `LEGACY_PARITE_TEMPORAIRE` / `À_RETIRER` | **non** |

### 13.2 Le chemin normal

    API Hostaway → hostaway_raw_service → tables RAW → Lot 4bis → reservations_calculees
                                                     → Lot 4ter  → reservations_historique_cloture
                                                     → Lot 4quater → reservations_resolues → application

Lot 1 écrit la couche RAW **directement**, depuis les lignes que l'API vient de rendre. Il importe
`hostaway_raw_service` plutôt que de réimplémenter l'écriture — cet import ne tire ni FastAPI ni
pandas, donc il reste sans effet de bord sur un lot moteur.

La reprise depuis les masters (`hostaway_adaptateurs.reprendre`) subsiste comme **outil de migration
et de parité**. Elle n'est plus nécessaire après une actualisation normale, et son mode
(`REPRISE_EXCEL`) la distingue explicitement d'une extraction API : un classeur peut dater.

### 13.3 Réservation et payout sont deux faits

Deux tables, à dessein. Un payout peut manquer, arriver plus tard, ou être incomplet : fondu dans la
réservation, « payout absent » deviendrait indistinguable de « payout à zéro », et un revenu manquant
ressemblerait à un revenu nul. Aucun payout n'est jamais reconstruit depuis la Banque — un mouvement
bancaire ne dit pas à quelle réservation il correspond.

### 13.4 Le payload brut est conservé

`hostaway_reservations.payload_json` garde la réponse de la plateforme. Ce n'est pas de la
redondance : le repli historique de `guestCount` en dépend, et c'est la seule preuve de ce que
Hostaway a dit un jour donné.

### 13.5 Ce que la migration a révélé

Quatre défauts, tous antérieurs et tous **silencieux** — c'est ce qui les rendait dangereux.

1. **`guestCount` perdu sur payload tronqué.** Les payloads stockés dans les masters sont coupés à
   4 000 caractères, la limite d'une cellule Excel. `json.loads` échoue donc sur la plupart d'entre
   eux. Un extracteur strict aurait fait disparaître tout le repli historique — 113 valeurs — sans
   qu'aucune erreur ne le signale. Le moteur retombait déjà sur une recherche textuelle ; la même
   stratégie est reprise, pour que les deux donnent le même nombre.

2. **Identifiants textuels contre index entiers.** Les identifiants Hostaway sont stockés en texte —
   correct pour un identifiant : le comparer numériquement n'a pas de sens et un zéro non
   significatif serait perdu. Mais tous les index du moteur ont été construits sur les valeurs que le
   classeur fournissait, c'est-à-dire des entiers. La jointure réservation ↔ payout échouait donc
   sans erreur : **231 payouts paraissaient absents et 55 388,94 € disparaissaient des totaux**. Le
   type est restitué à la frontière, une seule fois, dans `lib_db_moteur`.

3. **`reservation_calc_id` positionnel.** La clé vaut `RES-<mois>-HA-<n>`, où n est un compteur
   d'itération : elle dépend de l'ORDRE de lecture. Trier par identifiant plutôt que par ordre
   d'arrivée renumérotait chaque ligne **sans changer un seul total** — les agrégats restaient justes
   et toutes les clés étaient décalées d'un cran. L'ordre d'insertion est désormais respecté par le
   moteur ET par le service applicatif. *La faiblesse du modèle de clé reste ouverte : une clé stable
   devrait dériver de l'identifiant de la réservation, pas de sa position.*

4. **Premier passage de Lot 4ter incapable de migrer l'historique.** Base vide et classeur rempli, il
   lisait l'existant dans le classeur et traitait ses 1 269 lignes comme déjà figées. La base restait
   vide **en donnant l'impression d'être à jour**. La comparaison porte désormais sur ce qui est en
   base.

### 13.6 Parité vérifiée

| Étape | Volumes | Écart |
|---|---|---|
| Lot 1 | 1 542 réservations, 1 518 payouts, 17 listings, 644 frais, 892 champs financiers, 31 anomalies | 0 |
| Lot 4bis | 1 542 lignes × 19 colonnes | 0 |
| Lot 4ter | 1 269 lignes × 26 colonnes | 0 |
| Lot 4quater | 1 542 lignes × 27 colonnes | 0 |

Montants : `montant_retenu` 322 868,56 €, `payout_calcule` 320 525,08 €, `assiette_commission`
257 971,08 € — écart 0,00 € partout. `guestCount` : sommes 3 557 (Lot 4bis) et 2 178 (Lot 4quater),
répartitions identiques.

### 13.7 Adaptateurs de transition

Lot 8c et Lot 11 lisent encore des classeurs. Ils sont **fabriqués depuis la base** dans le workspace
du run (`banque_adaptateur_moteur`, `reservations_adaptateur_moteur`, mécanique commune dans
`adaptateur_workspace`). Ces fichiers sont jetables, non canoniques, jamais lus par l'application,
jamais versionnés. Ils disparaîtront avec la migration de Lot 9/10/11.

Écrire un `MASTER_TEMP.xlsx` dans un dossier permanent serait un faux progrès : on aurait déplacé le
fichier sans supprimer la dépendance.

### 13.8 Ce qui reste à faire

- Retirer les classeurs `LEGACY_PARITE_TEMPORAIRE` une fois la période de parité close.
- Corriger le modèle de clé des réservations, aujourd'hui positionnel.
- Migrer Lot 9, Lot 10 et Lot 11, ce qui supprimera les deux adaptateurs de workspace.
- Chaîne suivante : **ménages** (les tâches de ménage Hostaway restent hors périmètre, avec leur
  limitation de débit connue).

## 14. Mise à jour — Ménages (Lot6) : moteur sorti d'Excel, application pas encore

### 14.1 Ce qui a été fait

CleaningTasks Hostaway : couche RAW SQLite (`hostaway_cleaning_tasks`, migration 0035) — API H6 non
relancée en réel cette mission (429 connus), RAW alimentée via reprise/fixtures.

PDF facture prestataire ménage externe → SQLite direct : `facture_menage_pdf_service` adapte la
sortie de `lib_menages_externes_pdf.extraire_pdf` (extracteur réel, non réécrit) vers
`factures`/`facture_lignes_menage` (0037), sans passer par le module Charges (`facture_lignes`,
0022, exige un `charge_id` résolu par un écrivain Excel encore non migré — décision explicite :
satellite plutôt que dépendance croisée). Ventilation des frais externes sans logement, contrôle
somme lignes = total facture, dette intervenant interne (FIFO, réutilise
`compte_proprietaire_service.calculer_fifo`, tables séparées du compte propriétaire) : construits et
testés (migrations 0036/0037/0039).

Lot6a→6f (moteur pandas) : chaque script accepte `--source SQLITE` en plus du chemin Excel
historique (conservé pour parité, encore nécessaire à Lot9-12 non migrés). Sorties dans
`menages_taches_enrichies`/`menages_declarations_internes`/`menages_rapprochement`/
`menages_gainperte`/`menages_cout_complet` (migration 0038). Formules et clé de ventilation
inchangées. Deux bugs pré-existants trouvés et corrigés en le faisant (`proprietaire_id` inexistant
sur `REF_Logements`, mapping intervenant en dur dans lot6f) — détail dans JOURNAL_ANOMALIES.md.

### 14.2 Ce qui a été fait ensuite — `menages_reader` : 6/8 sources en SQLite

Après une première tentative de réécriture complète annulée (34 tests cassés, cf. plus haut), une
seconde passe bascule les fonctions une par une, testée à chaque étape : `hostaway_taches()`,
`hostaway_comptage()` (dérive désormais son agrégat des tâches, 0038, au lieu d'une seconde source
indépendante potentiellement incohérente comme dans le classeur legacy), `internes()`,
`rapprochement()`, `gainperte()`, `cout_complet()` — toutes lisent `menages_taches_enrichies`/
`menages_declarations_internes`/`menages_rapprochement`/`menages_gainperte`/`menages_cout_complet`
(0038), sans repli Excel (état VIDE/FICHIER_ABSENT affiché, jamais `FileNotFoundError`).
`rapprochement_available()`/`gainperte_available()` basculées sur l'état de la source. 101 tests
verts (`test_menages.py`, `test_menages_chaine.py`, `test_menages_rapprochement.py`,
`test_menages_recalcul.py`).

Régression pré-existante corrigée au passage : plusieurs tests de route (fixture `client` seul, sans
le fixture `sources` qui isole `cfg.MASTER_*`) lisaient sans le vouloir les classeurs Excel RÉELS du
projet via les chemins par défaut — invisible tant que le lecteur retombait sur Excel. Seed SQLite
minimal ajouté.

### 14.3 Ce qui n'a PAS été fait

- `externes()` (Lot6c) : exigerait d'étendre `facture_lignes_menage` avec `date_menage`/
  `precision_date_menage` (absentes de 0037/0039).
- `controles_rapprochement()`/`controles_lot11()`/`pools_charges()` : dérivation non triviale ou
  usage marginal (vérifié par grep), non traités.
- `menages_chaine_service`/`menages_recalcul_service`/`controles_runner_service` orchestrent la
  chaîne Excel complète jusqu'à Lot9-12 (non migrés, hors périmètre explicite de cette mission) —
  les migrer isolément aurait nécessité de commencer Lot9-12.
- `controles_detail_reader` dépend de la même limite que `externes()`
  (`MASTER_FACT_MEN_MenagesExternes`).

Excel entre Lots6 : toujours présent (M04, MASTER_FACT_MEN_MenagesExternes, classeurs Lot6d/e/f
legacy — consommés par les 3 services ci-dessus). Excel application Ménages : réduit (6/8 sources du
lecteur central migrées), pas éliminé.

### 14.4 Ce qui reste à faire

- Étendre `facture_lignes_menage` pour porter `date_menage`/`precision_date_menage`, puis basculer
  `externes()`.
- Adapter `menages_chaine_service`/`menages_recalcul_service`/`controles_runner_service` à la
  frontière Lot9-12 (adaptateur SQLite → workbook jetable, même mécanique que Lot8c/Lot11 pour
  Hostaway/Banque) sans commencer la migration de Lot9-12 elle-même.
- Chaîne suivante après Ménages : **Lot9 → SQLite**, puis **Lot10 → SQLite**.
