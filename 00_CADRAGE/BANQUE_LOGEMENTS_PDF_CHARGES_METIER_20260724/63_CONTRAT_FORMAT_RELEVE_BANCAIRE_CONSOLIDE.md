# 63 — Contrat du format « relevé bancaire consolidé » (Lot8, deux adaptateurs)

Décision : Lot8 conserve son import historique de l'export natif Crédit Mutuel **et** accepte en
plus le format « relevé bancaire consolidé » fourni. Aucun contrat remplacé — deux adaptateurs
convergent vers le même `raw_rows` canonique (7 colonnes), puis la même normalisation/dédoublonnage/
écriture déjà existante traite les deux indifféremment.

## 1. Audit de provenance (mené sur copie, jamais sur le réel)

| Élément | Constat | Preuve | Risque | Décision |
|---|---|---|---|---|
| Fichiers source du consolidé | 3 exports bruts successifs du même compte, téléchargés à des dates différentes | Onglet `Sources` : `comptes_4_complet_lignes_releve(1).xlsx` (relevé du 02/06/2026, période 03/11/2025→01/06/2026), `comptes (6).xlsx` (relevé du 02/07/2026), `comptes (7).xlsx` (relevé du 01/08/2026) | aucun — ce sont des exports bruts, pas des sorties applicatives | poursuivre |
| Circularité | Le consolidé ne provient ni de `BANQUE_LOT8_IMPORT.xlsx`, ni d'aucune sortie Lot9+ | Les 3 fichiers cités dans `Sources` ne portent aucun nom de sortie du pipeline (`BANQUE_LOT8_IMPORT`, `MASTER_CALC_*`, etc.) ; noms génériques d'export bancaire (`comptes*.xlsx`) | aucun | **NON CIRCULAIRE — poursuivre** (pas de `NO GO — SOURCE BANCAIRE CIRCULAIRE`) |
| Méthode de fusion | Priorité à la source la plus récente sur chaque zone de recouvrement, doublons retirés et comptés | `Sources` : « la source la plus récente prévaut dans chaque zone de recouvrement » ; 644 lignes lues, 541 retenues, 103 doublons retirés (détail par fichier) | aucun — méthode documentée et vérifiable | poursuivre |
| Cohérence interne | Raccordement de solde vérifié entre les 3 sources, solde final confirmé | `Controles` : solde au 31/05/2026 = 1 014,08 € (recoupé entre 2 sources), solde final 525,03 € au 01/08/2026 « reconstitution sans écart » | aucun | poursuivre |
| Grain de la feuille `Mouvements` | Une ligne = un mouvement bancaire canonique (pas d'agrégat) | 541 lignes dans `Mouvements`, colonnes `Débit`/`Crédit`/`Montant net`/`Solde consolidé` par ligne — cohérent avec une écriture ligne à ligne, pas une vue résumée | aucun | confirmé par l'exécution réelle : 541 mouvements produits dans `BANQUE_LOT8_IMPORT.xlsx`, totaux identiques à `Synthese` |
| Rôle de Synthese/Mensuel/Controles/Sources | Vues dérivées uniquement (totaux, période, méthode, contrôles) — aucune ne porte de mouvement individuel | Colonnes de ces 4 feuilles : agrégats mensuels ou textuels, jamais une ligne par opération avec Débit/Crédit propres | aucun | confirmé : seule `Mouvements` est lue comme source économique (§5 du contrat, respecté dans le code) |

**Conclusion de l'audit : source non circulaire, provenance vérifiable, méthode de fusion déjà
auditée par le fichier lui-même.** Poursuite autorisée vers l'implémentation.

## 2. Modèle canonique (inchangé — `raw_rows`, 7 colonnes)

Aucune nouvelle colonne inventée : le modèle canonique déjà en place (celui que le format natif
alimente depuis toujours) couvre le besoin. Les deux adaptateurs produisent la même structure
avant de rejoindre la normalisation existante (`NORM_Banque`, 23 colonnes, inchangée).

| Canonique (`raw_rows[i]`) | Crédit Mutuel natif | Consolidé/`Mouvements` | Transformation |
|---|---|---|---|
| `date_brute` | colonne `Date` | colonne `Date opération` | aucune (déjà `datetime`) |
| `valeur_brute` | colonne `Valeur` | colonne `Date de valeur` | aucune |
| `lib_brut` | colonne `Libellé` | colonne `Libellé` | aucune |
| `deb_brut` | colonne `Débit` (vide si inactif) | colonne `Débit` (**0** si inactif) | **`0` → `None`** — sans cette conversion, `to_float(0)` renvoie `(0.0, True)` et les deux côtés semblent « renseignés », déclenchant à tort `BANQUE_DEBIT_CREDIT_DOUBLES` sur chaque ligne |
| `cre_brut` | colonne `Crédit` (vide si inactif) | colonne `Crédit` (**0** si inactif) | idem, symétrique |
| `sol_brut` | colonne `Solde` | colonne `Solde consolidé` | aucune (jamais utilisé par la normalisation, recopié tel quel dans `BRUT_Banque`) |
| `dev_brut` | colonne `Devise` | colonne `Devise` | aucune (`EUR` dans les deux cas) |
| *(hors raw_rows)* `commentaire` (colonne 23, existante, jamais utilisée par le natif) | toujours `None` | colonne `Source du relevé` (ex. « Relevé du 02/06/2026 ») | traçabilité de provenance réutilisant une colonne déjà existante, jamais une colonne inventée |
| *(hors raw_rows)* `format_source` (nouvelle colonne, `LOG_Traitement` uniquement) | `CREDIT_MUTUEL_NATIF` | `RELEVE_CONSOLIDE` | ajout d'une seule colonne, en fin de feuille de log (positions 1-13 inchangées, aucun consommateur `lot8b`/`lot8c` cassé — ils lisent par position ≤13) |

Colonnes non retenues du format consolidé (`N°`, `Montant net`, `Mois`, `Ligne source`) : purement
dérivées des colonnes déjà mappées (`Montant net = Crédit − Débit`, `Mois` dérivable de la date) —
aucune perte d'information, pas de duplication de calcul.

## 3. Détection de format (jamais par nom de fichier)

```
FORMAT_CREDIT_MUTUEL_NATIF : feuille "Cpt 02211 00021321603" présente
FORMAT_RELEVE_CONSOLIDE    : {"Mouvements", "Controles", "Sources"} ⊆ feuilles présentes
FORMAT_INCONNU             : ni l'un ni l'autre → BLOQUANT explicite, sys.exit(1)
```

Le nom de fichier (`2026_03_BRUT_Banque_CreditMutuel.xlsx`) n'intervient jamais dans la détection.
Le contrôle `BANQUE_FICHIER_PERIODE_INCOHERENTE` — **déjà présent avant cette mission** (`CTR-9`
n'est pas concerné ici, c'est un contrôle Lot8a natif) — reste `A_CONTROLER` et non `BLOQUANT` :
le fichier consolidé fourni couvre 9 mois (03/11/2025→01/08/2026), largement hors du mois nominal
`2026-03` du nom de fichier — signalé, jamais bloquant, jamais fabriqué comme conforme.

## 4. Règles spécifiques au format consolidé (respectées)

- Seule `Mouvements` génère des mouvements canoniques (`lire_consolide()` ne lit que cette feuille).
- `Synthese`/`Mensuel`/`Controles`/`Sources` ne sont jamais lues comme source de mouvements.
- Conversion `0 → None` sur Débit/Crédit (cf. tableau ci-dessus) — sans elle, 100 % des lignes
  consolidées auraient été signalées `BANQUE_DEBIT_CREDIT_DOUBLES` à tort.
- Dédoublonnage : même moteur que le format natif (empreinte SHA256 déterministe sur
  `compte_id|date_op|date_val|sens|montant_centimes|libellé_norm|devise`) — un doublon résiduel
  que la consolidation du fichier lui-même n'avait pas éliminé (103 retirés côté fichier, mais 1
  supplémentaire trouvé par le moteur Lot8a) a été détecté et signalé, jamais masqué ni soustrait
  silencieusement du total.

## 5. Preuve d'exécution réelle sur copie

`lot8a_banque_import.py` exécuté deux fois sur la copie du relevé fourni
(`_RECETTES_GLOBALES/RECETTE_GLOBALE_20260801_004232/SOURCES_COPIEES/`) :

| Indicateur | Run 1 | Run 2 (idempotence) |
|---|---|---|
| Format détecté | `RELEVE_CONSOLIDE` | `RELEVE_CONSOLIDE` |
| Mouvements retenus | 541 | 541 |
| BLOQUANT | 0 | 0 |
| A_CONTROLER | 1 (`BANQUE_FICHIER_PERIODE_INCOHERENTE`) | 1 (identique) |
| Doublons détectés | 1 | 1 (même mouvement) |
| Total débit | 51 744,37 € | 51 744,37 € |
| Total crédit | 52 148,21 € | 52 148,21 € |
| Identifiants (`mouvement_id`) | — | identiques au run 1 |

Totaux **identiques au centime près** aux totaux propres du fichier (`Synthese` : « Total débits
51 744,37 » / « Total crédits 52 148,21 »). Fichier original jamais modifié (hash inchangé,
vérifié). `BANQUE_LOT8_IMPORT.xlsx` réellement produit dans l'environnement de copies.

## 6. Tests

`tests/test_lot8a_banque_import.py` (nouveau, 11 tests, fixtures entièrement fictives, aucune
donnée bancaire réelle) : format natif historique (régression), format consolidé (détection,
sortie canonique, conversion 0→None, totaux, multi-mois A_CONTROLER, doublon détecté non masqué,
idempotence, consolidé incomplet → INCONNU), format inconnu, fichier absent, confidentialité
stdout (aucun RIB/IBAN brut affiché). Suite complète du dossier `tests/` (moteur, 02_TRAVAIL) :
**262 passés, 0 échec** (incluait déjà 251 tests avant cette mission). Suite ciblée Banque de
l'application (`05_APPLICATION/tests/test_banques*.py`, `test_banque_controle*.py`,
`test_controles_runner_validation_positive.py`) : **58 passés, 17 ignorés, 0 échec** — aucune
régression, l'application lit `BANQUE_LOT8_IMPORT.xlsx` par nom de colonne, jamais affectée par la
colonne `format_source` ajoutée en fin de `LOG_Traitement`.

## Décision documentée

Lot8 accepte désormais deux formats d'entrée (natif historique + consolidé), produit un seul
format canonique (`BANQUE_LOT8_IMPORT.xlsx`, structure `NORM_Banque` inchangée). Format natif :
comportement 100 % préservé (11e test de régression). Format consolidé : nouveau, vert, idempotent,
totaux vérifiés identiques à la source.
