# 61 — Contrat de la source Banque Lot8 (audit ciblé, recette globale — suite)

Audit du producteur de `BANQUE_LOT8_IMPORT.xlsx`, mené sur les copies et sur le code réel
(`02_TRAVAIL/lot8a_banque_import.py`), jamais sur les données réelles en écriture.

## Tableau contrat

| Élément | Attendu | Présent sur copies | Présent dans le réel | Conclusion |
|---|---|---|---|---|
| Script producteur | `02_TRAVAIL/lot8a_banque_import.py` | oui (copié) | oui | présent, jamais exécuté en réel |
| Source brute d'entrée | `01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_CreditMutuel.xlsx` | **absent** | **absent — le dossier `Banque/` n'existe pas du tout sur disque** | source utilisateur jamais fournie |
| Feuille métier attendue | `Cpt 02211 00021321603`, en-tête ligne 5, données ligne 6 | — | — | contrat documenté dans le script (§13.2-§13.4) |
| Colonnes attendues (7, ordre fixe) | Date \| Valeur \| Libellé \| Débit \| Crédit \| Solde \| Devise | — | — | export brut Crédit Mutuel standard |
| Compte concerné | `CM_02211_00021321603` (identifiant opaque déjà construit dans le script) | — | — | un seul compte, en dur dans le script |
| Sortie produite | `02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx`, 6 onglets (`BRUT_Banque`, `NORM_Banque`, `CTRL_A_CONTROLER`, `LOG_Traitement`, `REF_Cloture_Mensuelle`, `POWER_QUERY_CODE`) | **absent** (dossier `Lot8_Banque/` inexistant) | **absent** | cohérent : pas de source = pas de sortie |
| Déduplication | empreinte SHA256 `compte_id\|date_op\|date_val\|sens\|montant_centimes\|libellé_norm\|devise` (règle B9) | — | — | déterministe, pas de recalcul possible sans la source |
| Contrôles bloquants | `BANQUE_DATE_INEXPLOITABLE`, `BANQUE_LIGNE_SANS_LIBELLE`, `BANQUE_DEBIT_CREDIT_VIDES`, `BANQUE_DEBIT_CREDIT_DOUBLES`, `BANQUE_MONTANT_NON_NUMERIQUE` | — | — | contrôles internes au script, pas déclenchables sans données |
| Consommateur direct | `lot9_construire_flux.py` (`SRC_BNQ`), onglet `NORM_Banque`, filtré `type_flux_id == 'TYPE_FLUX_016'` et `statut_controle == 'VALIDE'` | vérifié (chemin identique à `OUT_FILE` de lot8a) | — | contrat Lot8→Lot9 cohérent, pas de divergence code/doc |
| Réseau requis | aucun — lecture `openpyxl` d'un fichier local, écriture `openpyxl` locale | — | — | 100 % exécutable hors ligne |

## Réponses explicites

1. **`BANQUE_LOT8_IMPORT.xlsx` est-il une source utilisateur ou une sortie Lot8 ?**
   C'est une **sortie de Lot8** (`lot8a_banque_import.py`, `OUT_FILE`), jamais saisie directement.

2. **Quel fichier brut doit être fourni à Lot8 ?**
   `01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_CreditMutuel.xlsx` — export brut du compte
   Crédit Mutuel `02211 00021321603`, feuille nommée `Cpt 02211 00021321603`, en-têtes en ligne 5,
   données à partir de la ligne 6, colonnes Date/Valeur/Libellé/Débit/Crédit/Solde/Devise (format
   export banque standard, aucune transformation avant dépôt).

3. **Une source bancaire brute existe-t-elle déjà dans les 85 fichiers copiés ?**
   **Non.** Le dossier `01_SOURCES_BRUTES/Banque/` n'existe pas physiquement sur disque dans le
   réel — il n'a donc pas pu être copié (rien à copier). Confirmé par recherche sur le nom de
   fichier et sur le dossier, dans le réel et dans les copies.

4. **Lot8 peut-il être exécuté entièrement hors réseau ?**
   **Oui.** Le script ne fait qu'ouvrir un fichier Excel local (`openpyxl`) et en écrire un autre.
   Aucun appel réseau, aucune API, aucune dépendance externe autre que le fichier brut lui-même.

5. **Pourquoi Lot8 n'a-t-il jamais été exécuté dans le réel ?**
   Parce que le fichier brut n'a jamais été déposé dans `01_SOURCES_BRUTES/Banque/` côté réel — un
   geste opérationnel humain (exporter le relevé Crédit Mutuel et le déposer au bon endroit) qui
   n'a pas eu lieu, pas un défaut applicatif ni un défaut de script.

6. **Le chemin attendu par Lot9 correspond-il encore au contrat actuel ?**
   **Oui, sans divergence.** `lot9_construire_flux.py` lit
   `02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx`, onglet `NORM_Banque` — exactement le fichier et
   l'onglet que `lot8a_banque_import.py` produit. Code, documentation (`lot8a` docstring §13.2-
   §13.4) et architecture du pipeline sont alignés.

## Conclusion

**Cas B — aucune source bancaire brute n'existe**, ni dans le réel ni dans les copies. Aucun
contournement codé, aucune donnée fabriquée. Action requise, côté utilisateur :

1. Exporter le relevé du compte Crédit Mutuel `02211 00021321603` couvrant la ou les périodes
   souhaitées, au format Excel natif de la banque (export standard, sans retraitement).
2. Déposer ce fichier sous `01_SOURCES_BRUTES/Banque/` (réel) avec un nom conforme au motif attendu
   par le script (`AAAA_MM_BRUT_Banque_CreditMutuel.xlsx`), ou adapter `BRUT_FILE`/`SHEET_METIER`
   dans `lot8a_banque_import.py` si le nom/l'onglet réel diffère — **décision humaine**, pas une
   correction de code à faire ce tour.
3. Exécuter `lot8a_banque_import.py` (puis `lot8b`/`lot8c` si le rapprochement bancaire est
   souhaité) avant toute nouvelle tentative de ré-exécution complète du pipeline aval.

**Verdict pour ce volet (au 2026-08-01) : NO GO — SOURCE BANQUE REQUISE.**

## Suite (2026-08-02) — fichier fourni, contrat incompatible

Un fichier a été déposé au chemin attendu :
`01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_CreditMutuel.xlsx` (141 986 octets, SHA256
`a84c9b51b1c0eb50d17216272bd3c6cf2669d159bf7e1299c2b762face0ca4a8`, copié dans l'environnement de
copies avec hash identique vérifié). Reproduction réelle sur la copie (jamais sur le réel) :

```
[OK] Source brute : ...\SOURCES_COPIEES\01_SOURCES_BRUTES\Banque\2026_03_BRUT_Banque_CreditMutuel.xlsx
[ERREUR BLOQUANT] Feuille "Cpt 02211 00021321603" absente.
  Feuilles disponibles : ['Synthese', 'Mouvements', 'Mensuel', 'Controles', 'Sources']
EXITCODE=1
```

`BANQUE_LOT8_IMPORT.xlsx` **n'a pas été produit** — le script sort avant l'écriture. Aucune donnée
inventée, aucune ancienne sortie réutilisée, aucun contournement.

### Tableau de contrat (fichier réellement fourni)

| Élément | Attendu (`lot8a_banque_import.py`) | Trouvé dans le fichier fourni | Statut | Action |
|---|---|---|---|---|
| Nom de fichier / chemin | `01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_CreditMutuel.xlsx` | identique | **OK** | — |
| Feuille métier | `Cpt 02211 00021321603` | absente — feuilles présentes : `Synthese`, `Mouvements`, `Mensuel`, `Controles`, `Sources` | **INCOMPATIBLE** | ne pas renommer une feuille pour faire matcher — décision humaine sur le format à fournir |
| Compte (RIB) | `02211 00021321603` | **identique** — RIB `10278 02211 00021321603` cité dans `Synthese` (« C/C EUROCOMPTE PRO WONDERBNB ») | OK (même compte, tiers confirmé) | — |
| En-tête ligne 5 / données ligne 6 | ligne 5 = en-têtes, ligne 6 = première donnée | dans `Mouvements` : ligne 5 = en-têtes, ligne 6 = première donnée (coïncidence structurelle) | partiellement compatible | insuffisant seul — le nom de feuille bloque avant même de lire les colonnes |
| Colonnes (7, ordre fixe Date\|Valeur\|Libellé\|Débit\|Crédit\|Solde\|Devise) | 7 colonnes | `Mouvements` a **12 colonnes** : `N°`, `Date opération`, `Date de valeur`, `Libellé`, `Débit`, `Crédit`, `Montant net`, `Solde consolidé`, `Devise`, `Source du relevé`, `Mois`, `Ligne source` | **INCOMPATIBLE** | format différent, pas un export brut CM direct |
| Nature du fichier | export brut banque, non retraité | **rapport consolidé** — titre `"Relevé bancaire consolidé — WONDERBNB"`, note explicite : *« ancien consolidé retenu jusqu'au 31/05/2026, puis relevé du 01/08/2026 prioritaire »* | **INCOMPATIBLE** | ce fichier fusionne déjà plusieurs relevés et une logique de priorité — exactement le type de source dérivée que la mission interdit de traiter comme un export brut |
| Période | nominal `2026-03` (nom de fichier) | couvre `03/11/2025` → `01/08/2026` (9 mois) | **INCOMPATIBLE** (secondaire, masqué par le blocage sheet) | cohérent avec la nature consolidée du fichier |

### Réponse

Ce fichier est un **rapport consolidé** (probablement produit par un tiers outil ou une
compilation manuelle de plusieurs relevés Crédit Mutuel), pas l'**export brut** que
`lot8a_banque_import.py` attend en entrée. Le compte est le bon (RIB identique), mais la structure
ne correspond pas au contrat : ni le nom de feuille, ni le nombre de colonnes, ni la nature du
fichier (déjà consolidé/dédoublonné avec une logique de priorité entre deux sources) ne
correspondent à un export brut CM à traiter tel quel.

**Aucune correction de `lot8a_banque_import.py` n'a été appliquée** pour accepter ce format — cela
reviendrait à modifier le contrat uniquement pour s'adapter à un fichier absent du contrat
d'origine, explicitement interdit par la mission. Aucune tentative de renommer une feuille, de
réordonner des colonnes ou de convertir le fichier n'a été faite.

### Verdict pour ce fichier : NO GO — SOURCE BANQUE INCOMPATIBLE

Action requise, côté utilisateur — deux options, décision humaine :

1. **Fournir l'export brut natif** du compte Crédit Mutuel `02211 00021321603` (le fichier tel que
   téléchargé depuis l'espace bancaire en ligne, sans retraitement), au format attendu par
   `lot8a_banque_import.py` (feuille `Cpt 02211 00021321603`, 7 colonnes) ; ou
2. **Décider explicitement** d'adapter `lot8a_banque_import.py` pour consommer ce format consolidé
   à la place — un changement de contrat métier, pas une correction de bug, à trancher humainement
   avant toute implémentation (hors mandat de cette mission).
