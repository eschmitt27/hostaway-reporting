# 21 - Arbitrage semantique statuts et impacts

## Perimetre

Audit cible exclusivement sur :

1. Lot6d `statut_controle = INFO`.
2. Lot11 `impact_facture`.
3. `REF_Statuts`.

Aucune correction code, Excel, test, Power Query, Power BI ou cadrage n'a ete appliquee.

## 1. Lot6d - `statut_controle = INFO`

### Table exacte

Fichier : `02_TRAVAIL/Lot6d_Rapprochement_Menages/MASTER_CTRL_Rapprochement_Menages.xlsx`.

Feuilles observees :

- `TABLEAU_COMPARAISON` : colonne `statut_controle` avec `VALIDE`, `A_CONTROLER`, `INFO`.
- `RESUME_APPARTEMENT` : colonne `statut_controle` avec `VALIDE`, `A_CONTROLER`.
- `RESUME_INTERVENANT` : colonne `statut_controle` avec `VALIDE`, `A_CONTROLER`.
- `CONTROLES` : colonne `niveau` avec `INFO`, `A_CONTROLER`.

Valeurs reelles observees :

- `TABLEAU_COMPARAISON.statut_controle` : `VALIDE` = 11, `A_CONTROLER` = 6, `INFO` = 1.
- `TABLEAU_COMPARAISON.code_controle` : vide = 11, `MENAGE_PRESTATAIRE_ECART_HOSTAWAY` = 4, `MENAGE_TOTAL_ECART_HOSTAWAY` = 2, `TASK_NON_ASSIGNEE_HISTORIQUE_IGNOREE` = 1.
- `CONTROLES.niveau` : `INFO` = 2, `A_CONTROLER` = 1.

### Producteur

Script producteur : `02_TRAVAIL/lot6d_rapprochement_menages.py`.

Logique productrice identifiee :

- `VALIDE` : ecart nul entre tasks Hostaway et menages declares.
- `A_CONTROLER` : ecart, assignee non mappe, tache future sans intervenant, M04 non alimente ou volume facture superieur au volume Hostaway.
- `INFO` : cas `iid == "NON_ATTRIBUE"` sur mois historique cloture ; code `TASK_NON_ASSIGNEE_HISTORIQUE_IGNOREE`, commentaire "Tasks non assignees (historique), ignorees pour blocage".

### Consommateurs

Consommateurs actifs identifies :

- `02_TRAVAIL/lot13_export_powerbi.py` exporte les colonnes de rapprochement menages incluant `statut_controle`.
- Documentation active et journaux de controles mentionnent les repartitions par `statut_controle` du Lot6d.
- Les fichiers de sortie Lot6d sont des sorties de controle/rapprochement, pas des sources economiques directes.

Aucun consommateur actif ne prouve que `INFO` doit etre traite comme `VALIDE`.

### Signification reelle de `INFO`

`INFO` ne signifie pas "ligne validee". Dans Lot6d, `INFO` signifie : controle informatif ou exception historique ignoree pour blocage, notamment une task Hostaway non assignee sur un mois deja cloture.

Ce n'est pas un etat de traitement canonique. C'est une gravite/resultat de controle indiquant "non bloquant, conserve pour trace".

### Signification reelle des autres valeurs

- `VALIDE` : resultat de rapprochement sans ecart ; etat de traitement compatible avec `statut_traitement = VALIDE`.
- `A_CONTROLER` : resultat de rapprochement a examiner ; compatible avec `statut_traitement = A_CONTROLER`, mais la gravite devrait etre portee par un champ separe.
- `INFO` : niveau informatif / resultat de controle non bloquant ; non compatible avec `statut_traitement`.

### Conclusion Lot6d

`statut_controle` dans Lot6d melange au moins deux axes :

- etat de traitement (`VALIDE`, `A_CONTROLER`) ;
- niveau ou resultat informatif (`INFO`).

Migration globale vers `statut_traitement` interdite tant que `INFO` reste dans cette colonne. Modele cible recommande pour Lot6d :

- `statut_traitement` : `VALIDE` ou `A_CONTROLER` selon l'action requise ;
- `niveau_anomalie` : `INFO` ou `WARNING` selon la gravite ;
- `code_controle` ou `code_anomalie` : cause precise ;
- eventuellement `decision_controle` ou `statut_rapprochement` pour le resultat propre au rapprochement.

## 2. Lot11 - `impact_facture`

### Producteurs et consommateurs

Producteur central : `02_TRAVAIL/lib_controls.py` via `default_impact_facture`.

Producteur applicatif : `02_TRAVAIL/lot11_controles_coherence.py`, fonction `_ctrl`, qui ajoute `impact_facture = default_impact_facture(severity, impact_facture)`.

Consommateurs actifs :

- `lib_controls.control_targets_invoice` : un controle transverse sans mois ne cible une facture que si `impact_facture == BLOQUANT_FACTURE`.
- `lib_controls.facture_control_counts` : compte les blocages et controles selon `severity` et `impact_facture`.
- `lot11_controles_coherence.py` dashboard : `nb_bloquants`, `nb_a_controler`, `facturation_lot12_ok`.
- Tests `tests/test_controls.py`.

Observation importante : la sortie existante `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` lue en audit ne contient pas encore la colonne `impact_facture`, bien que le code courant la produise. Les valeurs ci-dessous sont donc les valeurs codifiees dans les producteurs/consommateurs actifs, pas seulement les valeurs presentes dans le classeur existant.

### Matrice semantique

| Valeur Lot11 | Signification metier | Effet facture/prefacture | Effet traitement ligne | Effet gravite | Equivalent statut_traitement | Equivalent niveau_anomalie | Equivalent impact_facture canonique |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `BLOQUANT_FACTURE` | Controle qui interdit la facture tant qu'il reste ouvert. Par defaut produit quand `severity = BLOQUANT`. | Bloque la facture ou prefacture ciblee ; pour un controle sans mois, il peut cibler une facture seulement s'il est bloquant facture. | La ligne/controle doit etre traite avant facturation ; pas un statut de ligne economique. | Generalement `BLOQUANT`. | Pas `VALIDE`; plutot `A_CONTROLER` si action humaine, mais le blocage doit rester dans `niveau_anomalie = BLOQUANT`. | `BLOQUANT`. | Probablement `OUI` si l'axe signifie "a un impact sur la facture" ; probablement `NON` si l'axe signifie "facture autorisee". Decision humaine requise avant mapping. |
| `NON_BLOQUANT_FACTURE` | Controle informatif qui ne doit pas bloquer la facture. Par defaut produit quand `severity = INFO`. | N'empeche pas la facture ; exclu des controles qui bloquent la cible facture. | Aucun traitement bloquant requis ; conserve pour trace. | `INFO`. | `VALIDE` possible seulement pour l'eligibilite de facturation, mais ce serait ambigu ; mieux separer du statut de traitement. | `INFO`. | Probablement `NON` si l'axe signifie "impact sur facture" ; probablement `OUI` si l'axe signifie "facture autorisee". Decision humaine requise. |
| `A_DECIDER` | Cas non tranche ou explicitement force a arbitrer. Valeur par defaut pour toute severity autre que `BLOQUANT` ou `INFO`, ou impact inconnu. | La facture ne devrait pas etre autorisee automatiquement ; le dashboard la compte avec les `A_CONTROLER` si ce n'est pas `NON_BLOQUANT_FACTURE`. | Traitement humain requis avant decision finale. | Anciennement souvent associe a `A_CONTROLER`, qui n'est pas une gravite canonique. | `A_CONTROLER`. | `WARNING` si l'ancien niveau etait `A_CONTROLER`; a confirmer pour certains controles. | `A_CONTROLER`. |

### Conclusion Lot11

`impact_facture` Lot11 ne porte pas seulement un impact oui/non. Il encode une decision de blocage facture : bloque, ne bloque pas, a arbitrer.

Il ne faut pas le mapper automatiquement vers le champ canonique `impact_facture = OUI/NON/A_CONTROLER` tant que le sens exact de `OUI`/`NON` n'est pas verrouille :

- `OUI` = la ligne a un impact sur la facture ; ou
- `OUI` = la facture est autorisee ; ou
- `OUI` = le controle doit etre pris en compte dans la decision facture.

Modele cible recommande pour Lot11 :

- `niveau_anomalie` : `INFO`, `WARNING`, `BLOQUANT` ;
- `statut_traitement` : `VALIDE` ou `A_CONTROLER` pour le controle ouvert/traite, si pertinent ;
- `impact_facture` canonique : uniquement apres clarification du sens ;
- champ derive possible : `blocage_facture` ou `decision_facturation` si le besoin metier est bien de porter "bloque / non bloquant / a decider".

## 3. REF_Statuts

### Structure observee

Fichier : `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm`.

Onglet : `REF_Statuts`.

Colonnes :

- `statut_id`
- `famille_statut`
- `statut`
- `ordre_affichage`
- `actif`
- `commentaire`

Lignes observees : 29.

Familles observees :

- `import` : 7 lignes.
- `rapprochement` : 4 lignes.
- `anomalie` : 4 lignes.
- `facture` : 5 lignes.
- `statut_controle` : 6 lignes.
- `niveau_anomalie` : 3 lignes.

Valeurs `statut` observees :

`A_IMPORTER`, `IMPORTE`, `A_CONTROLER`, `CONTROLE`, `CORRIGE`, `REJETE`, `ARCHIVE`, `NON_RAPPROCHE`, `RAPPROCHE_AUTO`, `RAPPROCHE_MANUEL`, `ECART_A_ANALYSER`, `OUVERTE`, `EN_COURS`, `CORRIGEE`, `IGNOREE_VALIDEE`, `A_EMETTRE`, `EMISE`, `PAYEE`, `IMPAYEE`, `ANNULEE`, `VALIDE`, `BLOQUANT`, `IGNORE_JUSTIFIE`, `EXCLU_RESULTAT`, `A_VENTILER`, `INFO`.

### Types de statuts melanges

`REF_Statuts` melange des domaines distincts, mais dispose deja d'un axe de separation : `famille_statut`.

Domaines representes :

- import / cycle d'import ;
- rapprochement bancaire ou rapprochement metier ;
- anomalie / resolution ;
- facturation ;
- traitement/controle de ligne ;
- niveau d'anomalie.

### Scripts consommateurs et role dans validations

Consommateurs actifs identifies par scan :

- scripts de lots avec `statut_controle` : lots 4bis, 5, 6b, 6c, 6d, 6e, 6f, 7, 8a, 8b, 9, 10, 11, 13 ;
- `lot5_master_acomptes_proprietaires.py` cree une validation de donnees Excel sur `statut_controle` ;
- `lot5` et plusieurs Power Query filtrent `statut_controle = VALIDE` ;
- `lot13_export_powerbi.py` exporte des colonnes de statut vers Power BI ;
- la documentation active indique que `REF_Statuts` sert de valeurs fermees pour plusieurs familles.

Risque principal : si les validations ne filtrent pas par `famille_statut`, une meme valeur textuelle peut etre interpretee avec un role different selon table.

### Option recommandee

Option C, avec durcissement obligatoire.

La structure actuelle possede deja un axe de domaine suffisant (`famille_statut`). Elle peut etre conservee si et seulement si les validations et consommateurs utilisent toujours le couple :

`famille_statut` + `statut`

et jamais seulement `statut` quand plusieurs domaines coexistent.

Corrections de fond a prevoir dans un lot ulterieur :

- retirer `A_CONTROLER` de la famille `niveau_anomalie` et le remplacer par `WARNING` ;
- garder `BLOQUANT` uniquement comme niveau d'anomalie, pas statut de traitement actif ;
- verifier que `STAT_022` inactif ne reste pas propose dans les validations ;
- separer ou renommer les colonnes applicatives quand elles portent un role specifique : `statut_rapprochement`, `statut_facturation`, `statut_resolution`, `statut_traitement`.

Option B reste possible a plus long terme si les validations Excel/Power Query ne permettent pas de filtrer proprement par `famille_statut`, mais elle augmenterait la maintenance des referentiels.

## Decisions humaines necessaires

1. Definir le sens exact de `impact_facture` canonique : impact sur facture, autorisation de facturer, ou prise en compte dans decision facture.
2. Decider si Lot11 doit conserver un champ separe `blocage_facture` / `decision_facturation` en plus de `impact_facture` canonique.
3. Requalifier Lot6d `INFO` : `niveau_anomalie = INFO` avec `statut_traitement = VALIDE`, ou ligne de controle separee sans statut de traitement.
4. Valider que `REF_Statuts` doit rester une table unique par `famille_statut` et non exploser en referentiels multiples.
5. Valider la substitution cible : `niveau_anomalie A_CONTROLER` vers `WARNING` dans tous les domaines qui utilisaient A_CONTROLER comme gravite.

## Modele cible propose par domaine

### Traitement de ligne economique

- `statut_traitement` : `VALIDE`, `A_CONTROLER`, `EXCLU_RESULTAT`, `A_VENTILER`.
- `code_anomalie` : cause precise si non valide.
- `niveau_anomalie` : `INFO`, `WARNING`, `BLOQUANT`.

### Controle / audit transversal

- `statut_resolution` : `OUVERT`, `EN_COURS`, `CORRIGEE`, `IGNOREE_VALIDEE`.
- `niveau_anomalie` : `INFO`, `WARNING`, `BLOQUANT`.
- `code_controle` ou `code_anomalie` : cause precise.
- `decision_facturation` ou `blocage_facture` : si le controle pilote directement Lot12.

### Facturation

- `statut_facturation` : `A_EMETTRE`, `EMISE`, `PAYEE`, `IMPAYEE`, `ANNULEE`.
- `impact_facture` canonique : uniquement apres definition du sens de `OUI/NON/A_CONTROLER`.

### Rapprochement

- `statut_rapprochement` : `NON_RAPPROCHE`, `RAPPROCHE_AUTO`, `RAPPROCHE_MANUEL`, `ECART_A_ANALYSER`.
- `statut_traitement` separe si la ligne doit etre incluse/exclue d'un calcul.

### Import

- `statut_import` : `A_IMPORTER`, `IMPORTE`, `CONTROLE`, `CORRIGE`, `REJETE`, `ARCHIVE`.

### REF_Statuts

- Conserver `REF_Statuts` avec `famille_statut` obligatoire.
- Les validations doivent pointer vers une famille donnee, pas vers toute la colonne `statut`.
- Renommer les colonnes applicatives selon le role metier si un meme nom porte plusieurs axes.
