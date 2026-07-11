# 22 - Plan migration par domaine statuts

## Decisions retenues

- `REF_Statuts` reste une table unique.
- Toute validation doit utiliser obligatoirement le couple `famille_statut + statut`.
- Un `statut` seul ne doit jamais etre recherche sans sa famille.
- `A_CONTROLER` est exclusivement un etat de traitement ou de resolution.
- `A_CONTROLER` ne doit jamais etre stocke dans `niveau_anomalie`.
- `niveau_anomalie = WARNING` remplace les usages historiques de `niveau_anomalie = A_CONTROLER`.
- `EFFET_FACTURATION` decrit uniquement l'effet d'un controle sur la possibilite de generer une facture ou prefacture.
- `code_impact` reste derive des impacts resultat reel et comptable, sans decision de facturation.

## Familles canoniques

- `STATUT_TRAITEMENT` : `VALIDE`, `A_CONTROLER`, `EXCLU_RESULTAT`, `A_VENTILER`.
- `NIVEAU_ANOMALIE` : `INFO`, `WARNING`, `BLOQUANT`.
- `STATUT_RESOLUTION` : `VALIDE`, `A_CONTROLER`, `INFO`.
- `EFFET_FACTURATION` : `BLOQUANT`, `NON_BLOQUANT`, `A_DECIDER`.
- `STATUT_FACTURATION` : `A_FACTURER`, `FACTURE`, `AVOIR_A_EMETTRE`, `AVOIR_EMIS`, `NON_CONCERNE`.
- `STATUT_RAPPROCHEMENT` : domaine rapprochement, valeurs a confirmer depuis usages actifs.
- `STATUT_IMPORT` : domaine import, valeurs a confirmer depuis usages actifs.

## Plan table par table

### 1. Lot11 controles coherence - `severity` vers `niveau_anomalie`

- Table : `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx`, feuilles `MASTER`, `BLOQUANTS_OUVERTS`, `A_CONTROLER_OUVERTS`, `DASHBOARD_MOIS`.
- Colonne actuelle : `severity`.
- Role reel : gravite du controle.
- Famille cible : `NIVEAU_ANOMALIE`.
- Colonne cible : `niveau_anomalie`.
- Mapping exact : `INFO -> INFO`, `A_CONTROLER -> WARNING`, `WARNING -> WARNING`, `WARN -> WARNING`, `AVERTISSEMENT -> WARNING`, `BLOQUANT -> BLOQUANT`.
- Producteurs : `02_TRAVAIL/lot11_controles_coherence.py`, helper `02_TRAVAIL/lib_controls.py`.
- Consommateurs : `lot11_controles_coherence.py`, `lib_controls.facture_control_counts`, `lib_controls.control_targets_invoice`, `lot12_generer_factures.py` indirectement via controles, `lot13_export_powerbi.py`, `tests/test_controls.py`.
- Power Query / Power BI concernes : exports Lot13 des controles, rapports utilisant `severity`, feuilles `BLOQUANTS_OUVERTS` et `A_CONTROLER_OUVERTS`.
- Tests necessaires : mapping deterministe de chaque ancienne valeur ; refus valeur inconnue ; absence de `A_CONTROLER` dans `niveau_anomalie` ; dashboard conserve les memes comptes fonctionnels.
- Condition de suppression ancienne colonne : aucun scan actif code/PQ/Power BI/test ne lit `severity`; les exports utilisent `niveau_anomalie`; les tests ne referencent plus `severity`.
- Risque : eleve, car `severity` pilote actuellement le blocage facture via `lib_controls`.

### 2. Tables avec `niveau_anomalie = A_CONTROLER` vers `WARNING`

- Tables : Lot4bis reservations, Lot4quater source resolue, Lot6b/6c menages, Lot7 IK/avantages, Lot8 banque, autres tables calcul/saisie portant `niveau_anomalie`.
- Colonne actuelle : `niveau_anomalie`.
- Role reel : gravite, mais contient historiquement `A_CONTROLER`.
- Famille cible : `NIVEAU_ANOMALIE`.
- Colonne cible : `niveau_anomalie` conservee.
- Mapping exact : `INFO -> INFO`, `A_CONTROLER -> WARNING`, `WARNING -> WARNING`, `BLOQUANT -> BLOQUANT`.
- Producteurs : `lot4bis_charger_reservations.py`, `lot4quater_resoudre_source_reservations.py`, `lot6b_m04_menages_internes.py`, `lot6c_menages_externes.py`, `lot7_ik_avantages.py`, `lot8a_banque_import.py`, `lot8b_banque_regles.py`, `lot9_construire_flux.py` selon colonnes propagees.
- Consommateurs : `lot10_calculer_resultats.py`, `lot11_controles_coherence.py`, `lot13_export_powerbi.py`, tests metier.
- Power Query / Power BI concernes : vues Lot13 exposant `niveau_anomalie`, filtres ou visuels sur anomalies.
- Tests necessaires : aucune generation de `niveau_anomalie=A_CONTROLER`; anciennes lignes migrent en `WARNING`; `statut_traitement=A_CONTROLER` reste possible.
- Condition de suppression ancienne colonne : aucune suppression, colonne conservee ; seule la valeur historique est decommissionnee.
- Risque : moyen, risque de confusion visuelle dans Power BI si `A_CONTROLER` et `WARNING` etaient auparavant assimiles.

### 3. Lot6d rapprochement menages - `statut_controle` vers `statut_resolution`

- Table : `02_TRAVAIL/Lot6d_Rapprochement_Menages/MASTER_CTRL_Rapprochement_Menages.xlsx`.
- Colonnes actuelles : `statut_controle`, `niveau` dans feuille `CONTROLES`.
- Role reel : resultat de controle/rapprochement, pas etat de traitement pur, car `INFO` signifie controle informatif historique ignore.
- Famille cible : `STATUT_RESOLUTION`.
- Colonne cible : `statut_resolution`.
- Mapping exact : `VALIDE -> VALIDE`, `A_CONTROLER -> A_CONTROLER`, `INFO -> INFO`.
- Producteur : `02_TRAVAIL/lot6d_rapprochement_menages.py`.
- Consommateurs : `lot13_export_powerbi.py`, documentation/journaux de controles, eventuels rapports Power BI de rapprochement menages.
- Power Query / Power BI concernes : dataset de rapprochement menages exporte par Lot13, visuels de controles menages.
- Tests necessaires : `INFO` reste informatif et n'est jamais converti en `statut_traitement`; `VALIDE/A_CONTROLER/INFO` appartiennent a `STATUT_RESOLUTION`; ancienne colonne absente apres migration complete.
- Condition de suppression ancienne colonne : Lot13 et tous consommateurs lisent `statut_resolution`; aucun scan actif ne lit `Lot6d.statut_controle`.
- Risque : moyen, surtout sur filtres aval qui comptent `statut_controle = VALIDE`.

### 4. Lot11 - `impact_facture` vers `effet_facturation`

- Table : `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` et vues derivees.
- Colonne actuelle : `impact_facture` dans le code courant ; sortie existante a verifier/regenerer car le classeur lu ne contenait pas encore la colonne.
- Role reel : effet d'un controle sur la possibilite de generer facture/prefacture.
- Famille cible : `EFFET_FACTURATION`.
- Colonne cible : `effet_facturation`.
- Mapping exact : `BLOQUANT_FACTURE -> BLOQUANT`, `NON_BLOQUANT_FACTURE -> NON_BLOQUANT`, `A_DECIDER -> A_DECIDER`.
- Producteurs : `lib_controls.default_impact_facture`, `lot11_controles_coherence._ctrl`.
- Consommateurs : `lib_controls.control_targets_invoice`, `lib_controls.facture_control_counts`, `lot11_controles_coherence.py` dashboard, `lot12_generer_factures.py` controles facture, `tests/test_controls.py`, exports Lot13.
- Power Query / Power BI concernes : dashboard controles, indicateurs facturation possible, filtres de controles bloquants.
- Tests necessaires : mapping des trois valeurs ; refus des anciennes valeurs dans la colonne cible ; conservation des comptes `nb_bloquants`/`nb_a_controler`; absence de decision facture dans `code_impact`.
- Condition de suppression ancienne colonne : aucun consommateur actif ne lit `impact_facture`; tous lisent `effet_facturation`; tests de compatibilite retires.
- Risque : eleve, car le champ pilote directement l'autorisation de facturation.

### 5. `statut_controle` dans les autres tables

- Tables : Lot4bis reservations, Lot4quater source resolue, Lot5 acomptes, Lot6b/6c menages, Lot7 IK/avantages, Lot8 banque selon table, Lot9 flux, Lot10 resultats intermediaires.
- Colonne actuelle : `statut_controle`.
- Role reel : generalement etat de traitement de ligne, mais a verifier table par table.
- Famille cible : `STATUT_TRAITEMENT` seulement si les valeurs sont strictement `VALIDE`, `A_CONTROLER`, `EXCLU_RESULTAT`, `A_VENTILER`.
- Colonne cible : `statut_traitement`.
- Mapping exact : identite pour `VALIDE`, `A_CONTROLER`, `EXCLU_RESULTAT`, `A_VENTILER` ; toute autre valeur bloque la migration de la table.
- Producteurs : lots producteurs de chaque table.
- Consommateurs : lots aval 9, 10, 11, 12, 13, Power Query de vues actives, tests.
- Power Query / Power BI concernes : filtres `[statut_controle] = "VALIDE"`, vues actives, exports PBI.
- Tests necessaires : refus valeur non canonique ; compatibilite filtres `VALIDE`; absence de l'ancienne colonne apres migration de chaque table.
- Condition de suppression ancienne colonne : tous producteurs et consommateurs d'une table utilisent `statut_traitement`, aucun scan actif `statut_controle` pour cette table.
- Risque : moyen a eleve selon table, car `statut_controle` sert souvent de filtre d'inclusion economique.

### 6. `code_impact` comme champ derive

- Tables : REF_Codes_Impact, Lot4bis, Lot6c, Lot8, Lot9, Lot10, exports Power BI.
- Colonne actuelle : `code_impact`.
- Role reel : compactage derive de `impact_resultat_reel` et `impact_resultat_comptable`.
- Famille cible : hors `REF_Statuts`; ne doit pas contenir decision de facturation.
- Colonne cible : `code_impact` conservee comme champ derive, jamais saisie separement quand les impacts canoniques existent.
- Mapping exact attendu : `impact_resultat_reel=OUI` et `impact_resultat_comptable=OUI -> IC`; `OUI/NON -> HC`; `NON/NON -> HR`; cas `A_CONTROLER` a traiter explicitement sans inventer de code comptable.
- Producteurs : lots qui calculent ou propagent les flux, notamment `lot4bis`, `lot8`, `lot9`, `lot10`.
- Consommateurs : resultats, exports Power BI, controles.
- Power Query / Power BI concernes : dimensions impact IC/HC/HR, visuels resultat reel/comptable.
- Tests necessaires : derivation centrale ; interdiction d'un code_impact divergent des impacts ; interdiction d'y stocker `BLOQUANT`, `NON_BLOQUANT`, `A_DECIDER`.
- Condition de suppression ancienne colonne : non supprimee ; elle reste derivee pour lisibilite/compatibilite.
- Risque : moyen, car champ deja largement consomme.

### 7. REF_Statuts et validations

- Table : `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm`, onglet `REF_Statuts`.
- Colonnes actuelles : `statut_id`, `famille_statut`, `statut`, `ordre_affichage`, `actif`, `commentaire`.
- Role reel : referentiel unique multi-domaines.
- Famille cible : normalisation des valeurs de `famille_statut` vers les familles canoniques majuscules.
- Colonne cible : conserver `famille_statut` et `statut`.
- Mapping familles : `statut_controle -> STATUT_TRAITEMENT`, `niveau_anomalie -> NIVEAU_ANOMALIE`, `anomalie -> STATUT_RESOLUTION` si role resolution confirme, `facture -> STATUT_FACTURATION`, `rapprochement -> STATUT_RAPPROCHEMENT`, `import -> STATUT_IMPORT`.
- Producteurs : edition referentiel Excel, scripts creant validations dans les classeurs.
- Consommateurs : validations Excel, Power Query, scripts de controle, lots de generation.
- Power Query / Power BI concernes : toutes les listes de statuts, dimensions de statuts.
- Tests necessaires : unicite `(famille_statut, statut)` ; interdiction recherche `statut` sans famille ; familles canoniques uniquement ; valeurs actives conformes par famille ; validations Excel filtrees par famille.
- Condition de suppression ancienne colonne : aucune suppression ; table conservee.
- Risque : eleve, car toute validation globale sur la colonne `statut` melange les domaines.

## Tables migrables sans ambiguite

- `niveau_anomalie` contenant `A_CONTROLER` dans les tables ou la colonne est deja une gravite : migration de valeur vers `WARNING`.
- Lot6d `statut_controle -> statut_resolution` avec mapping `VALIDE/A_CONTROLER/INFO`.
- Lot11 `impact_facture -> effet_facturation` avec mapping exact `BLOQUANT_FACTURE/NON_BLOQUANT_FACTURE/A_DECIDER`.
- Lot11 `severity -> niveau_anomalie` avec mapping exact, sous reserve d'adapter les consommateurs dans la meme passe.
- `statut_controle -> statut_traitement` seulement pour les tables dont les valeurs sont strictement dans `STATUT_TRAITEMENT`.

## Tables necessitant encore une decision

- `REF_Statuts` familles existantes `anomalie`, `facture`, `rapprochement`, `import` : mapping des valeurs historiques vers familles majuscules a valider ligne par ligne.
- `code_impact` pour cas ou un impact canonique vaut `A_CONTROLER` : decision sur absence de code ou code transitoire.
- Tables banque/rapprochement ou `statut` et `statut_controle` coexistent : qualifier par domaine avant renommage.
- Toute table contenant une valeur de `statut_controle` hors `VALIDE/A_CONTROLER/EXCLU_RESULTAT/A_VENTILER` autre que le cas Lot6d deja qualifie.

## Anciennes colonnes supprimables apres migration

- `severity` : supprimable apres bascule complete vers `niveau_anomalie`.
- Lot11 `impact_facture` : supprimable apres bascule complete vers `effet_facturation`.
- Lot6d `statut_controle` : supprimable apres bascule complete vers `statut_resolution`.
- Autres `statut_controle` : supprimables table par table apres bascule complete vers `statut_traitement` et mise a jour des filtres aval.
- `niveau_anomalie` n'est pas supprimable ; seule la valeur `A_CONTROLER` est supprimee.
- `code_impact` n'est pas supprimable ; il devient derive et controle.

## Modifications Power BI attendues

- Remplacer les references `severity` par `niveau_anomalie`.
- Remplacer les references Lot11 `impact_facture` par `effet_facturation`.
- Remplacer les references Lot6d `statut_controle` par `statut_resolution`.
- Remplacer les filtres `[statut_controle] = "VALIDE"` par `[statut_traitement] = "VALIDE"` uniquement pour les tables migrees vers `STATUT_TRAITEMENT`.
- Ajouter ou utiliser une dimension `REF_Statuts` filtree par `famille_statut`.
- Verifier que les visuels de facturation n'utilisent pas `code_impact` pour une decision facture.

## Ordre d'execution recommande

1. Creer une regle centrale pure pour familles et valeurs canoniques : validation `(famille_statut, statut)`, migration `severity`, migration `effet_facturation`, derivation `code_impact`.
2. Mettre a jour `REF_Statuts` et les validations Excel pour utiliser les familles majuscules canoniques.
3. Migrer `niveau_anomalie=A_CONTROLER` vers `WARNING` dans les producteurs et tests.
4. Migrer Lot11 : `severity -> niveau_anomalie`, `impact_facture -> effet_facturation`, puis adapter dashboard/tests/export.
5. Migrer Lot6d : `statut_controle -> statut_resolution`, puis adapter Lot13/Power BI.
6. Migrer les autres tables `statut_controle -> statut_traitement` une par une, uniquement si les valeurs sont strictement canoniques.
7. Centraliser et tester la derivation de `code_impact`.
8. Supprimer les anciennes colonnes table par table seulement apres scan actif propre et tests OK.
