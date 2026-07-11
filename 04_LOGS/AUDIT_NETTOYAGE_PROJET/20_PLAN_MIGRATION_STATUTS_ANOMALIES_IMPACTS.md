# 20 - Plan migration statuts, anomalies et impacts

## Perimetre

Audit cible des champs suivants : statut, statut_traitement, statut_controle, code_anomalie, niveau_anomalie, severity, code_impact, impact_facture, impact_resultat_reel, impact_resultat_comptable.

Aucune migration physique n'a ete appliquee dans cette etape : les consommateurs actifs et certaines valeurs non canoniques imposent une qualification avant suppression de colonnes.

## Regles cibles confirmees

- statut_traitement : VALIDE, A_CONTROLER, EXCLU_RESULTAT, A_VENTILER.
- code_anomalie : code precis expliquant une anomalie ou une exclusion.
- niveau_anomalie : INFO, WARNING, BLOQUANT.
- A_CONTROLER est un statut de traitement, jamais une gravite finale.
- impact_facture, impact_resultat_reel, impact_resultat_comptable : OUI, NON, A_CONTROLER.
- code_impact doit etre derive des impacts canoniques, jamais maintenu comme source separee.
- severity doit etre migre vers niveau_anomalie puis supprime quand plus aucun consommateur actif ne l'utilise.
- statut_controle doit etre migre vers statut_traitement puis supprime quand plus aucun consommateur actif ne l'utilise.

## Tables et usages identifies

### REF_Setup.xlsm / REF_Statuts

- Colonne : statut.
- Valeurs observees : A_CONTROLER, BLOQUANT, A_IMPORTER, IMPORTE, CONTROLE, CORRIGE, REJETE, ARCHIVE, NON_RAPPROCHE, RAPPROCHE_AUTO, RAPPROCHE_MANUEL, ECART_A_ANALYSER, OUVERTE, EN_COURS, CORRIGEE, IGNOREE_VALIDEE, A_EMETTRE, EMISE, PAYEE, IMPAYEE, ANNULEE, VALIDE, IGNORE_JUSTIFIE, EXCLU_RESULTAT, A_VENTILER, INFO.
- Role reel : referentiel historique heterogene de statuts metier.
- Statut audit : A_VERIFIER.
- Decision : ne pas renommer ni supprimer globalement dans ce lot.

### Lot4bis / MASTER_CALC_Reservations et vues derivees

- Colonnes : statut_controle, code_anomalie, niveau_anomalie, code_impact, impact_resultat_reel, impact_resultat_comptable.
- Valeurs observees : statut_controle = VALIDE, A_CONTROLER, EXCLU_RESULTAT ; niveau_anomalie = INFO, A_CONTROLER ; code_impact = IC, HC, HR ; impacts resultat = OUI, NON.
- Producteur : 02_TRAVAIL/lot4bis_charger_reservations.py.
- Consommateurs : lots de resultats, exports Power BI, tests.
- Role reel : statut_controle correspond au futur statut_traitement ; niveau_anomalie contient encore A_CONTROLER, a migrer en WARNING.
- Statut audit : REDONDANTE_POTENTIELLE pour statut_controle ; COMPLEMENTAIRE pour code_anomalie et impacts.
- Migration possible seulement apres adaptation coordonnee des consommateurs.

### Lot6c / MASTER_Menages_Externes et vue active

- Colonnes : statut_controle, code_anomalie, niveau_anomalie.
- Valeurs observees : statut_controle = VALIDE, A_CONTROLER ; niveau_anomalie = INFO, A_CONTROLER.
- Producteur : 02_TRAVAIL/lot6c_menages_externes.py.
- Consommateurs : resultats, controles, exports.
- Role reel : statut_controle correspond au futur statut_traitement ; niveau_anomalie doit exclure A_CONTROLER.
- Statut audit : REDONDANTE_POTENTIELLE pour statut_controle ; COMPLEMENTAIRE pour code_anomalie.
- Migration possible seulement apres adaptation coordonnee des consommateurs.

### Lot6d / rapprochement menages

- Colonnes : statut_controle, code_anomalie, niveau_anomalie.
- Valeur bloquante observee : statut_controle = INFO.
- Role reel : INFO est une gravite ou information, pas un statut_traitement canonique.
- Statut audit : A_VERIFIER.
- Decision : table bloquee tant que INFO n'est pas qualifie.

### Lot8 / banque et rapprochements

- Colonnes : statut, statut_controle, code_anomalie, niveau_anomalie.
- Valeurs observees : statut = EN_ATTENTE, A_CONTROLER, INFORMATIF selon les sorties ; statut_controle = VALIDE, A_CONTROLER selon les tables ; niveau_anomalie peut contenir A_CONTROLER.
- Role reel : statut de rapprochement bancaire pour certains fichiers ; statut_controle proche de statut_traitement pour d'autres.
- Statut audit : COMPLEMENTAIRE ou A_VERIFIER selon table.
- Decision : ne pas supprimer les colonnes statut ; qualifier chaque table avant migration.

### Lot9 / flux unifie

- Colonnes : statut_controle, niveau_anomalie, code_anomalie, code_impact.
- Valeurs observees : statut_controle = VALIDE ; niveau_anomalie = INFO ; code_impact = IC, HC.
- Role reel : flux comptable/resultat, code_impact derive des impacts.
- Statut audit : REDONDANTE_POTENTIELLE pour statut_controle ; CALCULEE attendue pour code_impact.
- Migration possible apres centralisation de code_impact et adaptation des consommateurs.

### Lot10 / resultats

- Colonnes consommees : statut_controle, code_anomalie, niveau_anomalie, code_impact selon les flux amont.
- Role reel : consommateur actif critique.
- Statut audit : consommateur a adapter avant suppression des anciennes colonnes.

### Lot11 / controles coherence

- Colonnes : severity, impact_facture, code_anomalie, statut_resolution.
- Valeurs observees : severity = INFO, A_CONTROLER dans les sorties existantes.
- Producteur : 02_TRAVAIL/lot11_controles_coherence.py via lib_controls.py.
- Consommateurs : exports Power BI, tests, controles.
- Role reel : severity est l'ancienne gravite ; impact_facture porte une ancienne nomenclature BLOQUANT_FACTURE / NON_BLOQUANT_FACTURE / A_DECIDER dans le code.
- Statut audit : COMPATIBILITE pour severity ; A_VERIFIER pour impact_facture.
- Decision : migration bloquee tant que le mapping metier de impact_facture vers OUI/NON/A_CONTROLER n'est pas valide explicitement.

### Lot12 / factures

- Colonnes : statut, code_anomalie.
- Valeurs observees : statut = RATTACHE_PROPRIETAIRE dans CONTROLE_MENSUEL.
- Role reel : statut metier de rattachement/facturation, pas duplicat direct de statut_traitement.
- Statut audit : COMPLEMENTAIRE.
- Decision : conserver statut.

### Lot13 / export Power BI

- Colonnes consommees/exportees : statut_controle, niveau_anomalie, code_anomalie, severity, impacts.
- Role reel : consommateur aval actif.
- Statut audit : consommateur a adapter avant suppression des anciennes colonnes.

## Doublons confirmes ou probables

- severity et niveau_anomalie : doublon fonctionnel tres probable, mais suppression differee car severity reste produit/consomme activement.
- statut_controle et statut_traitement : doublon probable quand les valeurs sont VALIDE, A_CONTROLER, EXCLU_RESULTAT, A_VENTILER ; bloque pour les tables contenant INFO ou d'autres valeurs.
- code_impact et impacts canoniques : code_impact doit devenir calcule/derive, pas source separee.

## Champs complementaires justifies

- code_anomalie : complementaire, explique la cause precise.
- niveau_anomalie : complementaire, porte la gravite unique.
- impact_facture, impact_resultat_reel, impact_resultat_comptable : complementaires, axes d'impact distincts.
- statut : a conserver quand il represente reservation, paiement, rapprochement, rattachement ou autre statut metier specifique.

## Valeurs libres ou incoherentes

- niveau_anomalie = A_CONTROLER dans plusieurs sorties : doit migrer vers WARNING si la table est migree.
- severity = A_CONTROLER dans les controles : doit migrer vers WARNING, mais ne peut pas rester gravite finale.
- statut_controle = INFO dans Lot6d : non compatible avec statut_traitement canonique, bloque la migration de cette table.
- impact_facture = BLOQUANT_FACTURE / NON_BLOQUANT_FACTURE / A_DECIDER dans le code de controles : mapping cible a qualifier avant modification.
- statut = INFORMATIF, RATTACHE_PROPRIETAIRE, OUVERT, EN_ATTENTE : statuts metier specifiques a classifier, pas suppression automatique.

## Decisions humaines necessaires

1. Valider le sens exact de impact_facture cible : impact sur emission facture, eligibilite facture, ou blocage facture.
2. Qualifier statut_controle = INFO dans Lot6d.
3. Decider si REF_Statuts reste un referentiel historique multi-domaines ou s'il doit etre separe par domaine.
4. Valider la derivation canonique de code_impact pour les cas contenant A_CONTROLER.
5. Confirmer les roles precis des colonnes statut par domaine avant toute suppression.

## Plan de convergence recommande

1. Ajouter une regle centrale pure pour statut_traitement, niveau_anomalie, impacts et code_impact derive.
2. Adapter les producteurs pour ecrire les colonnes canoniques tout en conservant temporairement les anciennes colonnes de compatibilite.
3. Adapter les consommateurs actifs : lots 10, 11, 12, 13 et exports Power BI.
4. Migrer les tables deterministes : Lot4bis, Lot6c, Lot9 apres adaptation des consommateurs.
5. Bloquer les tables avec valeurs inconnues ou non compatibles : Lot6d et controles impact_facture tant que les mappings ne sont pas valides.
6. Supprimer severity et statut_controle seulement quand les scans actifs ne trouvent plus aucun consommateur.
7. Ajouter des tests unitaires sur valeurs canoniques, refus des valeurs inconnues et derivation de code_impact.
