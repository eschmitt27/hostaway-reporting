# Lots proposés après recette humaine — 2026-07-17

La recette a révélé un **hotfix** (livré ci-dessous) et **5 modules d'écriture** à construire, chacun
trop important pour tenir dans le même lot. Découpage proposé (aucun n'est encore commité).

## HOTFIX-1 — LIVRÉ ET TESTÉ (Phases 1-5)
- **#1 500 Ménages** : `_construire_workspace` copiait lot6d/lot6e sans vérifier leur présence →
  `FileNotFoundError` → 500. Corrigé : préflight des scripts (`E_SCRIPT_MOTEUR_ABSENT`) + garde
  d'exception générale (`E_INATTENDU`) dans `confirmer()` ET `executer_chaine()` + routes défensives.
  Plus de 500 ; erreur métier lisible ; jamais de traceback navigateur.
- **#2 Écran chaîne** : texte « extraction manuelle » remplacé par le texte exact (Aissata/Mounir
  automatiques) ; diagnostic PDF (mode, 2 reconnus, 1439 €/942 €) ; libellés utilisateur des 7 étapes
  + détail technique repliable ; sources séparées obligatoires (6a-6e) / facultatives (6f, non
  bloquantes : `SAISIE_Charges_Flux`) / lot11.
- **#4 Filtre logement** : nom officiel du logement (repli « Logement non identifié — LOG_XXXX »),
  valeur technique `logement_id` conservée.
- **#5 Périodes futures** : filtre normal ≤ mois courant ; mois futurs isolés dans « Données futures
  à contrôler » (non masqués). Cause : le MASTER moteur contient un calendrier étendu 2025-01→2027-02.

Fichiers : `menages_recalcul_service.py`, `menages_chaine_service.py`, `routes/menages.py`,
`templates/menages_run.html`, `menages_chaine.html`, `proprietaires_reglements_service.py`,
`proprietaires_reglements_reader.py`, `templates/reglements_list.html`, `tests/test_menages_recalcul_hotfix.py`.

## APP-4B — Contrôle & catégorisation bancaire sur copies (Phase 6)
Writer SQLite sur copie (jamais la banque réelle). Fiche mouvement : catégorie/type flux, propriétaire,
logement, réservation, facture, sens, commentaire, statut (À contrôler / Contrôlé / Rapproché /
Ignoré+justif). Proposition moteur → accepter/corriger/rattacher/commenter/enregistrer. Identifiant
mouvement **opaque** (hash stable, aucune donnée de compte dans l'URL — corrige le résidu
`MVT-CM_02211_00021321603` constaté). Flags : `BANQUE_REAL_WRITE_ENABLED=False`,
`BANQUE_REAL_WRITE_CONFIRMATION_ENABLED=False`.

## APP-3D — Gestion propriétaires & contrats sur copies (Phase 7)
Writer copies : créer/modifier propriétaire (identité, contact, logements, taux+historique, statut) ;
déclarer fin de contrat (date sortie, logements, motif, historique conservé, aucun effacement).
Contrôles : chevauchement périodes, logement multi-propriétaires actifs, taux sans période, sortie
avant entrée, propriétaire sans logement, modification rétroactive signalée. Flags :
`PROPRIETAIRES_REAL_WRITE_ENABLED=False`, `PROPRIETAIRES_REAL_WRITE_CONFIRMATION_ENABLED=False`.

## APP-3E — Factures propriétaires sur copies (Phase 8)
Statuts : BROUILLON, À CONTRÔLER, VALIDÉE, ÉMISE, PAYÉE, ANNULÉE, AVOIR À ÉMETTRE, AVOIR ÉMIS.
Sur copies : ouvrir, modifier champs autorisés, ligne manuelle justifiée, recalcul d'affichage depuis
le moteur (commissions jamais réinventées), transitions de statut, aperçu + PDF final (copie),
historique, blocage modif d'une facture ÉMISE/PAYÉE (avoir ou nouvelle version). Tests de transitions.
Flags : `FACTURES_REAL_WRITE_ENABLED=False`, `FACTURES_REAL_WRITE_CONFIRMATION_ENABLED=False`.

## APP-5B — Contrôles actionnables sur copies (Phase 9)
**Grain audité** : les 7 A_CONTROLER sont AGRÉGÉS (1 ligne/code, entités listées dans le message) ;
les 20 INFO sont informatifs. À construire : éclatement au grain entité (une ligne actionnable par
logement/mouvement/réservation) ; workflow copie OUVERT/EN_COURS/RÉSOLU/ACCEPTÉ_AVEC_JUSTIFICATION/
ROUVERT (responsable, commentaire, dates, preuve, ancien/nouveau statut). **Distinguer** : anomalie
moteur présente ≠ contrôle pris en charge ≠ anomalie corrigée ≠ exception acceptée. Les INFO ne sont
PAS présentés comme « OUVERT » (niveau informatif, aucune action requise).

## APP-5C — Clôture mensuelle sur copies (Phase 10)
Transitions OUVERT→EN_CONTROLE→PRÊT_À_CLÔTURER→CLÔTURÉ→ROUVERT (copie). Conditions : 0 BLOQUANT moteur,
0 ligne bancaire requise non résolue, A_CONTROLER traités, sources du mois générées, factures
compatibles, commissions/nets disponibles, cohérence Lot11, snapshot + journal. Prévisualiser /
clôturer sur copie / rouvrir avec motif. Bouton réel désactivé « Clôturer réellement le mois — non
activé ». Flags : `CLOTURE_REAL_WRITE_ENABLED=False`, `CLOTURE_REAL_WRITE_CONFIRMATION_ENABLED=False`.

## Analyse des 27 contrôles (recette)
| Contrôle | Sévérité | Grain actuel | Entités |
|---|---|---|---|
| RESERVATION_A_CONTROLER_SANS_COMMISSION | A_CONTROLER | agrégé | 59 réservations |
| MENAGE_EXTERNE_ECART_HOSTAWAY | A_CONTROLER | agrégé | 4 logements (LOG_0010/0011/0013/0016) |
| MENAGE_EXTERNE_LOGEMENT_HORS_HA | A_CONTROLER | agrégé | 2 logements (LOG_0009/0016) |
| VRBO_MONTANT_NON_RENSEIGNE | A_CONTROLER | agrégé | 5 réservations VRBO |
| CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE (2026-02) | A_CONTROLER | agrégé | 1 ligne bancaire |
| CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE (2026-03) | A_CONTROLER | agrégé | 31 lignes bancaires |
| CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE (2026-04) | A_CONTROLER | agrégé | 20 lignes bancaires |
| (20 autres) | INFO | informatif | aucune action requise |
→ APP-5B doit éclater ces agrégats en listes détaillées actionnables.
