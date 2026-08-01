# 60 — Verdict GO / NO GO (recette globale sur copies, 2026-08-01, mis à jour le 2026-08-02)

> Re-vérifié le 2026-08-02 (suite 1) : une mission a annoncé un relevé Crédit Mutuel « nouvellement
> fourni » — vérifié absent en pratique. Verdict inchangé à cette étape.
>
> **Suite 2 (2026-08-02)** : le fichier a réellement été déposé
> (`01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_CreditMutuel.xlsx`, SHA256
> `a84c9b51b1c0eb50d17216272bd3c6cf2669d159bf7e1299c2b762face0ca4a8`, copié et hash vérifié
> identique dans l'environnement de copies). Exécution réelle de `lot8a_banque_import.py` sur la
> copie : **ÉCHEC reproduit, code retour 1** — `[ERREUR BLOQUANT] Feuille "Cpt 02211 00021321603"
> absente. Feuilles disponibles : ['Synthese', 'Mouvements', 'Mensuel', 'Controles', 'Sources']`.
> Le fichier fourni est un **rapport consolidé** (titre interne *« Relevé bancaire consolidé —
> WONDERBNB »*), pas l'export brut attendu — même compte (RIB identique), mais 12 colonnes au lieu
> de 7 et une logique de fusion entre deux sources. **`BANQUE_LOT8_IMPORT.xlsx` n'a pas été
> produit.** Détail complet : `61_CONTRAT_SOURCE_BANQUE_LOT8.md` (section « Suite »).

## Verdict final

**NO GO — SOURCE BANQUE INCOMPATIBLE**

Un fichier a été fourni au bon emplacement, pour le bon compte, mais dans un format que
`lot8a_banque_import.py` ne peut pas traiter tel quel (feuille attendue absente, structure de
colonnes différente, fichier déjà consolidé/dédoublonné plutôt qu'un export brut). Reproduit
réellement sur copie (code retour 1, aucune sortie produite). Aucune correction de
`lot8a_banque_import.py` appliquée pour accepter ce format — cela aurait été un changement de
contrat métier décidé unilatéralement, explicitement hors mandat. `lot9` continue de refuser
systématiquement de s'exécuter (`CTR-9-001`, comportement voulu, source toujours indisponible dans
le format attendu).

Ce verdict porte spécifiquement sur la **capacité à ré-exécuter le pipeline aval complet sur
données réelles** — il ne remet pas en cause la validité de l'application sur les modules déjà
alimentés (Analytique/Résultats sur les sorties Lot9-13 existantes, toutes réconciliations
disponibles vertes, 0 fuite, 0 double comptage, migrations saines — cf. sections ci-dessous,
inchangées depuis le 2026-08-01). Aucune anomalie logicielle bloquante n'a été trouvée : c'est un
verdict sur la **donnée**, pas sur le **code**.

Le second écart (deux mois Lot10 manquants) est **résolu et expliqué** ce tour
(`62_RAPPORT_MOIS_LOT10_MANQUANTS.md`) : filtrage upstream cohérent, pas un défaut — statut
VIDE_VALIDE/NON_APPLICABLE, aucune correction nécessaire.

## Modules validés (sur copies réelles)

- Lecture de toutes les sources réelles configurées, sauf 2 absences documentées (Banque, export
  Power BI) — jamais transformées en zéro.
- Migrations : `integrity_check` ok, 0 violation de clé étrangère, idempotentes (comptage de
  lignes), sur une copie de l'`app.db` réel ET sur une base vierge.
- Pipeline `charges` (lot3) : exécuté deux fois sur copies, succès, idempotent, 0 doublon.
- Réconciliations A, B, D, H : **OK** sur 24 mois de données réelles (0,00€ d'écart). B confirme en
  conditions réelles le correctif du tour précédent (grains incompatibles).
- Invariant REEL = COMPTABLE + HORS_COMPTA : vérifié sur le total réel (291 779,67€), écart 0,00€.
- Sécurité : 0 fuite (chemins, PII, secrets) sur les pages et exports scannés.
- Performance : toutes les pages < 1 s, rien à corriger (pas de problème mesuré).
- Intégrité des sources réelles : 85/85 fichiers identiques avant/après (aucune écriture réelle,
  aucun résidu).

## Modules non validés / non exercés en réel

- Comptabilité (0 écriture réelle), Factures (0), Règlements (0), Ménages (0) — jamais alimentés en
  réel, réconciliations C/E/F/G correctement `A_CONTROLER`/`NON_DISPONIBLE`, pas un défaut.
- Chaîne aval complète (lot9→lot13) : non ré-exécutable ce tour (source Banque manquante) ; les
  sorties réelles utilisées datent du 2026-07-24, cohérence interne confirmée mais fraîcheur non
  garantie face à l'état actuel exact des sources.

## Anomalies

- **BLOQUANT (donnée, pas code)** : le fichier fourni pour la source Banque est un rapport
  consolidé, pas l'export brut attendu — `BANQUE_LOT8_IMPORT.xlsx` ne peut pas être produit. Cf.
  `61_CONTRAT_SOURCE_BANQUE_LOT8.md`.
- **RÉSOLU (expliqué)** : deux mois absents de la série réelle Lot10 (2026-11, 2027-01) — cause
  identifiée (filtrage upstream cohérent d'une réservation placeholder sans montant), pas un
  défaut. Cf. `62_RAPPORT_MOIS_LOT10_MANQUANTS.md`.
- **MINEUR** : `réconciliation /resultats/reconciliation` à ~1 s — acceptable, surveiller si le
  volume augmente.

Aucune anomalie de code n'a été trouvée ni corrigée ce tour : **0 commit de correction sur
l'application**. La suite de tests déterministe reste celle du tour précédent
(`TEST_SHARDS_ANALYTIQUE_RESULTATS.txt`, 2281 passés / 75 ignorés / 1 échec préexistant connu) —
inchangée car aucun code n'a été modifié ; un nouveau manifeste n'a donc pas lieu d'être créé
(règle explicite : uniquement si la liste des tests a changé).

## Risques

1. Toute décision de rejouer la chaîne aval en réel devra d'abord résoudre l'absence de source
   Banque — sinon `lot9` refusera systématiquement de s'exécuter (comportement voulu, pas un bug).
2. Les sorties Lot9-13 réelles actuelles ne sont pas prouvées fraîches par rapport à l'état exact
   de `01_SOURCES_BRUTES` aujourd'hui (seulement mutuellement cohérentes entre elles).
3. Comptabilité/Factures/Ménages restent à zéro en réel — tout arbitrage sur leur activation en
   mode réel reste entièrement en attente (`GUIDE_ACTIVATION_MODE_REEL.md`, verdict NO GO inchangé
   pour le mode réel lui-même).

## Prochaines actions

1. **Décision utilisateur requise** : soit (a) fournir l'export **brut natif** Crédit Mutuel du
   compte `02211 00021321603` (fichier tel que téléchargé depuis l'espace bancaire, sans
   retraitement, feuille `Cpt 02211 00021321603`, 7 colonnes), soit (b) décider explicitement
   d'adapter `lot8a_banque_import.py` pour consommer le format consolidé déjà fourni — un
   changement de contrat métier à trancher humainement, hors mandat de cette mission.
2. Une fois une source conforme disponible : exécuter `lot8a_banque_import.py`, puis reprendre la
   recette globale à partir de Lot8/Lot9.
3. Mois 2026-11/2027-01 : compléter la saisie Hors-Hostaway pour `LOG_0015`/`PROP_0011` si
   l'activité de ces mois doit apparaître dans les Résultats — sinon, aucune action requise (état
   correct).
4. Nettoyer l'environnement de copies (`_RECETTES_GLOBALES/RECETTE_GLOBALE_20260801_004232/`) une
   fois ce rapport validé — il contient une copie de l'`app.db` réel, à ne jamais committer ni
   partager tel quel (hors du dépôt Git par construction, mais toujours une donnée sensible locale).

## Commandes exactes de reprise

```
cd "C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER"
git branch --show-current   # feature/banque-logements-pdf-charges-metier
git log -1 --format="%H %s"
git status --porcelain      # doit être vide après le commit de ce tour
```

Ne relance pas le mode réel. Ne modifie aucune donnée réelle. Ne déclare pas le projet terminé.
