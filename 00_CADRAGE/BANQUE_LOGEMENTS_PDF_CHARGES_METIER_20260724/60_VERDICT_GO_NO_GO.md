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

## Verdict final (mis à jour 2026-08-02, suite 3 — après ajout du support du format consolidé)

**GO POUR VALIDATION HUMAINE PARTIELLE**

Décision prise et implémentée : Lot8 accepte désormais deux formats (natif Crédit Mutuel + relevé
consolidé), sans remplacer le contrat historique (`63_CONTRAT_FORMAT_RELEVE_BANCAIRE_CONSOLIDE.md`).
Provenance du consolidé auditée et **non circulaire** (3 exports bruts successifs, jamais une
sortie Lot8/Lot9+). Exécuté réellement sur copie : `lot8a` produit `BANQUE_LOT8_IMPORT.xlsx` (541
mouvements, 0 BLOQUANT, totaux identiques au centime près à ceux du fichier source), idempotent.
**Chaîne aval complète rejouée avec succès** (lot9→lot10→lot11→lot12→lot13, scripts moteur
directs) : REEL=COMPTABLE+HC vérifié (291 852,76 = 281 328,60 + 10 524,16, écart 0,00 €),
idempotence confirmée sur un second passage. Réconciliations A/B/D/H **OK** (écart 0,00 €) sur les
sorties fraîchement régénérées ; C/E/F/G toujours `A_CONTROLER`/`NON_DISPONIBLE` (attendu, 0
écriture réelle Comptabilité). Sécurité : 0 fuite. Sources réelles : 88/88 intactes (le seul écart
est la modification intentionnelle et commitée de `lot8a_banque_import.py`, du code).

**« Partielle » et non « complète »**, honnêtement, pour deux raisons :
1. La chaîne aval a été rejouée en exécutant les **scripts moteur directement**, pas via l'écran
   `/calculs` de l'application — une anomalie **nouvelle, distincte, hors mandat** de cette mission
   a été trouvée en tentant cette voie (`lot4quater` régénère `VUE_FLUX` scopé au seul mois demandé
   via le pipeline applicatif, produisant 104 lignes au lieu de 1349, ce qui déclenche
   `CTR-9-003` — sans rapport avec le format Banque). Non corrigée (hors mandat), consignée pour
   une mission dédiée. Cf. `63` section 9, `JOURNAL_ANOMALIES.md`.
2. Lot8b (classification)/Lot8c (rapprochement bancaire) n'ont pas été exécutés — non requis pour
   démontrer la compatibilité du format, mais signifie que le rapprochement Banque↔réconciliation D
   reste vérifié sur un état « 0 mouvement classifié », pas sur un cycle complet de classification.

Le second écart (deux mois Lot10 manquants) reste **résolu et expliqué** depuis le tour précédent
(`62_RAPPORT_MOIS_LOT10_MANQUANTS.md`) : filtrage upstream cohérent, pas un défaut.

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
- Lot8b (classification déterministe) / Lot8c (rapprochement bancaire) : non exécutés ce tour, non
  requis pour prouver la compatibilité de format — reste à faire pour un cycle Banque complet.
- Chaîne aval réellement rejouée (lot9→lot13) **via les scripts moteur directs** — pas encore via
  l'écran `/calculs` de l'application pour un mois donné, bloqué par l'anomalie ci-dessous.

## Anomalies

- **CORRIGÉ (code, ce tour)** : format Banque consolidé désormais accepté par Lot8, sans casser le
  format natif historique (11 tests de régression/nouveaux, tous verts). Cf. `63`.
- **NOUVELLE ANOMALIE, HORS MANDAT, NON CORRIGÉE** : `lot4quater_resoudre_source_reservations.py`,
  invoqué via l'écran `/calculs` de l'application pour un mois donné, régénère `VUE_FLUX` scopé à
  ce seul mois (104 lignes) au lieu de l'historique complet (1349 lignes) — déclenche
  `BLOQUANT [CTR-9-003] VUE_FLUX volume suspect`. Sans rapport avec le format Banque. Sorties
  restaurées, aucune correction appliquée (hors mandat strict : « ne commence aucune nouvelle
  fonctionnalité »). Cf. `63` section 9, `JOURNAL_ANOMALIES.md`.
- **RÉSOLU (expliqué)** : deux mois absents de la série réelle Lot10 (2026-11, 2027-01) — cause
  identifiée (filtrage upstream cohérent d'une réservation placeholder sans montant), pas un
  défaut. Cf. `62_RAPPORT_MOIS_LOT10_MANQUANTS.md`.
- **MINEUR** : `réconciliation /resultats/reconciliation` à ~1 s — acceptable, surveiller si le
  volume augmente.

Suite de tests : **262 passés** dans `tests/` (moteur, dont 11 nouveaux pour Lot8), 0 échec.
Suite ciblée Banque de l'application : **58 passés, 17 ignorés**, 0 échec. Campagne complète par
shards (`TEST_SHARDS_ANALYTIQUE_RESULTATS.txt`) non rejouée dans son intégralité ce tour — le code
modifié (`lot8a_banque_import.py`) est un script moteur `02_TRAVAIL/`, hors du périmètre couvert
par ce manifeste (`05_APPLICATION/tests/`) ; sa propre suite ciblée (`tests/` racine, 262 tests) a
servi de campagne complète pertinente pour ce changement.

## Risques

1. Toute décision de rejouer la chaîne aval en réel devra d'abord résoudre l'absence de source
   Banque — sinon `lot9` refusera systématiquement de s'exécuter (comportement voulu, pas un bug).
2. Les sorties Lot9-13 réelles actuelles ne sont pas prouvées fraîches par rapport à l'état exact
   de `01_SOURCES_BRUTES` aujourd'hui (seulement mutuellement cohérentes entre elles).
3. Comptabilité/Factures/Ménages restent à zéro en réel — tout arbitrage sur leur activation en
   mode réel reste entièrement en attente (`GUIDE_ACTIVATION_MODE_REEL.md`, verdict NO GO inchangé
   pour le mode réel lui-même).

## Prochaines actions

1. **Décision humaine (mission dédiée future)** : traiter l'anomalie `lot4quater` (régénération
   scopée au mois via `/calculs`, sans rapport avec Banque) — nécessaire pour que la chaîne aval
   soit rejouable depuis l'écran applicatif, pas seulement via les scripts moteur directs.
2. Exécuter Lot8b (classification)/Lot8c (rapprochement) si un cycle Banque complet est souhaité.
3. Mois 2026-11/2027-01 : compléter la saisie Hors-Hostaway pour `LOG_0015`/`PROP_0011` si
   l'activité de ces mois doit apparaître dans les Résultats — sinon, aucune action requise (état
   correct).
4. Nettoyer l'environnement de copies (`_RECETTES_GLOBALES/RECETTE_GLOBALE_20260801_004232/`) une
   fois ce rapport validé — il contient une copie de l'`app.db` réel et le relevé bancaire réel, à
   ne jamais committer ni partager tel quel (hors du dépôt Git par construction, mais toujours des
   données sensibles locales).

## Commandes exactes de reprise

```
cd "C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER"
git branch --show-current   # feature/banque-logements-pdf-charges-metier
git log -1 --format="%H %s"
git status --porcelain      # doit être vide après le commit de ce tour
```

Ne relance pas le mode réel. Ne modifie aucune donnée réelle. Ne déclare pas le projet terminé.
