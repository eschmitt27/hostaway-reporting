# 60 — Verdict GO / NO GO (recette globale sur copies, 2026-08-01)

## Verdict

**GO POUR VALIDATION HUMAINE**

Pas de blocage applicatif : l'application se comporte correctement face aux données réelles
(mêmes réconciliations vertes qu'en fictif, aucune fuite, aucun double comptage, migrations saines
sur une copie de l'`app.db` réel). Le seul blocage trouvé est un **écart de données réelles**
(source Banque jamais alimentée), pas un défaut applicatif — il ne bloque ni la lecture, ni les
écrans, ni les réconciliations disponibles ; il bloque seulement la **ré-exécution** du pipeline
aval au-delà de lot9. D'où : pas de NO GO applicatif, mais une décision humaine reste nécessaire
sur la donnée manquante avant d'aller plus loin.

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

- **BLOQUANT (donnée, pas code)** : `BANQUE_LOT8_IMPORT.xlsx` absent du réel — bloque la
  ré-exécution du pipeline aval à partir de lot9. Cf. `55_MATRICE_ECARTS_CONTRATS_REELS.md`.
- **MINEUR** : deux mois absents de la série réelle Lot10 (2026-11, 2027-01) — à vérifier auprès du
  métier, pas fabriqué.
- **MINEUR** : `réconciliation /resultats/reconciliation` à ~1 s — acceptable, surveiller si le
  volume augmente.

Aucune anomalie de code n'a été trouvée ni corrigée ce tour : **0 commit de correction**. La suite
de tests déterministe reste celle du tour précédent (`TEST_SHARDS_ANALYTIQUE_RESULTATS.txt`,
2281 passés / 75 ignorés / 1 échec préexistant connu) — inchangée car aucun code n'a été modifié ;
un nouveau manifeste n'a donc pas lieu d'être créé (règle explicite : uniquement si la liste des
tests a changé).

## Risques

1. Toute décision de rejouer la chaîne aval en réel devra d'abord résoudre l'absence de source
   Banque — sinon `lot9` refusera systématiquement de s'exécuter (comportement voulu, pas un bug).
2. Les sorties Lot9-13 réelles actuelles ne sont pas prouvées fraîches par rapport à l'état exact
   de `01_SOURCES_BRUTES` aujourd'hui (seulement mutuellement cohérentes entre elles).
3. Comptabilité/Factures/Ménages restent à zéro en réel — tout arbitrage sur leur activation en
   mode réel reste entièrement en attente (`GUIDE_ACTIVATION_MODE_REEL.md`, verdict NO GO inchangé
   pour le mode réel lui-même).

## Prochaines actions

1. Décision humaine : alimenter (ou non) la source Banque réelle avant toute nouvelle recette
   pipeline complète.
2. Vérifier auprès du métier les deux mois manquants (2026-11, 2027-01).
3. Nettoyer l'environnement de copies (`_RECETTES_GLOBALES/RECETTE_GLOBALE_20260801_004232/`) une
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
