# APP-5A — Contrôles & clôture : lecture (lecture seule)

**Statut : `APP-5A_CONTROLES_CLOTURE_LECTURE_EN_ATTENTE_VALIDATION`**

Module visible de suivi des contrôles moteur et de l'état de clôture des mois. Lecture seule.
**Aucune clôture, aucun changement d'état OUVERT/EN_CONTROLE/CLOTURE, aucun acquittement, aucune
écriture, aucun pipeline.** Développé dans un worktree détaché (HEAD `8b47807`). Aucun commit.

## 1. Sources (carte)

| source | onglet | grain | code | niveau | période | statut | consommateur |
|---|---|---|---|---|---|---|---|
| MASTER_CTRL_Coherence.xlsx (Lot11) | MASTER | contrôle | code_controle | severity | mois | statut_resolution | APP-5A liste/détail/bloquants |
| MASTER_CTRL_Coherence.xlsx | DASHBOARD_MOIS | mois | — | — | mois | cloture_possible | APP-5A clôturabilité |
| MASTER_CTRL_Coherence.xlsx | BLOQUANTS_OUVERTS / A_CONTROLER_OUVERTS | contrôle | — | severity | mois | — | (vues ouvertes) |
| REF_Setup.xlsm | REF_Cloture_Mensuelle | mois | — | — | mois | statut_mois | APP-5A statut de mois |

Clé stable = `ctrl_pk` (moteur) ; à défaut, clé d'affichage `GEN-<hash(mois|module|code|entité|source_pk)>`
(jamais écrite en source). Volumétrie réelle : MASTER 27 contrôles, DASHBOARD_MOIS 4 mois,
REF_Cloture_Mensuelle 18 mois (statuts OUVERT/EN_CONTROLE/CLOTURE).

## 2. Écrans livrés

- **`/controles-cloture`** — 8 cartes (total, bloquants, à contrôler, informatifs, mois ouverts,
  mois clôturés, mois non clôturables, sources indisponibles), filtres (période, module, niveau, code,
  bloquants seuls, mois non clôturables, recherche, tri), tableau paginé + section « clôture par mois »,
  export CSV.
- **`/controles-cloture/{stable_id}`** — code, niveau, période, module, entité, message moteur,
  explication (dictionnaire), commentaire, statut, impact sur la clôture, traçabilité (fichier/onglet
  par nom). 404 propre.
- **`/controles-cloture/mois/{mois}`** — statut du mois, date de clôture, clôturable/raison (moteur),
  bloquants empêchant la clôture, à-contrôler, sources manquantes, **anomalie après clôture** signalée.
  Bouton **« Clôturer le mois — prochain lot » désactivé** (aucune route POST).
- **`/controles-cloture/bloquants`** — niveau BLOQUANT uniquement, groupés par module, liens vers
  fiches et mois.

## 3. Règles respectées

- **aucun contrôle inventé** : tout vient de MASTER_CTRL_Coherence ;
- **aucun BLOQUANT déclassé** ; niveau/statut repris tels quels ;
- **aucune clôture** depuis l'application ; REF_Cloture_Mensuelle jamais modifié ;
- un mois clôturé reste affiché clôturé ; une **anomalie après clôture reste visible** ;
- SQLite n'est pas une source de clôture ; aucun acquittement réel ;
- explications = dictionnaire d'affichage ; aucun chemin absolu ; aucune donnée sensible inutile.

## 4. Limites

- Clôture réelle non active (bouton désactivé).
- Acquittement / résolution de contrôle non actif.
- Périmètre = contrôles consolidés Lot11 ; les MASTER_CTRL par lot (Lot1, Lot6d) ne sont pas
  re-agrégés (Lot11 est la consolidation officielle via source_module).
- « Contrôle sans période » et « code inconnu » sont affichés sans invention (explication vide si code
  hors dictionnaire).

## 5. Prochaines étapes

Passage en contrôle / clôture réelle d'un mois, acquittement tracé des contrôles — lots ultérieurs,
avec gardes d'écriture dédiées et modification contrôlée de REF_Cloture_Mensuelle. Non livrés ici.

## 6. Tests

`05_APPLICATION/tests/test_controles_cloture.py` — 32 tests sur fixtures isolées : lecture & états
(alimentée/absente/onglet absent/vide), niveaux (bloquant/à contrôler/info), BLOQUANT jamais déclassé,
contrôle sans période, doublon, mois ouvert/en contrôle/clôturé, mois non clôturable, **anomalie après
clôture**, code inconnu sans explication, filtres (période/module/niveau/bloquants/non-clôturable/
recherche), pagination, tri bloquants d'abord, détail/404, bloquants par module, écran mois, export CSV,
aucun chemin absolu, aucun import moteur ni écriture, aucune source réelle modifiée, routes (dashboard,
détail 200/404, mois, bloquants, export), aucune route POST de clôture, nav sidebar.
