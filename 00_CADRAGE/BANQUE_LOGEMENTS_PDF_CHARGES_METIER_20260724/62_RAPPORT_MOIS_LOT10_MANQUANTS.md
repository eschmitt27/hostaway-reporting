# 62 — Rapport des deux mois Lot10 manquants (2026-11, 2027-01)

Audit mené sur les copies, en remontant la chaîne Lot10 → Lot9 → Lot4quater (`VUE_FLUX` puis
`MASTER`) jusqu'à la cause racine. Aucune donnée réelle modifiée.

## Constat

| Mois | Lot9 présent | Lot10 présent | Sources amont (`MASTER` Lot4quater) | Cause | Statut |
|---|---|---|---|---|---|
| 2026-11 | non | non | **1 ligne** : `RES-2026-11-HA-001`, LOG_0015/PROP_0011, `montant_retenu=0`, `impact_resultat_reel=OUI`, `impact_resultat_comptable=NON`, `statut_controle=A_CONTROLER`, `code_anomalie=DIRECT_SANS_SAISIE_HH`, canal DIRECT | mois filtré : la seule réservation du mois est un placeholder A_CONTROLER sans montant, exclu de `VUE_FLUX` (Lot4quater) | **VIDE_VALIDE / NON_APPLICABLE** |
| 2027-01 | non | non | **1 ligne** : `RES-2027-01-HA-001`, même logement/propriétaire, même profil (`montant_retenu=0`, `A_CONTROLER`, `DIRECT_SANS_SAISIE_HH`, DIRECT) | idem | **VIDE_VALIDE / NON_APPLICABLE** |

## Chaîne de preuve

1. `MASTER_CALC_Resultats.xlsx` (Lot10, `PAR_MOIS_LOGEMENT`) : 2026-11 et 2027-01 absents.
2. `MASTER_CALC_Flux.xlsx` (Lot9, onglet `MASTER`) : **les deux mois sont déjà absents en amont**
   — Lot10 ne fait donc que refléter fidèlement ce que Lot9 lui donne, aucun défaut Lot10.
3. `MASTER_CALC_Reservations_Resolues.xlsx` (Lot4quater), onglet `VUE_FLUX` (celui que `lot9_
   construire_flux.py` consomme réellement, `SRC_RES`) : **1349 lignes, les deux mois absents.**
4. Le même fichier, onglet `MASTER` (celui qui garde l'historique complet, y compris les lignes
   non retenues) : **1391 lignes, les deux mois PRÉSENTS** (1 ligne chacun).
5. Différence : 42 lignes filtrées entre `MASTER` et `VUE_FLUX` — Lot4quater exclut de sa vue
   financière (`VUE_FLUX`) les réservations dont le statut est `A_CONTROLER` sans montant validé.
   Pour ces deux mois précis, cette unique ligne filtrée est la SEULE activité du mois : sa
   suppression vide entièrement le mois de `VUE_FLUX`, donc de tout ce qui en découle (Lot9, Lot10,
   Analytique, Résultats).

## Conclusion

**Ce n'est ni une erreur de contrat, ni un défaut logiciel, ni une ancienne sortie périmée.**
C'est un **filtrage upstream cohérent et documenté** (Lot4quater ne fait remonter en flux financier
que les réservations validées ou dotées d'un montant exploitable). Le code de Lot9 et de Lot10 est
fidèle à ce qu'il reçoit — aucune divergence entre eux, aucune correction nécessaire dans
l'application (`app/`) ni dans les scripts moteur.

**Ce qui reste ouvert, côté métier** : les deux réservations à l'origine du vide
(`RES-2026-11-HA-001`, `RES-2027-01-HA-001`, et leur voisine `RES-2026-12-HA-002` — même logement
LOG_0015/propriétaire PROP_0011, même profil `DIRECT_SANS_SAISIE_HH`, trois mois consécutifs) sont
des réservations directes sans saisie Hors-Hostaway, en attente de complément de données humain.
Tant qu'elles ne sont pas complétées (montant réel saisi), ces mois resteront vides dans les
Résultats — **c'est le comportement voulu**, pas un défaut à corriger applicativement.

## Statut

**VIDE_VALIDE / NON_APPLICABLE** pour les deux mois — jamais remplacé par un zéro fabriqué. Aucun
test rouge/vert nécessaire (aucun code à corriger). Recommandation : compléter la saisie
Hors-Hostaway pour `LOG_0015`/`PROP_0011` sur ces trois mois si l'activité réelle doit apparaître
dans les Résultats.
