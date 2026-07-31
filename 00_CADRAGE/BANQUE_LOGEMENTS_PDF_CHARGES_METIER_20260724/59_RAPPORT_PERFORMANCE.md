# 59 — Rapport performance (recette globale, copies de données réelles)

Mesures `curl -w "%{time_total}"`, serveur local (port 8030), copies réelles (24 mois de données
Lot10, 531 lignes `PAR_MOIS_LOGEMENT`).

| Page | Temps |
|---|---:|
| `/` | 0,011 s |
| `/resultats?mois=2026-06` | 0,016 s |
| `/resultats/logements?mois=2026-06` | 0,007 s |
| `/resultats/proprietaires?mois=2026-06` | 0,006 s |
| `/resultats/reconciliation?mois=2026-06` | 0,967 s |
| `/resultats/export.csv?mois=2026-06` | 0,011 s |
| `/calculs` | 0,313 s |

## Constat

Toutes les pages < 20 ms sauf `/resultats/reconciliation` (0,97 s — exécute 8 réconciliations,
chacune relisant un ou plusieurs fichiers Excel réels sans cache, aucun résultat mis en mémoire
entre les 8 appels) et `/calculs` (0,31 s — liste les runs et calcule la comparaison au mois
précédent).

## Décision

**Aucune correction appliquée.** ~1 seconde reste largement acceptable pour un écran de contrôle
consulté ponctuellement (pas une page de liste à fort trafic), sur une application locale
mono-utilisateur. Corriger sans un problème mesuré et reproduit serait de l'optimisation
prématurée, explicitement hors mandat de cette mission (« ne fais pas d'optimisation générale sans
preuve »). À surveiller si le volume de mois/logements réels venait à décupler.
