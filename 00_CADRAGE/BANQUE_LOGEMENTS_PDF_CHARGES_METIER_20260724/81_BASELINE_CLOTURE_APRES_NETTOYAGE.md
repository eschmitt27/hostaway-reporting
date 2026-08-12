# 81 — Baseline de clôture après nettoyage complet (nuit du 2026-08-11/12)

**Mission de nuit autonome.** Simulation canonique fraîche reconstruite depuis HEAD `6b60577`
(01_SOURCES_BRUTES/02_TRAVAIL/02_DONNEES_NORMALISEES actuels + Banque réelle régénérée sur copie,
plus le jeu Lot1_Hostaway cohérent déjà validé pour Payout/Details). **Aucune donnée réelle
modifiée. Aucun code modifié — audit complet, 0 nouveau bug trouvé.**

## 1. Baseline fraîche — vérité de travail de cette mission

Chaîne rejouée sur copie : lot1(recalc-payout)→lot8a→lot8b→lot8c→lot4bis→lot4quater→lot9→lot10→
lot11. Idempotent (2 runs consécutifs, mêmes compteurs exacts).

| Famille | Baseline fraîche | Sévérité |
|---|---:|---|
| GESTION_LOGEMENT_MISSING | 472 | BLOQUANT |
| CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE | 69 | BLOQUANT |
| CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE | 9 (mois) | A_CONTROLER |
| RESERVATION_A_CONTROLER_SANS_COMMISSION | 42 (agrégé, 1 ligne) | A_CONTROLER |
| VRBO_MONTANT_NON_RENSEIGNE | 4 (agrégé, 1 ligne) | A_CONTROLER |
| MENAGE_EXTERNE_ECART_HOSTAWAY | 1 (4 logements) | A_CONTROLER |
| MENAGE_EXTERNE_LOGEMENT_HORS_HA | 1 (2 logements) | A_CONTROLER |
| SOURCE_SHEET_PROVENANCE_INCOMPLETE | 1 | A_CONTROLER |
| HC_ZERO_SOURCES_VIDES | 4 | INFO |
| Sources absentes (Aircover/Imputations Airbnb/Ajustements/Suivi associé) | 4 | INFO |
| MENAGE_HA_SANS_FACTURE_EXTERNE, MENAGE_EXTERNE_RAPPROCHE_HOSTAWAY | 2 | INFO |
| **TOTAL** | **565** | **541 BLOQUANT / 14 A_CONTROLER / 10 INFO** |

**Résultat majeur : les 541 BLOQUANT sont EXACTEMENT et UNIQUEMENT `GESTION_LOGEMENT_MISSING`
(472) + `CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` (69).** Aucun autre bloqueur BLOQUANT n'existe.

REEL = 313 756,48 € / COMPTABLE = 303 232,32 € / HORS_COMPTA = 10 524,16 € — écart 0,00 €.
(Delta vs la simulation précédente sans Banque réelle : -130,01 € sur REEL/COMPTABLE, explicable
et attendu : 24 flux de frais bancaires réels désormais comptés au lieu de 0 avec le stub.)

## 2. Banque — reconstruction fraîche réussie (Phase 2)

Source réelle utilisée en lecture seule : `01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_
CreditMutuel.xlsx` (format consolidé, déjà géré par `lot8a`). Pipeline **déjà validé** rejoué sur
copie (`lot8a`→`lot8b`→`lot8c`), aucune réaudition métier, aucun matching Banque↔Réservation.

| Mesure | Valeur |
|---|---:|
| Mouvements bruts | 541 |
| Doublons détectés | 1 |
| Classés déterministement (`CLASSE`) | 236 |
| Rapprochement requis (Airbnb 166 + propriétaires 56) | 222 |
| File humaine `A_ENVOYER_IA` (catch-all) | 83 |
| Produit économique créé | AUCUN (rapprochement/catégorisation seulement) |

**Banque statut Lot11 : `BANQUE_DISPONIBLE`** (vs `BANQUE_NON_DISPONIBLE_GIT` avec le stub
précédent). C'est la première fois cette session que la Banque fraîche entre dans le calcul
global — confirme que le pipeline Lot9/10/11 fonctionne correctement avec des données réelles.

## 3. CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE — cause confirmée, pas un bug séparé

**Vérification programmatique (Phase 9)** : les 69 couples logement×mois sont un **sous-ensemble
strict à 100 %** des 77 couples `GESTION_LOGEMENT_MISSING` (jan-juil 2025). 0 couple hors gestion.
Cause identique : `charge_fixe=0` faute de période de gestion applicable dans
`REF_Gestion_Logements_Hist` avant le 01/08/2025 — **pas un bug moteur, pas une donnée
indépendante à corriger**. Se résoudra automatiquement (jusqu'à 69, potentiellement moins si
certains logements ont un forfait à 0 €) si et seulement si les 77 couples sont un jour
renseignés — décision utilisateur déjà identifiée comme ABSENT (voir §5).

Classification (Phase 8) : **100 % type F. HISTORIQUE_INCOMPLET / DÉPENDANCE_AUTRE_FAMILLE.**
0 BUG_MOTEUR, 0 RÉFÉRENTIEL_INCOMPLET indépendant, 0 CHARGE_RÉELLEMENT_EXCEPTIONNELLE, 0 DOUBLON.

## 4. Résiduels — tous classifiés, 0 bug technique (Phase 11-12)

| Code | Type | Bloque clôture ? |
|---|---|---|
| CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE | ACTION_HUMAINE | Oui (A_CONTROLER) |
| RESERVATION_A_CONTROLER_SANS_COMMISSION | ACTION_HUMAINE | Oui (A_CONTROLER) |
| VRBO_MONTANT_NON_RENSEIGNE | ACTION_HUMAINE | Oui (A_CONTROLER) |
| MENAGE_EXTERNE_ECART_HOSTAWAY | ACTION_HUMAINE (réconciliation ménage) | Oui (A_CONTROLER) |
| MENAGE_EXTERNE_LOGEMENT_HORS_HA | ACTION_HUMAINE | Oui (A_CONTROLER) |
| SOURCE_SHEET_PROVENANCE_INCOMPLETE | INFORMATION (garde-fou provenance ménages) | Oui (A_CONTROLER) mais non prioritaire |
| HC_ZERO_SOURCES_VIDES, sources absentes, MENAGE_HA_SANS_FACTURE/RAPPROCHE | INFO_LÉGITIME | Non |

**0 BUG_TECHNIQUE trouvé cette nuit.** Aucune correction de code nécessaire. Les 4 codes
Ménages (ECART_HOSTAWAY, LOGEMENT_HORS_HA, HA_SANS_FACTURE, RAPPROCHE) et
SOURCE_SHEET_PROVENANCE_INCOMPLETE sont hors périmètre explicite de cette mission (Ménages,
Charges internes) — documentés, non traités en profondeur, aucune anomalie structurelle
détectée qui justifierait une intervention immédiate.

## 5. Les 42 Direct/VRBO — recherche de preuve renouvelée, 0 nouvelle preuve (Phase 5)

**Requête API Hostaway en direct (lecture seule)** sur les 42 `reservation_id` cette nuit :
100 % ont encore `paymentStatus=Unknown`, `airbnbExpectedPayoutAmount=None`,
`cancellationAmount=None`. `totalPrice` existe (prix voyageur) mais **n'est pas un payout** —
utiliser ce champ inventerait une formule de conversion prix→payout non validée métier
(interdit explicitement). **0 nouvelle PREUVE_A trouvée.** Confirmation fraîche, pas une
supposition reportée de la mission précédente.

38 Direct + 4 VRBO = 42, inchangé. Voir Pack A (§7).

## 6. Les 472 GESTION_LOGEMENT_MISSING — non ré-audités (Phase 13, sur instruction explicite)

Rien de nouveau : 77 couples logement×mois, 14 logements, jan-juil 2025, PREUVE_A=0/PREUVE_B=0/
AMBIGU=0/ABSENT=77 (conclusion de `77_RECONSTRUCTION_GESTION_LOGEMENTS_HIST.md`, non remise en
cause). Voir Pack B (§8).

## 7. PACK A — Saisies humaines Direct/VRBO (42 cas)

38 réservations Direct sans saisie HH + 4 réservations VRBO sans backfill CSV disponible. Aucun
payout Hostaway, `paymentStatus=Unknown` confirmé en direct cette nuit. Mécanisme de résolution
déjà existant : saisie manuelle dans `SAISIE_ReservationsHorsHostaway.xlsx` (formulaire
`/reservations/nouvelle`), même procédure que pour les 506/1 cas déjà résolus précédemment.

**Action humaine requise** : pour chacune des 42 réservations (identifiants opaques Hostaway déjà
listés dans `80_AUDIT_RESERVATIONS_VRBO_DIRECT_A_CONTROLER.md` §10), fournir le montant réellement
perçu (voyageur → société directement, hors plateforme) pour permettre la saisie HH.

## 8. PACK B — Historique gestion jan-juil 2025 (77 couples)

14 logements, 7 mois (2025-01→2025-07), regroupés par logement dans
`77_RECONSTRUCTION_GESTION_LOGEMENTS_HIST.md` §1-9 (non dupliqué ici). **Question métier
identique à celle déjà posée** : qui gérait ces 14 logements avant le 01/08/2025, et depuis
quand ? Sans réponse, ces 77 couples et les 69 charges dépendantes restent BLOQUANT
définitivement.

## 9. PACK C — Banque humaine (222 mouvements)

166 mouvements Airbnb en attente d'export détaillé (rapprochement bloqué par l'absence d'export
Airbnb officiel détaillé — contrat documenté dans `74_CONTRAT_SOURCE_AIRBNB_RAPPROCHEMENT.md`,
pas une décision aujourd'hui) + 56 virements propriétaires en attente de saisie d'acompte (Lot5).
**Aucune classification effectuée cette nuit** (interdit explicitement). 83 mouvements
supplémentaires en file `A_ENVOYER_IA` (catch-all, décisions humaines déjà en cours de
traitement selon `73_JOURNAL_DECISIONS_VALIDATION_HUMAINE.md` — non reproduit ici).

## 10. PACK D — Autres règles/données non déterministes

- **SOURCE_SHEET_PROVENANCE_INCOMPLETE** (lot6b/lot6f) : garde-fou existant signalant une
  provenance de source Ménages non prouvable formellement. Pas un bug — décision : accepter tel
  quel (comportement voulu) ou renforcer la traçabilité (hors périmètre technique de cette nuit).
- **MENAGE_EXTERNE_ECART_HOSTAWAY** (4 logements) / **MENAGE_EXTERNE_LOGEMENT_HORS_HA**
  (2 logements) : écarts de réconciliation ménages externes vs comptage Hostaway, nécessitent une
  revue humaine du rapprochement factures/Hostaway (hors périmètre Charges/Réservations de cette
  mission, à traiter dans une mission Ménages dédiée si prioritaire).

## 11. Tests / idempotence / intégrité

**0 code modifié cette nuit → aucun test rouge à corriger.** Suite de régression de la mission
précédente reste la référence valide (274 passed moteur + app suite complète exit 0). Idempotence
vérifiée sur la simulation fraîche : 2 runs consécutifs de la chaîne complète, mêmes compteurs
exacts (1522 flux, 42 A_CONTROLER, 565/541/14/10 contrôles, REEL/COMPTABLE/HORS_COMPTA
identiques). Intégrité réelle : 950/950 fichiers baseline, 3 diffs tous déjà committés lors des
missions précédentes (`REF_Setup.xlsm`, `MASTER_FACT_HA_Reservations.xlsx`,
`HIST_Reservations_Cloturees.xlsx`) — **0 nouvelle modification réelle cette nuit**. Port
8000/PID 21136 intact tout du long.

## 12. Verdict

- **APPLICATION : VALIDÉE** (0 bug technique résiduel trouvé, pipeline complet rejouable de bout
  en bout sur copies, y compris Banque fraîche).
- **DONNÉES POUR CLÔTURE : 541 contrôles BLOQUANT** empêchent encore la clôture (472 gestion +
  69 charges dépendantes — même cause racine unique).
- **CORRECTIONS DÉTERMINISTES : TOUTES ÉPUISÉES.** Aucune correction de code supplémentaire
  possible sans invention de règle ou de donnée.
- **PRÉPARATION MODE RÉEL : NO GO** (bloqueurs de clôture réels non résolus, Banque humaine non
  traitée, arbitrages comptables non rendus).
- **MODE RÉEL : NO GO — NON ACTIVÉ.**

## 13. Nombre exact de bloqueurs humains restants

**3 décisions/actions humaines distinctes** ferment tout le reste :
1. **Historique gestion 14 logements, jan-juil 2025** (77 couples → résout aussi les 69 charges
   dépendantes → 541 BLOQUANT deviennent potentiellement 0).
2. **42 saisies manuelles Direct/VRBO** (montants réels perçus, hors plateforme).
3. **222 mouvements bancaires** (166 Airbnb export détaillé absent + 56 acomptes propriétaires) +
   83 en file de classification assistée déjà engagée.

Rien d'autre ne bloque techniquement la clôture.

## 14. Décision (1) appliquée — 541 BLOQUANT → 0 (2026-08-12)

Décision utilisateur reçue : « le propriétaire a toujours été le même pour chaque logement. »
Couverture `REF_Gestion_Logements_Hist` prolongée 2025-08-01 → **2025-01-01** pour les 14
logements concernés. Détail complet : `77_RECONSTRUCTION_GESTION_LOGEMENTS_HIST.md` §13.

| Mesure | Avant | Après |
|---|---:|---:|
| `GESTION_LOGEMENT_MISSING` | 472 | **0** |
| `CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` | 69 | **0** |
| BLOQUANT total | 541 | **0** |
| A_CONTROLER | 14 | 14 |
| INFO | 10 | 10 |
| REEL / COMPTABLE / HORS_COMPTA | 313 756,48 / 303 232,32 / 10 524,16 € | identiques, delta 0,00 € |

Simulation canonique fraîche (Banque réelle incluse), idempotence vérifiée (2 runs identiques),
14/14 cellules réelles modifiées et relues, backup + SHA256 vérifiés, intégrité 3/950 diffs
(exactement attendus), pipeline réel non relancé, 24/24 tests ciblés verts.

**Verdict mis à jour** :
- **DONNÉES POUR CLÔTURE : 0 contrôle BLOQUANT.** Familles gestion/charges définitivement closes.
- **CLÔTURE TECHNIQUE (sur ces deux familles) : GO.**
- **PRÉPARATION MODE RÉEL : NO GO** — décisions (2) et (3) ci-dessus restent ouvertes, ainsi que
  les 14 A_CONTROLER résiduels et les mappings comptables provisoires.
- **MODE RÉEL : NO GO — NON ACTIVÉ.**

Restes humains désormais **2 décisions** (au lieu de 3) : 42 saisies Direct/VRBO, Banque humaine
(222 + 83 file assistée). Plus 14 A_CONTROLER résiduels mineurs et mappings comptables non
bloquants moteur.

## 15. Correction de reporting — la vraie file Banque humaine (2026-08-12)

**Correction de libellé, pas de donnée** : les « 222 mouvements bancaires humains » cités ci-dessus
et dans `HANDOFF_CANONIQUE.md` amalgamaient à tort deux populations très différentes. Ventilation
exacte (détail complet : `82_PACK_FINAL_ACTIONS_HUMAINES.md` §3) :

| Sous-catégorie | Lignes | Décision humaine ? |
|---|---:|---|
| `PAYOUT_PLATEFORME` (Airbnb, catégorie moteur déjà correcte, exclue du matching réservation) | 166 | **NON** — 0 décision, informatif, en attente export Airbnb |
| `VIREMENT_PROPRIETAIRE_A_RAPPROCHER` (candidats légitimes, Lot 5 non alimenté) | 56 | OUI |

**Vrai total de décisions Banque humaines : 56 (propriétaires) + 82 (A_ENVOYER_IA distincts, 83
lignes physiques dédupliquées par `mouvement_id`) = 138**, et non 222+83=305. Vérifié :
`app/services/banques_candidats_service.py` exclut déjà `PAYOUT_PLATEFORME` du rapprochement
réservation (conforme au commit `78877da`) — **0 bug de code**, uniquement notre propre reporting
de mission qui était imprécis.

**Verdict final mis à jour** :
- **DÉCISIONS HUMAINES BANQUE : 138** (au lieu de 305 brut).
- **SAISIES HUMAINES RÉSERVATIONS : 42** (38 Direct + 4 VRBO), inchangé.
- **A_CONTROLER résiduel réel (hors reformulations des blocs ci-dessus) : 3** (Ménages/provenance).
- **CLÔTURE TECHNIQUE : GO. PRÉPARATION MODE RÉEL : NO GO. MODE RÉEL : NO GO — NON ACTIVÉ.**

## 16. Audit Lot 5 — file humaine finale chiffree (2026-08-13)

Detail : `83_AUDIT_LOT5_RAPPROCHEMENT_PROPRIETAIRES.md`, pack operateur :
`82_PACK_FINAL_ACTIONS_HUMAINES.md`.

Lot 5 est FONCTIONNEL mais NON ALIMENTE (0 ligne partout). Consequence : les 56 mouvements
proprietaires sont tous ABSENT de preuve (PREUVE_A = 0), aucun rapprochement possible. Ce n'est
pas un echec moteur : il n'existe aucun objet a rapprocher.

| Bloc | Objets | Decisions humaines (min-max) |
|---|---:|---|
| Proprietaires / Lot 5 | 56 | 7 - 56 |
| Banque A_ENVOYER_IA | 82 | 37 - 82 |
| Reservations Direct/VRBO | 42 | 42 |
| Autres A_CONTROLER | 3 | 3 |
| **TOTAL** | **183** | **89 - 183** |

Baseline controles inchangee : 0 BLOQUANT / 14 A_CONTROLER / 10 INFO / 24 total. Sur les 14
A_CONTROLER, 11 reformulent les blocs ci-dessus, 3 sont distincts (Menages/provenance).
