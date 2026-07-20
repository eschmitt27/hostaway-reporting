# APP-5B — Contrôles détaillés, actionnables et reliés aux modules métier

État : prêt pour validation humaine. Aucun commit, aucune écriture réelle, flags réels False.
APP-5C (clôture réelle) non démarré.

## 1. Principe : moteur vs suivi humain

Le moteur (Lot11 → `MASTER_CTRL_Coherence.xlsx`) reste **la vérité de l'anomalie**. L'application
n'invente, ne reclasse et ne masque aucune anomalie. Le suivi humain est journalisé séparément dans
l'app.db isolée et **ne masque jamais une anomalie moteur encore présente**.

Quatre dimensions distinctes, jamais confondues :

| Dimension | Valeurs |
|-----------|---------|
| Anomalie moteur | PRÉSENTE / ABSENTE (fournie par le moteur) |
| Prise en charge | NON_TRAITÉ / EN_COURS / TRAITÉ |
| Résultat humain | CORRIGE / EXCEPTION_ACCEPTEE / NON_CORRIGE / A_REVOIR |
| Statut de suivi | OUVERT / EN_COURS / RESOLU / ACCEPTE_AVEC_JUSTIFICATION / ROUVERT |

Cas signalés sans masquage : anomalie présente + suivi RÉSOLU → **incohérence** ; anomalie absente +
suivi EN_COURS → **suivi à clôturer** ; exception acceptée + anomalie présente → affiché **exception**,
pas « corrigé ».

## 2. Grain actionnable (agrégé → détaillé)

Le Lot11 agrège chaque anomalie en une ligne. APP-5B rouvre chaque agrégat en éléments unitaires en
relisant la **source détaillée réelle** (le grain vient de la source, jamais d'une reconstruction) :

| code_controle | module | grain cible | source détaillée | lien module |
|---|---|---|---|---|
| RESERVATION_A_CONTROLER_SANS_COMMISSION | COMMISSIONS | 1/réservation (59) | MASTER_CALC_Commissions · A_CONTROLER | — (saisie Lot 4) |
| VRBO_MONTANT_NON_RENSEIGNE | RESERVATIONS | 1/réservation (**5**, périmètre moteur) | MASTER_CALC_**Reservations_Resolues** (Lot4quater) · source=HOSTAWAY_VRBO_A_CONTROLER | — (saisie Lot 4) |
| MENAGE_EXTERNE_ECART_HOSTAWAY | MENAGES_EXT | 1/logement×mois (4) | VUE_ECART_HOSTAWAY | /menages |
| MENAGE_EXTERNE_LOGEMENT_HORS_HA | MENAGES_EXT | 1/logement×mois (2) | VUE_ECART_HOSTAWAY | /menages |
| MENAGE_HA_SANS_FACTURE_EXTERNE (INFO) | MENAGES_EXT | 1/logement×mois (50) | VUE_ECART_HOSTAWAY | /menages |
| CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE | BANQUE | 1/mouvement (1+31+20) | NORM_Banque · RAPPROCHEMENT_REQUIS | /banques-caisse/…/modifier (APP-4B) |
| CHARGE_FIXE_DATE_ENTREE_GESTION_INCOHERENTE (INFO) | RESERVATIONS | déjà 1/logement (14) | MASTER_CALC_Reservations | — |

**Correctif VRBO (périmètre moteur exact)** : APP-5B lit désormais la source RÉSOLUE (Lot4quater) que
Lot11 utilise → **5** réservations actionnables (mois ouverts), et non les 32 de la table live. Les 27
autres (mois clôturés, historisées) sont exposées dans une vue TECHNIQUE séparée
(`NON_CONCERNÉE_PAR_LE_CONTRÔLE`), jamais comme anomalies. Voir AUDIT_VRBO.md.

## 3. Identifiant public opaque des éléments

`CTRL-<sha256(sel + "|" + code + "|" + entite + "|" + mois + "|" + index)[:12]>`
(`controles_detail_service.id_opaque`). Stable, unique, sans donnée sensible, sans compte bancaire,
sans chemin, sans concaténation brute visible. Correspondance interne côté serveur uniquement.
Les mouvements bancaires n'exposent que leur **MVT opaque** (APP-4B), jamais le compte.

## 4. Journal de suivi (SQLite isolé)

Migration `0007_controles_suivi.sql` : `controles_suivi` (une décision ACTIVE / contrôle),
`controles_suivi_historique` (append-only), `controles_runs` (recalculs copie). Version optimiste :
conflit refusé. Aucune suppression physique. Le journal ne fait jamais autorité.

Workflow : Prendre en charge → EN_COURS ; Corrigé exige une **preuve** ET l'absence d'anomalie moteur ;
Exception exige une **justification** (l'anomalie reste affichée présente) ; Rouvrir exige un **motif** ;
Annuler repasse à OUVERT ; réapparition (RÉSOLU + anomalie de nouveau présente) → **ROUVERT auto**,
sans doublon, ancienne résolution conservée.

## 5. Liens vers les modules

Banque → `/banques-caisse/mouvements/MVT-opaque/modifier` (APP-4B) ; Ménages → `/menages` ;
autres → « Aucun écran de correction disponible pour ce contrôle » + module futur documenté. Aucun
lien vers une page inexistante.

## 6. Recalcul moteur sur copies (Lot8c + Lot11)

**Lot8c et Lot11 sont désormais RÉELLEMENT exécutés sur copie.** Les deux scripts ont reçu une racine
INJECTABLE (`--project-root` / env `PILOTAGE_PROJECT_ROOT` / défaut CLI inchangé) : toutes les entrées
et sorties dérivent de cette racine. `controles_runner_service.recalculer_sur_copie` : préflight
(flags False) → workspace isolé (copie des 21 entrées, refus de tout chemin hors workspace) → Lot11
baseline → classification (décision APP-4B) sur COPIE de NORM_Banque → **Lot8c** → **Lot11** →
comparaison du contrôle bancaire du mois AVANT/APRÈS → journalisation `controles_runs` → nettoyage.

Le runner prend les scripts du WORKTREE injecté (`APP_ROOT.parent/02_TRAVAIL`), jamais l'arbre réel, et
refuse tout script sans le marqueur d'injection. Interpréteur = premier python avec pandas
(`PILOTAGE_ENGINE_PYTHON`, sinon Python312). Verdicts prouvés par le MASTER_CTRL recalculé, jamais par
SQLite : RESOLU_MOTEUR / TOUJOURS_PRESENT / TRANSFORME / ERREUR_MOTEUR / NON_COMPARABLE.

Preuve : 2026-02 (1 mouvement) classé → contrôle 1 → 0 = RESOLU_MOTEUR ; sans classification → 1 → 1 =
TOUJOURS_PRESENT. Réel intact vérifié par SHA256 (banque baca5dbc, MASTER_CTRL d4504b33). Voir
AUDIT_RUNNER_LOT8C_LOT11.md. Note : le MASTER_CTRL réel (27) est un snapshot antérieur ; recalculé sur
les entrées actuelles il donne ~856 contrôles (dérive d'état des sources) — le runner compare le
contrôle bancaire spécifique sur le même workspace, ce qui neutralise cette baseline.

## 7. Préparation clôture (sans clôturer — futur APP-5C)

Statut par mois calculé en affichage seul (NON_PRÊT / À_CONTRÔLER / PRÊT_SOUS_EXCEPTION / PRÊT) à
partir des anomalies moteur, du suivi et des exceptions. Aucun bouton de clôture réelle ; bandeau
« Clôture réelle non active ». Cette synthèse alimentera APP-5C.

## 8. Écrans

`/controles-cloture` : 8 vues (Tous, À traiter, En cours, Résolus, Exceptions acceptées, Informatifs,
Réapparus, Incohérences), cartes (anomalies moteur, à traiter, en cours, résolus, exceptions,
réapparus, incohérences, informatifs, bloquants clôture), filtres (période, module, niveau, code,
statut suivi, responsable, propriétaire, logement, recherche, actionnables, bloque clôture).
Les INFO sont présentés « Informatif — aucune action requise », non comptés dans les ouverts, non
bloquants. `/controles-cloture/element/{CTRL-opaque}` : fiche actionnable (moteur / suivi / historique /
lien module / actions). Sous-écrans APP-5A (bloquants, mois, détail moteur) conservés.

## 9. Limites

- Runner : nécessite un python avec pandas (`PILOTAGE_ENGINE_PYTHON` sinon Python312) ; sinon BLOQUE
  lisible. Recalcul sur copie disponible pour les mouvements **bancaires** (Lot8c/Lot11).
- Recalcul non bancaire : la disparition se constate à la prochaine génération moteur.
- Le MASTER_CTRL réel (27) est un snapshot antérieur aux entrées actuelles (recalcul complet ≈ 856,
  dérive d'état des sources) — le runner compare le contrôle bancaire spécifique, pas le total absolu.
- « Masquer localement un INFO » : action prévue côté vue utilisateur, non persistée.

## 10. Flags & garde-fous

`CONTROLES_REAL_WRITE_ENABLED = False`, `CONTROLES_REAL_WRITE_CONFIRMATION_ENABLED = False`. Suivi
écrit uniquement dans l'app.db isolée. Aucun MASTER_CTRL / REF_Cloture_Mensuelle / source métier
réelle modifiée. Aucune clôture réelle. Aucun 500 : contrôle inconnu → 404, action invalide →
message lisible, runner en échec → statut ÉCHEC/BLOQUE.
