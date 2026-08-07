# 76 — Validation finale du bloc Banque / Trésorerie (recette navigateur, pipeline complet, campagne, intégrité)

Mission exécutée sur copies isolées, HEAD `db7a381` inchangé pendant toute la mission (aucun commit
de code applicatif requis : aucun bug fonctionnel trouvé dans l'application elle-même).

## A. Recette navigateur (sur copies isolées, ports 8020/8021, distincts de 8000)

- **Trésorerie propriétaires** : parcours complet réel (création, prévisualisation, confirmation,
  validation, détail, historique append-only, refus de modification sur objet validé) exécuté via
  requêtes réelles au serveur (logs `server_8020.log`). Toutes les étapes se comportent comme
  attendu.
- **Rapprochements** : quatre scénarios rejoués sur un jeu fictif isolé sans PII (`FIXTURE_ROOT`,
  port 8021) :
  - **EXACT** (516,00 €) : confirmation directe, statut VALIDE.
  - **PARTIEL** (objet 1000 €, mouvement 400 €) : reste calculé à 600 €, tentative de double
    consommation via un groupe alternatif sommant au même mouvement déjà consommé → refusée avec
    le message attendu (« Le montant rapproché dépasserait le montant du mouvement »).
  - **GROUPÉ** (mouvement 1500 € = 500+600+400) : proposition créée en statut `PROPOSE`, jamais
    auto-confirmée, confirmation individuelle vérifiée.
  - **AMBIGU** (mouvement 500 €, 29 combinaisons valides) : forcé en `A_CONTROLER`, aucune
    confirmation en masse.
  - **PARTIEL-PUIS-GROUPÉ** (objet A reste 600 € après partiel + objet B 500 €, mouvement 1100 €) :
    le moteur de groupe utilise correctement le *reste* (600 €) de l'objet partiellement consommé,
    pas son montant d'origine (1000 €) — vérifié sur le payload JSON `affectations`
    (`montant_affecte: 600`).
- **File A_ENVOYER_IA** : décompte réel non forcé, parcours de classement exercé.
- **Statut Airbnb** : `SOURCE_AIRBNB_DETAILLEE_ABSENTE` confirmé, aucun bouton d'import actif.
- Aucune anomalie BLOQUANTE/MAJEURE trouvée dans l'application → **aucune correction de code
  nécessaire**.

## B. Pipeline complet sur copies (Lot8a → Lot13), deux exécutions intégrales (idempotence)

| Lot | Statut | Sorties clés | Contrôles |
|---|---|---|---|
| 8a | SUCCES | 541 lignes NORM | 1 A_CONTROLER, 1 doublon potentiel (attendu) |
| 8b | SUCCES | 24 VALIDE / 517 A_CONTROLER | 236 CLASSE / 222 RAPPROCHEMENT_REQUIS / 83 A_ENVOYER_IA |
| 8c | SUCCES | 166 Airbnb (14 467,27 €) / 56 propriétaires (27 069,18 €) en attente | 0 confirmation automatique, produit économique créé : AUCUN |
| 9 | SUCCES | MASTER_CALC_Flux, 1409 flux | CTR-9-004 à CTR-9-011 OK, 0 colonne bancaire sensible, 0 doublon |
| 10 | SUCCES | Commissions/Résultats/NetProprietaire | CTR-LOT10-01 à 26, 0 bloquant, identité REEL=COMPTABLE+HORS_COMPTA OK (291 722,75 = 281 198,59 + 10 524,16) |
| 11 | SUCCES | MASTER_CTRL_Coherence | 1536 contrôles générés, 959 BLOQUANT / 567 A_CONTROLER ouverts (données métier incomplètes, pas un défaut de pipeline), Lot 12 facturation → BLOQUÉ (attendu) |
| 12 | SUCCES | MASTER_FACT_Proprietaires, 3132 lignes | 0 BLOQUANT Lot 12, 0 facture finale (attendu, en attente validation humaine) |
| 13 | SUCCES | 13/13 exports PowerBI + dictionnaire | Confidentialité vérifiée (aucune donnée sensible exportée) |

**Deuxième exécution intégrale** de la chaîne (idempotence) : tous les chiffres ci-dessus
**identiques au chiffre près** entre les deux passages (541/24/517/222/236/83/166/56/1409/
291722.75/281198.59/10524.16/1536/959/567/3132/13-13). Aucune duplication, aucune dérive.

**Technique de ciblage** (due diligence appliquée avant chaque lancement, pour ne jamais écrire
dans le worktree réel) :
- `lot8a_banque_import.py` : variables d'environnement `LOT8A_BRUT_FILE_OVERRIDE` /
  `LOT8A_OUT_FILE_OVERRIDE`.
- `lot8b_banque_regles.py` : aucun mécanisme de redirection — le script lui-même a été copié dans
  l'arborescence de copies puis supprimé après exécution (sa racine se dérive de son propre chemin
  `__file__`).
- `lot8c_rapprochement_banque.py` et `lot11_controles_coherence.py` : flag natif `--project-root`.
- `lot9_construire_flux.py`, `lot10_calculer_resultats.py`, `lot12_generer_factures.py`,
  `lot13_export_powerbi.py` : même technique que lot8b (copie temporaire du script, suppression
  après exécution).

**Incident et correction (transparence)** : un premier lancement de `lot8a` a utilisé
`--project-root` en croyant ce flag supporté partout ; le script l'a silencieusement ignoré et a
écrit dans le worktree réel (`02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx`, fichier gitignoré,
jamais suivi par git). Détecté immédiatement via le chemin « Sortie » imprimé par le script, fichier
supprimé, `git status --short` vérifié vide. Aucune source réelle (`01_SOURCES_BRUTES`) touchée —
le script ne fait que lire ce dossier. Relancé correctement ensuite via les bonnes variables
d'environnement.

## C. Rollback (exercé sur les copies)

Sauvegardes horodatées automatiques confirmées présentes dans `99_ARCHIVES/LOT8_Banque/` après
chaque étape (`*_PRE_LOT8B_*`, `*_PRE_LOT8C_*`). Restauration testée : copie d'une sauvegarde
pré-Lot8c, relecture confirmée cohérente (541 lignes, état attendu avant rapprochement), fichier de
test supprimé immédiatement après vérification. Aucune sortie orpheline laissée. Moteur relançable
sans réinitialisation.

## D. Campagne de tests complète (miniconda, `05_APPLICATION`)

2500 tests collectés (138 fichiers), exécutés en totalité par lots pour rester sous les limites de
temps d'exécution. **Résultat : 0 échec nouveau.** Un seul échec, déjà documenté avant cette
mission comme antérieur à tout ce chantier
(`tests/test_appsec1_diagnostic.py::test_07_diagnostic_local_avec_flag_explicite`, cf.
`HANDOFF_CANONIQUE.md`) : le chemin temporaire pytest de cette machine contient le nom d'utilisateur
Windows (`pytest-of-Ewan`), ce que le test interdit — dépendant de l'environnement d'exécution, pas
du code. Reproduit ici tel quel, non corrigé (hors mandat, antérieur).

Total largement supérieur à la référence précédente (2284 passés/75 ignorés/1 échec) : couverture
accrue par les modules Trésorerie propriétaires et rapprochements groupés ajoutés depuis. Aucun test
ignoré ou supprimé de façon opportuniste pendant cette mission.

## E. Intégrité avant/après (sources réelles du worktree)

Comparaison SHA256 contre la baseline `RECETTE_VALIDATION_20260807/BASELINE/before_pipeline.tsv`
(88 fichiers référencés, capturée à un état antérieur du projet, avant plusieurs commits déjà
landés de ce chantier) :

- **85/88 fichiers identiques.**
- **3 écarts, tous classés ATTENDU/INTENTIONNEL, aucun INATTENDU :**
  1. `02_TRAVAIL/lot8a_banque_import.py` — code, correspond exactement au commit HEAD `db7a381`
     (`git diff HEAD` vide) ; évolution déjà documentée (commit `da76d0f`, format relevé consolidé).
  2. `02_TRAVAIL/lot8c_rapprochement_banque.py` — code, correspond exactement au commit HEAD
     (`git diff HEAD` vide) ; évolution déjà documentée (commit `f71c566`, suppression des noms
     réels en dur `PROP_LABELS`).
  3. `05_APPLICATION/data/app.db` — base réelle de l'application en usage courant (gitignorée,
     jamais suivie par git) ; écart attribuable à l'usage normal de l'application réelle
     (port 8000/PID 21136, préexistant à cette session) entre la date de capture de la baseline et
     aujourd'hui, sans lien avec les environnements isolés de cette mission (aucune instance de
     recette n'a jamais pointé vers `APP_DATA_DIR` réel).
- `git status --short` sur le worktree entier : **vide** en permanence pendant toute la mission
  (sauf pendant la correction immédiate de l'incident lot8a ci-dessus). Aucun fichier fictif, log
  de recette, base synthétique ou copie de source n'a été laissé dans l'arborescence réelle.
- Port 8000 / PID 21136 : vérifié intact à plusieurs reprises, toujours en écoute, jamais redirigé.
- Mode réel : toujours désactivé (`RECETTE_MODE`/write-guards jamais levés sur une instance
  pointant vers les données réelles).

## F. Verdict (séparé)

- **Bloc Banque / Trésorerie (UI + rapprochements exact/partiel/groupé/ambigu + file A_ENVOYER_IA +
  pipeline Lot8a→13 + idempotence + rollback + campagne + intégrité) : VALIDÉ SUR COPIES.**
- **Airbnb : NO GO — SOURCE AIRBNB DETAILLEE ABSENTE** (inchangé, aucun export exploitable fourni).
- **Mode réel : NO GO** (inchangé — aucune validation humaine signée, aucune confirmation de
  rapprochement réelle, aucun arbitrage métier décidé par cette mission).

Prochaine action exacte : obtenir la fiche de signature `72_CHECKLIST_GO_NO_GO_MODE_REEL.md`
remplie par un humain, et/ou un export Airbnb détaillé exploitable, avant toute activation
progressive du mode réel décrite dans `71_DOSSIER_PREPARATION_MODE_REEL.md`.
