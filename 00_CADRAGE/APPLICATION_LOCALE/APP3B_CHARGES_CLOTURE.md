# APP-3b — Nouvelle charge guidée : clôture technique

> **STATUT : TERMINE_TECHNIQUEMENT_NON_ACTIVE.**
> La chaîne complète (saisie → prévisualisation → confirmation → écriture transactionnelle →
> journal → recalculs aval) est construite, testée et prouvée de bout en bout **sur copies isolées**.
> **Aucune écriture réelle n'a jamais eu lieu.** Les deux flags restent `False` :
>
> ```
> CHARGES_REAL_WRITE_ENABLED = False
> CHARGES_REAL_WRITE_CONFIRMATION_ENABLED = False
> ```
>
> La première écriture réelle est une **recette humaine distincte** (voir §9, runbook).
> Trace de contrôle : `CTR-APP3B-CLOTURE-01` (JOURNAL_CONTROLES.md).

---

## 1. Périmètre livré

| Brique | Commit |
|---|---|
| Writer bas niveau (temporaires validés, aucun remplacement) | `ae6ade5` |
| Orchestrateur transactionnel deux fichiers (+ rollback vérifié) | `7f9896e` |
| Verrou interprocessus + réservation du `charge_id` sous verrou | `a1d75f8` |
| Journal durable des tentatives (SQLite) | `eb883a4` |
| Générateur Lot3 (le MASTER charges est une sortie calculée) | `e1ffce5` |
| **Confirmation de bout en bout (ce lot)** | *en attente de commit* |

## 2. Architecture

```
Formulaire ──► previsualiser()  ──► manifest scellé + copies (dry-run, aucune écriture réelle)
                                          │
                     POST /fournisseurs/nouvelle/confirmer/{token}
                                          │   ← SEUL le token transite. Aucune donnée métier
                                          │     n'est acceptée du navigateur.
                     charges_confirmation_service.confirmer(token)
                       │
                       ├─ 0. flags (les deux)          ─┐
                       ├─ 1. manifest serveur           │  refus AVANT toute écriture,
                       ├─ 2. sceau d'intégrité          │  chacun tracé au journal
                       ├─ 3. fraîcheur (24 h)           │
                       ├─ 4. token déjà écrit ?         │
                       ├─ 5. copie conforme             │
                       ├─ 6. empreintes des fichiers réels
                       ├─ 7. REVALIDATION MÉTIER (mois clôturé, référentiels)
                       │
                       └─ confirmer_ecriture_charge()  ── verrou ── charge_id sous verrou
                                          │              ── temporaires validés
                                          │              ── sauvegardes
                                          │              ── os.replace ×2 (impacts puis charge)
                                          │              ── vérification post-commit
                                          │              ── rollback si échec
                                          │              ── journal
                                          │
                          ═══════ succès uniquement ═══════
                                          │
                       charges_post_write_service ──► sous-processus (interpréteur moteur)
                          runners/charges_post_write_runner.py
                             ├─ Lot3  : régénère MASTER_FACT_MAN_Charges
                             ├─ Lot7  : régénère MASTER_CALC_AVANTAGES (si avantage)
                             └─ Lot11 : contrôles du suivi associé
                                          │
                       303 ──► GET /fournisseurs/nouvelle/resultat/{token}
```

## 3. Fichiers de vérité

| Fichier | Rôle | Qui écrit |
|---|---|---|
| `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx` | **vérité métier** — la ligne de charge | l'app, transactionnellement |
| `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Impacts.xlsx` | **vérité métier** — affectations / ménage / réserve | l'app, transactionnellement |
| `02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx` | **sortie calculée** | le moteur (Lot3), hors transaction |
| `02_TRAVAIL/Lot7_IK_Avantages/MASTER_FACT_MAN_IK_Avantages.xlsx` | **sortie calculée** (suivi associé, HR) | le moteur (Lot7), hors transaction |
| `05_APPLICATION/data/app.db` | **journal applicatif** — jamais une vérité métier | l'app |

`file_registry` interdit à l'application d'écrire ailleurs que dans les `SAISIE_*` : les `MASTER_*`
et tout `02_TRAVAIL` lui sont fermés. Les masters ne sont donc **jamais** écrits par FastAPI, mais
régénérés par le moteur, dans un autre processus.

## 4. Protections

**Le navigateur ne fournit qu'un token.** Montant, catégorie, affectations, avantage : tout est relu
du manifest serveur. Un POST chargé de champs métier ne laisse aucune trace (testé).

**Sceau d'intégrité du manifest.** Les champs de décision sont hashés à la prévisualisation et
revérifiés à la confirmation. *Limite assumée* : le fichier est local et réinscriptible — c'est une
détection de corruption/altération, pas une frontière de sécurité.

**Empreintes des fichiers réels.** Si l'un des deux fichiers de saisie a bougé depuis la
prévisualisation, la décision est périmée et l'écriture est refusée.

**Revalidation métier à la confirmation.** Les empreintes ne couvrent **pas** les référentiels : un
mois clôturé entre-temps, une catégorie désactivée, un logement sorti du parc ne changent aucune
empreinte. Les règles sont donc rejouées sur l'état actuel (`validate_charge` + `compute_guidee`).

**Verrou interprocessus** (`O_CREAT|O_EXCL`), tenu de la résolution du `charge_id` jusqu'à la
vérification post-commit. C'est lui qui tient la réservation de l'identifiant.

**Durée de vie d'une prévisualisation : 24 heures** (décision validée le 2026-07-14). Au-delà, le
manifest est **expiré** et ne peut plus être confirmé — une **nouvelle prévisualisation est
obligatoire**, afin de repartir de l'état actuel des données.

> **Une expiration ne signifie jamais qu'une charge a été écrite.** Un manifest expiré est refusé
> *avant* toute écriture : aucun fichier n'est touché, rien n'a été enregistré. Le message invite
> simplement à refaire la saisie.
>
> **L'idempotence ne repose pas sur cette durée.** Elle est fondée sur le **token journalisé**,
> consulté **sous verrou** : c'est le journal qui dit si ce token a déjà produit une écriture, et
> lui seul. Un token expiré n'est donc pas « une écriture perdue » — c'est une décision périmée.

**Idempotence par token**, consultée sous verrou et **fail-closed** : si le journal est illisible
alors qu'un token est fourni, l'écriture est refusée (une saisie bloquée vaut mieux qu'une charge
comptée deux fois).

**Transaction deux fichiers.** `os.replace` n'est atomique que par fichier ; l'atomicité est
reconstruite au niveau applicatif — préparer tout, ne rien remplacer avant que tout soit prêt,
restaurer ce qui a été remplacé si la fin échoue. Ordre délibéré : **impacts d'abord, charge
ensuite**, pour que le pire cas soit des impacts orphelins (invisibles) plutôt qu'une charge sans
impacts (comptée à tort).

**POST-Redirect-Get.** Rafraîchir la page de résultat est une simple lecture ; un second POST
redirige sans réexécuter.

## 5. Transaction Excel vs recalculs aval — la distinction qui compte

La **transaction** porte sur les deux `SAISIE_*`, et sur eux seuls. Lot3, Lot7 et Lot11 sont
**hors transaction**, déclenchés après un succès.

**Un échec de recalcul n'annule jamais la charge écrite** et n'est jamais présenté comme un échec
d'écriture. Le résultat expose quatre statuts distincts (écriture / Lot3 / Lot7 / Lot11) et affiche :

> « La charge est bien écrite, mais le recalcul aval a échoué : les masters doivent être régénérés
> avant exploitation. Ne resaisissez pas la charge. »

Tant que Lot3 n'a pas tourné, la charge existe dans la SAISIE mais **reste invisible** pour Lot9,
Lot10 (net propriétaire), Lot11 et Lot12, qui lisent le MASTER.

## 6. Journal (`saisie_charges_writes`)

Toute tentative est tracée, **y compris les refus**. Neuf statuts métier — `REFUSE_FLAGS`,
`REFUSE_VERROU`, `REFUSE_VALIDATION`, `REFUSE_CHARGE_ID`, `ECHEC_PREPARATION`, `SUCCES`,
`ROLLBACK_REUSSI`, `ROLLBACK_CRITIQUE`, `ETAT_INCOHERENT` — distincts des codes techniques `E_*`.
La trace porte les SHA256 avant/après, les fichiers remplacés, l'état du rollback et, en cas
d'incident critique, **les fichiers non restaurés avec le chemin de leur sauvegarde**.

Aucune donnée métier n'y figure (ni montant, ni logement, ni propriétaire) : on journalise la
*tentative*, pas la charge.

Une panne du journal **après** l'écriture n'empêche ni la transaction ni un rollback : elle est
signalée (`journal_erreur`), jamais avalée.

## 7. Tests

| Suite | Contenu |
|---|---|
| `test_saisie_charges_writer.py` | 27 — préparation, refus, idempotence |
| `test_saisie_charges_transaction_service.py` | 35 — transaction, rollback, multiprocessus |
| `test_saisie_charges_lock_service.py` | 20 — verrou, diagnostic, verrou périmé |
| `test_saisie_charges_journal_service.py` | 20 — 9 statuts, fail-closed, `ETAT_INCOHERENT` |
| `test_charges_confirmation_e2e.py` | 18 — cas A→N sur copies isolées |
| `test_charges_confirmation_route.py` | 12 — routes, PRG, double soumission |
| `test_charges_confirmation_audit.py` | 18 — clôture : revalidation, fraîcheur, concurrence, E2E HTTP |

Aucun test n'ouvre les quatre Excel réels ni `app.db` réelle : c'est vérifié par empreinte.

## 8. Limites connues

**OneDrive.** La détection `~$` couvre « Excel est ouvert », pas « OneDrive resynchronise pendant
l'`os.replace` ». Aucun verrou applicatif ne protège de cela. Mitigation : mettre la synchro en
pause pendant l'écriture (voir runbook), et vérifier les empreintes après.

**Atomicité inter-fichiers inexistante.** Le pire cas est *choisi* (impacts orphelins), pas éliminé.

**Verrou périmé.** Un processus tué en section critique laisse un verrou qui bloque toute écriture.
`inspecter_verrou()` diagnostique mais **ne supprime rien** — un PID est recyclable, et le supprimer
automatiquement rouvrirait la course qu'on ferme. La levée est une décision humaine (§10).

**Le sceau du manifest n'est pas une frontière de sécurité** (fichier local réinscriptible).

**Aucune écriture réelle n'a jamais eu lieu.** Tout est prouvé sur copies ; la SAISIE réelle contient
toujours 0 charge.

## 9. Première recette réelle → voir `APP3B_RUNBOOK_PREMIERE_ACTIVATION.md`

## 10. Procédures d'incident

### 10.1 Verrou périmé (`.saisie_charges_write.lock`)

Symptôme : toute confirmation est refusée avec « Une autre écriture de charge est en cours ».

1. **Ne pas supprimer le fichier à l'aveugle.**
2. Diagnostiquer : `inspecter_verrou()` → `ACTIF_PROBABLE` (attendre), `POTENTIELLEMENT_PERIME`
   (le PID détenteur n'existe plus), ou `INDETERMINABLE` (autre machine, verrou corrompu).
3. Si `POTENTIELLEMENT_PERIME` : vérifier qu'**aucune** instance de l'app ne tourne, consulter le
   journal (`saisie_charges_writes`) pour savoir si une transaction est restée ouverte, puis
   **décider humainement** de supprimer le fichier de verrou.
4. Après suppression, vérifier les empreintes des deux `SAISIE_*` et l'absence de `.bak`.

### 10.2 `ROLLBACK_CRITIQUE` — état incohérent

Symptôme : écran rouge « INCIDENT — état incohérent », `fichiers_non_restaures` listés.

1. **Ne rien relancer. Ne pas ressaisir la charge.**
2. Les **sauvegardes sont conservées** (`<fichier>.<jeton>.bak.xlsx`, à côté du fichier réel) —
   c'est la seule voie de retour. Ne pas les supprimer.
3. Lire la trace `ROLLBACK_CRITIQUE` dans `saisie_charges_writes` : elle donne le `transaction_id`,
   les SHA256 avant/après et les fichiers non restaurés.
4. Restaurer manuellement les fichiers concernés depuis leur `.bak`, puis vérifier que leur SHA256
   correspond au `sha256_*_avant` de la trace.
5. Régénérer Lot3 (et Lot7 si nécessaire), relancer Lot11.
6. Consigner l'incident au JOURNAL_CONTROLES avant toute nouvelle écriture.

### 10.3 Écriture réussie, recalcul aval en échec

La charge **est** écrite : ne pas la ressaisir. Relancer le moteur (Lot3, puis Lot7 si avantage,
puis Lot11) et vérifier que la charge apparaît bien dans `MASTER_FACT_MAN_Charges`.

## 11. Décisions qui restent humaines

- Activer les deux flags (et la première charge réelle).
- Supprimer un verrou diagnostiqué périmé.
- Restaurer après un `ROLLBACK_CRITIQUE`.
- Mettre OneDrive en pause pendant une écriture.
