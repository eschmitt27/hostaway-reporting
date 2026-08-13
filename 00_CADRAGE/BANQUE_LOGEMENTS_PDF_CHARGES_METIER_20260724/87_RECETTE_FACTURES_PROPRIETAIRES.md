# 87 — Recette des factures propriétaires (2026-08-13)

Recette **100 % fictive**, exécutée sur une copie de base migrée en 0027, port 8042 (jamais 8000).
Aucune donnée réelle, aucune identité réelle, aucune facture réelle.

## 1. Jeu de recette

Base : copie de `app.db` migrée 0016 → **0027** (les 4 tables `factures_proprietaires*` créées).

| Propriétaire | Logement | Commission | Ménage | Charge fixe | Canapé | Refac. | Total facture |
|---|---|---:|---:|---:|---:|---:|---:|
| PROP_RECETTE_A | LOG_RECETTE_1 | 300,00 | 150,00 | 50,00 | — | — | **500,00 €** |
| PROP_RECETTE_A | LOG_RECETTE_2 | 220,00 | 90,00 | 50,00 | 40,00 | — | **400,00 €** |
| PROP_RECETTE_B | LOG_RECETTE_3 | 410,00 | 180,00 | 50,00 | — | 75,00 | **715,00 €** |

2 propriétaires, 3 logements, un propriétaire multi-logement, un cas avec canapé, un cas avec
refacturation. Mois fictif 2026-06.

## 2. Parcours exécuté sur l'application en fonctionnement

| Étape | Résultat |
|---|---|
| Ouvrir « Factures propriétaires » | liste affichée, 3 factures BROUILLON, bandeau MODE RECETTE visible |
| Ouvrir une fiche | identités, 3 lignes facturées, total 500,00 €, solde 500,00 € NON_REGLEE, historique CREATION |
| Identité émetteur absente | badge `FACTURE_PROPRIETAIRE_IDENTITE_INCOMPLETE`, action « Émettre » indisponible |
| Identité émetteur renseignée | contrôle levé, validation possible |
| Valider | HTTP 303, statut → VALIDE |
| Émettre (date 2026-07-01) | HTTP 303, statut → EMIS, numéro `RECETTE-2026-00001` attribué |
| Télécharger le PDF | HTTP 200, `application/pdf`, 3 582 octets, en-tête `%PDF-1.3` |
| Vérifier le document servi | SHA256 du corps HTTP **identique** au hash figé à l'émission |

Le rendu graphique de Chrome a échoué à plusieurs reprises en fin de session (time-out du
renderer). Les étapes de navigation, d'affichage et de clic ont bien été effectuées et
constatées visuellement ; les dernières transitions (valider, émettre, télécharger) ont été
exécutées par de véritables requêtes HTTP sur **la même instance en fonctionnement**, ce qui
exerce exactement le même code applicatif. Les tests de routes (§4) rejouent ce parcours complet
de façon automatisée.

## 3. Contenu du PDF émis (extrait réel, données fictives)

```
FACTURE
Numero : RECETTE-2026-00001
Date : 2026-07-01

Emetteur                              Destinataire
Conciergerie Recette (fictive)        PROP_RECETTE_A
1 rue de Recette, 00000 Villetest
SIRET 00000000000000                  Reference proprietaire : PROP_RECETTE_A

Periode : 2026-06    Logement : LOG_RECETTE_1

N   Prestation                    Montant
1   Commission de conciergerie    300,00 EUR
2   Prestations de menage         150,00 EUR
3   Charge fixe mensuelle          50,00 EUR
    TOTAL                         500,00 EUR

Cette facture ne reprend que les prestations facturees par la conciergerie. Le detail des
revenus, des acomptes et du solde figure sur le releve proprietaire de la meme periode, qui
est un document distinct.

TVA non applicable - regime declare par l'emetteur.
Document genere par Pilotage Conciergerie - reference interne FPR-...
```

Réconciliation : 300 + 150 + 50 = **500,00 €** = `montant_du_conciergerie`. Le payout
(2 000,00 €) et le revenu net (1 500,00 €) **n'apparaissent pas** sur la facture : ils
appartiennent au relevé.

**Défaut corrigé en cours de recette** : les tirets cadratins sortaient en `?` sur le document
(caractère absent du jeu latin-1 des polices de base). Une table de translittération vers ASCII
a été ajoutée ; les accents, eux, existent en latin-1 et sont conservés.

## 4. Contrôles vérifiés sur l'instance vivante

| Contrôle | Résultat |
|---|---|
| Anti-doublon | recréer le même (mois, propriétaire, logement) → refus `FACTURE_PROPRIETAIRE_DOUBLON` citant la facture existante et son statut |
| Immutabilité | snapshot relu après modification des sources → numéro, total et lignes strictement identiques |
| Avoir | avoir créé avec lignes inversées (−300 / −150 / −50, total −500,00 €) ; **originale intacte** : EMIS, 500,00 €, numéro conservé |
| Un seul avoir | second avoir refusé (`..._DOUBLON`) |
| Numéro non réutilisé | l'avoir reçoit un numéro distinct de la facture d'origine |
| Solde dérivé | 0 → NON_REGLEE · 200 → PARTIELLEMENT_REGLEE (300,00 €) · 500 → REGLEE (0,00 €) · 600 → TROP_PERCU_A_CONTROLER |
| Règlement non intrusif | après imputation : montant, version et hash du document inchangés |
| Document avant émission | HTTP 404 avec `FACTURE_PROPRIETAIRE_PDF_ABSENT` |

## 5. Tests automatisés

| Suite | Résultat |
|---|---|
| `test_factures_proprietaires.py` (modèle, snapshot, PDF, avoir, solde, journal) | **25 passés** |
| `test_factures_proprietaires_routes.py` (parcours HTTP complet) | **6 passés** |
| Non-régression factures fournisseurs + charges | **58 passés** |
| **Sous-total ciblé** | **89 passés, 0 échec** |

**Régression ciblée élargie** (factures fournisseurs + propriétaires, règlements, comptabilité,
propriétaires) : **555 passés, 0 échec**, 6 min 20. La campagne complète de la suite applicative a
été lancée mais interrompue par l'environnement (`killed`) ; la régression ci-dessus couvre les
modules touchés et ceux exposés au risque.

**Bug d'isolation trouvé et corrigé pendant cette recette** : le répertoire de stockage des PDF
était calculé à l'import de la configuration, ce qui contournait la redirection de `DATA_DIR` — un
PDF de fixture avait été écrit dans le vrai dossier `data/`. Constaté au scan avant commit,
supprimé, cause corrigée (résolution à l'appel, comme `cfg.DB_PATH`), test de non-régression ajouté
et règle `.gitignore` posée.

## 6. Intégrité

Aucune donnée réelle touchée : base réelle non migrée (toujours 0016, hash inchangé), REF_Setup,
MASTER Hostaway, HIST, Banque et sources Lot 5 inchangés. Toute la recette s'est faite sur une copie
dans le scratchpad. Le serveur de recette (port 8042) a été arrêté à la fin — et lui seul. **Le port
8000 n'a jamais été utilisé ni manipulé.** Mode réel resté OFF.

**Aucun PDF de recette n'est versionné dans Git.**
