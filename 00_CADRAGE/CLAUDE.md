# CLAUDE.md

> Consignes permanentes pour Claude / Claude Code / Cowork sur le projet **Pilotage_Conciergerie**.
> Court, opérationnel, non redondant avec les autres `.md`. Lire **en premier** à chaque session.

---

## 1. Rôle du projet

Système de pilotage financier et opérationnel d'une conciergerie courte durée (~16 logements, Toulouse / Blagnac). Concilie Hostaway, réservations hors Hostaway, banque, ménages internes / externes, charges perso / liquide, IK / avantages associés. Produit trois lectures : **réel / comptable / hors compta** via filtre sur `code_impact` (IC / HC / HR).

La **SAS porteuse est nouvelle** : pas d'historique comptable à reconstituer. Le système prépare les flux futurs.

## 2. Emplacement officiel des fichiers de cadrage

```text
Pilotage_Conciergerie/99_DOCUMENTATION/
├── CLAUDE.md                  ← ce fichier (consignes permanentes)
├── README_PROJET.md
├── REGLES_METIER.md
├── ARCHITECTURE_DONNEES.md
└── PLAN_CONSTRUCTION.md
```

## 3. Ordre de priorité des documents

En cas de conflit entre documents, l'ordre suivant tranche :

```text
1. REGLES_METIER.md       = vérité métier
2. ARCHITECTURE_DONNEES.md = vérité structurelle (tables, clés, flux)
3. PLAN_CONSTRUCTION.md   = ordre de construction
4. README_PROJET.md       = porte d'entrée
5. CLAUDE.md              = consignes permanentes d'exécution
```

Si une règle de `REGLES_METIER.md` contredit `ARCHITECTURE_DONNEES.md`, la règle métier gagne et l'architecture doit être corrigée.

## 4. Règles non négociables

- Ne jamais modifier les sources brutes (Banque, Hostaway, PDF, exports sources).
- Ne jamais coder avant que le lot soit clairement validé.
- Ne jamais construire plusieurs lots en une seule passe.
- Ne jamais inventer une règle métier.
- Ne jamais supposer une écriture comptable passée : la SAS est nouvelle.
- Ne jamais utiliser Hostaway pour valoriser le coût réel ménage.
- M04 = ménages internes, main-d'œuvre uniquement = **toujours HC — sans Courses, sans lavage, sans achats (D027 irrévocable)**.
- Ménages externes = futur fichier depuis factures PDF, **IC par défaut** mais sélectionnable.
- Banque = rapprochement, sauf si elle est source économique principale.
- Règles bancaires déterministes **avant** IA.
- Prudence maximale sur les virements (l'IA ne valide jamais définitivement une ligne sensible).
- Ligne bancaire non classée → mois **non clôturable**.
- Table commune des réservations obligatoire avant `MASTER_CALC_Flux`.
- `MASTER_CALC_Flux` = colonne vertébrale.
- Un flux = une source, une clé, un hash, un statut, un code impact.
- **`SAISIE_Charges_Flux.xlsx` = source unique des achats/charges/consommables/linge/matériel (D026). Exclut IK.**
- **`revenu_net_exploitation_proprietaire` ne doit jamais contenir acomptes, paiements reçus, achats exceptionnels (D031/D033).**
- **Aucun lot ne peut être marqué FAIT sans entrée dans JOURNAL_CONTROLES (D029 irrévocable).**

## 5. Lecture minimale par session (budget tokens)

Ne **jamais** lire tous les `.md` en entier à chaque session. Lecture par défaut, dans l'ordre :

```text
1. CLAUDE.md (ce fichier).
2. Le lot courant dans PLAN_CONSTRUCTION.md (uniquement ce lot + ses dépendances directes).
3. Les sections ciblées de REGLES_METIER.md et ARCHITECTURE_DONNEES.md indiquées
   par la matrice §5.bis ci-dessous.
```

Règles de budget :
- **Lecture ciblée par section.** ARCHITECTURE_DONNEES.md (~18 k tokens) ne se lit jamais en entier : utiliser sa table des matières (en tête de fichier) pour ne charger que les `§` utiles au lot.
- **Ne pas relire README_PROJET.md** hors onboarding : c'est une porte d'entrée, pas une source de vérité.
- **OBJECTIF_PROJET_…V3.md** = vision générale, à lire une fois ; ne pas le recharger par lot (le détail constructible vit dans ARCHITECTURE + PLAN + REGLES).
- En cas de doute sur « quelle section lire », préférer la matrice §5.bis à une relecture intégrale.

## 5.bis Matrice lot → sections à lire / fichiers sources / à éviter

> Sections référencées par leur numéro `§`. La table des matières d'ARCHITECTURE_DONNEES.md (en tête de fichier) permet d'atteindre directement la bonne section.

| Lot | Lecture documentaire ciblée | Fichiers / référentiels sources à ouvrir | À ne PAS ouvrir |
|---|---|---|---|
| 0 — REF_Setup `[À PRÉPARER / audit requis / non démarré]` | PLAN Lot 0 ; REGLES §1, §13 ; ARCHI §2, §4, §16 | `REF_Setup.xlsm` réel | Banque, factures, Hostaway détaillé |
| 1 — Hostaway (extraction existante, non validée) | ARCHI §6, §7 ; REGLES §2 | Tables `*_HA_*` | Reste |
| 2 — Mapping logements | PLAN Lot 2 ; REGLES §9 ; ARCHI §4, §16.3, §18.3, §20 | `REF_Logements`, `REF_Mapping_Logements`, listings Hostaway, en-têtes Google Sheet `Suivi ménage` | Banque, factures complètes |
| 3 — `SAISIE_Charges_Flux.xlsx` | PLAN Lot 3 ; REGLES §3.bis, §1, §8 ; ARCHI §10 | `SAISIE_Charges_Flux.xlsx` à créer, réf. charges/modes paiement/cartes/associés/logements | Hostaway détaillé, banque brute |
| 4 — Hors Hostaway | PLAN Lot 4 ; REGLES §10 ; ARCHI §9 | Table HH, réservations Hostaway `direct`/VRBO | Banque |
| 4 bis — Table commune | PLAN Lot 4 bis ; REGLES §10 ; ARCHI §9.5 | Réservations HA + HH | Banque |
| 5 — Acomptes propriétaires | PLAN Lot 5 ; ARCHI §10.4 | Sortie Lot 4, `REF_Proprietaires`, `REF_Logements` | Banque, ménages |
| 6a — Hostaway ménage (comptage) | PLAN Lot 6a ; REGLES §2 (H6), §3 ; ARCHI §11.1, §11.2 | `MASTER_FACT_HA_CleaningTasks_Discovery`, `REF_Types_Lignes_Menage` | Coût Hostaway (interdit en valorisation) |
| 6b — M04 ménages internes | PLAN Lot 6b ; REGLES §3, §5 ; ARCHI §11.4 | `M04_MENAGES_PowerQuery.xlsx`, mapping logements (**sans onglet achats, sans Courses, sans lavage — D027**) | Factures ménage externe, banque, `SAISIE_Charges_Flux` (séparé) |
| 6c — Ménages externes | PLAN Lot 6c ; REGLES §4, §5 ; ARCHI §11.5 | Factures PDF prestataires, `REF_Intervenants`, mapping logements | M04, Hostaway détaillé |
| 7 — IK & avantages | PLAN Lot 7 ; REGLES §10 ; ARCHI §12 | `REF_Associes`, `REF_Types_Flux`, dérivés Lots 3/4 | Banque brute |
| 8 — Banque | PLAN Lot 8 ; REGLES §6, §7, §8 ; ARCHI §13 | Export Crédit Mutuel, `REF_Banque_Regles` | Hostaway détaillé non nécessaire |
| 9 — Flux unifié | PLAN Lot 9 ; REGLES §1, §2 (H4), §8 ; ARCHI §2.3, §14 | Tables calculées déjà produites | Sources brutes PDF/API |
| 10 — Résultats & commissions | PLAN Lot 10 ; REGLES §2 ; ARCHI §8, §15, §17 | `MASTER_CALC_Flux`, `MASTER_CALC_Reservations`, taux propriétaires | Banque brute |
| 11 — Contrôles | PLAN Lot 11 ; ARCHI §18 ; REGLES §1, §6, §8, §11 | Toutes tables selon le contrôle | Sources brutes sauf investigation |
| 12 — Livrables | PLAN Lot 12 ; ARCHI §15, §17 ; REGLES §11 | `NetProprietaire`, résultats, acomptes, charges refacturables | Données brutes |

## 5.ter Méthode de travail attendue

```text
1. Lire CLAUDE.md + lecture ciblée §5 / §5.bis pour le lot courant.
2. Travailler lot par lot (jamais plusieurs lots en une passe).
3. Avant modification : annoncer les fichiers touchés (cf. §7).
4. Après modification : résumer changements, incohérences détectées, points à valider (cf. §8).
```

## 6. Interdictions

- Ne pas renommer les tables sans justification documentée.
- Ne pas changer les règles métier pour simplifier le code.
- Ne pas fusionner ménages internes, ménages externes et tâches Hostaway dans une même table.
- Ne pas créer de classification bancaire automatique sur libellé ambigu.
- Ne pas utiliser `APPARTEMENT_DIVERS` / `LOGEMENT_DIVERS` pour cacher un mauvais mapping.
- Ne pas modifier les fichiers bruts.
- Ne pas écraser une règle déterministe validée par une décision IA.

## 7. Règles de validation avant toute modification

Avant toute modification d'un fichier de cadrage, de table ou de référentiel :

```text
1. Identifier le ou les lots concernés (cf. PLAN_CONSTRUCTION.md).
2. Vérifier qu'aucune règle de REGLES_METIER.md n'est contredite.
3. Vérifier qu'aucune clé / table de ARCHITECTURE_DONNEES.md n'est cassée.
4. Annoncer en début de réponse : fichiers touchés, sections concernées, nature de l'édit.
5. Préférer une édition ciblée (str_replace) à une réécriture intégrale.
6. Si une décision métier est requise → poser la question, ne pas trancher seul.
```

## 8. Format attendu des réponses de Claude

Toute réponse qui modifie un fichier ou propose une action doit se terminer par :

```text
1. Fichiers touchés (chemin + sections).
2. Résumé des changements.
3. Incohérences détectées (le cas échéant).
4. Points à valider côté humain (questions explicites).
5. Prochaine action recommandée.
```

Pour les réponses purement informatives (lecture, contrôle, vérification), ce bloc n'est pas obligatoire mais reste recommandé si une décision en découle.

---

## Limites connues (rappel)

- API Hostaway, GitHub Actions, Power Query, Power BI, exports bancaires : exécutés hors environnement Claude. Claude **conçoit et génère**, ne fait pas tourner ces outils.
- Connexions vivantes (banque, API tierces) : hors périmètre Claude.
- Validation finale métier : toujours côté humain.
