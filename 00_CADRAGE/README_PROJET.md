# README_PROJET.md — Pilotage_Conciergerie

> **Version V2 — cadrage de lancement.** À compléter lot par lot sans modifier les décisions verrouillées.
> Porte d'entrée du projet. Le détail vit dans les fichiers de cadrage référencés.

---

## 1. Objectif du projet

Produire une vision financière et opérationnelle fiable d'une conciergerie courte durée gérant ~16 logements, en conciliant :

- réservations Hostaway (Airbnb, Booking, VRBO),
- réservations hors Hostaway (canal direct, encaissées en liquide ou virement),
- flux bancaires du compte professionnel,
- ménages internes et externes,
- charges payées par comptes personnels / liquide,
- IK et avantages associés,
- acomptes et factures propriétaires,

et de produire **trois lectures de résultat** : réel, comptable, hors compta — exploitables dans Excel, Power Query, Power BI.

### Contexte juridique

L'activité est **opérationnelle** mais la **SAS porteuse est nouvelle** et son enregistrement n'est pas encore complètement stabilisé. Le système prépare les flux et la future comptabilité, mais **ne suppose pas d'historique comptable** existant. Aucune écriture comptable passée n'est à rechercher.

---

## 2. Fichiers de cadrage

| Fichier | Rôle |
|---|---|
| `CLAUDE.md` | **À lire en premier.** Consignes permanentes + **stratégie de lecture ciblée par lot (§5 / §5.bis)** pour ne pas tout relire à chaque session. |
| `OBJECTIF_PROJET_PILOTAGE_CONCIERGERIE_V3.md` | Vision / objectif général. Lecture d'onboarding, non rechargée par lot. |
| `ARCHITECTURE_DONNEES.md` | Modélisation : sources, tables, clés, modules, contrôles, ordre des modules. **Table des matières en tête pour lecture par section.** |
| `REGLES_METIER.md` | Règles métier validées (Hostaway, M04, ménages externes, banque, clôture, anti-double-comptage). |
| `PLAN_CONSTRUCTION.md` | Plan de construction lot par lot (sans imposer de méthode). |
| `README_PROJET.md` | Présent document. Porte d'entrée. |

Le détail vit dans ces fichiers. Ce README **n'en duplique pas le contenu**, il y renvoie.

---

## 3. Structure des dossiers (conventions)

```text
Pilotage_Conciergerie/
├── 01_SOURCES_BRUTES/
│   ├── Banque/                     ← exports bancaires bruts, jamais modifiés
│   │   └── 2026_03_BRUT_Banque_CreditMutuel.xlsx
│   └── factures/                   ← factures prestataires / fournisseurs
├── 02_DONNEES_NORMALISEES/
│   └── menages/
│       └── M04_MENAGES_PowerQuery.xlsx
├── exports/
│   └── hostaway/master/            ← CSV master produits par GitHub Actions
└── 00_CADRAGE/
    ├── ARCHITECTURE_DONNEES.md
    ├── REGLES_METIER.md
    ├── PLAN_CONSTRUCTION.md
    └── README_PROJET.md
```

---

## 4. Fichiers sources clés

| Fichier / source | Statut | Détail |
|---|---|---|
| API Hostaway | Opérationnelle | Archi §6 |
| `REF_Setup.xlsm` (19 onglets) | Opérationnel | Archi §4 |
| `M04_MENAGES_PowerQuery.xlsx` | Source officielle ménages internes | Archi §11.4 |
| `2026_03_BRUT_Banque_CreditMutuel.xlsx` | Source brute bancaire | Archi §13.6 |

---

## 5. Ordre de construction (résumé)

Détail dans `PLAN_CONSTRUCTION.md`.

```text
Lot 0  · REF_Setup                     [À PRÉPARER / audit requis / non démarré]
Lot 1  · Module Hostaway               [extraction existante éventuelle, non validée sur données réelles]
Lot 2  · Réconciliation logements
Lot 3  · Charges perso / liquide
Lot 4  · Réservations hors Hostaway
Lot 4 bis · Table commune des réservations
Lot 5  · Acomptes propriétaires
Lot 6  · Ménages — 6a Hostaway (comptage) / 6b M04 interne / 6c externes (prérequis distincts, cf. plan)
Lot 7  · IK & avantages
Lot 8  · Banque & rapprochement
Lot 9  · Table de flux unifiée (colonne vertébrale)
Lot 10 · Résultats & commissions
Lot 11 · Contrôles de cohérence
Lot 12 · Livrables propriétaires Excel / données prêtes Power BI
```

Chemin critique : 0 → 1 → 2 → 3 → 4 → 4 bis → 7 → 9 → 10 → 11 → 12.

---

## 6. Règles critiques à ne jamais casser

| # | Règle |
|---|---|
| 1 | Ne jamais modifier une source brute. |
| 2 | Ne jamais double-compter banque et Hostaway. |
| 3 | Ne jamais utiliser Hostaway pour valoriser le coût réel ménage. |
| 4 | M04 = ménages internes uniquement, toujours `HC`. |
| 5 | Ménages externes = factures prestataires, `IC` par défaut mais sélectionnable. |
| 6 | Banque = prudence maximale sur les virements ; règles déterministes d'abord, IA ensuite, contrôle humain si doute. |
| 7 | Une ligne bancaire non classée = mois non clôturable. |
| 8 | Le nom du fichier bancaire ≠ période réelle. Rattachement par `Date` ou `Valeur`. |
| 9 | Table de flux unifiée `MASTER_CALC_Flux` = colonne vertébrale ; les trois résultats sont des filtres sur `code_impact`. |
| 10 | Pas de suppression automatique : PK + ROW_HASH, conservation des lignes disparues d'un extract. |

Le détail de chaque règle est dans `REGLES_METIER.md`.

---

## 7. Limites connues (rappel)

- API Hostaway, GitHub Actions, exports bancaires : tournent sur ta machine, hors Cowork.
- Excel macros / Power Query / Power BI : production hors Cowork.
- Arbitrages métier finaux (barème IK, format facture, périodicité) : à ton initiative.

---

## À compléter

- Compléter la structure du dossier `00_CADRAGE/` si tu adoptes une autre organisation.
- Compléter le mapping `REF_Mapping_Logements` pour les libellés Google Sheet ménages et factures prestataires.
- Compléter `REGLES_METIER.md` au fil des arbitrages futurs.
