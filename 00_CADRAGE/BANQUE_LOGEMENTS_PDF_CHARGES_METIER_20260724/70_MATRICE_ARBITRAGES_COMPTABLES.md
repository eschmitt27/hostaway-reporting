# 70 — Matrice d'arbitrages comptables

Aucune règle comptable modifiée par cette mission. Décisions distinguées explicitement par nature
(technique / métier / comptable / fiscale) — Claude ne fournit aucun conseil fiscal définitif.

| Flux métier | Compte actuel | Statut | Justification | Arbitrage requis |
|---|---|---|---|---|
| Achats fournisseurs (défaut) | `606000` | **PROVISOIRE** (filet générique) | Mécanisme de résolution (`mapping_comptable_regles`) relié et testé, mais aucune règle `VALIDE` n'a été arbitrée par catégorie de charge | **Comptable** — choisir les comptes par catégorie (entretien, ménage, énergie, etc.) |
| Fournisseurs (dette) | `401000` | actif, non provisoire | Compte standard, aucune ambiguïté métier | aucun |
| Propriétaires (créance conciergerie) | `411000` | actif, non provisoire | Compte standard | aucun |
| Banque | `512000` | actif, non provisoire | Compte standard | aucun |
| Caisse | `530000` | actif, non provisoire | Compte standard, cœur Comptabilité (`49`) | aucun |
| Associés (OD/caisse) | `467000` | actif, non provisoire | Compte standard | aucun |
| Ventes / commissions (adaptateur Lot12) | `706000` | **PROVISOIRE** (libellé `SOURCE_PROVISOIRE_LOT12`) | Le montant vient de Lot12 (jamais recalculé), mais le compte lui-même n'a jamais été validé comme définitif face à un futur objet « facture propriétaire émise » | **Métier** — décider si le circuit Lot12→VENTES reste l'adaptateur définitif (cf. `67` §Fonctions différées : décision actuelle = oui) |
| Frais bancaires (Lot8b, nouveau) | `TYPE_FLUX_016` côté Lot9, pas encore mappé à un compte Comptabilité spécifique | **PROVISOIRE** (filet générique `606000` par défaut si une écriture est un jour générée) | Le flux existe dans Lot9 (24 lignes ce cycle) mais aucune écriture Comptabilité n'a encore été générée pour ces frais | **Comptable** — décider du compte dédié (charges bancaires, ex. `627000`) |
| TVA | aucun compte dédié construit | **NON TRAITÉ** | Hors périmètre de tous les chantiers Comptabilité à ce jour | **Fiscal** — nécessite un avis professionnel, pas une décision technique |
| Ventilation charge multi-logements (pool) | pas de mécanisme | **GAP CONNU** (`41` §7bis) | Aucune clé de poids n'existe ; ne jamais improviser une répartition | **Métier** — définir une clé de répartition (surface, nombre de nuitées, etc.) |

## Décisions techniques déjà prises (rappel, pas des arbitrages en attente)

- Le mécanisme de résolution de compte (`mapping_comptable_regles`, historisé par date de
  validité) est fonctionnel et testé — ce n'est pas la mécanique qui manque, seul le contenu des
  règles `VALIDE` reste à arbitrer.
- La ventilation analytique (`ecriture_ligne_ventilation`) trace déjà la méthode utilisée par
  ligne (affectation directe, facture multi-lignes, ménage, sans dimension) — aucun nouveau
  mécanisme à construire pour cette partie.

## Ce qui n'a jamais été traité par aucun chantier (à ne pas confondre avec un oubli de cette mission)

- Toute question fiscale (TVA, régime d'imposition, obligations déclaratives) — nécessite un
  professionnel qualifié, jamais une réponse générée automatiquement.
- Le plan de comptes complet au sens d'un plan comptable général français détaillé (le système
  actuel couvre les comptes strictement nécessaires au fonctionnement applicatif, pas un plan
  comptable exhaustif).
