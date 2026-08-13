# 86 — Facturation propriétaires : l'application crée désormais ses factures (2026-08-13)

Avant cette mission, l'application savait **calculer** une préfacture propriétaire (Lot 12) mais ne
savait pas **créer** la facture qu'elle émet : pas d'objet facture, pas de numéro, pas de document,
pas de cycle de vie, pas de suivi de règlement. C'est ce manque que ce chantier comble.

## 1. Les trois objets à ne plus confondre

| Objet | Qui le produit | Ce qu'il contient | Où il vit |
|---|---|---|---|
| **Préfacture / relevé** (Lot 12) | le moteur de calcul | 12 ou 13 lignes : payout, ménage, commission, canapé, charge fixe, revenu net, refacturations, acomptes, reste à payer, statut | Excel (`MASTER_FACT_Proprietaires.xlsx`) |
| **Facture propriétaire émise** | **l'application** | uniquement les prestations facturées | SQLite + PDF |
| **Facture fournisseur reçue** | le fournisseur | pièce externe, importée/enregistrée | SQLite (`factures`, 0017/0022) |

Le relevé **explique** au propriétaire son revenu et son solde. La facture **facture**. Les deux
documents coexistent ; l'un n'est pas une vue de l'autre.

## 2. Ce qui est facturable — et ce qui ne l'est pas

Le total d'une facture réconcilie exactement `montant_du_conciergerie`, calculé par Lot 10 comme :

```
commission + ménage + préparation canapé + charge fixe + charges exceptionnelles refacturées
```

| Type Lot 12 | Rôle | Dans la facture ? | Dans le relevé ? |
|---|---|:---:|:---:|
| `COMMISSION_CONCIERGERIE` | prestation | **oui** | oui |
| `MENAGE_FACTURE` | prestation | **oui** | oui |
| `PREPARATION_CANAPE` | prestation | **oui** | oui |
| `CHARGE_FIXE` | prestation | **oui** | oui |
| `CHARGES_EXCEPT_REFAC` | refacturation | **oui** | oui |
| `TOTAL_PAYOUT` | information d'exploitation | non | oui |
| `REVENU_NET_EXPLOITATION` | information d'exploitation | non | oui |
| `ACOMPTE_AIRBNB` | paiement déjà reçu | non | oui |
| `PAIEMENT_DEJA_RECU` | paiement déjà reçu | non | oui |
| `ACOMPTES_PROPRIETAIRES` | paiement déjà reçu | non | oui |
| `RESTE_A_PAYER` | solde dérivé | non | oui |
| `STATUT_REGLEMENT` | statut | non | oui |

Ces deux listes sont **explicites dans le code** (`TYPES_FACTURABLES` / `TYPES_NON_FACTURABLES`) :
un type non classé ne peut pas se retrouver facturé par inadvertance. Une ligne à zéro n'est pas
facturée. Un écart entre le total facturé et le montant dû calculé est **signalé**
(`FACTURE_PROPRIETAIRE_TOTAL_INCOHERENT`), jamais corrigé en silence.

## 3. Grain

**Mois × propriétaire × logement**, identique à celui de Lot 12 (`PREF-{mois}-{prop}-{log}-{seq}`).
Non modifié : aucune contradiction n'a été trouvée dans les documents existants qui justifierait de
changer le grain, et un propriétaire multi-logement reçoit donc une facture par logement.

## 4. Modèle (migration 0027)

Table **séparée**, pas de réutilisation de `factures` (0017/0022). Raison mesurée :
`factures.fournisseur_id_opaque` est `NOT NULL`, un index unique porte sur (fournisseur, référence),
et `facture_lignes.charge_id` est `NOT NULL`. Une facture propriétaire n'a ni fournisseur, ni
référence externe (c'est nous qui attribuons le numéro), et ses lignes ne sont pas des charges
fournisseur. La réutiliser aurait imposé un fournisseur fictif et une charge fictive par ligne.

- `factures_proprietaires` — en-tête, statut, numéro, snapshot, hashs, dates.
- `factures_proprietaires_lignes` — lignes facturées, avec référence vers l'objet de calcul source.
- `factures_proprietaires_evenements` — journal **append-only**.
- `factures_proprietaires_sequence` — compteur de numérotation.

La migration est additive, `IF NOT EXISTS` / `INSERT OR IGNORE`, cohérente avec 0017→0026.

## 5. Cycle de vie

```
BROUILLON ──valider──> VALIDE ──émettre──> EMIS ──créer avoir──> (AVOIR lié)
    └──annuler──> ANNULE          └──annuler──> ANNULE
```

- **BROUILLON** : modifiable, annulable.
- **VALIDE** : contenu contrôlé, prêt à émission.
- **EMIS** : **immutable**. Ni annulation en place, ni édition, ni suppression.
- **ANNULE** : reste dans l'historique ; libère le grain pour un remplaçant.

Aucune validation silencieuse : chaque refus porte un code stable
(`FACTURE_PROPRIETAIRE_IDENTITE_INCOMPLETE`, `..._SOURCE_INCOMPLETE`, `..._TOTAL_INCOHERENT`,
`..._DOUBLON`, `..._PDF_ABSENT`, `..._SNAPSHOT_INCOHERENT`, `..._EMISE_MODIFIEE`).

## 6. Snapshot — le point le plus important

À l'émission, l'intégralité du contenu (émetteur, destinataire, lignes, total, période, régime TVA)
est figée dans un JSON horodaté avec son SHA256. **Toute relecture d'une facture émise passe par ce
snapshot, jamais par les sources.**

Conséquence vérifiée par test : après émission, un recalcul Lot 10, un changement de taux, une
correction de charge ou un changement de propriétaire **ne modifient pas** la facture. Le total, les
lignes, les identités et le hash du PDF restent identiques. Une altération du snapshot en base est
détectée (`FACTURE_PROPRIETAIRE_EMISE_MODIFIEE`).

## 7. Numérotation

Séquence dédiée, incrémentée sous `BEGIN IMMEDIATE` : deux émissions concurrentes ne peuvent pas
obtenir le même numéro. L'index unique sur `numero_facture` est **sans clause `WHERE`** — un numéro
attribué n'est jamais réutilisé, même après annulation.

Format actuel `<serie>-<seq:05d>` (recette : `RECETTE-2026-00001`). **C'est un défaut technique, pas
une décision juridique.** Le format officiel et les mentions légales obligatoires restent à arbitrer
avec l'utilisateur avant toute émission réelle — ils ne sont pas inventés dans le code.

## 8. Document PDF

Généré **uniquement depuis le snapshot**. Déterministe : date de création figée, compression
désactivée, donc *même snapshot = même hash*, ce qui rend le contrôle d'intégrité exploitable.

Stockage `<répertoire configurable>/<AAAA>/<MM>/<numéro>.pdf`. Seul le **nom** est persisté en base,
jamais un chemin absolu. Le téléchargement sert le fichier figé correspondant au hash enregistré —
jamais un PDF reconstruit à la volée.

**TVA** : aucune TVA n'est calculée ni affichée, conformément à la décision utilisateur en vigueur.
Le snapshot porte `regime_tva = NON_ASSUJETTI_DECISION_UTILISATEUR` et le document affiche une
mention neutre.

## 9. Identités

Émetteur lu depuis la configuration (`SOCIETE_NOM`, `SOCIETE_ADRESSE`, `SOCIETE_SIRET`), **vide par
défaut**. Destinataire lu depuis le référentiel propriétaires. Si une information obligatoire manque,
la validation est refusée et la facture reste BROUILLON : **rien n'est inventé pour compléter un
document**.

## 10. Règlement et Banque

Le solde est **dérivé, jamais stocké** : `total − paiements imputés`. Un règlement impute une créance
qui existe déjà — il ne recrée ni commission, ni charge, ni réservation, ni facture (vérifié par
test : ni le montant, ni la version, ni le hash du document ne bougent).

Côté Banque, la règle absolue reste inchangée : **Banque ≠ Réservation**. Un mouvement bancaire peut
être rapproché d'une créance propriétaire légitime via le moteur de rapprochement existant, jamais
d'une réservation. Aucun générateur de candidat réservation n'a été introduit.

## 11. Interface

`Factures propriétaires` (entrée de navigation distincte de `Factures`, qui reste les factures
fournisseurs reçues) :

- **liste** — période, propriétaire, logement, numéro, total, solde, statut, date d'émission ;
- **prévisualisation d'un mois** — lecture pure, **aucune écriture avant confirmation** : chaque
  proposition porte `PRETE` / `A_CONTROLER` / `NON_CONCERNE`, avec les lignes facturées d'un côté et
  les éléments de relevé repliés et étiquetés « non facturés » de l'autre ;
- **génération en lot** — crée les BROUILLON des seules propositions `PRETE` ;
- **fiche** — identités, lignes, total, règlement, actions selon l'état, historique complet.

## 12. Ce qui n'a pas été fait, et pourquoi

- **Écriture comptable de vente** : non branchée. L'analyse du risque de double comptage
  (commission Lot 10 → facture → règlement → Banque) demande un arbitrage sur la source unique de
  l'écriture de vente, qui n'existe pas encore dans les documents. Brancher un générateur VENTES sans
  cet arbitrage créerait précisément le double comptage que le cadrage interdit. **Point ouvert,
  consigné, non contourné.**
- **Factures voyageurs / tiers** : hors périmètre (aucun besoin concret actuel). Le modèle ne
  l'empêche pas — `type_document` est extensible.
- **Rattachement automatique facture ↔ mouvement bancaire** : le moteur de rapprochement existant
  n'a pas été étendu à ce nouveau type d'objet. Le solde se calcule déjà, l'imputation reste à
  câbler sur le moteur générique.

## 13. Verdict

| Axe | État |
|---|---|
| FACTURES FOURNISSEURS | VALIDÉES — non-régression vérifiée |
| FACTURES PROPRIÉTAIRES | **APPLICATION CAPABLE DE LES CRÉER** |
| PDF | **GÉNÉRÉ**, déterministe, téléchargeable |
| NUMÉROTATION | PRÊTE (format à arbitrer juridiquement) |
| SNAPSHOT | **VALIDÉ** — immutabilité prouvée |
| RÈGLEMENT | Solde dérivé validé ; imputation Banque à câbler |
| COMPTABILITÉ | **écriture de vente non branchée — arbitrage requis** |
| ÉMISSION RÉELLE | NON AUTORISÉE |
| APP.DB RÉELLE | NON MIGRÉE |
| MODE RÉEL | **NO GO — NON ACTIVÉ** |
