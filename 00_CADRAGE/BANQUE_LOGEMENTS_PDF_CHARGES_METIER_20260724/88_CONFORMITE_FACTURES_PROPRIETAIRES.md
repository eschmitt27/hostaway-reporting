# 88 — Conformité des factures propriétaires (2026-08-13)

Le module de facturation était fonctionnel mais produisait un document incomplet du point de vue
réglementaire. Ce chantier le rend **techniquement prêt** à émettre de vraies factures. Ce qui
reste manquant n'est plus de l'architecture : ce sont des **valeurs à renseigner** et une
**obligation future de transmission électronique**.

## 1. Principe directeur

**Aucune donnée juridique n'est inventée.** Le code ne choisit ni le régime de TVA, ni le taux de
pénalités, ni le montant de l'indemnité forfaitaire, ni la rédaction d'une mention légale. Tout
cela vient de la configuration, **vide par défaut**. Une valeur absente ne reçoit pas de repli
« raisonnable » : elle bloque l'émission réelle et se nomme.

C'est aussi la raison pour laquelle le type de client n'est jamais deviné : sans information
explicite il vaut `A_CONTROLER` et bloque, plutôt que d'être supposé « particulier ».

## 2. Numérotation

| Document | Format | Exemple |
|---|---|---|
| Facture | `F-AAAA-NNNNNN` | `F-2026-000001` |
| Avoir | `A-AAAA-NNNNNN` | `A-2026-000001` |

Deux **séries indépendantes**, portées par le compteur `factures_proprietaires_sequence` déjà en
place (sa clé est la série). Propriétés vérifiées par test :

- numéro **unique** (index unique sans clause `WHERE` : jamais réutilisé, même après annulation) ;
- **consommé seulement à l'émission** — un brouillon abandonné ne crée aucun trou ;
- **figé** une fois attribué, jamais réécrit ;
- **concurrence protégée** (`BEGIN IMMEDIATE` sérialise deux émissions simultanées).

**Changement d'année** : chaque année ouvre une série explicite (`F-2026` → `F-2027`), le compteur
repart à 1 dans la nouvelle série. Choix retenu parce qu'il ne demande aucune mécanique nouvelle —
la série était déjà la clé du compteur — et parce que l'année reste lisible dans le numéro.

## 3. Identité de l'émetteur

Champs supportés : dénomination, forme juridique, capital, SIREN, SIRET, RCS, adresse du siège,
identifiant TVA, contact, coordonnées de paiement.

**Requis pour émettre** : `denomination`, `adresse_siege`, `siren`. Leur absence déclenche
`FACTURE_IDENTITE_EMETTEUR_INCOMPLETE`.

`DECISIONS_METIER.md` acte que « les coordonnées définitives de la société seront fournies plus
tard ». Elles ne sont donc **pas** dans le dépôt : la configuration les attend, la recette utilise
des valeurs fictives explicites (`SAS DEMO CONCIERGERIE`), et l'émission réelle reste fermée.

## 4. Type de client et mentions

| Type | Données figées | Clauses B2B |
|---|---|---|
| `PARTICULIER` | nom, adresse, adresse de facturation | **non** |
| `PROFESSIONNEL` | dénomination, adresse, SIREN, TVA intra | **oui** — pénalités + indemnité exigées |
| `A_CONTROLER` | — | émission bloquée |

Les clauses professionnelles sont exigées à la validation **et** filtrées à l'impression : une
facture adressée à un particulier — ou à un client dont le type n'est pas tranché — n'affiche
aucune clause B2B, même si le taux et l'indemnité sont configurés globalement. Ce point a été
trouvé pendant la recette et corrigé (voir §10).

## 5. TVA

Vocabulaire aligné sur la **décision D083**, déjà en vigueur pour les prestataires de ménage :

| Régime | Effet |
|---|---|
| `FRANCHISE_TVA` | TVA = 0, HT = TTC, mention configurée obligatoire |
| `ASSUJETTI_TVA` | taux configuré appliqué, TVA calculée |
| `EXONERATION_AUTRE` | TVA = 0, mention configurée obligatoire |
| `A_CONTROLER` (défaut) | **émission bloquée** — `FACTURE_REGIME_TVA_NON_CONFIRME` |

La décision utilisateur « pas de TVA pour le moment » ne suffit pas à sélectionner un fondement
juridique : elle dit qu'aucune TVA n'est facturée, pas *pourquoi*. Le régime reste donc
`A_CONTROLER` tant qu'il n'est pas déclaré, et le régime seul ne suffit pas non plus — la mention
réglementaire correspondante doit être configurée (`FACTURE_MENTION_TVA_MANQUANTE`).

La facture affiche toujours **TOTAL HT / TVA / TOTAL TTC**, même quand la TVA vaut 0,00 €.

## 6. Nature de l'opération et période

- `nature_operation = PRESTATION_DE_SERVICES` (le projet ne vend aucun bien) ;
- `adresse_livraison = NON_APPLICABLE` ;
- **période de prestation** distincte de la date d'émission : le mois facturé, du premier au
  dernier jour (« Periode des prestations : du 01/07/2026 au 31/07/2026 »), calculé sans se tromper
  sur les mois courts ni les années bissextiles.

## 7. Conditions de règlement

Délai de paiement, échéance calculée (date de facture + délai), conditions d'escompte, taux de
pénalités, indemnité forfaitaire de recouvrement — tous configurables, aucun défaut choisi par le
code. Sans délai configuré, l'échéance est incalculable et l'émission est bloquée
(`FACTURE_ECHEANCE_NON_CONFIGUREE`).

## 8. Contrôle de pré-émission

Un point d'entrée unique, `verifier()`, renvoie `PRETE_A_EMETTRE` ou `BLOQUEE` **avec la liste des
manques**, chacun portant un code stable — jamais un simple booléen : l'utilisateur doit pouvoir
lire pourquoi une facture ne peut pas être émise.

Codes : `FACTURE_IDENTITE_EMETTEUR_INCOMPLETE`, `FACTURE_IDENTITE_CLIENT_INCOMPLETE`,
`FACTURE_TYPE_CLIENT_NON_DETERMINE`, `FACTURE_REGIME_TVA_NON_CONFIRME`,
`FACTURE_MENTION_TVA_MANQUANTE`, `FACTURE_PENALITES_RETARD_NON_CONFIGUREES`,
`FACTURE_INDEMNITE_RECOUVREMENT_NON_CONFIGUREE`, `FACTURE_PERIODE_PRESTATION_MANQUANTE`,
`FACTURE_ECHEANCE_NON_CONFIGUREE`, `FACTURE_SANS_LIGNE`, `FACTURE_TOTAL_INCOHERENT`.

Le contrôle est **toujours affiché**, mais ne **bloque** que si l'émission réelle est ouverte : la
recette peut ainsi exercer le parcours complet avec une configuration incomplète, sans que cela
crée un chemin permissif en production.

## 9. Source de vérité et facturation électronique

La source de vérité est le **snapshot structuré**, enrichi du bloc de conformité. Le PDF en est un
**rendu dérivé figé** ; une future facture électronique sera un autre rendu du même snapshot.
**Aucun recalcul métier à l'export.**

Champs préparés (migration 0028), tous neutres et nullables : `electronic_invoice_status`
(`NON_APPLICABLE` par défaut), `_provider`, `_external_id`, `_format`, `_sent_at`,
`_received_status`. Le SIREN client et `nature_operation` sont déjà structurés.

**Volontairement non fait** : aucune plateforme choisie, aucune intégration API, aucun envoi,
aucun e-reporting, pas de Factur-X. L'objectif était de ne pas avoir à remodeler la table le jour
venu. Chantier futur identifié : `FACTURATION_ELECTRONIQUE_PA`.

## 10. Défaut trouvé et corrigé pendant la recette

Le PDF imprimait les clauses B2B dès qu'elles étaient configurées, sans regarder le type de client.
Constaté en E2E sur une facture dont le client était `A_CONTROLER` : pénalités et indemnité
apparaissaient à tort. Corrigé — l'impression est désormais conditionnée à `PROFESSIONNEL` — et le
test du profil particulier a été **renforcé** : il configure maintenant les clauses B2B avant de
vérifier leur absence, sans quoi il ne pouvait pas attraper ce défaut.

## 11. Migration 0028

Additive comme 0017→0027 : deux tables créées (`factures_proprietaires_conformite`,
`factures_proprietaires_lignes_detail`), aucun `ALTER` sur l'existant.

| Étape | integrity | version | tables | index | pertes |
|---|---|---:|---:|---:|---:|
| → 0027 | ok | 27 | 71 | 118 | 0 |
| **→ 0028** | **ok** | **28** | **73** | **122** | **0** |

Séquentielle et automatique **identiques** hors horodatages, idempotence sur 3 passages, rollback
avec hash exact restitué après écriture dans la nouvelle table. **La base réelle reste en 0016.**

## 12. Ce qui manque réellement

**Uniquement des valeurs à renseigner** — aucun trou d'architecture :

1. identité de la société : dénomination, forme juridique, capital, SIREN, SIRET/RCS, adresse du
   siège, contact, coordonnées de paiement ;
2. régime de TVA applicable + la mention réglementaire correspondante ;
3. délai de paiement et conditions d'escompte ;
4. taux de pénalités de retard et indemnité forfaitaire de recouvrement (requis seulement pour
   facturer un client professionnel) ;
5. type de client par propriétaire (particulier ou professionnel), et SIREN pour les
   professionnels.

Plus une obligation future : le raccordement à une plateforme de facturation électronique.

## 13. Verdict

| Axe | État |
|---|---|
| FACTURATION PROPRIÉTAIRE | **TECHNIQUEMENT COMPLÈTE** |
| CONFORMITÉ DOCUMENT | **PRÊTE** (structure et contrôles en place) |
| NUMÉROTATION | **VALIDÉE** — format légal, séries, continuité, figeage |
| IDENTITÉ SOCIÉTÉ | **DONNÉES À FOURNIR** |
| RÉGIME TVA | **À CONFIRMER** |
| PARTICULIER | **VALIDÉ** |
| PROFESSIONNEL | **VALIDÉ** |
| COMPTABILITÉ | VALIDÉE (source unique, pas de double comptage) |
| FACTURATION ÉLECTRONIQUE | **ARCHITECTURE PRÊTE**, raccordement non fait |
| ÉMISSION RÉELLE | **NON AUTORISÉE** |
| APP.DB RÉELLE | **NON MIGRÉE** (0016) |
| MODE RÉEL | **NO GO — NON ACTIVÉ** |
