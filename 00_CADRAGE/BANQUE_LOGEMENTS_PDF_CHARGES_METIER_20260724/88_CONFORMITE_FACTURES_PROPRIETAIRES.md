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

### 4.1 Où le type est stocké (migration 0073)

Table compagne **`proprietaires_facturation`** (`proprietaires_facturation_service`), et non une
colonne de `ref_proprietaires` : cette dernière est **reconstruite à chaque import** de
`REF_Setup.xlsm` (`ref_setup_import_service` supprime les lignes dont l'`import_id` diffère), donc
un classement qui y serait rangé disparaîtrait au prochain import. Un journal
`proprietaires_facturation_evenements` conserve chaque changement avec sa valeur précédente.

Le champ est **fermé** : `PARTICULIER` ou `PROFESSIONNEL`, garanti par un `CHECK` SQL et par le
service. `A_CONTROLER` **n'est pas stockable** — c'est l'absence de ligne. Stocker « on ne sait
pas » ferait perdre la différence entre une question jamais posée et une réponse jamais tranchée.

### 4.2 Le type n'est JAMAIS déduit

Ni le nom, ni l'adresse, ni la présence d'un SIREN ne permettent de conclure. Un particulier peut
faire figurer un nom de société dans son adresse de facturation ; un professionnel peut être
facturé sans que son SIREN soit connu. **Pour les propriétaires historiques, l'application ne
devine pas** : elle bloque l'émission et demande la saisie, avec un message actionnable —

> « Indiquez si le propriétaire est un particulier ou un professionnel avant d'émettre la facture.
> Les mentions légales obligatoires ne sont pas les mêmes. »

Le code technique (`FACTURE_TYPE_CLIENT_NON_DETERMINE`) reste à côté, pour les journaux et les
tests, mais n'est jamais la seule information donnée à l'utilisateur. La saisie se fait depuis la
fiche facture ; elle est mémorisée et vaut pour les factures suivantes.

L'enjeu est concret : imprimer des pénalités de retard et une indemnité de recouvrement de 40 €
sur la facture d'un particulier serait au mieux inexact, au pire une menace de recouvrement sans
fondement. Une déduction automatique à partir d'un nom produirait exactement cela.

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

### 5.1 Régime retenu (confirmé le 2026-09-10) : franchise en base

```
FACTURATION_REGIME_TVA=FRANCHISE_TVA
FACTURATION_MENTION_FRANCHISE_TVA=TVA non applicable, art. 293 B du CGI
```

Le blocage `FACTURE_REGIME_TVA_NON_CONFIRME` est **levé**. Les factures sont émises **sans TVA**
et portent la mention de l'article 293 B du CGI.

**La mention est un paramètre, jamais une constante du gabarit PDF.** Écrite en dur, elle
survivrait à un changement de régime : le jour où la société franchirait le seuil de la franchise,
les factures continueraient d'affirmer un fondement juridique devenu faux — une fausse mention
fiscale sur un document opposable. Le gabarit imprime `mention_tva`, point.

**HT = TTC est garanti mathématiquement, pas par convention** : `taux_tva_applicable()` renvoie
`0.0` pour tout régime non assujetti, donc `total_tva = 0` et `total_ttc = total_ht` par
construction — aucun arrondi ne peut les faire diverger, y compris sur des montants non ronds
(test `test_franchise_ht_egale_ttc_sur_montants_non_ronds`).

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

### 7.1 Modalité retenue (confirmée le 2026-09-10) : paiement à réception

```
FACTURATION_DELAI_PAIEMENT_JOURS=0
```

`0` ne signifie **pas** « aucun délai configuré » mais « échéance le jour de l'émission ». La
distinction décide si la facture est immédiatement exigible, et elle est piégeuse en Python
(`not 0` est vrai) : `delai_paiement_jours()` renvoie donc `int | None` et le code teste
`is None`, jamais la véracité. Un test dédié verrouille les deux cas
(`test_delai_zero_nest_pas_confondu_avec_absence`).

Conséquences, toutes calculées dans le service canonique — **rien n'est écrit dans le gabarit** :

| Élément | Valeur |
|---|---|
| Conditions de règlement | « Paiement à réception » (dérivé du délai, jamais saisi en parallèle) |
| Échéance de paiement | **= date d'émission** |
| Escompte | « Escompte pour paiement anticipé : néant » |

Le libellé des conditions est **dérivé** du délai et non stocké à côté : deux valeurs
indépendantes finiraient par se contredire, la facture affichant « Paiement à 30 jours » au-dessus
d'une échéance calculée à réception. Le blocage `FACTURE_ECHEANCE_NON_CONFIGUREE` est **levé**.

### 7.2 Clauses B2B — aucune valeur métier inventée

`FACTURATION_TAUX_PENALITES_RETARD` et `FACTURATION_INDEMNITE_RECOUVREMENT` sont des paramètres
**sans défaut dans le code**. Un audit préalable a confirmé qu'aucun taux contractuel n'existe
ailleurs dans le projet ; plutôt que d'en fabriquer un, le code exige la saisie et **bloque
l'émission professionnelle** tant qu'elle n'a pas eu lieu (`FACTURE_PENALITES_RETARD_NON_CONFIGUREES`,
`FACTURE_INDEMNITE_RECOUVREMENT_NON_CONFIGUREE`). Une valeur métier incertaine écrite en dur serait
invisible et fausse ; un blocage explicite est visible et corrigible.

Ces deux mentions ne sont exigées **ni imprimées** que face à un client explicitement
`PROFESSIONNEL` — jamais pour un particulier, jamais pour un type non tranché.

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

Mise à jour du **2026-09-10** — les points 1 à 4 sont **résolus** :

1. ~~identité de la société~~ → **fournie** par le Kbis (voir §14) ;
2. ~~régime de TVA + mention~~ → **franchise en base**, art. 293 B du CGI (§5.1) ;
3. ~~délai de paiement et escompte~~ → **paiement à réception**, escompte néant (§7.1) ;
4. ~~pénalités et indemnité B2B~~ → **paramétrées**, exigées seulement en B2B (§7.2) ;
5. **type de client par propriétaire** → le mécanisme existe (§4.1) et le champ est saisissable
   depuis la fiche facture, mais **les propriétaires historiques restent à classer un par un**.
   C'est volontaire : l'application ne devine pas. Chaque propriétaire doit être classé avant sa
   première émission définitive.

Deux limites connues, sans effet sur la validité juridique du document :

- **SIRET et TVA intracommunautaire non fournis** — le Kbis n'en contient pas. Ils ne sont ni
  inventés, ni complétés, ni affichés (§14) ;
- **glyphes `€` et `—`** : les polices de base de fpdf2 sont strictement latin-1 et ne les
  contiennent pas. Ils sont translittérés en `EUR` et `-`. Le pied de page imprime donc
  « SAS au capital de 200,00 EUR » là où la charte écrirait « 200,00 € ». Le **contenu légal est
  identique** ; seule la typographie diffère. Le corriger suppose d'embarquer une police Unicode
  redistribuable dans le dépôt — décision à prendre, non prise ici.

Plus une obligation future : le raccordement à une plateforme de facturation électronique.

## 14. Identité légale (Kbis, 2026-09-10)

| Élément | Valeur | État |
|---|---|---|
| Dénomination | CHOUETTE PATRIMOINE | **OK** |
| Forme juridique | Société par actions simplifiée — affichée « SAS » | **OK** |
| Capital | 200,00 € | **OK** |
| Siège | 48E Route de Larnavey, 33650 Saint-Selve | **OK** |
| SIREN | 109 624 767 | **OK** |
| RCS | 109 624 767 R.C.S. Bordeaux | **OK** |
| **SIRET** | — | **NON FOURNI** |
| **TVA intracommunautaire** | — | **NON FOURNIE** |

Le numéro fourni compte **9 chiffres** : c'est un SIREN, pas un SIRET. Les 5 chiffres du NIC ne
sont **jamais** complétés, et les étiquettes « SIRET : » et « TVA intracommunautaire : » ne sont
**pas imprimées** tant que les valeurs sont inconnues — mieux vaut une mention absente qu'un
numéro inventé sur un document opposable. `valider()` accepte donc SIRET **ou** SIREN.

Pied de page imprimé sur chaque facture :

```
CHOUETTE PATRIMOINE - SAS au capital de 200,00 EUR
48E Route de Larnavey - 33650 Saint-Selve
109 624 767 R.C.S. Bordeaux
```

## 15. Comptabilisation en franchise

L'écriture de vente est produite par le **moteur existant** (`comptabilite_ecritures_service`) —
aucune règle comptable concurrente n'a été créée pour la facturation propriétaire.

| Compte | Sens | Montant |
|---|---|---|
| `411000` clients | débit | **total facturé** |
| `706000` prestations de services | crédit | **total facturé** |

- **aucune ligne `4457*`** (TVA collectée) : en franchise, rien n'est collecté. Une ligne de TVA
  ici créerait une dette fiscale imaginaire ;
- **débit = crédit**, vérifié sur l'en-tête et sur la somme des lignes ;
- la vente est constatée pour le **montant total facturé**, jamais pour le montant restant dû.
  Un **acompte est un règlement déjà reçu**, pas une remise : le déduire du produit minorerait le
  chiffre d'affaires d'un encaissement qui a bien eu lieu. D'où, sur le document,
  `TOTAL FACTURE ≠ MONTANT RESTANT À PAYER` dès qu'un acompte existe ;
- la génération est **idempotente** : confirmer deux fois ne produit pas deux écritures.

## 13. Verdict

| Axe | État |
|---|---|
| FACTURATION PROPRIÉTAIRE | **TECHNIQUEMENT COMPLÈTE** |
| CONFORMITÉ DOCUMENT | **PRÊTE** (structure et contrôles en place) |
| NUMÉROTATION | **VALIDÉE** — format légal, séries, continuité, figeage |
| IDENTITÉ SOCIÉTÉ | **FOURNIE** (Kbis) — SIRET et TVA intra non fournis, non inventés |
| RÉGIME TVA | **CONFIRMÉ** — franchise en base, art. 293 B du CGI |
| CONDITIONS DE RÈGLEMENT | **CONFIRMÉES** — paiement à réception, échéance = émission |
| PARTICULIER | **VALIDÉ** — aucune clause B2B imprimée |
| PROFESSIONNEL | **VALIDÉ** — pénalités et indemnité exigées et imprimées |
| TYPE DE CLIENT | **MÉCANISME EN PLACE** — propriétaires historiques à classer, jamais devinés |
| COMPTABILITÉ | VALIDÉE (source unique, pas de double comptage, pas de TVA collectée) |
| FACTURATION ÉLECTRONIQUE | **ARCHITECTURE PRÊTE**, raccordement non fait |
