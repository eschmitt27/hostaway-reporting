# Plan de remise à zéro opérationnelle — À EXÉCUTER PLUS TARD

> **AUCUNE ACTION DE CE DOCUMENT N'A ÉTÉ EXÉCUTÉE.**
> Rédigé le 2026-09-11 pendant que la recette utilisateur se poursuit. Il ne sera mis en œuvre
> **qu'après un ordre explicite de l'utilisateur**, une fois les tests terminés.
>
> En attendant, la base réelle **conserve toutes ses données de recette** : elles servent encore.

---

## 1. Objectif

Faire partir l'application sur un historique opérationnel propre, sans traîner les données créées
pour essayer le logiciel.

| | |
|---|---|
| Date de début opérationnelle | **01/09/2026** |
| Premier mois de gestion réel | **2026-09** |
| Premier mois clôturable | **2026-09**, et seulement **une fois septembre terminé** |

La règle générale reste inchangée : **un mois ne se clôture que lorsqu'il est entièrement écoulé.**
Septembre 2026 devient donc éligible à la clôture à partir d'octobre 2026 — pas avant.

---

## 2. État actuel à traiter (relevé le 2026-09-11)

| Objet | Volume | Sort proposé |
|---|---|---|
| Factures propriétaires 2026-06 | 13 BROUILLON | **retirer** — antérieures au démarrage |
| Factures propriétaires 2026-08 | 12 BROUILLON | **retirer** — antérieures au démarrage |
| Facture émise `F-11/0-000001` | 1 EMIS | **neutraliser** (§4) |
| Charges | 4 ACTIVE + 1 ANNULEE | **retirer** — charges d'essai |
| Positions de refacturation | 1 `A_TRAITER` | retirée avec sa charge |
| Écritures comptables | 1 VENTES 465,88 € | **contrepasser** (§4) |
| Classements type client | 1 | **conserver** — information métier réelle |
| Référentiels (`ref_*`) | — | **CONSERVER INTÉGRALEMENT** |
| Réservations / ménages Hostaway | — | **conserver** : données réelles, pas des essais |
| Sauvegardes | — | conserver (historique de sécurité) |

> **Point d'attention.** Les réservations et tâches de ménage Hostaway sont des données **réelles**,
> pas des essais. Les supprimer ferait perdre l'historique d'exploitation. Le reset porte sur ce que
> la recette a **créé**, pas sur ce qu'elle a **lu**.

### 2bis. Ce que la mission « FIN DU LEGACY » a changé pour ce plan

Trois points, tous **favorables** — le reset est plus simple qu'avant, pas plus compliqué :

| Changement | Effet sur le reset |
|---|---|
| **Migration 0078** appliquée (suppression de `charges.impact_resultat_reel` / `impact_resultat_comptable`) | Le schéma a changé depuis la rédaction de ce plan. Toute requête de reset visant ces colonnes échouerait : **ne pas les nommer**. L'impact d'une charge se lit par `code_impact`. |
| **Migration 0079** — `HR` supprimé, `motif_exclusion` ajouté sur `reservations_calculees` / `reservations_resolues` | Aucune requête de reset ne doit filtrer sur `code_impact='HR'` : la valeur n'existe plus. Une réservation exclue se reconnaît à `statut_controle IN ('EXCLU_RESULTAT','EXCLU_LEGACY')`. |
| **Migration 0080** — `CONFORME` unifié en `VALIDE` sur `charges.statut_controle` | Le vocabulaire de contrôle d'une charge est `A_CONTROLER` / `VALIDE` / `ANOMALIE` / `REJETE`. Ne plus écrire `CONFORME`. |
| **Charges non validées = contrôle BLOQUANT** (`CHARGE_NON_VALIDEE_HORS_CALCULS`) | Les 4 charges d'essai actives sont `A_CONTROLER` : elles bloquent la clôture tant qu'elles existent. Le reset les retire, ce qui lève le contrôle — l'ordre §5 reste valable. |
| **`lot6f` ne lit plus aucun classeur** | Le recalcul post-reset du coût complet ménage n'exige plus ni accès réseau, ni `SAISIE_Charges_Flux`, ni `REF_Setup`. Il devient déterministe et rejouable hors ligne — donc vérifiable dans la foulée du reset. |
| **`HR` retiré du vocabulaire des charges** | Une charge d'essai portant `HR` ressortirait `A_CONTROLER` au lieu d'être silencieusement neutre. Sans objet ici : aucune des 5 charges d'essai ne porte ce code (4 `IC`, 1 `HC`). |

**~~Point de vigilance sur la fraîcheur~~ — RÉSOLU le 2026-09-11.** `menages_declarations_internes`
portait la photographie du **2026-09-02** de la Google Sheet M04, alors que la feuille avait changé
depuis. La cause n'était pas un oubli : l'actualisation depuis `/menages` échouait en 0,0 s sur un
**préflight Hostaway**, trois fois de suite, et gelait au passage deux sources qui n'ont aucune
dépendance à Hostaway (PDF et Google Sheet). Le préflight ne gate plus que sa propre étape ; le
résultat devient PARTIEL au lieu d'ÉCHEC total.

`lot6b` a été relancé pour de vrai, après sauvegarde : **31 → 39 déclarations**, extraction
2026-09-11, le mois 2026-08 apparaît (25 ménages) et 2026-07 passe de 42 à 41 (la correction de la
feuille identifiée lors de la mission précédente). Aucun classeur écrit : SQLite est le défaut.

**Ce qu'il reste à faire avant le reset :** recalculer `menages_cout_complet`, qui reflète encore
les déclarations d'avant (29 lignes, mois 2026-03..2026-07). Un recalcul ciblé par mois depuis
`/menages` suffit ; il n'a pas été déclenché ici pour ne pas modifier de résultat économique hors
du périmètre de la mission.

### 2ter. Ce que la mission « lot6c vers SQLite » (2026-09-11, Mission 25) change pour ce plan

Aucun impact sur les objets à retirer ni sur l'ordre §5 — l'impact porte sur **comment** le
recalcul de `menages_cout_complet` mentionné juste au-dessus devra être mené, une fois décidé :

| Changement | Effet sur ce plan |
|---|---|
| `lot6b` n'écrit plus AUCUN classeur (export legacy `--export-legacy` supprimé du code) | Rien à surveiller côté `M04_MENAGES_PowerQuery.xlsx`/`MASTER_NORM_Declarations_Internes.xlsx` avant un recalcul futur : ils ne bougent plus jamais, quelle que soit la commande lancée. |
| `lot6c` a désormais un mode `--source SQLITE` | Le recalcul de `menages_cout_complet` (§8 « Critère de réussite ») peut être mené par la chaîne `lot6b → lot6c → lot6d → lot6e → lot6f` entièrement en SQLite, sans plus jamais dépendre de `MASTER_FACT_MEN_MenagesExternes.xlsx`. |
| Recalcul réel **toujours pas effectué** | La mission « lot6c vers SQLite » a **prouvé** que la chaîne SQLite fonctionne bout en bout sur une base **jetable** (recette), mais n'a **pas** mené la comparaison legacy/SQLite sur copie isolée qui conditionne un recalcul de la VRAIE table (§11-13 de cette mission) — donc `menages_cout_complet` réel reste, à ce stade, dans le même état qu'avant : non recalculé. Le critère §8 ci-dessous reste donc ouvert, inchangé. |

---

## 3. Ce qui doit être conservé, sans discussion

- **tous les référentiels** (`ref_logements`, `ref_proprietaires`, `ref_categories_charges`,
  `ref_modes_paiement`, `ref_gestion_logements_hist`, taux de commission…) ;
- les **classements particulier / professionnel** déjà saisis : ce sont de vraies décisions ;
- l'**historique Hostaway** (réservations, tâches de ménage, payouts) ;
- les **sauvegardes** ;
- le **schéma** et l'historique des migrations.

---

## 4. Facture `F-11/0-000001` — empreinte exacte et traitement

Relevé complet, pour qu'aucun fragment ne soit oublié :

| Élément | Valeur |
|---|---|
| Facture | `FPR-6196BE4620DF` · statut **EMIS** · mois 2026-08 · **465,88 €** |
| Numéro | `F-11/0-000001` (format corrompu par une date saisie en `11/09/2026`) |
| Lignes | 3 |
| Écriture comptable | `ECR-F3D2501E56ED` · journal VENTES · **VALIDEE** · 465,88 € |
| Imputation Airbnb | `IMPA-862AECBFE514` · 425,00 € · VALIDE |
| Séquence consommée | série `F-11/0`, compteur **1** |
| Conformité figée | 1 enregistrement |
| Document | `F-11-0-000001.pdf` |

**Deux voies possibles, à trancher par l'utilisateur :**

**Voie A — neutralisation comptable (recommandée si l'on veut conserver la trace).**
1. contrepasser `ECR-F3D2501E56ED` par le mécanisme existant (jamais de suppression d'écriture) ;
2. émettre un **avoir** annulant la facture, via le circuit normal ;
3. conserver facture, avoir et écritures : l'historique montre une opération annulée, ce qui est la
   vérité de ce qui s'est passé ;
4. la séquence `F-11/0` reste consommée — un numéro attribué ne se libère pas.

**Voie B — retrait pur (si la base doit être vierge au 01/09/2026).**
1. sauvegarde préalable **obligatoire** ;
2. suppression, dans une seule transaction : écriture, lignes d'écriture, imputation Airbnb,
   conformité figée, lignes de facture, facture, ligne de séquence `F-11/0` ;
3. suppression du PDF figé ;
4. contrôle `foreign_key_check` = 0 après opération.

> La voie B efface une opération qui a réellement eu lieu dans l'application. Elle n'est
> acceptable que parce qu'il s'agit d'un essai, jamais transmis à un tiers — et cette qualification
> appartient à l'utilisateur, pas au logiciel.

La série `F-11/0` disparaît de toute façon du futur : la numérotation est désormais mensuelle
(`2026-09-001`), et son mode de calcul rend ce format impossible à reproduire.

---

## 5. Ordre d'exécution proposé

1. **sauvegarde** via `backup_service`, libellé `AVANT_RESET_PRODUCTION_<horodatage>` ;
2. relever et **archiver** l'état avant (volumétrie, `integrity_check`, `foreign_key_check`,
   empreinte SHA-256 de la base) ;
3. traiter `F-11/0-000001` selon la voie retenue ;
4. retirer les **factures brouillon** de 2026-06 et 2026-08 (et leurs lignes) ;
5. retirer les **charges d'essai**, leurs positions, périmètres et événements ;
6. contrôler : `integrity_check` = ok, `foreign_key_check` = 0, aucun orphelin ;
7. vérifier qu'aucune facture, charge ni écriture ne subsiste **avant 2026-09** ;
8. relever l'état après et le consigner dans `JOURNAL_CONTROLES.md` ;
9. redémarrer l'instance et vérifier les écrans principaux.

**Aucune migration n'est nécessaire** : le reset est une opération de données. Si l'exécution en
révélait le besoin, elle serait numérotée à la suite (`0079…`, la `0078` étant prise) — jamais en
modifiant une migration existante.

---

## 6. Compteurs et numérotation

| Compteur | Sort |
|---|---|
| `factures_proprietaires_sequence` | **remettre à zéro** en même temps que les factures : une série sans facture n'a pas de raison d'être |
| Numérotation future | `2026-09-001`, `2026-09-002`… séquence par mois de prestation |
| Séries antérieures (`F-11/0`) | disparaissent avec les factures ; aucun risque de collision |

---

## 7. Preuves à produire

Avant / après, dans `JOURNAL_CONTROLES.md` :

- compte de lignes par table concernée ;
- `integrity_check` et `foreign_key_check` ;
- SHA-256 de la base ;
- identifiant de la sauvegarde préalable ;
- liste nominative de ce qui a été retiré ;
- confirmation qu'aucune donnée **antérieure à 2026-09** ne subsiste dans les tables opérationnelles ;
- confirmation que les **référentiels sont intacts** (comptes identiques avant/après).

---

## 8. Critère de réussite

À l'issue du reset :

- la plus ancienne facture, charge et écriture datent de **2026-09 au plus tôt** ;
- `menages_cout_complet` a été recalculé après l'actualisation de `lot6b` du 2026-09-11
  (cf. §2bis) — sinon le coût complet décrit un mois que les déclarations ne décrivent plus ;
- aucun contrôle `CHARGE_NON_VALIDEE_HORS_CALCULS` ne reste ouvert ;
- les référentiels sont **inchangés** ;
- l'historique Hostaway est **inchangé** ;
- `integrity_check` **ok**, `foreign_key_check` **0** ;
- l'application démarre et tous les écrans répondent ;
- **septembre 2026 n'est pas clôturable** tant qu'il n'est pas terminé.

### 2quater. Ce que la recette utilisateur n°3 (2026-09-12) change pour ce plan

| Changement | Effet sur le reset |
|---|---|
| **`menages_cout_complet` a été recalculé** (29 → 50 lignes, 6 mois, 9 178,00 €) | Le point resté ouvert au §2bis et au critère §8 est **levé** : le coût complet ne décrit plus un mois que les déclarations ne décrivent plus. |
| **Migration 0081** (`facture_lignes_menage.statut_ligne`, `motif_correction`) | Le schéma a changé depuis la rédaction de ce plan. Toute requête de reset touchant les lignes de facture doit en tenir compte ; aucune donnée existante n'a été modifiée par cette migration. |
| **Une facture fournisseur validée crée désormais son écriture ACHATS** | Un reset qui retirerait des factures fournisseurs devra aussi traiter leurs écritures — contrepassation, jamais suppression. À ce jour la base réelle n'en porte aucune (les deux factures de juillet ont un écart non résolu). |
| **Les mois non déclarés postérieurs à la dernière clôture sont ouverts** | Le reset n'a plus besoin d'« ouvrir » un mois dans `ref_cloture_mensuelle` pour que la saisie reprenne au 01/09/2026 : un mois non déclaré est ouvert par construction. Clôturer reste un acte explicite. |
| **Une charge validée peut revenir à contrôler** | Sans effet sur le reset lui-même, mais la règle est symétrique de la sienne : le retour est refusé sur un mois clôturé. |

**Inchangé** : F-11/0-000001 conservée, aucune séquence remise à zéro, aucun reset exécuté.
