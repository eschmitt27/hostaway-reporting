# HR (« hors résultat ») — audit avant suppression

> Mission « FIN DU LEGACY CHARGES / MÉNAGES », **DÉCISION 2** : « HORS COMPTA + HORS RÉSULTAT n'a
> PAS de pertinence métier. HR doit être SUPPRIMÉ COMPLÈTEMENT. »
>
> **ARBITRAGE RENDU LE 2026-09-11 — ce document est désormais un historique.**
>
> La décision a été appliquée sur l'axe CHARGES (migration 0078), puis sur l'axe RÉSERVATIONS
> (migration 0079) une fois l'arbitrage tranché : **`HR` disparaît des deux axes, et l'effet
> d'exclusion est conservé explicitement** sous `statut_controle` + `motif_exclusion`.
>
> L'option retenue est la n°2 du §4 (« renommer / déplacer le concept »), dans sa forme la plus
> sobre : aucun nouveau code n'a été inventé. L'exclusion était DÉJÀ portée par
> `statut_controle = EXCLU_RESULTAT` ; il ne manquait que le MOTIF, jusque-là noyé dans un
> commentaire en texte libre. Les 125 réservations n'ont perdu ni leur exclusion, ni un euro :
> équivalence économique prouvée à **0,00 €** sur CA, commissions, résultat, net propriétaire et
> flux reconstruits. Voir `RAPPORT_ARBITRAGES_HR_ARRONDIS_LOT6B.md`, §C.

---

## 1. Le point de départ : HR n'est pas un seul concept, c'est deux

L'audit a révélé une chose qui n'apparaissait pas dans l'énoncé : le code `HR` est utilisé sur
**deux axes indépendants**, qui n'ont en commun que trois lettres.

| | **Axe CHARGES** | **Axe RÉSERVATIONS** |
|---|---|---|
| Ce que HR y signifie | « cette dépense ne compte nulle part » | « cette réservation n'est pas une vente » |
| Données réelles portant HR | **0** | **125** |
| Le retirer coûte | rien | 125 réservations basculent en résultat |
| Verdict | **SUPPRIMÉ** | **ARBITRAGE REQUIS** |

---

## 2. Axe CHARGES — la décision est appliquée, elle ne coûtait rien

### 2.1 Ce que l'audit a trouvé dans la donnée réelle

| Source | Lignes HR |
|---|---|
| `charges.code_impact` | **0** (4 × IC, 1 × HC) |
| `ref_charges_recurrentes.code_impact_defaut` | **0** (1 × IC, 1 × HC) |
| `flux_unifies.code_impact` | **0** (284 × IC, 52 × HC) |

**Aucune charge réelle n'a jamais porté HR.** Il n'y a donc rien à convertir — et la consigne
« NE PAS les convertir silencieusement en IC ou HC » ne rencontre aucun cas.

### 2.2 Ce qui l'offrait quand même

HR restait **atteignable** par trois portes :

1. `factures_proprietaires_edition_service.MODES_CHARGE["HR"]` — un mode « Charge hors résultat et
   hors comptabilité » proposé à l'utilisateur lors de l'ajout d'une ligne de facture. Il créait
   une charge dont le montant n'apparaissait ensuite **nulle part** : ni résultat, ni compta.
2. `charges_controle.html` — le filtre « Impact » de l'écran de contrôle listait `['IC','HC','HR']`
   en dur.
3. `lot3_generateur_charges.IMPACT_REEL/IMPACT_COMPTA` — la table de dérivation acceptait `HR` et
   le traduisait silencieusement en `NON/NON`.

Le formulaire « Nouvelle charge », lui, l'excluait déjà (`STANDARD_CODES_IMPACT`). Le vocabulaire
était donc **incohérent selon le point d'entrée** : interdit par un écran, offert par un autre.

### 2.3 Ce qui a été fait

| Où | Changement |
|---|---|
| `app/moteurs/charges_engine.py` | **Source unique créée** : `IMPACT_CHARGE` (IC, HC) et `CODES_IMPACT_CHARGE`. |
| `charges_preview_service.STANDARD_CODES_IMPACT` | devient un alias de la source unique (plus de copie). |
| `factures_proprietaires_edition_service.MODES_CHARGE` | devient la source unique ; `HR` retiré. |
| `charges_controle.html` + sa route | la liste en dur est remplacée par les codes canoniques. |
| `charges_saisie_service.valider()` | **refuse** un `code_impact` hors vocabulaire (`CHARGE_CODE_IMPACT_INVALIDE`). |
| `lot3_generateur_charges` | `HR` retiré des tables de dérivation. |

Le refus est placé dans `charges_saisie_service` parce que c'est **la seule porte d'écriture** de
la table `charges` (vérifié : un unique `INSERT INTO charges` dans tout le dépôt). Retirer HR des
formulaires sans fermer le service l'aurait laissé atteignable par l'API et par tout appelant
interne — une suppression cosmétique.

### 2.4 Conséquence voulue sur lot3

En retirant `HR` de `IMPACT_REEL`/`IMPACT_COMPTA`, une charge qui porterait encore ce code ne
devient plus « neutre » en silence : elle tombe sur `IMPACT_INCONNU` et ressort en `A_CONTROLER`.
**Visible et corrigeable**, plutôt que muette. Aucune charge réelle n'est concernée.

---

## 3. Axe RÉSERVATIONS — ARBITRAGE MÉTIER REQUIS (bloquant)

### 3.1 Ce que HR y porte réellement

```
SELECT code_impact, statut_controle, COUNT(*), SUM(montant_retenu) FROM reservations_resolues
```

| code_impact | statut_controle | Lignes | Montant retenu |
|---|---|---|---|
| **HR** | `EXCLU_RESULTAT` | **80** | 0.00 € |
| **HR** | `EXCLU_LEGACY` | **45** | 0.00 € |
| HC | (divers) | 353 | 2 235,92 € |
| IC | (divers) | 7 431 | 1 582 949,66 € |

Les 125 lignes HR ont toutes `impact_resultat_reel = 'NON'` et un montant retenu de **0,00 €**.

### 3.2 Ce qu'elles sont

Trois producteurs, tous dans `lot4bis_charger_reservations.py` :

- **`OWNERSTAY_EXCLU`** (le gros du lot) — commentaire `"Hostaway ownerStay — exclu résultat"`.
  Ce sont les **séjours du propriétaire dans son propre logement**. Ce ne sont pas des ventes : il
  n'y a ni voyageur, ni encaissement, ni commission.
- **`STATUT_HOSTAWAY_HORS_PERIMETRE`** — réservations dont le statut Hostaway n'est ni `new` ni
  `modified` (annulées, par exemple).
- **`HORS_PARC_TECHNIQUE` / `STATUT_PARC_INVALIDE`** — logements hors parc ou à statut invalide.

Et un quatrième, sur un autre objet : `lot7_generateur_avantages.CODE_IMPACT_SUIVI = "HR"` (D012),
qui marque le **suivi associé** — une dimension de suivi qui n'impacte ni le résultat conciergerie
ni le net propriétaire.

### 3.3 Pourquoi la décision ne peut pas y être appliquée telle quelle

Supprimer HR de cet axe impose de choisir l'un de ces trois chemins — **et aucun ne peut être pris
sans vous** :

| Chemin | Conséquence |
|---|---|
| **A.** Reclasser les 125 en IC ou HC | 80 séjours propriétaire deviennent du chiffre d'affaires. Le résultat économique devient **faux**. Explicitement interdit par la mission : « NE PAS les convertir silencieusement en IC ou HC ». |
| **B.** Créer un code de remplacement | Explicitement interdit : « NE PAS créer un remplaçant équivalent ». Ce serait HR sous un autre nom. |
| **C.** Ne plus produire ces lignes du tout | Les séjours propriétaire disparaîtraient du système. Or ils occupent un logement, génèrent un ménage, et doivent rester visibles — simplement sans produit. |

La consigne « HORS COMPTA + HORS RÉSULTAT n'a PAS de pertinence métier » est **vraie pour une
dépense** : une charge qui ne pèse nulle part n'est pas une charge. Elle ne l'est **pas pour une
occupation sans vente** : un séjour propriétaire est un fait réel qu'il faut enregistrer et exclure
du résultat — c'est exactement ce que HR fait ici, et c'est un besoin, pas un reliquat.

### 3.4 Ce qui N'A PAS été touché — et pourquoi

Rien sur cet axe. Aucune des 125 lignes n'a été modifiée, aucun producteur n'a été désactivé, la
ligne `HR` de `ref_codes_impact` reste en place (la supprimer orphelinerait ces 125 lignes).

`flux_unifie_service._IMPACT_FLAGS` conserve également son entrée `HR` : ce constructeur de flux
sert **les deux axes**.

---

## 4. Ce qu'il vous reste à trancher

> **ARBITRAGE_METIER_REQUIS — HR sur l'axe RÉSERVATIONS**
>
> **Question :** comment un séjour propriétaire (et une réservation annulée, et un logement hors
> parc) doit-il être enregistré, s'il ne doit plus l'être avec un code « hors résultat » ?
>
> **Options réellement ouvertes :**
> 1. **Conserver HR sur cet axe** et acter que le code a deux significations selon l'objet — c'est
>    l'état actuel, et il est cohérent. La décision 2 s'applique alors aux charges seulement.
> 2. **Renommer** le code sur l'axe réservations pour lever l'ambiguïté (ex. `EXCLU`), sans changer
>    aucun comportement. Coût : une migration de données sur 125 lignes + les producteurs.
> 3. **Sortir ces lignes de `reservations_resolues`** vers une table dédiée aux occupations sans
>    vente. Coût : élevé, touche lot4bis/lot4quater/lot9/lot10 — hors périmètre de cette mission
>    (« Ne lance pas une refonte de 15 autres lots »).
>
> **Recommandation :** option 1 ou 2. L'option 2 satisfait l'intention de la décision — plus aucun
> « hors résultat » dans le vocabulaire — sans falsifier le résultat économique. Elle n'a pas été
> exécutée ici parce qu'elle touche de la donnée réelle et relève de votre arbitrage.

---

## 5. Vérification

| Preuve | Où |
|---|---|
| HR refusé à l'écriture d'une charge | `tests/test_hr_supprime_axe_charges.py` |
| Vocabulaire à une seule source | idem (`CODES_IMPACT_CHARGE`) |
| Les 125 réservations HR intactes | idem (test de non-régression sur l'axe réservations) |
