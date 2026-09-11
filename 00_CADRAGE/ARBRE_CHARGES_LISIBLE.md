# Ce qui se passe quand je crée une charge

> Version métier. Chaque ligne décrit l'effet **réel** d'un clic dans l'application — tout a été
> exécuté et vérifié le 2026-09-11. Détail technique : [`ARBRE_CHARGES_CONSEQUENCES.md`](ARBRE_CHARGES_CONSEQUENCES.md).

---

## Les deux questions à ne jamais mélanger

Quand vous saisissez une dépense, l'application y répond séparément :

**1. « Combien ça coûte à chaque logement ? »** → la dépense est **divisée** entre les logements
concernés. 700 € sur 2 logements pèsent **350 € sur le résultat de chacun**.

**2. « Combien je récupère auprès des propriétaires ? »** → le montant reste **entier**. Vous avez
**700 € à refacturer**, et vous choisissez comment : tout sur une facture, 350/350, 500/200…

> La première réponse **n'impose rien** à la seconde. C'est vous qui décidez ce que vous
> refacturez et à qui.

---

## Tableau de bord : je crée… il se passe quoi ?

| Je crée… | Résultat logement | Comptabilité | À refacturer | Facture propriétaire | Clôture |
|---|---|---|---|---|---|
| **Charge générale** (aucun logement) | aucun logement touché | aucune écriture | non | n'apparaît pas | à contrôler |
| **Charge sur 1 logement** | ce logement, montant entier | aucune écriture | non | n'apparaît pas | à contrôler |
| **Charge sur 1 logement, refacturable** | ce logement, montant entier | aucune écriture | **oui, montant entier** | proposée à son propriétaire | à contrôler |
| **Charge sur 2 logements** | **moitié sur chacun** | aucune écriture | non | n'apparaît pas | à contrôler |
| **Charge sur 2 logements, refacturable** | **moitié sur chacun** | aucune écriture | **oui, montant ENTIER** | proposée aux **deux** propriétaires | à contrôler |
| **Charge sur un propriétaire** | ses logements actifs du mois | aucune écriture | selon le choix | selon le choix | à contrôler |
| **Charge de ménage** | réparti sur les intervenants ou logements choisis | aucune écriture | **jamais** | n'apparaît **jamais** | à contrôler |
| **Charge « hors compta » (HC)** | **compte quand même** dans le résultat réel | **aucune écriture, jamais** | possible | possible | à contrôler |
| **Charge personnalisée** (catégorie Autre) | aucun logement | aucune écriture | **jamais** | n'apparaît jamais | à contrôler |

---

## Trois choses vraies pour **toutes** les charges

**1. Créer une charge ne crée jamais d'écriture comptable.**
La comptabilité naît de la **facture propriétaire**, jamais de la dépense elle-même. Une charge est
une dépense constatée ; l'écriture de vente constate une créance sur le propriétaire. Les lier
directement compterait deux fois la même opération.

**2. Toute charge arrive « à contrôler ».**
Aucune ne naît validée. Vous la validez depuis sa fiche (« Valider la charge »), ou vous signalez
une anomalie. L'état est conservé et survit à un redémarrage.

**3. La somme des parts vaut toujours exactement le montant.**
100 € sur 3 logements donnent 33,34 + 33,33 + 33,33 — jamais 99,99. Le centime en trop va au
premier logement, toujours le même, de façon reproductible.

---

## « Refacturable » : ce que ça change vraiment

| Sans « refacturable » | Avec « refacturable » |
|---|---|
| la dépense pèse sur le résultat | la dépense pèse sur le résultat **de la même façon** |
| elle n'apparaît nulle part ailleurs | elle devient un **élément à refacturer** |
| aucune facture ne la propose | elle est proposée sur la facture des propriétaires concernés |

**Une charge refacturable n'est pas automatiquement facturée.** Elle entre dans une réserve. Tant
que vous ne l'ajoutez pas à une facture, elle ne crée ni créance, ni écriture — elle attend.

**Vous ne pouvez pas refacturer plus que la dépense.** 700 € se répartissent comme vous voulez,
mais 500 + 300 est refusé : le total ne peut pas dépasser 700.

**Si vous n'en refacturez qu'une partie, un motif est demandé.** Ne récupérer que 500 € sur 700 est
une décision : l'application vous demande pourquoi, et conserve la réponse.

---

## « Hors compta » (HC) — le point qui trompe le plus

Ce n'est **pas** « sans effet ».

| | Résultat économique réel | Comptabilité générale |
|---|---|---|
| **IC** (intra-comptable) | compte | **compte** |
| **HC** (hors compta) | **compte aussi** | ne compte pas |

Une dépense en `HC` pèse bel et bien sur votre résultat réel. Elle n'entre simplement pas dans la
comptabilité générale. Choisir `HC` ne fait donc **pas** disparaître la dépense de vos chiffres.

*(Un troisième code, « hors résultat », existe dans le référentiel et neutralise tout — il n'est
pas proposé à la saisie.)*

---

## Ce que l'application refuse, et pourquoi

| Si vous tentez… | Elle refuse parce que… |
|---|---|
| refacturable sans aucun logement | on ne peut refacturer à personne |
| un montant nul ou négatif | une charge est une dépense |
| une charge de ménage refacturable | le ménage se facture par son propre circuit |
| un ménage sans intervenant ni logement | il n'y aurait rien à répartir |
| une catégorie personnalisée refacturable | elle n'est rattachée à aucun logement |
| une date au mauvais format | une date illisible fausserait la période |
| un mode de paiement non autorisé | le circuit financier ne le prévoit pas |

Dans **tous** ces cas : rien n'est écrit, rien ne reste à moitié fait, et le message dit lequel des
points bloque.

---

## Deux limites connues aujourd'hui

**Les charges de ménage saisies ici n'alimentent pas encore le coût complet ménage.** Ce calcul lit
toujours le classeur Excel historique. L'information nécessaire est désormais enregistrée — le
branchement reste une décision à prendre.

**Une charge créée avant le 2026-09-10 peut n'avoir aucun logement.** Si elle est refacturable, sa
fiche affiche « Périmètre à compléter » : indiquez les logements concernés et elle redevient
refacturable. L'application ne les devine pas — ils n'ont jamais été enregistrés.
