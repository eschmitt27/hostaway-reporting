> **STATUT : PRÊTE À EXÉCUTER.** Cette recette n'a pas été jouée par l'utilisateur.
> Elle ne pourra être marquée validée qu'après son retour explicite, point par point.

# 89 — Recette manuelle avant bascule

Objectif : vous permettre de vérifier **vous-même** que vous pouvez piloter toute la conciergerie
depuis l'application, avant toute préparation de cut-over.

## Avant de commencer

Lancez une instance de recette **sur un autre port que 8000** :

```powershell
cd "C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER\05_APPLICATION"
$env:RECETTE_MODE = "1"
$env:APP_DATA_DIR = "<un dossier de recette, jamais 05_APPLICATION\data>"
python -m uvicorn app.main:app --port 8042
```

Puis ouvrez `http://127.0.0.1:8042`.

**Le port 8000 ne doit pas être utilisé.** La base réelle (`05_APPLICATION\data\app.db`) reste en
migration 0016 et ne doit pas être touchée : la recette travaille sur une copie migrée.

Pour chaque point : notez **OK**, **KO** (avec ce que vous avez vu) ou **N/A**.

---

## 1. Vue d'ensemble

| # | À vérifier | Où | Résultat |
|---|---|---|---|
| 1.1 | Le tableau de bord s'affiche et les chiffres vous parlent | `/` | |
| 1.2 | Aucun widget vide, faux ou « à venir » sans raison | `/` | |
| 1.3 | Tous les liens du menu de gauche mènent à un écran qui fonctionne | menu | |
| 1.4 | Le bandeau MODE RECETTE est bien visible | toutes pages | |

## 2. Référentiels

| # | À vérifier | Où | Résultat |
|---|---|---|---|
| 2.1 | Liste des logements, recherche, ouverture d'une fiche | `/logements` | |
| 2.2 | La fiche logement montre le propriétaire et l'historique de gestion | `/logements/<id>` | |
| 2.3 | Liste des propriétaires et ouverture d'une fiche | `/proprietaires` | |
| 2.4 | La fiche propriétaire montre ses logements et son historique | `/proprietaires/<id>` | |
| 2.5 | Référentiel fournisseurs consultable | `/referentiel-fournisseurs` | |

## 3. Réservations

| # | À vérifier | Où | Résultat |
|---|---|---|---|
| 3.1 | Liste des réservations, filtres, ouverture d'un détail | `/reservations` | |
| 3.2 | Le détail montre canal, statut, nombre de voyageurs | `/reservations/<id>` | |
| 3.3 | Créer une réservation hors Hostaway fictive | `/reservations/nouvelle` | |
| 3.4 | La prévisualisation montre bien ce qui sera enregistré **avant** de confirmer | idem | |
| 3.5 | Après confirmation, la réservation apparaît dans la liste | `/reservations` | |

## 4. Ménages

| # | À vérifier | Où | Résultat |
|---|---|---|---|
| 4.1 | Liste des ménages du mois | `/menages` | |
| 4.2 | Ménages à contrôler, avec la raison affichée | `/menages/a-controler` | |
| 4.3 | Cycle de vie : créer, affecter, suivre | `/menages/cycle` | |
| 4.4 | Export CSV téléchargeable | `/menages/export.csv` | |

## 5. Charges et fournisseurs

| # | À vérifier | Où | Résultat |
|---|---|---|---|
| 5.1 | Consulter les charges, comprendre leur catégorie | `/fournisseurs` | |
| 5.2 | Créer une facture fournisseur fictive | `/factures/nouvelle` | |
| 5.3 | Enregistrer un règlement **partiel** | fiche facture | |
| 5.4 | Le solde restant est correct après ce règlement partiel | `/factures/<id>` | |
| 5.5 | Enregistrer le solde : la facture passe à réglée | idem | |
| 5.6 | Contrôles factures compréhensibles | `/factures/controles` | |

## 6. Créances, dettes et échéancier *(nouveau)*

| # | À vérifier | Où | Résultat |
|---|---|---|---|
| 6.1 | Vos créances propriétaires sont listées avec leur solde | `/creances` | |
| 6.2 | Les filtres propriétaire / logement / mois / échues fonctionnent | `/creances` | |
| 6.3 | Le total par propriétaire est cohérent avec le détail | bas de page | |
| 6.4 | Vos dettes fournisseurs sont listées avec leur solde | `/dettes` | |
| 6.5 | L'échéancier ventile correctement échu / 7 j / 30 j / au-delà | `/echeancier` | |
| 6.6 | La position nette est compréhensible (et vous comprenez que ce n'est pas votre trésorerie) | `/echeancier` | |

## 7. Facturation propriétaire

| # | À vérifier | Où | Résultat |
|---|---|---|---|
| 7.1 | Prévisualiser les factures d'un mois **sans rien créer** | `/factures-proprietaires/proposer` | |
| 7.2 | Vous distinguez les lignes facturées des éléments de relevé | idem | |
| 7.3 | Créer les brouillons après confirmation | idem | |
| 7.4 | La fiche facture montre la checklist de conformité | `/factures-proprietaires/<id>` | |
| 7.5 | Vous comprenez pourquoi une facture est bloquée, le cas échéant | idem | |
| 7.6 | Valider puis émettre une facture fictive | idem | |
| 7.7 | Le numéro attribué est au format `F-2026-000001` | idem | |
| 7.8 | Télécharger le PDF et vérifier son contenu | idem | |
| 7.9 | Le PDF contient période de prestation, HT/TVA/TTC, échéance, mentions | PDF | |
| 7.10 | La section Comptabilité montre l'écriture de vente générée | fiche | |
| 7.11 | Créer un avoir : la facture d'origine reste intacte | idem | |
| 7.12 | Tenter de créer deux fois la même facture : refus expliqué | proposer | |

## 8. Banque

| # | À vérifier | Où | Résultat |
|---|---|---|---|
| 8.1 | Importer un relevé fictif | `/banques-caisse/importer` | |
| 8.2 | La prévisualisation montre ce qui sera importé avant validation | idem | |
| 8.3 | File des mouvements à classer | `/banques-caisse/a-classer` | |
| 8.4 | Classer un mouvement et retrouver la décision dans l'historique | idem | |
| 8.5 | Mouvements à rapprocher | `/banques-caisse/a-rapprocher` | |
| 8.6 | **Aucun écran ne propose de rapprocher un mouvement à une réservation** | partout | |
| 8.7 | Export CSV | `/banques-caisse/export.csv` | |

## 9. Trésorerie propriétaires

| # | À vérifier | Où | Résultat |
|---|---|---|---|
| 9.1 | Liste des mouvements | `/proprietaires/tresorerie` | |
| 9.2 | Créer un mouvement fictif avec sens et nature explicites | `/proprietaires/<id>/tresorerie/nouveau` | |
| 9.3 | Valider le mouvement ; il devient immuable | fiche mouvement | |
| 9.4 | Annuler un mouvement avec justification ; il reste dans l'historique | idem | |

## 10. Comptabilité

| # | À vérifier | Où | Résultat |
|---|---|---|---|
| 10.1 | Liste des écritures, filtres, ouverture du détail | `/comptabilite/ecritures` | |
| 10.2 | Chaque écriture est équilibrée et porte sa source | détail | |
| 10.3 | Les cinq journaux sont consultables | `/comptabilite/journaux` | |
| 10.4 | Soldes par auxiliaire (fournisseur, propriétaire, associé) | `/comptabilite/auxiliaires` | |
| 10.5 | **Balance générale** : totaux, soldes par compte, équilibre | `/comptabilite/balance` | |
| 10.6 | Filtrer la balance par période et par journal | idem | |
| 10.7 | Les écritures `A_CONTROLER` sont identifiables | `/comptabilite/a-controler` | |
| 10.8 | Périodes comptables et plan comptable consultables | `/comptabilite/periodes` | |

## 11. Analytique et résultats

| # | À vérifier | Où | Résultat |
|---|---|---|---|
| 11.1 | Résultat mensuel | `/resultats/mensuel` | |
| 11.2 | Résultat cumulé | `/resultats/cumule` | |
| 11.3 | Par logement, et le détail d'un logement | `/resultats/logements` | |
| 11.4 | Par propriétaire, et le détail d'un propriétaire | `/resultats/proprietaires` | |
| 11.5 | Par catégorie, plateforme, prestataire, fournisseur | `/resultats/...` | |
| 11.6 | **REEL / COMPTABLE / HORS_COMPTA** et leur écart | `/resultats/comptabilite` | |
| 11.7 | Réconciliation : vous comprenez d'où vient l'écart | `/resultats/reconciliation` | |
| 11.8 | Depuis un montant, vous pouvez descendre au détail des lignes | `/resultats/lignes/<id>` | |
| 11.9 | Un mois **historique** (2025) reste consultable | filtres de période | |

## 12. Contrôles et clôture

| # | À vérifier | Où | Résultat |
|---|---|---|---|
| 12.1 | Liste des contrôles, filtrable par niveau et par mois | `/controles-cloture` | |
| 12.2 | Pour chaque contrôle, vous comprenez l'action attendue | détail | |
| 12.3 | Les bloquants sont isolés | `/controles-cloture/bloquants` | |
| 12.4 | Préparation de clôture : vous voyez ce qui empêche de clôturer | `/clotures/<id>/preparation` | |
| 12.5 | Clôturer une période fictive | `/clotures` | |
| 12.6 | Réouvrir avec justification ; l'historique conserve tout | `/clotures/<id>/reouvrir` | |

## 13. Erreurs et refus

L'application doit **expliquer** ses refus, jamais afficher une erreur technique brute.

| # | À tenter volontairement | Attendu | Résultat |
|---|---|---|---|
| 13.1 | Créer deux fois la même facture propriétaire | refus nommé | |
| 13.2 | Émettre une facture avec identité société incomplète | refus expliqué | |
| 13.3 | Modifier une facture déjà émise | impossible | |
| 13.4 | Régler plus que le montant dû | signalé | |
| 13.5 | Écrire sur une période clôturée | refus | |
| 13.6 | Importer deux fois le même relevé bancaire | doublon détecté | |
| 13.7 | Saisir un montant invalide | refus lisible | |

## 14. Redémarrage

| # | À vérifier | Résultat |
|---|---|---|
| 14.1 | Créez une facture propriétaire et une facture fournisseur non réglées | |
| 14.2 | Arrêtez le serveur de recette (Ctrl+C) | |
| 14.3 | Redémarrez-le | |
| 14.4 | La créance est toujours visible dans `/creances` | |
| 14.5 | La dette est toujours visible dans `/dettes` | |
| 14.6 | Un résultat analytique consulté avant redémarrage est identique après | |
| 14.7 | Réglez partiellement, redémarrez : le solde est correct | |

## 15. Navigation

| # | À vérifier | Résultat |
|---|---|---|
| 15.1 | Propriétaire → logement → réservation → calcul → facture → règlement, sans impasse | |
| 15.2 | Fournisseur → facture → charge → règlement, sans impasse | |
| 15.3 | Banque → décision → objet métier, sans impasse | |
| 15.4 | Les statuts affichés sont compréhensibles sans connaître le code | |

---

## Après la recette

Arrêtez **uniquement** l'instance de recette que vous avez lancée.

Renvoyez la liste des points **KO** avec ce que vous avez observé. Chacun sera traité avant toute
préparation de cut-over.

Rappel du cadrage : le cut-over n'est pas la prochaine étape. L'ordre reste — complétude
fonctionnelle, corrections, consolidation, tests, **cette recette**, puis seulement préparation de
la bascule.
