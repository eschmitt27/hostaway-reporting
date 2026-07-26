# 33 — Module Fournisseurs / Factures / Règlements : état

Suite de l'audit `32_AUDIT_FOURNISSEURS_FACTURES.md`. Ce document dit ce qui est **réellement
construit et prouvé**, et ce qui ne l'est pas.

## Ce qui est construit et prouvé

| Fonction | Statut | Preuve |
|---|:--:|---|
| Référentiel fournisseurs (liste/détail/création/modification/archivage/réactivation/historique) | ✅ **préexistant** | migration 0010 + `fournisseurs_referentiel_service.py`, non retouché |
| Champs métier étendus fournisseur (raison sociale, contact, TVA, conditions de paiement…) | ⚠️ **schéma seulement** | table `fournisseur_details` créée (migration 0017), **pas encore de service ni d'UI** |
| Modèle Facture (4 objets distincts) | ✅ | migration 0017, `32_AUDIT_...md` |
| Doublon facture certain (fournisseur + référence) | ✅ | interdit **au niveau du schéma** (index unique) + `test_doublon_certain_refuse` |
| Doublon facture probable (même montant, date proche) | ✅ | `test_doublon_probable_detecte` |
| Validations facture (fournisseur archivé, référence, HT+TVA=TTC, échéance) | ✅ | 8 tests dans `test_factures.py` |
| Statuts + transitions contrôlées | ✅ | `test_transition_interdite_refusee` |
| Facture validée jamais supprimée (annulation tracée) | ✅ | `test_facture_validee_jamais_supprimee_mais_annulable` |
| Lien facture → charge (jamais de charge créée ici) | ✅ | `lier_charge()`, index unique, `test_une_charge_ne_peut_pas_etre_liee_a_deux_factures` |
| Règlement total / partiel / multiple | ✅ | `test_paiement_partiel_puis_solde` |
| Paiement groupé (1 règlement, N factures) | ✅ | `test_paiement_groupe_de_plusieurs_factures` |
| Moyens (banque, caisse, personnel associé, acompte, avoir, remboursement) | ✅ | `test_paiement_personnel_associe_et_avoir` |
| Refus dépassement / double paiement / facture annulée / fournisseur incohérent | ✅ | 5 tests dédiés |
| Annulation d'un règlement libère le solde | ✅ | `test_annulation_libere_le_solde` |
| Solde facture et solde fournisseur **toujours recalculés** | ✅ | jamais de colonne dénormalisée ; `test_solde_fournisseur_integre_les_reglements` |
| Statut de facture dérivé du solde | ✅ | `_rafraichir_statut_facture` |
| Flags dédiés `FACTURES_REAL_WRITE_*` (double verrou RECETTE_MODE) | ✅ | `test_creer_refuse_si_flags_off` |
| Écrans : liste, à payer, nouvelle, fiche, règlements | ✅ | 15 tests HTTP + recette navigateur |
| Navigation « Factures » | ✅ | `base.html`, `test_nav_expose_factures` |
| Recette navigateur réelle | ✅ | voir ci-dessous |

## Recette navigateur réalisée (serveur 8023/8024, données fictives isolées)

1. **Créer un fournisseur** — « Menage Externe Recette SARL » (MENAGE) via `/referentiel-fournisseurs`.
2. **Créer une facture** — `FA-RECETTE-2026-06`, 100 HT / 20 TVA / 120 TTC, échéance 2026-07-12.
   Le fournisseur créé en 1 apparaît bien dans le sélecteur. Statut initial `A_CONTROLER`.
3. **Valider** — `A_CONTROLER → VALIDEE`, historisé.
4. **Rattacher une charge** — `CHG_SEED_001` (charge existante du jeu de recette, **jamais créée**
   par le module Factures).
5. **Règlement partiel** — 50 € BANQUE → statut `PARTIELLEMENT_REGLEE`, solde **70,00 €**.
6. **Payer le reste** — 70 € CAISSE → statut `REGLEE`, solde **0,00 €**, 2 règlements listés.
7. **Historique** — 5 événements dans le bon ordre (CREATION, VALIDATION, CHARGE_LIEE, 2×REGLEMENT
   avec « statut dérivé du solde »).
8. **Redémarrage** — après relance du serveur : facture toujours `REGLEE`, et `/factures/a-payer`
   affiche correctement « Aucune facture à payer ». **Persistance vérifiée.**

## Ce qui N'EST PAS construit (honnêteté)

| Attendu par la consigne | État | Raison |
|---|---|---|
| Import PDF de facture (`/factures/importer`) | ⛔ non construit | L'extracteur `lib_menages_externes_pdf.py` est réutilisable (PyMuPDF disponible) mais ne connaît que 2 formats fournisseur. Le parcours upload → prévisualisation → confirmation reste à écrire. |
| Import CSV/XLSX de factures | ⛔ non construit | — |
| Champs fournisseur étendus (UI + service) | ⛔ schéma seulement | table créée, service/écran non faits |
| Fiche fournisseur avec solde, factures, règlements, ancienneté | ⛔ non construit | `solde_fournisseur()` existe côté service et est testé, mais **aucun écran ne l'expose** |
| Vues « factures en retard », « partiellement réglées », « fournisseurs créditeurs » | ⚠️ partiel | seule `/factures/a-payer` existe (avec indicateur « échue ») |
| Rapprochement bancaire depuis une facture / un règlement | ⛔ non construit | `marquer_rapproche()` existe et est testé, mais n'est **branché sur aucun écran** ; le service Banque générique reste à câbler dans les deux sens |
| Catalogue de contrôles Factures (§15 : ~20 contrôles) | ⚠️ partiel | Les contrôles **bloquants** sont dans les services (doublon, dépassement, TVA, échéance, fournisseur archivé/incohérent, facture annulée) et refusent l'écriture. Il n'existe **pas** de page `/factures/controles` équivalente à celle de Banque, ni les contrôles transverses (justificatif absent, facture de ménage non rapprochée, refacturation sans propriétaire, double comptage facture/charge/banque). |
| Jeu de recette fictif complet (§17 : 16 cas) | ⚠️ partiel | La recette a été faite sur des données créées à la main dans le navigateur ; le générateur `build_data_recette.py` ne seed **pas** de factures/fournisseurs. |
| Tests Lots 3, 6, 9, 10, 11, 12 pour ce module | ⛔ non faits | Aucun impact moteur n'est produit par ce module à ce stade : il ne modifie **aucun fichier Excel**, uniquement SQLite. Le lien vers la charge est déclaratif. |
| Avoir / remboursement comme objets à part entière | ⚠️ partiel | Supportés comme **moyens de règlement** (testés), pas comme objets distincts avec leur propre cycle. |

## Statut : **PARTIEL**

Le cœur est réellement utilisable depuis l'application : créer un fournisseur, créer une facture,
la valider, la rattacher à une charge, la régler en une ou plusieurs fois, suivre le solde et
l'historique, le tout persistant et prouvé en navigateur. La chaîne
`Fournisseur → Facture → Charge → Règlement → Solde` fonctionne de bout en bout.

Il manque, pour être TERMINÉ : l'import PDF/CSV, la fiche fournisseur avec solde, le branchement du
rapprochement bancaire dans les deux sens, la page de contrôles dédiée, et le jeu de recette
fictif complet.

## Tests

| Fichier | Nb |
|---|--:|
| `test_factures.py` | 19 |
| `test_reglements_fournisseurs.py` | 15 |
| `test_factures_routes.py` | 15 |
| **Total module** | **49** |

Suite complète : **1811 passés / 65 skipés / 1 échec pré-existant**
(`test_appsec1_diagnostic.py`, environnemental, antérieur à ce chantier).
