# 34 — Module Fournisseurs / Factures / Règlements : état final

Clôt le module ouvert en PARTIEL par `33_MODULE_FACTURES_ETAT.md`. Les 5 écarts qui y étaient
listés sont traités.

## Tableau de clôture

| Fonction | Validée | Partielle | Non validée | Preuve |
|---|:--:|:--:|:--:|---|
| Référentiel fournisseurs (CRUD + archivage + historique) | ✅ | | | préexistant (migration 0010), réutilisé sans duplication |
| Saisie manuelle de facture | ✅ | | | `test_factures.py`, recette navigateur (tour précédent) |
| **Import PDF** | ✅ | | | `test_factures_import_pdf.py` (12), `test_factures_import_routes.py` (8) |
| Extraction réutilisée (jamais réécrite) | ✅ | | | appelle `lib_menages_externes_pdf.extraire_pdf`, texte natif PyMuPDF, aucun OCR |
| Repli propre sur format inconnu | ✅ | | | `test_format_inconnu_bascule_en_saisie_manuelle` — l'import ne casse pas, il pré-remplit et signale |
| Prévisualisation avant écriture | ✅ | | | `test_aucune_facture_creee_a_la_previsualisation` |
| Fichier importé jamais modifié | ✅ | | | `test_fichier_copie_sous_dryruns_et_original_intact` |
| Validation / statuts / transitions | ✅ | | | `test_transition_interdite_refusee` |
| Lien facture → charge (jamais créée ici) | ✅ | | | `test_confirmer_ne_cree_jamais_de_charge`, index unique |
| Paiement total / partiel / multiple / groupé | ✅ | | | `test_reglements_fournisseurs.py` |
| Avoir, caisse, personnel associé | ✅ | | | `test_paiement_personnel_associe_et_avoir` |
| Annulation de règlement | ✅ | | | `test_annulation_libere_le_solde` |
| **Rapprochement bancaire** | ✅ | | | `test_factures_banque.py` (18), `test_factures_banque_routes.py` (13), recette navigateur |
| Aucun second moteur de rapprochement | ✅ | | | `test_verite_du_lien_reste_dans_banque_rapprochements`, `test_depassement_du_mouvement_refuse_par_le_service_banque` |
| Partiel + multiple (plusieurs mouvements / facture) | ✅ | | | `test_rapprochement_partiel_puis_multiple` + recette navigateur (10 € sur 25 €) |
| Vue inverse (mouvement → facture, solde avant/après) | ✅ | | | `test_contexte_metier_du_mouvement`, `test_fiche_mouvement_affiche_facture_et_soldes` |
| **Solde fournisseur affiché** | ✅ | | | `/fournisseurs-soldes/{id}`, recette navigateur (394 / 75 / 319 €) |
| **Page de contrôles** | ✅ | | | `/factures/controles`, `test_factures_controles.py` (17) |
| **Jeu de recette complet** | ✅ | | | 3 fournisseurs, 13 factures, 8 règlements ; idempotent (vérifié 2×) |
| Persistance après redémarrage | ✅ | | | vérifiée sur facture REGLEE et sur rapprochement CONFIRME |
| Import CSV/XLSX de factures | | | ⛔ | non construit — aucun besoin démontré (le besoin réel constaté est le PDF) |
| Avoir comme objet à cycle propre | | ⚠️ | | supporté comme moyen de règlement, pas comme objet distinct |

## Bug réel trouvé et corrigé pendant cette phase

La page de contrôles que je venais d'écrire a immédiatement détecté une incohérence produite par
mon propre code : `CTRL_FAC_REGLEE_AVEC_SOLDE_NON_NUL` sur `FA-MAINT-REGANN`.

**Cause** : `_rafraichir_statut_facture` ne faisait *monter* le statut (vers PARTIELLEMENT_REGLEE /
REGLEE) et jamais redescendre. Après annulation d'un règlement, la facture restait « REGLEE » avec
un solde de 45 €.

**Correction** : le statut de règlement est une valeur **dérivée** ; il est maintenant recalculé
dans les deux sens (retour à VALIDEE quand plus aucun règlement actif, à PARTIELLEMENT_REGLEE quand
il en reste), sans jamais écraser une décision humaine (ANNULEE, LITIGE). 3 tests ajoutés, et le
contrôle ne remonte plus l'anomalie après régénération du jeu de recette (0 bloquant).

## Deux états impossibles par construction

Deux contrôles exigés par la consigne portent sur des états que **le schéma interdit** (index
uniques de la migration 0017), même en écriture SQL directe :
- doublon certain (même fournisseur + même référence) ;
- une charge rattachée à deux factures.

Les tests le prouvent explicitement (`pytest.raises(sqlite3.IntegrityError)`) au lieu de simuler une
détection. Les contrôles restent présents en défense en profondeur.

## Recette navigateur réalisée

Tour précédent : fournisseur → facture → validation → charge → règlement partiel → solde →
règlement final → REGLEE → persistance après redémarrage.

Ce tour :
1. `/factures` — les 13 factures du jeu de recette, tous statuts représentés.
2. `/factures/controles` — 0 bloquant, 7 critiques, 11 avertissements, action recommandée par ligne.
3. `/reglements?non_rapproches=1` — 7 règlements avec lien « Rapprocher » (gap d'interface corrigé
   pendant la recette : la liste n'exposait pas l'action).
4. Écran de rapprochement — 4 mouvements candidats, tous correctement notés FAIBLE avec la raison
   (« montant compatible avec un rapprochement partiel », « date éloignée (7 j d'écart) »).
5. **Rapprochement partiel** 10 € sur un règlement de 25 € → PARTIEL, 15 € restant, et le
   disponible du mouvement passe de 45 € à 35 €.
6. **Confirmation** → CONFIRME.
7. Fiche mouvement bancaire → facture `FA-MEN-PERSO`, solde avant/après, « Ouvrir la facture ».
8. `/fournisseurs-soldes/…` — 394 € facturé, 75 € réglé, 319 € à payer, 5 factures, 4 échues,
   règlements avec état de rapprochement, 8 anomalies, historique.
9. **Redémarrage** → rapprochement CONFIRME et montants intacts.

## Tests

| Fichier | Nb |
|---|--:|
| `test_factures.py` | 19 |
| `test_reglements_fournisseurs.py` | 18 |
| `test_factures_routes.py` | 15 |
| `test_factures_banque.py` | 18 |
| `test_factures_banque_routes.py` | 13 |
| `test_factures_controles.py` | 17 |
| `test_factures_import_pdf.py` | 12 |
| `test_factures_import_routes.py` | 8 |
| **Total module** | **120** |

## Statut : **TERMINÉ**

Les critères de clôture sont satisfaits : saisie, import PDF, validation, lien charge, paiements,
rapprochement bancaire, solde fournisseur, contrôles, recette navigateur, persistance, tests, et
aucune anomalie bloquante inexpliquée (0 bloquant sur le jeu de recette).

L'import CSV/XLSX de factures reste non construit, faute de besoin démontré — cela ne bloque pas la
clôture, exactement comme OFX pour le module Banque.
