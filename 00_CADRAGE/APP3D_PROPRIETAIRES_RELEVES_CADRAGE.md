# APP-3D — Relevés/préfactures propriétaires : cadrage final, correction architecturale

## 1. Décision architecturale

Point d'entrée UNIQUE : `/proprietaires-reglements`. L'ancien module parallèle
`/proprietaires-releves` (créé par erreur pendant le développement initial d'APP-3D) est **supprimé**
(routes, template de liste dédié, entrée de menu) et fusionné dans `/proprietaires-reglements` :

- `/proprietaires-reglements` — liste mensuelle (dashboard APP-3C, inchangé)
- `/proprietaires-reglements/{proprietaire_id}` — fiche historique (comportement APP-3C inchangé,
  compatibilité ascendante)
- `/proprietaires-reglements/{REG-xxxx}` — fiche de suivi de facturation (APP-3D), même route,
  branchement runtime sur le préfixe `REG-`
- `/proprietaires-reglements/{REG-xxxx}/releve`, `/prefacture`, `/historique`, `/export-releve.csv`
- `/proprietaires-reglements/demarrer`, `/marquer-a-facturer`, `/marquer-facture`,
  `/demander-avoir`, `/emettre-avoir`, `/reouvrir`

Aucune route dupliquée, aucune entrée de menu dupliquée (1 seul lien sidebar "Propriétaires &
règlements"). Preuve : `tests/test_proprietaires_releve.py::test_44_aucune_route_dangereuse` et
smoke test dédié (0 référence à `proprietaires-releves` dans `base.html`).

## 2. Matrice fonctionnelle (état final, module unique)

| Fonction | `/proprietaires` (ancien, orphelin) | `/proprietaires-reglements` (module unique) | Source de vérité | Décision |
|---|---|---|---|---|
| Liste mensuelle | absent (jamais eu de liste) | présent (APP-3C) | `proprietaires_reglements_service.load_owners` | conservé |
| Fiche propriétaire | logique présente, jamais routée | présent (raw id, APP-3C) | idem | logique réutilisée, pas dupliquée |
| Détail logements | présent | présent (`load_owner_detail`) | moteur Lot10/Lot12 | conservé |
| Détail réservations | présent | présent (via détail) | moteur | conservé |
| Exploitation | présent (calcul) | affiché en lecture seule | moteur | conservé, jamais recalculé ici |
| Règlement | présent (calcul) | affiché en lecture seule | moteur | conservé |
| Acomptes | absent | présent (lecteur APP-3D) | `proprietaires_extras_reader` | nouveau, source vide documentée |
| Reversements | via règlement moteur | affiché | moteur | conservé |
| Ajustements post-clôture | absent | présent (lecteur APP-3D) | `proprietaires_extras_reader` | nouveau, source vide documentée |
| AirCover | absent | présent (lecteur APP-3D) | `proprietaires_extras_reader` | nouveau, source vide documentée |
| Charges | via `/fournisseurs` (charges) | référencé, pas dupliqué | `fournisseurs.py` (routes charges) | réutilisé tel quel |
| Fournisseur (référentiel) | absent (n'existe nulle part) | `/referentiel-fournisseurs` (APP-3D) | `fournisseurs_referentiel_service` | créé (minimal, cf. §4) |
| Relevé | logique présente, non routée | `/…/{REG}/releve` | fusion ancien module + suivi | réutilisé + suivi ajouté |
| Préfacture | `lot12_generer_factures.py` (offline) | `/…/{REG}/prefacture` | idem | réutilisé, jamais généré automatiquement |
| Historique | absent | `/…/{REG}/historique` (append-only) | `proprietaires_suivi_service` | nouveau |
| Contrôles | via APP-5B | référencé (lien statut moteur) | `controles_cloture_reader` | réutilisé |
| Blocages | absent | 12 codes (§3) | `proprietaires_blocages_service` | nouveau |
| Exports | absent | `/…/{REG}/export-releve.csv` | `proprietaires_releve_export_service` | nouveau, CSV sécurisé |
| Statut de clôture | via APP-5C (mois) | affiché (lecture) | `clotures_service` | réutilisé, jamais recalculé |
| Statut de facturation | absent | `ST_NON_CONCERNE…ST_AVOIR_EMIS` | `proprietaires_suivi_service` | nouveau, distinct du statut clôture |

`/proprietaires` reste orphelin (aucune route ne pointe dessus) — code legacy non supprimé (hors
périmètre de cette mission : suppression nécessiterait une recherche de références exhaustive
séparée), mais aucune fonctionnalité n'est dupliquée depuis ce module : tout ce qui est utile a été
soit réutilisé directement (imports de services), soit reproduit dans le module unique.

## 3. Audit comptabilité — classification : **couche analytique uniquement**

| Élément recherché | Présent ? | Constat |
|---|---|---|
| Journal comptable | Absent | aucune table d'écritures |
| Plan comptable | Absent | aucun référentiel de comptes |
| Débit/crédit | Absent | — |
| Comptes auxiliaires | Absent | — |
| Numérotation de pièces | Ambigu | `PREF-{mois}-{prop}-{log}-{compteur}` = id de préfacture, pas une séquence légale |
| Grand livre | Absent | — |
| Balance | Absent | — |
| Gestion TVA | Ambigu/partiel | flag statique `regime_tva_prestataire`, pas un moteur TVA |
| Période comptable distincte | Absent structurellement | `DATE_BASCULE_SOCIETE` conceptuel, non implémenté en table |
| Clôture comptable distincte de la clôture APP-5C | Absent | une seule notion de clôture (opérationnelle) |
| Export vers logiciel comptable | Absent | — |

Aucune comptabilité générale n'a été ajoutée par APP-3D. Limite documentée visible dans ce fichier
et à faire apparaître dans le guide utilisateur (§7).

## 4. Audit fournisseurs — avant/après

| Fonction | Avant (audit) | Après (APP-3D) |
|---|---|---|
| Création fournisseur | Absente (`/fournisseurs` = charges, pas de nom fournisseur) | `POST /referentiel-fournisseurs/creer` |
| Table dédiée | Absente | migration 0010 : `fournisseurs`, `fournisseur_evenements` |
| Identifiant logique | Absent | `FRS-<hash>` (salt réutilisé `PROPRIETAIRE_OPAQUE_SALT`) |
| Fiche | Absente | `/referentiel-fournisseurs/{FRS-xxxx}` |
| Statut actif/inactif | Absent | `statut` ACTIF/INACTIF, désactivation logique uniquement |
| Lien charge↔fournisseur | Absent (charges n'ont pas de `fournisseur_id`) | **non ajouté** — nécessiterait modification du schéma `charges`, hors périmètre minimal ; documenté comme limite |
| Lien ménage↔fournisseur | Existe déjà via `intervenant_id`/`REF_Intervenants` (concept distinct, non généralisé) | non fusionné avec le nouveau référentiel — décision : les deux référentiels restent séparés tant qu'aucun besoin métier de fusion n'est exprimé |
| Détection doublon | Absente | `rechercher_doublons` (insensible casse/espaces) |
| Trace de modification | Absente | `fournisseur_evenements` (append-only, CREATION/MODIFICATION/DEACTIVATION/REACTIVATION) |

Champs interdits (IBAN, coordonnées bancaires, numéro de carte) : absents du modèle, prouvé par
`test_12_aucun_champ_bancaire_dans_le_modele`.

## 5. Contrats des 4 nouvelles sources (actuellement vides)

Voir `tests/test_proprietaires_extras_contract.py` (18 tests) pour le détail colonne-par-colonne.
Résumé :

| Source | Onglet | Colonnes obligatoires | Comportement source vide |
|---|---|---|---|
| Acomptes | SAISIE | proprietaire_id, mois, montant_acompte | état `VIDE` explicite, jamais un `[]` silencieux |
| AirCover | MASTER | proprietaire_id, mois, montant | idem |
| Imputations Airbnb | MASTER | proprietaire_id, mois, montant_impute | idem |
| Ajustements post-clôture | MASTER | proprietaire_id, mois_effet, montant, motif | idem |

Colonne manquante → filtrage sans crash (liste vide). Colonne nouvelle → ignorée. Montant invalide →
`None` (jamais d'exception). Mois invalide → aucune correspondance. Doublons → jamais dédupliqués
silencieusement (visibles pour contrôle humain). Cellule formule → neutralisée (`data_only=True`).

## 6. Règles de blocage (12 codes)

| Code | Gravité | Source | Message | Bloquant | Déclenchement | Résolution | Test |
|---|---|---|---|---|---|---|---|
| MOIS_INCOHERENT | BLOQUANT | format mois | "Format de mois invalide." | oui | mois mal formé | corriger le mois | test_23 (indirect via evaluer) |
| PROPRIETAIRE_INCONNU | BLOQUANT | moteur | "Propriétaire inconnu du référentiel." | oui | id absent du moteur | vérifier l'id | evaluer() branch |
| SOURCE_OBLIGATOIRE_ABSENTE | BLOQUANT | moteur | "Source obligatoire indisponible." | oui | `detail.status != OK` | rendre la source moteur disponible | evaluer() branch |
| NET_ABSENT | BLOQUANT | moteur | "Net non calculé." | oui | `vue` ou `net` absent | recalcul moteur | evaluer() branch |
| PAYOUT_ABSENT | BLOQUANT | moteur | "Payout non retenu." | oui | `ca_retenu` absent | recalcul moteur | evaluer() branch |
| TAUX_COMMISSION_ABSENT | BLOQUANT | référentiel | "Taux de commission absent." | oui | pas d'historique taux | compléter le référentiel | evaluer() branch |
| CLOTURE_MOTEUR_INCOMPATIBLE | BLOQUANT | APP-5C | "Mois non clôturé par le moteur." | oui | `statut_mois != CLOTURE` | attendre/forcer clôture moteur | test_23 |
| AJUSTEMENT_SANS_MOTIF | BLOQUANT | source ajustements | "Ajustement sans motif." | oui | motif vide sur un ajustement | saisir le motif | evaluer() branch |
| DOUBLON_FACTURATION | BLOQUANT | suivi | "Déjà facturé." | oui | statut suivi = FACTURE | ouvrir un avoir si erreur | test_27 |
| PREFACTURE_DEJA_PREPAREE | BLOQUANT | suivi | "Préfacture déjà en préparation." | oui | statut suivi = A_FACTURER, nouvelle préparation demandée | reprendre la préparation existante | test_49 (nouveau) |
| SOURCE_SCHEMA_INVALIDE | BLOQUANT | lecteurs extras | "Source complémentaire illisible." | oui | un des 4 lecteurs en état ILLISIBLE | corriger le fichier source | test_50 (nouveau) |
| ANOMALIE_MOTEUR_OUVERTE | INFORMATIF | moteur | "Anomalie moteur ouverte." | non | anomalie sévérité != INFO | traiter l'anomalie côté moteur | evaluer() branch |

Codes explicitement **non implémentés, avec justification** (pas de règle métier inventée) :

- `FOURNISSEUR_OBLIGATOIRE_ABSENT` — non applicable : le modèle `charges` actuel n'a pas de champ
  `fournisseur_id` obligatoire ; l'ajouter serait une extension de schéma hors périmètre minimal.
- `DIVERGENCE_MOTEUR_DETAIL` — non applicable sans recalcul (interdit dans cette couche) ; aucune
  seconde source de vérité indépendante n'existe à comparer sans recalculer.
- `VERSION_OBSOLETE` — déjà géré nativement par l'écriture optimiste (colonne `version` + `rowcount`)
  au niveau service, pas un blocage de préparation pré-affiché.
- `WRITER_REEL_ACTIVE` — c'est un indicateur global de sécurité applicative (flag), pas une règle
  métier par propriétaire/mois.

## 7. Limites à afficher dans l'application / guide utilisateur

- Aucune comptabilité générale (pas de journal, pas de grand livre, pas de TVA calculée).
- Aucune préfacture n'est une facture légale définitive.
- Aucun virement, aucune écriture bancaire réelle générée depuis ce module.
- Le référentiel fournisseur est minimal (pas de données bancaires, pas de lien automatique aux
  charges existantes).

## 8. Recette visuelle (4 largeurs)

Serveur de recette isolé sur port 8010 (jamais le port 8000 / PID de l'instance existante). 7 écrans
× 4 largeurs (1920×1080, 1366×768, 1024×768, 768×1024) = 28 captures : liste, référentiel
fournisseurs, fiche propriétaire (raw id historique), fiche de suivi (REG-), relevé, préfacture,
historique. Vérifications automatisées : absence de débordement horizontal global, absence de double
entrée de menu active, absence de référence résiduelle à `/proprietaires-releves`.

2 régressions visuelles réelles détectées et corrigées pendant la recette :
1. En-tête de la liste (`page-header`) sans retour à la ligne sur mobile → `flex-wrap` ajouté.
2. Tableaux 3/4 de la fiche règlement non scrollables localement (`grid-2` sans `min-width: 0` sur
   les cellules) → `table-container` ajouté aux deux tableaux + règle CSS `.grid-2 > * { min-width: 0; }`.

Recette rejouée après correction : 0 débordement, 0 double menu, 0 référence résiduelle sur les 28
combinaisons.
