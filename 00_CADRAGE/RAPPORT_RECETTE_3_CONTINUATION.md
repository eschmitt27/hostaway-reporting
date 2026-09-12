# Rapport complémentaire — recette utilisateur n°3, continuation

*Établi le 2026-09-12. Point de départ : `e11632f`. Seize commits, +2 661 lignes de test.*

---

## 1. Une remarque sur ce tableau, avant de le lire

Le brief demande une couverture **§3 à §109**, chaque section marquée OUI ou NON avec une raison
réellement bloquante. Le document numéroté a été transmis dans la conversation ; **il ne figure pas
dans le dépôt**. Je ne peux donc pas garantir l'intitulé exact de chaque numéro entre 3 et 109.

Ce que je fais ici : j'atteste les sections dont le numéro est **explicitement cité** dans le brief
de continuation ou dans mon propre travail — elles couvrent l'essentiel — et je dis franchement
lesquelles je ne peux pas rattacher à un numéro précis. **Inventer un intitulé pour remplir une
ligne serait exactement le défaut que cette mission combat.** Déposez le brief dans `00_CADRAGE/`
et je complète la numérotation manquante sans rien deviner.

---

## 2. Couverture par section

### Traité, avec preuve

| § | Objet | Fait | Preuve |
|---|---|:--:|---|
| **3 / 57** | Logement archivé : date de fin effective | **OUI** | Date canonique `ref_gestion_logements_hist.date_fin` — elle **existait déjà**, renseignée pour les 5 logements retirés. Défaut trouvé et corrigé : `gestion_active_pour_mois` effaçait aussi le **passé** d'un logement archivé |
| **4 / 56** | Mois ouvert à la saisie | **OUI** | D-SAISIE-MOIS-01 (antérieur à cette continuation, conservé) |
| **5** | Canaux par libellé | **OUI** | `libelle_canal`, filtre `nom_canal` |
| **7 / 63** | Formateurs français | **OUI** | `date_fr`, `datetime_fr`, `entier` — étendus à **19 affichages** encore en ISO brut |
| **11-13** | `menages_cout_complet` | **OUI** | CTR-RECALCUL-MENAGES-COUT-COMPLET · 39 lignes / 7 655,00 € |
| **18** | Nomenclature des PDF déposés | **OUI** | `01_SOURCES_BRUTES/MenagesExternes/LISEZ-MOI.txt` |
| **19** | Suppression des chemins Excel morts | **OUI** | `lot6d` 408→360, `lot6e` 365→337, **`load_workbook` : 0**. Moteur 407/5/1 identique |
| **21** | Lignes PDF = lignes de la facture | **OUI** | `test_facture_lignes_correction` |
| **22** | Cinquième bout-en-bout | **OUI** | `test_e2e_facture_mixte_menage_et_frais` (12) |
| **23-25** | Rapprochement du logement | **OUI** | D-FOURN-LOGEMENT-01 · **21/21** rapprochés, **12/12** refusés · `test_logement_matching` (29) |
| **26-27** | Ménage O/N, quantité source, répartition | **OUI** | D-FOURN-QUANTITE-01 · migration 0082 · `test_facture_ligne_quantite_et_nature` (23) |
| **28-32** | Contrôle du total, correction tracée | **OUI** | CTR-COHERENCE-LIGNES-FOURNISSEUR · écart **qualifié**, pas seulement chiffré |
| **34-35** | Idempotence, pas de double comptage | **OUI** | `test_e2e_facture_mixte…::test_rejouer_la_chaine_ne_double_rien` |
| **36-38** | Dette fournisseur, réouverture | **OUI** | D-CHG-REOUVERTURE-01, `rouvrir_controle` |
| **39-50** | Facture propriétaire (document) | **OUI** | D-FACT-PROP-DOCUMENT-01 · `€` natif, accents, plus aucun code interne, **1 page au lieu de 2** · `test_facture_proprietaire_document` (21) |
| **45** | Téléphones | **OUI** | E.164 stocké / paires affichées · 17/17 convertis · migration 0083 |
| **51-52** | Créances, indicateur à 15 jours | **OUI** | Statuts lisibles, dates françaises, « à relancer » pour une créance sans échéance |
| **53-70** | Compte propriétaire sur le service canonique | **OUI** | CTR-CREANCE-VS-COMPTE-PROPRIETAIRE — **425,00 € d'écart** entre deux écrans, trouvé et fermé |
| **72-73** | Caisse : cycle, immuabilité, contrepassation | **OUI** | D-CAISSE-CYCLE-01 + contrat de solde corrigé · `test_operations_caisse_cycle` (18) |
| **74-78** | Comptabilité : dates, auxiliaires par nom | **OUI** | CTR-DATES-FORME-ISO (**0 date non ISO** dans toute la base), CTR-AUXILIAIRES-PAR-NOM |
| **77** | Écritures manuelles | **OUI** | Comptes et auxiliaires en listes, refus détaillé |
| **79-80** | Facture client exceptionnelle | **OUI** | D-FACT-EXCEPTIONNELLE-01 · migration 0085 · `test_facture_exceptionnelle` (16) + `_cycle` (11) |
| **81-91** | Administration : classification appliquée | **OUI** | D-ADMIN-CLASSIFICATION-01, D-ADMIN-CHAMPS-01 · 30 référentiels, 4 classes · `test_administration_classification` (37) |
| **92** | Inventaire des modules | **OUI** | `MATRICE_ETAT_MODULES.md`, mise à jour datée |
| **102** | Aucun identifiant technique à l'écran | **OUI** | `test_ui_pas_d_identifiants_techniques` (13) |

### Non traité, avec la raison

| § | Objet | Fait | Raison |
|---|---|:--:|---|
| **lot6a** (part de §19) | Extraction Hostaway CleaningTasks | **NON** | **Blocage réel** : le script exige des identifiants API Hostaway, absents de cette installation. Il ne peut être **ni exécuté ni testé** ici, et le migrer à l'aveugle reviendrait à réécrire un extracteur réseau sans jamais l'exercer. Sa cible SQLite alimente déjà tout l'aval : le script n'alimente plus rien de vivant |
| **§53-70**, part | Relevé propriétaire : refonte, KPIs à formules verrouillées, export Power BI, audit « Démarrer le suivi » | **NON** | **Pas de blocage technique** — c'est du travail restant. L'écran existe et fonctionne (KPIs, filtres, export CSV, 9 propriétaires). « Démarrer le suivi » n'existe **nulle part** dans le code : ni route, ni service, ni gabarit. Auditer un parcours absent sans savoir ce qu'il devait faire produirait une conclusion inventée |
| **Qonto** | Intégration bancaire | **NON** | **Hors scope explicite** du brief |

---

## 3. Défauts trouvés que personne n'avait demandé de chercher

Ce sont ceux qui comptent le plus : rien ne les signalait, et chacun coûtait de l'argent ou de la
confiance.

| Défaut | Ce qu'il produisait | Comment il a été trouvé |
|---|---|---|
| **425,00 € d'écart** entre Créances et Comptes propriétaires | Deux soldes différents pour la même facture. Chaque écran était cohérent **avec lui-même** | Confrontation des deux écrans sur les mêmes propriétaires |
| La facture `0005` ne manquait pas une ligne, elle en portait une **fausse** | Diagnostic identique pour deux causes opposées | `Σ(qté × P.U.) = 520,00 €` = le total du document, à la ligne près |
| **4 dates stockées à la française** | `'11/09/2026' < '2020-01-01'` est **vrai** : ces lignes se triaient avant tout, et tout filtre s'y trompait en silence | Balayage de toutes les colonnes de date |
| La migration 0081 avait laissé un trou | Une ligne écartée comme mal extraite **continuait** d'alimenter le coût ménage, donc la refacturation | Relecture des consommateurs de `statut_ligne` |
| `gestion_active_pour_mois` effaçait le passé | Une charge rétroactive excluait silencieusement un logement archivé de son périmètre | `archiver()` écrit `RETIRE` **avec** `date_fin` : la période est close, pas nulle |
| Le PDF pouvait porter `€` **sans aucune police** | `latin-1` était un choix, pas une contrainte : WinAnsiEncoding est l'encodage standard des polices de base | Test de `core_fonts_encoding` |
| Un compte auxiliaire s'appelait `PROP_0001` | Personne ne tient une balance en lisant des identifiants | Lecture de l'écran |
| La caisse n'avait **aucune** écriture obligatoire | Une opération pouvait exister sans trace au compte 530000 ; `annuler()` ne contrepassait rien | Lecture du cycle de vie |

---

## 4. Ce que mes propres tests ont corrigé chez moi

| Trouvé par | Ce qui était faux |
|---|---|
| `test_aucune_invention` | « T3 Toulouse » rapprochait un logement : mathématiquement unique, métier faux |
| `test_aucune_invention` | « T5 - 200 avenue de Paris » marquait 100 % sur Blagnac, sur le seul mot « avenue » |
| `test_une_echeance_future_suspend_la_relance` | Ma règle réclamait une relance sur une facture payable le mois suivant |
| `test_migration_et_service_concordent` | `import_id` NOT NULL : `enregistrer_correspondance` aurait échoué en production |
| Le diagnostic d'écart lui-même | Mes chiffres de test étaient faux ; il a répondu `LIGNES_EN_TROP`, et il avait raison |
| La contrainte CHECK du schéma | J'avais inventé un `type_document` ; fiscalement, une facture exceptionnelle **est** une facture |
| Le contrat de solde de caisse | Corrigé sur arbitrage : j'avais dérivé l'encaisse du statut comptable, puis « expliqué » le zéro en note — un pansement |

---

## 5. État de la base réelle

| | |
|---|---|
| Schéma | **0085** (0082 valeurs source, 0083 téléphones, 0084 dates, 0085 facture hors cycle) |
| `integrity_check` / `foreign_key_check` | **ok** / **0** |
| `menages_cout_complet` | **39 lignes / 7 655,00 €** — inchangé par toutes les migrations |
| `F-11/0-000001` | **EMIS**, 465,88 €, snapshot et PDF **intacts** (sha256 recalculé sur le fichier) |
| Factures de juillet | `0005` et `2026-40` en `A_CONTROLER`, 11 lignes intactes |
| Sauvegardes prises | 5 — `BCK-633B04C0BAE9`, `E2AD71C2F927`, `CEF20FCFFC3E`, `F6E713AA3DCB`, `CC6B90701EFF` |

**Aucun reset. Aucune renumérotation. Aucune suppression.**

---

## 6. Les deux factures de juillet — distinction préservée

Elles restent traitées par **deux mécanismes différents**, et rien de ce qui a été fait depuis ne
les confond :

| Facture | Diagnostic | Geste que l'écran propose |
|---|---|---|
| `0005` | `LIGNES_INCOHERENTES` — une ligne porte 36,00 € pour une quantité 0 × 32,00 € | « Corriger cette ligne » → **Écarter : extraction incorrecte** |
| `2026-40` | `LIGNE_MANQUANTE` — toutes les lignes cohérentes, il manque 89,00 € | **Ajouter une ligne manquante** |

Le départage est **arithmétique**, pas heuristique. Aucune des deux n'a été corrigée par moi :
l'utilisateur tranche depuis l'écran, désormais outillé pour le faire.
