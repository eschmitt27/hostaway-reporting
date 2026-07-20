# APP-4A — Banques & caisse : lecture & rapprochement (lecture seule)

**Statut : `APP-4A_BANQUES_CAISSE_LECTURE_EN_ATTENTE_VALIDATION`**

Module **visible et navigable**, alimenté par les vraies sorties du pipeline banque. Il ne produit
**aucune écriture**, ne déclenche **aucun import**, n'ouvre **aucune connexion bancaire** et ne
**rapproche rien** : il lit et présente ce que le moteur a produit.

Développé dans un worktree détaché (HEAD `8b47807`), hors du dépôt principal. **Aucun commit.**

---

## 1. Audit des sources

Le pipeline banque est un enchaînement lot8a → lot8b → lot8c qui construit **un seul classeur** :
`02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx`.

- **lot8a** — importe `01_SOURCES_BRUTES/Banque/AAAA_MM_BRUT_Banque_CreditMutuel.xlsx` (Crédit Mutuel),
  normalise → onglet `NORM_Banque` + `CTRL_A_CONTROLER` + `LOG_Traitement`.
- **lot8b** — ajoute `REF_Banque_Regles`, classe les mouvements (TYPE_FLUX_016 FRAIS_BANCAIRES, etc.),
  enrichit `NORM_Banque` + `IA_Classification`.
- **lot8c** — rapprochement **partiel en attente** : `RAPPROCH_AIRBNB_ATTENTE`,
  `RAPPROCH_PROPRIETAIRES_ATTENTE`, `CTRL_RAPPROCHEMENT_8C`. **Aucun rapprochement validé
  automatiquement, aucun produit économique créé.**

| Élément | Constat |
|---|---|
| Onglets exploités | NORM_Banque, CTRL_A_CONTROLER, RAPPROCH_AIRBNB_ATTENTE, RAPPROCH_PROPRIETAIRES_ATTENTE, CTRL_RAPPROCHEMENT_8C, LOG_Traitement |
| Grain | 1 ligne = 1 mouvement bancaire (`mouvement_id`, clé stable du moteur) |
| Volumétrie réelle | ~132 mouvements sur **2026-02 / 2026-03 / 2026-04** (1 compte Crédit Mutuel) |
| Statut réel | quasi tous `A_CONTROLER` / en attente ; **0 rapproché validé** |
| Comptes / IBAN | `compte_id` interne (ex. `CM_02211_00021321603`) — **masqué** en interface |
| **Fichiers absents** | le classeur banque et `01_SOURCES_BRUTES/Banque` sont **untracked** (produits localement) : absents d'un checkout git propre → le reader affiche « source absente » tant que `PROJECT_ROOT` ne pointe pas sur le vrai projet |
| **Caisse** | **aucune source, aucun module moteur** — affichée « non alimentée », jamais fabriquée |
| Scripts producteurs | `lot8a_banque_import.py`, `lot8b_banque_regles.py`, `lot8c_rapprochement_banque.py` |
| Risques | source untracked (fraîcheur/versionnement) ; libellés bancaires contiennent des tiers → `libelle_brut` jamais exposé ; un seul compte, un seul établissement |

## 2. Architecture livrée

- **Reader** `app/readers/banques_reader.py` — openpyxl `read_only=True`, cache (mtime + taille),
  **états distincts** (OK / fichier absent / onglet absent / vide / illisible / non alimenté),
  masquage de compte, convertisseurs (jamais `None` → 0). Aucune écriture.
- **Service** `app/services/banques_service.py` — `load_dashboard`, `load_movements`, `load_detail`,
  `load_unmatched`, `load_available_periods`, `load_filter_options`, `load_freshness`,
  `export_movements_csv`. Statuts et rapprochements **repris du moteur** ; le rattachement
  mouvement ↔ attente se fait par `mouvement_id`, jamais par un calcul de correspondance ajouté.
- **Routes** `app/routes/banques.py` — `GET /banques-caisse`, `/a-rapprocher`, `/export.csv`,
  `/mouvements/{stable_id}`.
- **Templates** — `banques_list.html`, `banques_a_rapprocher.html`, `banques_detail.html`.
- **Nav** — item « Banques & caisse » activé dans `base.html` ; router enregistré dans `main.py`.
- **CSS** — bloc utilitaire APP-4 ajouté (`data-table`, `status-badge`, `pagination`, …).

## 3. Écrans

- **`/banques-caisse`** — période, fraîcheur, 6 cartes (mouvements bancaires, **caisse « — »**,
  rapprochés, à rapprocher, à contrôler, débit/crédit), filtres (période, compte, sens, statut, montant
  min/max, recherche, non rapprochés), tableau paginé (25), export CSV. Numéros de compte masqués.
- **`/banques-caisse/mouvements/{mouvement_id}`** — mouvement, rapprochement moteur, anomalies +
  contrôles, traçabilité (import_id, ROW_HASH, règle). 404 propre si inconnu.
- **`/banques-caisse/a-rapprocher`** — lignes signalées par le moteur + contrôles Lot 8c. Bouton
  **« Rapprocher manuellement » désactivé** (« prochain lot ») — aucun faux fonctionnement.

## 4. Règles respectées

- banque = **source de flux**, pas une vérité comptable ; un mouvement ne devient jamais
  automatiquement une charge/revenu ;
- **aucun rapprochement inventé** dans FastAPI (test dédié) ;
- caisse distincte, **non fabriquée** ;
- **IBAN / numéros de compte masqués**, `libelle_brut` jamais exposé, aucun chemin absolu en interface ;
- SQLite = journal applicatif uniquement ; **aucun MASTER écrit**, **aucun import réel déclenché**.

## 5. Limites & prochaine étape

- Un seul compte / un seul établissement (Crédit Mutuel) ; pas de Qonto.
- **Caisse non livrée** (pas de source moteur).
- **Rapprochement manuel non livré** (bouton désactivé — lot ultérieur).
- **Import bancaire réel non livré** (aucune connexion API, aucun virement).
- Source untracked : l'app la lit via `PROJECT_ROOT` ; dans un checkout propre elle est absente.
- Écart montant/date par ligne non calculé : le moteur n'a pas encore de correspondance validée
  (tout est `EN_ATTENTE`) — non inventé côté application.

## 6. Tests

`05_APPLICATION/tests/test_banques.py` — 31 tests sur fixture banque **isolée** : lecture & états
(alimentée / fichier absent / onglet absent / vide), débit/crédit, caisse non alimentée, doublon non
fusionné, à-contrôler, filtres (période/sens/non-rapproché/montant/recherche), pagination, fiche détail,
404, export CSV, **masquage compte**, aucun chemin absolu ni `libelle_brut`, **aucun rapprochement
inventé**, aucune source réelle modifiée, aucun import moteur ni écriture openpyxl dans `app/`, routes
(dashboard, détail 200/404, à-rapprocher, export), nav sidebar. Smoke sur données réelles : 132
mouvements lus, compte masqué, fichier réel intact.
