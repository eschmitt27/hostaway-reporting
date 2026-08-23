# Moteurs métier purs — Phase 1 (2026-08-23)

Mission « simplification architecture Python / extraction progressive des moteurs métier purs ».
Expérience contrôlée sur UN SEUL moteur pilote, pas un chantier général. HEAD départ `05fd8a4`.

## 1. Audit (fork lecture seule)

Modules cartographiés : commissions (Lot10, `lib_ref_history.py`), ménages (`lib_menage_costs.py`,
Lot6f), charges (7 modules `charges_*_service.py`), facturation propriétaires (7 modules
`factures_*_service.py`), trésorerie propriétaires (`compte_proprietaire_service.py`),
flux_unifié, Lot11, Lot12.

| Module | I/O mélangées ? | Pandas | SQL | Couplage | Tests | Risque |
|---|---|---|---|---|---|---|
| `compte_proprietaire_service.calculer_fifo` | **Non** — déjà isolée, déjà documentée comme fonction pure | Aucun | Aucun dans la fonction (SQL dans `_factures`/`_sources`, séparées) | Aucun dans la fonction | `test_compte_proprietaire_fifo.py` (24 tests, 4 déjà purs) | **Bas** |
| `lib_menage_costs.py` (résolution tarif ménage interne) | Non — déjà pur | Aucun | Aucun | Aucun | `tests/test_menage_costs.py` (9 tests, racine) | Bas (déjà fait, référence) |
| `lib_ref_history.py` (résolution taux/période datée) | Non — déjà pur | Aucun | Aucun | Aucun | `tests/test_ref_history.py` (13 tests, racine) | Bas (déjà fait, référence) |
| Lot10 (`lot10_calculer_resultats.py`, 1533 lignes) | Oui, fortement — résolution taux appelée dans des boucles pandas vectorisées, écriture SQL dans la même fonction | Massif (`pd.concat`, `.loc` vectorisé) | Direct, imbriqué au calcul | SQLite direct + pandas mêlés | Couverture large mais diffuse | **Haut** |
| Lot11 (`lot11_controles_coherence.py`, 1461 lignes) | Oui | Oui | Oui | SQLite direct + pandas | Non quantifié | **Haut** |
| Lot12 / facturation propriétaires | I/O fort (PDF filesystem, import fichier) | Probable | Direct | Filesystem (PDF) | Non quantifié | **Haut** |
| Charges (7 modules) | Faible — déjà découpé par responsabilité | Faible/aucun | Direct SQLite | `get_db`, routes FastAPI en amont | Nombreux tests, non quantifiés précisément | **Moyen** |
| flux_unifié | Non audité (hors budget de l'audit) | Inconnu | Probable | Inconnu | Non quantifié | **Inconnu** |

### Classement

- **A — EXCELLENT CANDIDAT** : `compte_proprietaire_service.calculer_fifo`.
- **A (déjà fait, référence méthode)** : `lib_menage_costs.py`, `lib_ref_history.py` — déjà des
  moteurs purs, déjà testés sans DB. Cités ici comme preuve que le pattern existait avant cette
  mission, pas inventé par elle.
- **B — CANDIDAT PLUS TARD** : Charges (déjà découpé, mais non audité fonction par fonction),
  Lot12/facturation (filesystem PDF = effet de bord non trivial à isoler rapidement).
- **C — NE PAS TOUCHER MAINTENANT** : Lot10 (1533 lignes, pandas+SQL+calcul massivement imbriqués),
  Lot11 (1461 lignes, même profil), flux_unifié (non audité, budget insuffisant pour trancher).

## 2. Moteur pilote choisi : `calculer_fifo`

**Pourquoi lui** : la plus petite unité candidate (33 lignes), déjà 100 % pure et déjà documentée
comme telle, déjà couverte par 4 tests unitaires sans DB, et surtout déjà **réutilisée par un
second domaine métier** — `intervenant_menage_compte_service.py` (compte d'un intervenant ménage
interne, rien à voir avec un propriétaire) important `calculer_fifo` directement depuis le module
de service `compte_proprietaire_service`. C'est un couplage cross-domaine concret et mesurable, pas
un défaut hypothétique — exactement le type de couplage que la mission demande de réduire.

**Surface actuelle** : une fonction, deux appelants (`compte_proprietaire_service.recalculer`,
`intervenant_menage_compte_service`), un fichier de test dédié déjà séparé en section « Moteur
pur » vs « Scénarios avec DB ».

**Risque** : quasi nul — aucune ligne de logique modifiée, extraction = déplacement de module.

**Résultat attendu** : la fonction vit dans un module neutre (`app/moteurs/fifo_engine.py`) sans
aucune dépendance applicative ; les deux domaines qui la consomment l'importent depuis ce module
neutre au lieu que l'un dépende du service de l'autre.

## 3. Architecture avant / après

**Avant** :
```
intervenant_menage_compte_service.py
    → import calculer_fifo depuis compte_proprietaire_service.py (service d'un AUTRE domaine)

compte_proprietaire_service.py
    TOLERANCE, calculer_fifo() définis ici, mélangés dans le même fichier que
    get_db/sqlite3/recalculer()/position() (I/O + orchestration + moteur, un seul module)
```

**Après** :
```
app/moteurs/fifo_engine.py          ← moteur pur, zéro dépendance applicative
    TOLERANCE, calculer_fifo()

compte_proprietaire_service.py      ← ré-exporte TOLERANCE/calculer_fifo (compat, 0 régression)
    _factures()/_sources()  (lecture SQL)
    recalculer()             (orchestration + transaction + persistance)
    position()               (agrégation)

intervenant_menage_compte_service.py
    → import calculer_fifo depuis app.moteurs.fifo_engine (moteur neutre, plus de dépendance
      cross-domaine vers le service propriétaire)
```

## 4. Contrats de données

Aucun nouveau contrat créé : `calculer_fifo` reste générique sur des `dict` (clés
`facture_id_opaque`/`montant_total` pour les créances, `source_type`/`source_ref`/`source_date`/
`montant` pour les sources) — c'est déjà le contrat existant, utilisé à l'identique par les deux
domaines (propriétaire, intervenant ménage). Le forcer vers une dataclass aurait exigé d'adapter
les DEUX appelants sans bénéfice de parité — non fait, conforme à la règle « ne pas réécrire plus
que nécessaire ».

## 5. Règles métier — aucune modification

Le corps de `calculer_fifo` est un déplacement **littéral** : même code, même ordre d'opérations,
mêmes arrondis (`_round`, `round(float(x or 0), 2)`), même `TOLERANCE = 0.005`, même float (pas de
migration vers Decimal — hors mandat, la mission l'interdit explicitement sans nécessité). Aucun
taux, aucune assiette, aucun statut, aucun arrondi, aucune règle temporelle modifiés.

## 6. Tests de caractérisation / unitaires purs

- `tests/test_compte_proprietaire_fifo.py` (24 tests, déjà existants, inchangés) — continue de
  passer par le ré-export `compte_proprietaire_service.calculer_fifo` : preuve que le déplacement
  n'a rien cassé côté consommateur historique.
- `tests/test_fifo_engine.py` (9 tests, nouveau) — importe directement `app.moteurs.fifo_engine`,
  sans DB, sans `tmp_db`/`tmp_path`, sans FastAPI, sans monkeypatch. Reprend les scénarios déjà
  couverts + 4 nouveaux cas limites (aucune source, montant exactement égal au dû, source
  infra-tolérance, ordre de consommation des sources) + un test structurel qui grep le fichier
  source pour garantir l'absence de `sqlite3`/`fastapi`/`app.config`/`app.db`/`Path(`/`os.environ`.

Double exécution (règle §9 de la mission) : les deux fichiers de test exercent le **même objet
Python** (`calculer_fifo` importé depuis deux chemins différents pointant vers le même module) —
la parité OLD/NEW est garantie par identité, pas mesurée après coup.

## 7. Parité économique

Écart : **0,00 €**, par construction — aucune ligne de la fonction n'a changé. Les 24 tests
existants (scénarios chiffrés du cahier des charges, `test_compte_proprietaire_fifo.py`) restent
verts sans modification.

## 8. Performance

Non mesurée par un benchmark dédié : la fonction est inchangée bit à bit, un import supplémentaire
(`from app.moteurs.fifo_engine import ...`) n'a pas d'impact mesurable sur une fonction déjà
appelée en mémoire pure. Aucune régression manifeste attendue ni observée sur la campagne complète.

## 9. Complexité avant/après

- `compte_proprietaire_service.py` : -33 lignes de logique moteur, +1 ligne d'import ; le fichier
  ne contient plus que lecture SQL, orchestration et agrégation — plus proche de « Repository +
  Service » que « Repository + Service + Moteur mélangés ».
- `intervenant_menage_compte_service.py` : 1 ligne d'import changée, docstring mise à jour ; ne
  dépend plus du service d'un autre domaine métier pour un algorithme générique.
- `app/moteurs/fifo_engine.py` : nouveau module, 0 dépendance applicative, testable en <100ms sans
  fixture.

## 10. Code mort

Aucun — la fonction déplacée reste utilisée par ses deux appelants existants ; le ré-export dans
`compte_proprietaire_service.py` est un choix délibéré de compatibilité (0 régression), pas du
code mort (il a un rôle : la surface publique historique du module, testée par
`test_compte_proprietaire_fifo.py`).

## 11. Pandas

Aucun usage pandas dans le périmètre extrait (`calculer_fifo` n'en a jamais utilisé). Aucun
chantier pandas engagé — hors mandat pour ce module pilote.

## 12. Campagne finale

Moteur (`tests/` racine) : **345 passed**, 0 failed — suite non concernée par cette mission,
confirmée inchangée.

Application (`05_APPLICATION/tests/`, 180 fichiers dont le nouveau `test_fifo_engine.py`,
9 lots) : **2 661 passed**, 0 failed (quelques `skipped` pré-existants, non liés à cette mission).

Consommateurs aval vérifiés explicitement : `test_compte_proprietaire_fifo.py` (24),
`test_intervenant_menage_compte.py`, `test_comptes_proprietaires_routes.py` — tous verts.

## 13. Intégrité réelle

`app.db` réelle : hash inchangé (`8e299b935ef1e0d4`) avant/après. `REF_Setup.xlsm` réel : non
touché. Mode réel : `OFF`. Scheduler Hostaway réel : `INACTIF` (non concerné par cette mission).
Aucune migration — aucun schéma modifié.

## 14. Limites

- Un seul moteur extrait sur cette mission (règle explicite : expérience contrôlée, pas un
  chantier général).
- Lot10/Lot11 (candidats C) restent en l'état — leur extraction exigerait de séparer calcul et
  persistance dans des fichiers de 1500 lignes, un risque de régression que cette mission
  n'accepte pas de prendre sans un audit dédié et beaucoup plus de tests de caractérisation.
- `flux_unifié` n'a pas été audité en détail (budget de l'audit initial insuffisant) — à faire
  avant de le classer.

## 15. Prochain moteur recommandé

Non décidé ici — la mission suivante choisira, à partir de ce résultat, si le pattern doit
s'appliquer à un autre module. Candidats B les plus proches : le module Charges (déjà découpé par
responsabilité, mérite un audit fonction par fonction) et `lib_menage_costs.py`/`lib_ref_history.py`
(déjà purs, à formaliser dans `app/moteurs/` sans changement de logique si jugé utile).
