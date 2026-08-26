# Contrats de données branchés en production — Mission 11 (2026-08-26)

## Principe RAW / CANONIQUE / OUTPUT

- **RAW/import** : validation structurelle minimale, tolère les anomalies que les contrôles
  métier doivent détecter en aval (ex. `sens` bancaire hors domaine dans une ligne déjà en base).
- **CANONIQUE/métier** : contrats plus stricts, imposés APRÈS la phase qui doit pouvoir voir
  l'anomalie — jamais avant.
- **OUTPUT/moteur** : les moteurs purs (Commission, Charges, Ménages, FIFO) restent la seule
  source de vérité économique ; cette mission ne les modifie pas.

## Audit des 4 contrats existants (`app/contrats_donnees.py`, hardening initial, jamais branchés)

| Contrat | Niveau | Compatible données réelles ? | Branchable tel quel ? | Modification nécessaire |
|---|---|---|---|---|
| `Charge` | CANONIQUE | OUI (`type="date"` sur le formulaire réel, jamais de format non-ISO observé) | OUI | Aucune |
| `ReservationHH` | CANONIQUE | NON — `montant_retenu` obligatoire dans le contrat, mais `reservations_hh_saisie_service.valider()` ne l'exige jamais (placeholder réel, ex. `DIRECT_SANS_SAISIE_HH`, montant connu plus tard) | Après ajustement | `montant_retenu` rendu optionnel (`float \| None`) |
| `MouvementBanque` | dépend de la frontière | Le champ `sens` (DEBIT/CREDIT strict) rejetterait l'anomalie `sens="INCONNU"` que `banques_controles_catalogue_service.py::test_sens_incoherent_detecte` doit détecter — **mais** `banques_import_service.py` calcule TOUJOURS `sens` en DEBIT/CREDIT par construction (jamais copié brut d'une colonne source) | OUI, mais UNIQUEMENT en auto-contrôle interne de `banques_import_service.py` sur sa propre ligne déjà normalisée — jamais sur des lignes déjà en base | Aucune (le contrat reste strict ; c'est la PORTÉE du branchement qui évite le piège du CHECK SQLite retiré en migration 0056) |
| `MouvementTresorerieProprietaire` | CANONIQUE | OUI (`type="date"` réel, `sens`/`montant` déjà validés à l'identique par `previsualiser()`) | OUI | Aucune |

## Frontières branchées

**Charges** — `charges_saisie_service.py::creer()`/`modifier()`, après `valider()` (validation
métier, inchangée), avant écriture SQLite. Avant : `date_charge` non calendaire (ex.
`"12/06/2026"` ou `"2026-02-30"`) était silencieusement acceptée — `_mois_depuis_date()` en aurait
dérivé un mois invalide, propagé jusqu'à Lot9/Lot10 sans jamais être détecté. Après : refusée ici
(`CHARGE_CONTRAT_INVALIDE`), 0 ligne écrite.

**Réservations HH** — `reservations_hh_saisie_service.py::creer()`/`modifier()`, après
`valider()`. Avant : une date calendairement impossible n'était vérifiée que par comparaison de
chaînes (`depart < arrivee`), jamais par validité calendaire réelle. Après : refusée
(`RESHH_CONTRAT_INVALIDE`). `montant_retenu` reste optionnel (contrat ajusté) : un placeholder
réel sans montant continue d'être accepté.

**Banque** — `banques_import_service.py`, à l'intérieur de `_normaliser()`, juste après la
construction de la ligne normalisée et avant sa mise en `valides`. Auto-contrôle : si ce service
produisait un jour un `sens` hors DEBIT/CREDIT (bug de régression dans son propre calcul), la
ligne serait explicitement écartée plutôt que silencieusement écrite. **N'affecte jamais** les
lignes déjà en base par un autre chemin (import legacy, correction manuelle) — `banques_controles_
catalogue_service.py` continue de les détecter exactement comme avant (prouvé par test, la même
anomalie `sens="INCONNU"` reste détectée).

**Trésorerie propriétaire** — `proprietaires_tresorerie_service.py::creer()`, après
`previsualiser()`. Avant : `date_mouvement` n'était vérifiée que non-vide, jamais calendairement
valide. Après : refusée (`V06_CONTRAT_INVALIDE`), 0 ligne écrite.

**Factures** — pas de nouveau contrat dataclass créé (audité, jugé non nécessaire : `factures_
service.py::valider()` couvre déjà fournisseur/référence/montant/cohérence HT+TVA+TTC/doublon).
Un gap réel trouvé et corrigé directement dans `valider()` : `date_facture`/`date_echeance`
présentes mais calendairement invalides étaient absorbées par un `except ValueError: pass` muet —
ni signalées, ni bloquées. Corrigé : nouveau code `V10_DATE_CALENDAIRE_INVALIDE`, les deux dates
restent optionnelles (formulaire réel sans `required`).

## Anomalies RAW volontairement conservées

- Banque : un mouvement au `sens` hors domaine déjà en base (import legacy, correction manuelle)
  reste détectable par `banques_controles_catalogue_service.py` — jamais filtré par
  `MouvementBanque`, qui ne s'applique qu'à l'auto-contrôle de `banques_import_service.py`.

## Validations bloquantes ajoutées

- Charge : `date_charge` doit être une date calendaire réelle au format AAAA-MM-JJ.
- ReservationHH : `date_arrivee`/`date_depart` calendaires réelles, cohérentes entre elles.
- MouvementBanque : auto-contrôle `sens ∈ {DEBIT, CREDIT}` sur la propre sortie de l'import.
- MouvementTresorerieProprietaire : `date_mouvement` calendaire réelle.
- Facture : `date_facture`/`date_echeance` calendaires réelles si présentes.

## Pydantic / dataclass

Dataclasses uniquement (déjà la convention du projet, `app/contrats_donnees.py` créé lors du
hardening initial) — 0 dépendance Pydantic ajoutée, cohérent avec §11 de la mission.

## Dates et montants

Aucun refactor global. Les contrats vérifient la STRUCTURE (calendaire réelle, numérique fini),
sans changer la représentation interne (`str` pour les dates, `float` pour les montants) ni migrer
vers `date`/`Decimal`.

## Performance

Chaque contrat est appelé une fois par écriture (création/modification), jamais en boucle sur un
DataFrame massif — impact négligeable (déjà beaucoup plus de travail par ligne dans les services
concernés : hash, parsing, requêtes SQLite).

## Erreurs

Chaque violation produit un refus structuré (`{"ok": False, "code": ..., "message": ...}`),
jamais une exception brute ni un `Traceback` — cohérent avec le pattern déjà en place dans chaque
service (`_refus(code, message)`).

## Tests

Nouveaux : 10 (`tests/test_contrats_donnees_branches.py`) — refus canonique avant écriture (5
domaines), anomalie RAW banque non bloquée (stop-gate), import réel ne déclenche jamais le
contrat. `tests/test_contrats_donnees.py` (existant, contrat `ReservationHH` adapté à l'ajustement
`montant_retenu` optionnel — les tests existants restent inchangés, aucun ne testait ce champ
comme obligatoire).

## Migration

Aucune — contrats applicatifs uniquement, aucune contrainte SQLite ajoutée.

## Limites

- Aucun contrat de facture propriétaire distinct créé (hors mandat, aucun gap trouvé au-delà de
  la correction directe du `except` muet).
- `MouvementBanque` n'est branché qu'à l'auto-contrôle de l'import — pas comme contrainte
  générale sur toute écriture dans `banque_mouvements` (correctement, pour ne pas répéter
  l'erreur du CHECK SQLite retiré).

## Prochaine action

Aucune décidée par cette mission. STOP explicite — ne pas commencer le durcissement SQLite sans
nouvelle mission.
