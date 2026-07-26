# 28 — Cycle de vie d'un logement existant

Suite de `AUDIT_CIBLE_FORFAITS_ET_TAUX_LOGEMENTS.md` (§ « ce qui peut être implémenté
immédiatement, sans arbitrage »). Complète `logements_creation_service.py` (création) par le
service `app/services/logements_gestion_service.py`.

## Fonctions livrées

| Fonction | Effet | Historisation |
|---|---|---|
| `modifier(logement_id, form)` | Champs descriptifs (nom, adresse, ville, type, forfait, hostaway) | Aucune — ne touche jamais `logement_id`/`proprietaire_id`/taux |
| `archiver(logement_id, date_fin)` | `actif=NON`, `statut_parc=RETIRE` | Clôture le rattachement `REF_Gestion_Logements_Hist` actif à `date_fin` |
| `reactiver(logement_id, date_debut, proprietaire_id)` | `actif=OUI`, `statut_parc=GERE` | Ouvre un **nouveau** rattachement (jamais de réouverture d'une ligne close) |
| `changer_proprietaire(logement_id, proprietaire_id, date_debut)` | Rattachement propriétaire | Clôture la ligne active à `date_debut − 1j`, ouvre une nouvelle ligne |
| `changer_taux_commission(logement_id, taux, date_debut, proprietaire_id="")` | `REF_Taux_Commission` | Clôture la ligne active (grain logement) à `date_debut − 1j`, ouvre une nouvelle ligne |

## Pourquoi le grain logement pour le taux ne pose plus problème

`resolve_commission_rate()` (`02_TRAVAIL/lib_ref_history.py:82`) priorise déjà une ligne à grain
**logement** (`logement_id` renseigné, priorité 2) sur une ligne à grain **propriétaire**
(`logement_id` vide, priorité 1). L'arbitrage soulevé dans l'audit ciblé (§3.2) est donc résolu par
lecture du code : `changer_taux_commission()` peut écrire au grain logement sans modification du
moteur Lot10, et sans risquer d'ambiguïté avec d'éventuelles lignes réelles à grain propriétaire
(elles restent applicables aux logements qui n'ont pas encore de ligne dédiée).

## Règle commune : jamais de modification d'une ligne historique déjà close

Chaque fonction qui change une donnée historisée (gestion, taux) **clôture** la ligne active
(`date_fin = veille de la nouvelle date_debut`) puis **ajoute** une nouvelle ligne — jamais
d'update d'une ligne dont `date_fin` est déjà renseignée. Vérifié par test
(`test_changer_proprietaire_ne_modifie_jamais_la_ligne_close`,
`test_changer_taux_grain_logement_pas_de_regression_sur_taux_historique`).

## Sécurité

Même socle que `logements_creation_service.py` : flags `CHARGES_REAL_WRITE_*` requis, écriture
atomique via `_remplacer_fichier` (write-guard mode recette, refus fail-closed hors
`RECETTE_ROOT`). Aucune route HTTP câblée à ce tour — **service testé, pas encore exposé au
navigateur** (voir `29_ROUTES_A_CABLER.md`).

## Tests — `tests/test_logements_gestion.py` (19 tests, tous verts)
modifier (champs descriptifs, logement inconnu, type inconnu, flags désactivés), archiver
(désactive + clôture gestion, déjà archivé, date invalide), réactiver (réactive + nouveau
rattachement, déjà actif, propriétaire inconnu), changer_proprietaire (clôture + ouvre, jamais de
modification d'une ligne close, propriétaire inconnu/manquant), changer_taux_commission (clôture +
ouvre au grain logement, pas de régression sur taux historique, taux invalide, logement inconnu,
write-guard hors racine recette).

## Non fait ce tour

- Aucune route `/logements/{id}/...` ni template : le service est prêt à être câblé, l'UI reste à
  faire.
- Le forfait logiciel/consommables **historisé** (nouvelle feuille dédiée + lecture par
  `build_charge_fixe()` côté Lot10) reste hors périmètre — c'est une modification du moteur Lot10,
  signalée comme non prise à la légère dans l'audit ciblé.
