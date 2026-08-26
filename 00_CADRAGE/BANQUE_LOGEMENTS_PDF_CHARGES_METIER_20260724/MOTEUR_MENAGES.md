# Moteur Ménages pur — Mission 9 (2026-08-26)

## Audit préalable — verdict : rien à extraire, déjà pur et déjà unique source

Contrairement à Commission (Mission 7, formules dupliquées à supprimer) et même à Charges
(Mission 8, relocalisation utile mais sans changement de logique), l'audit de la chaîne Ménages
montre que **la logique économique interne est déjà isolée dans un module pur, à son emplacement
canonique correct**, et que **les ménages externes n'ont aucune formule économique du tout**.

| Étape | Code actuel | Input | Output | Technique / métier |
|---|---|---|---|---|
| Résolution taux horaire (legacy, avant 2026-06-01) | `lib_menage_costs.resolve_hourly_rate` | intervenant, date | taux €/h historisé | MÉTIER (historique, encore utilisé — voir §Paramètres) |
| Résolution coût fixe (actuel, depuis 2026-06-01) | `lib_menage_costs.resolve_fixed_internal_cost` | intervenant, logement, type logement, date, nb ménages | coût total = tarif unitaire × nb ménages | MÉTIER — règle canonique |
| Dispatch heures/fixe par pivot de date | `lib_menage_costs.resolve_internal_cleaning_cost` | date économique | délègue à l'un des deux ci-dessus | MÉTIER (bascule calendaire, `PIVOT_FIXED_COST = 2026-06-01`, jamais de repli croisé) |
| Dette réelle par ménage validé (production) | `intervenant_menage_compte_service.tarif_menage`/`generer_dettes` | 1 ménage interne VALIDÉ à la fois | 1 dette = 1 tarif résolu (`nb_menages=1`) | MÉTIER — consomme le moteur, ne le duplique pas |
| Vue analytique coût complet (Lot6f) | `lot6f_cout_complet_menages.py` | lignes ménage (nb_menages, nb_heures, dates) | coût standard + coût complet + écart | MÉTIER — consomme `resolve_internal_cleaning_cost`, aucune formule dupliquée |
| Montant ménage externe | `lot6c_menages_externes.py` (ligne facture PDF) | ligne de facture prestataire | `montant = montant_ligne_ttc` | **TECHNIQUE PUR** — aucune formule, pass-through de la valeur facturée |

## Emplacement du moteur — décision : AUCUNE relocalisation

`lib_menage_costs.py` reste dans `02_TRAVAIL`, son emplacement actuel. Décision documentée (§11 de
la mission demande explicitement de documenter ce choix) :

- le module est déjà pur (0 sqlite3/FastAPI/pandas, ni lui-même ni sa seule dépendance
  `lib_ref_history.py`) — vérifié par test dédié ;
- il est déjà consommé identiquement par **les deux mondes** du projet : `lot6f_cout_complet_
  menages.py` (monde pandas, `Program Files\Python312`) l'importe directement ; `app/services/
  intervenant_menage_compte_service.py` (monde FastAPI, miniconda) l'importe via un
  `sys.path.insert` déjà établi vers `02_TRAVAIL` (voir son propre docstring, lignes 37-44) ;
- le relocaliser vers `app/moteurs/` (comme pour Charges, Mission 8) inverserait la direction de
  dépendance que le projet évite explicitement (`lib_db_moteur.py` : ne jamais faire dépendre
  `02_TRAVAIL` de `05_APPLICATION`) — `lot6f` a besoin de continuer à l'importer directement, sans
  passer par le paquet `app`.

Conclusion : **`02_TRAVAIL` est déjà le bon emplacement**, choisi précisément pour éviter la
dépendance circulaire que la mission demande d'éviter (§11). Aucun code déplacé.

## Ménage interne — règle canonique confirmée

**PRESTATIONS VALIDÉES × TARIF STANDARD APPLICABLE**, jamais heures × taux, pour le mécanisme réel
de paiement (dette intervenant, `intervenant_menage_compte_service.py`, migration 0029+). Confirmé
par lecture du code : `generer_dettes` crée UNE dette PAR ménage interne VALIDÉ
(`WHERE type_menage='INTERNE' AND statut='VALIDE'`), chacune tarifée individuellement via
`tarif_menage(..., nb_menages=1)` → `resolve_fixed_internal_cost`. Granularité confirmée (question
§20 de la mission tranchée par le code lui-même, pas devinée) : **une ligne par prestation**,
jamais une agrégation `nb × tarif` en une seule ligne — 3 ménages validés produisent 3 dettes
distinctes, chacune au tarif résolu à sa propre date.

Prestation non validée (§21) : jamais de dette créée (filtre SQL `statut='VALIDE'`) — exclue
silencieusement de `generer_dettes`, pas un statut `A_CONTROLER` (celui-ci est réservé à l'absence
de tarif résolu, cf. `A_CONTROLER_METIER`).

## Ménage externe — pure donnée d'import, aucun moteur inventé

Audit de `lot6c_menages_externes.py` (774 lignes) : le montant retenu pour une prestation externe
est **directement `montant_ligne_ttc`**, la valeur de la ligne de facture PDF/prestataire, jamais
recalculée à partir de `quantité × prix_unitaire` (ces deux champs sont extraits/conservés comme
métadonnées de la ligne, mais la valeur économique retenue est le TTC facturé tel quel). Conforme
à la mission (§4/§15) : **NE PAS créer de faux moteur** pour une donnée qui n'a aucune formule
économique — documentée ici comme TECHNIQUE (extraction + normalisation + réconciliation), pas
MÉTIER. Aucun code créé pour les ménages externes ; aucun test PDF fabriqué (mission §31 : ne pas
toucher/reconstruire l'extracteur PDF réel, hors mandat).

## Date économique

Confirmée sans contradiction : date de réalisation du ménage (`date_realisation`, ou `date_prevue`
en repli — cf. `intervenant_menage_compte_service.generer_dettes`), résolue AVANT tout appel au
moteur. Aucune date concurrente trouvée ; aucun arbitrage nécessaire.

## Coût standard historisé et fail-closed

Résolution par date économique + hiérarchie de priorité (intervenant+logement > logement >
intervenant+type > type > global), déjà entièrement testée (`tests/test_menage_costs.py`,
9 tests préexistants : pivot, hiérarchie, ambiguïté, absence). Fail-closed confirmé : absence de
tarif applicable → statut `MISSING`, jamais un repli vers un tarif actuel ou une valeur inventée
(`tarif_menage` : « Ne devine rien : MISSING/AMBIGUOUS remontés tels quels »).

## Paramètre `TAUX_HORAIRE_MENAGE_INTERNE`

**Encore utilisé économiquement**, mais uniquement par le chemin analytique historique
(`resolve_hourly_rate`, appelé par `resolve_internal_cleaning_cost` pour toute date antérieure au
`PIVOT_FIXED_COST` du 2026-06-01) — ce chemin sert la vue analytique gain/perte de `lot6f`, PAS le
mécanisme réel de paiement des intervenants (qui n'a jamais utilisé les heures, cf. règle
canonique ci-dessus, confirmée par le docstring même du service). Ni réintroduit dans le nouveau
mécanisme, ni supprimé — traité exactement comme avant cette mission.

## Arrondis

Identiques, non modifiés : `round(unit * count, 2)` pour le coût fixe, `round(hours * rate, 2)`
pour le legacy horaire — aucun changement, aucun arrondi nouveau introduit.

## Preuve A/B

`tests/test_menage_costs_moteur_pur.py` (nouveau, 6 tests, moteur pur isolé) : pureté du module +
temporalité (tarif 2027 de fixture ne modifie jamais 2026, même rejoué après coup) + correction
rétroactive volontaire (change 2026 exprès).

`05_APPLICATION/tests/test_menages_engine_moteur_pur.py` (nouveau, 2 tests, SQLite réelle via
`tmp_db`) : `intervenant_menage_compte_service.tarif_menage` (chaîne de production) comparé à un
appel direct du moteur pur avec les mêmes lignes — 0 diff. Temporalité re-testée au niveau SQLite
réel (tarif 2027 inséré dans `ref_couts_menage_interne` : 2026 reste inchangé, même rejoué après
l'insertion).

Aucune ligne manquante/supplémentaire possible ici : il n'y a jamais eu deux implémentations
distinctes à comparer — le service de production APPELLE directement la même fonction que le test
direct, la preuve A/B confirme cette architecture (pas un changement qu'elle introduit).

## Tests

Nouveaux : 5 (`test_menage_costs_moteur_pur.py`) + 2 (`test_menages_engine_moteur_pur.py`) = 7.
Aucun test existant modifié. Existants inchangés et tous verts : `test_menage_costs.py` (9 tests),
`test_intervenant_menage_compte.py` (9 tests), `test_menages_pivot_historique.py` (9 tests).

## Migration

Aucune — aucun besoin de stockage nouveau, aucune table/colonne changée.

## Limites

- Ménage externe : documenté comme pure donnée d'import (aucun moteur créé), pas testé via un
  fixture PDF complet — hors mandat de cette mission (§31), et sans formule économique à
  caractériser de toute façon.
- Aucun code de production modifié cette mission (seulement de nouveaux tests + documentation) —
  résultat honnête d'un audit qui a trouvé le moteur déjà correctement isolé.

## Prochaine action

Aucune décidée par cette mission. STOP explicite — ne pas commencer une autre extraction de
moteur sans nouvelle mission (décision réservée à un audit des moteurs métier restants).
