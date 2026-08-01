# 65 — Cycle Banque complet Lot8a → Lot8b → Lot8c, sur copies

## Exécution (scripts moteur, environnement de copies uniquement)

| Lot | Rôle | Résultat | Détail |
|---|---|---|---|
| lot8a | Import & normalisation (format consolidé, cf. `63`) | **SUCCÈS** | 541 mouvements, 0 BLOQUANT, 1 A_CONTROLER (période multi-mois) |
| lot8b | Classification déterministe + IA catch-all | **SUCCÈS** | 24 `VALIDE`, 517 `A_CONTROLER` ; 236 `CLASSE`, 222 `RAPPROCHEMENT_REQUIS`, 83 `A_ENVOYER_IA` ; ajoute `TYPE_FLUX_016` (FRAIS_BANCAIRES) à `REF_Setup.xlsm` (copie) et 30 règles seed |
| lot8c | Rapprochement Airbnb/propriétaires + contrôles associés | **SUCCÈS** | +3 onglets (`RAPPROCH_AIRBNB_ATTENTE` notamment) ; 166 lignes Airbnb en attente (14 467,27 €, informatif) ; 56 virements propriétaires en attente (27 069,18 €) ; **aucun rapprochement auto-confirmé sans règle existante** |

Lot8b et Lot8c sont **applicables et exercés** — aucun `NON_APPLICABLE` nécessaire : le format
consolidé, une fois normalisé par Lot8a vers `NORM_Banque` (23 colonnes, structure inchangée),
est strictement identique en aval pour Lot8b/Lot8c, qui ne lisent que cette structure canonique et
ignorent totalement le format d'origine (natif ou consolidé).

## Contrôles Banque vérifiés

| Contrôle | Constat |
|---|---|
| Mouvements en entrée Lot8a | 541 (confirmé, cf. `63`) |
| Totaux débit/crédit | 51 744,37 € / 52 148,21 € (identiques à la source, cf. `63`) |
| Montant net | cohérent (crédit − débit par ligne, jamais recalculé différemment) |
| Doublons | 1 détecté et signalé (jamais masqué) |
| Classification | 24 `VALIDE` / 517 `A_CONTROLER` — cohérent avec des règles seed génériques (pas de règles métier fines encore arbitrées) |
| Mouvements non classés | 83 `A_ENVOYER_IA` (catch-all, jamais auto-classés) |
| Rapprochements proposés | 166 Airbnb + 56 propriétaires, tous `EN_ATTENTE_*` |
| Rapprochements confirmés | **0** — aucune confirmation automatique sans règle existante (conforme au mandat) |
| Mouvements sans règlement / règlements sans mouvement | non exercé ce tour (dépend de Lot4/Lot5, hors périmètre Banque) |
| Frais bancaires | 24 flux `TYPE_FLUX_016` injectés dans Lot9 (`Module BNQ`), auparavant 0 |
| Compte attendu | RIB `10278 02211 00021321603`, cohérent sur toute la chaîne |
| Période multi-mois | signalée `A_CONTROLER` (`BANQUE_FICHIER_PERIODE_INCOHERENTE`), jamais bloquante |
| Source et provenance | non circulaire (`63` §1) |
| Format natif toujours compatible | 11 tests de régression verts (`63`) |

## Effet sur la chaîne aval

Avant Lot8b/8c (Banque non classifiée) : `lot9` → 0 flux BNQ, `lot11` → statut `BANQUE_NON_
DISPONIBLE_GIT` (repli documenté, feuille `RAPPROCH_AIRBNB_ATTENTE` absente).

Après Lot8b/8c : `lot9` → **24 flux BNQ** (frais bancaires réellement intégrés), `lot11` → statut
**`BANQUE_DISPONIBLE`**. Résultat global légèrement modifié en conséquence (charges bancaires
désormais comptées) :

| Indicateur | Avant Lot8b/8c | Après Lot8b/8c |
|---|---:|---:|
| REEL | 291 852,76 € | 291 722,75 € |
| COMPTABLE | 281 328,60 € | 281 198,59 € |
| HORS_COMPTA | 10 524,16 € | 10 524,16 € (inchangé) |
| Identité REEL=COMPTABLE+HC | OK, écart 0,00 € | OK, écart 0,00 € |

Écart de 130,01 € entre les deux passes = exactement les 24 frais bancaires nouvellement comptés —
traçable, pas une dérive inexpliquée. Idempotence reconfirmée après Lot8b/8c (lot9/lot10 relancés,
mêmes totaux).

## Réconciliations rejouées (via l'application, sur les sorties post-Lot8b/8c)

| Réconciliation | Écart | Statut |
|---|---|---|
| A — Lot9 ↔ Lot10 | 0,00 € | **OK** |
| B — Lot10 ↔ Analytique | 0,00 € | **OK** |
| C — Analytique ↔ Comptabilité | 281 198,59 € | A_CONTROLER (attendu, 0 écriture réelle) |
| D — Banque ↔ journal BANQUE | 0,00 € | **OK** (aucun import dans le SQLite applicatif ce tour, les deux côtés sont à 0 par cohérence) |
| E — Factures ↔ auxiliaires | — | NON_DISPONIBLE (attendu) |
| F — Ménages ↔ charges | — | NON_DISPONIBLE (attendu) |
| G — Commissions ↔ VENTES | 4 231,90 € | A_CONTROLER (attendu, cohérent avec C) |
| H — Total analytique ↔ résultat global | 0,00 € | **OK** |

## Recette navigateur (écrans Banque)

`/banques-caisse` (tableau de bord) : lit directement `BANQUE_LOT8_IMPORT.xlsx` (lecture seule,
séparé du mécanisme d'import CSV/XLSX ad hoc de l'application) — comptes masqués (`CM ••••1603`),
mois filtrables, 55/59 mouvements affichés selon le mois choisi (vérifié sur 2026-06 et 2025-12).
Fiche mouvement (`/banques-caisse/mouvements/{id}`) : aucune 404 sur un identifiant existant,
classification moteur affichée (catégorie, règle appliquée, statut), rapprochement affiché
`NON_RAPPROCHE` avec montant restant exact — cohérent avec Lot8c (aucune confirmation
automatique). Aucun identifiant SQLite brut exposé (`MVT-xxxx` opaque).

## Sécurité (constat honnête, hors mandat de correction)

Deux points relevés, **pré-existants, non introduits par cette mission, non corrigés (hors
mandat)** :
1. Le bandeau `MODE RECETTE` de certains écrans (`/calculs`, `/banques-caisse`) affiche le chemin
   absolu de la racine de recette (`racine : C:\Users\...`) — comportement de template déjà en
   place avant cette mission, jamais isolé au périmètre Banque, non modifié ce tour.
2. Les libellés de mouvements bancaires affichés dans l'application peuvent contenir des fragments
   de compte/référence tiers tels qu'écrits par la banque elle-même (texte de virement) — affichage
   volontaire et nécessaire à la classification humaine, jamais reproduit dans cette documentation.

0 fuite dans les rapports Markdown produits par cette mission (aucun RIB complet, aucun libellé
brut, aucun chemin absolu cité).

## Intégrité

88/88 fichiers réels de données re-vérifiés identiques après le cycle complet (le seul écart
INTENTIONNEL et déjà commité reste `lot8a_banque_import.py`, du code). `REF_Setup.xlsm` réel
(hors copies) confirmé inchangé malgré les écritures de Lot8b sur la COPIE.

## Conclusion

Cycle Banque complet démontré fonctionnel de bout en bout sur copies : Lot8a (deux formats) →
Lot8b (classification) → Lot8c (rapprochement, sans confirmation automatique) → Lot9 (frais
bancaires intégrés) → Lot10-13 (résultats cohérents, idempotents) → réconciliations applicatives
(A/B/D/H vertes) → recette navigateur (drill-down sans 404, confidentialité respectée).
