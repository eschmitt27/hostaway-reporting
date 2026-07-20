# APP-4B — Contrôle & catégorisation bancaire sur copies — 2026-07-17

Base : worktree isolé restauré depuis le checkpoint d'intégration (71 fichiers). Aucun commit.
Aucune écriture bancaire réelle. Flags `BANQUE_REAL_WRITE_ENABLED` / `_CONFIRMATION` = False.

## Architecture bancaire (audit)
- **Source de vérité** : `02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx`. Onglets : `BRUT_Banque`
  (import), `NORM_Banque` (mouvements normalisés — categorie/type_flux_id/statut_controle),
  `CTRL_A_CONTROLER`, `IA_Classification` (proposition IA), `RAPPROCH_AIRBNB_ATTENTE`,
  `RAPPROCH_PROPRIETAIRES_ATTENTE`, `CTRL_RAPPROCHEMENT_8C`, `REF_Cloture_Mensuelle`, `LOG_Traitement`.
- Scripts : `lot8a` (import) → `lot8b` (règles/classification) → `lot8c` (rapprochement).
- Clé de mouvement : `mouvement_id` (contenait le compte : `MVT-CM_02211_00021321603-…`).

## Modèle retenu (Excel = vérité, SQLite = journal)
- **SQLite ne fait que journaliser** les décisions applicatives (migration 0006 : `banque_overrides`,
  `banque_controle_runs`). Jamais une nouvelle vérité, jamais un masquage d'anomalie moteur.
- **Override écrit dans un onglet DÉDIÉ** `OVERRIDE_APP4B` de la COPIE (onglets moteur préservés) et
  appliqué sur une COPIE de `NORM_Banque`. Aucune écriture réelle.

## Identifiant opaque
`MVT-<sha256(sel + mouvement_id)[:12]>` — stable, unique, sans compte/IBAN/référence. Correspondance
interne préservée (`index_opaque`). Les URL publiques n'exposent aucune donnée bancaire.

## Workflow (statuts + transitions)
À contrôler → En cours → Contrôlé → Rapproché ; À contrôler/En cours/Contrôlé → Ignoré (justification
obligatoire) ; Rapproché/Ignoré → Rouvert → En cours/À contrôler. Statut inconnu / transition interdite
/ entité inconnue / conflit de version → refus lisible (jamais 500).

## Rattachements (référentiels lisibles)
Propriétaire « Prénom NOM », logement « nom officiel », type de flux (REF_Types_Flux), catégorie
(libellé dérivé) ; valeurs techniques conservées ; replis explicites (Propriétaire/Logement/Réservation/
Facture non identifié — <id>). Aucun rapprochement approximatif silencieux.

## Writer sur copies
Refuse le réel (flags False). Copie `BANQUE_LOT8_IMPORT.xlsx` dans un workspace isolé sous data/,
snapshot, écrit `OVERRIDE_APP4B` + applique sur `NORM_Banque` (copie), vérifie SHA256 du réel
inchangé, journalise (avant/après). Transactionnel : toute erreur → run ECHEC lisible.

## Impact contrôles
L'application sur `NORM_Banque` (copie) fait **évoluer réellement** les statuts (source des contrôles
`CTRL_A_CONTROLER`) : le run expose le comptage avant/après. L'anomalie n'est jamais masquée dans
SQLite. La relance moteur complète `lot8c`/`lot11` sur copie est le niveau supérieur (même mécanisme
runner que la chaîne ménages) — voir Limites.

## Écrans
`/banques-caisse/controle` (liste + filtres), `/banques-caisse/mouvements/{id_opaque}` (fiche :
proposition moteur ET décision séparées + historique), `/…/modifier`, POST `/…/previsualiser`,
POST `/…/enregistrer-copie`, `/banques-caisse/actions/{run_id}` (impact). Aucune route réelle active.

## Limites
- Relance `lot8c`/`lot11` complète sur copie : non incluse (impact démontré par l'évolution des
  statuts `NORM_Banque`, source des contrôles). Extension possible via runner engine.
- Résolution fine réservation/facture : repli explicite (source réservations non câblée ici).
- Deux formats de compte connus ; masquage `CM ••••1603` conservé (APP-4A).
