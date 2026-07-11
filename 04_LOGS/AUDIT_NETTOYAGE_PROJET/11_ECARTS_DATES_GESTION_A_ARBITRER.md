# 11 - ?carts dates de gestion ? arbitrer

Date audit : 2026-06-28 22:47:59

Aucune modification, aucune correction de donn?es, aucun backup, aucun commit. Rapport uniquement.

## 1. Les deux ?carts mat?riels

### `APPARTEMENT_DIVERS` - Appartement divers (hors parc)

- logement_id : `APPARTEMENT_DIVERS`
- nom logement : Appartement divers (hors parc)
- proprietaire_id concern? : aucun dans historique (ligne absente)
- date_entree_gestion dans REF_Logements : (vide)
- date_sortie_gestion dans REF_Logements : (vide)
- date_debut dans REF_Gestion_Logements_Hist : aucune ligne
- date_fin dans REF_Gestion_Logements_Hist : aucune ligne
- statut_gestion historique : aucune ligne
- statut REF_Logements : actif=OUI, sur_hostaway=NON
- commentaire REF_Logements : Code technique hors parc - jamais utilise pour masquer un mauvais mapping
- r?servations ou flux potentiellement concern?s : aucune ligne locale identifi?e dans les sorties analys?es.
- scripts consommateurs concern?s :
  - `02_TRAVAIL/lot10_calculer_resultats.py` lit `date_entree_gestion` lignes 552, 586, 600, 622
  - `02_TRAVAIL/lot10_calculer_resultats.py` lit `date_sortie_gestion` lignes 551, 579
  - `02_TRAVAIL/lot11_controles_coherence.py` lit `date_entree_gestion` lignes 804, 816, 819
  - `02_TRAVAIL/lot13_export_powerbi.py` lit `date_entree_gestion` lignes 76
  - `02_TRAVAIL/lot13_export_powerbi.py` lit `date_sortie_gestion` lignes 76
  - `02_TRAVAIL/lot4bis_charger_reservations.py` lit `date_sortie_gestion` lignes 210, 225, 254, 256
  - `02_TRAVAIL/lot6c_menages_externes.py` lit `date_sortie_gestion` lignes 77
- risque m?tier concret : si ce code technique hors parc est utilis? par erreur, aucune p?riode/propri?taire officielle ne peut ?tre r?solue via l?historique ; risque de contr?le bloquant ou de mauvais contournement si un script retombe sur REF_Logements.
- hypoth?se sur la valeur correcte : probablement code technique hors parc ? exclure de la gestion historis?e, mais validation humaine requise ; ne pas cr?er de p?riode/propri?taire automatiquement.

### `LOGEMENT_DIVERS` - Logement divers (hors parc)

- logement_id : `LOGEMENT_DIVERS`
- nom logement : Logement divers (hors parc)
- proprietaire_id concern? : aucun dans historique (ligne absente)
- date_entree_gestion dans REF_Logements : (vide)
- date_sortie_gestion dans REF_Logements : (vide)
- date_debut dans REF_Gestion_Logements_Hist : aucune ligne
- date_fin dans REF_Gestion_Logements_Hist : aucune ligne
- statut_gestion historique : aucune ligne
- statut REF_Logements : actif=OUI, sur_hostaway=NON
- commentaire REF_Logements : Code technique hors parc - jamais utilise pour masquer un mauvais mapping
- r?servations ou flux potentiellement concern?s : aucune ligne locale identifi?e dans les sorties analys?es.
- scripts consommateurs concern?s :
  - `02_TRAVAIL/lot10_calculer_resultats.py` lit `date_entree_gestion` lignes 552, 586, 600, 622
  - `02_TRAVAIL/lot10_calculer_resultats.py` lit `date_sortie_gestion` lignes 551, 579
  - `02_TRAVAIL/lot11_controles_coherence.py` lit `date_entree_gestion` lignes 804, 816, 819
  - `02_TRAVAIL/lot13_export_powerbi.py` lit `date_entree_gestion` lignes 76
  - `02_TRAVAIL/lot13_export_powerbi.py` lit `date_sortie_gestion` lignes 76
  - `02_TRAVAIL/lot4bis_charger_reservations.py` lit `date_sortie_gestion` lignes 210, 225, 254, 256
  - `02_TRAVAIL/lot6c_menages_externes.py` lit `date_sortie_gestion` lignes 77
- risque m?tier concret : si ce code technique hors parc est utilis? par erreur, aucune p?riode/propri?taire officielle ne peut ?tre r?solue via l?historique ; risque de contr?le bloquant ou de mauvais contournement si un script retombe sur REF_Logements.
- hypoth?se sur la valeur correcte : probablement code technique hors parc ? exclure de la gestion historis?e, mais validation humaine requise ; ne pas cr?er de p?riode/propri?taire automatiquement.

## 2. Les r?f?rences Excel actives

| Fichier | Feuille | Cellule/objet | Type | Lecture r?elle de la date | Bloquante suppression | Action future requise | Valeur |
|---|---|---|---|---|---|---|---|
| `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm` | REF_Logements | `H1` | table Excel | oui | oui | Adapter l?en-t?te/table avant suppression. | `date_entree_gestion` |
| `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm` | REF_Logements | `I1` | table Excel | oui | oui | Adapter l?en-t?te/table avant suppression. | `date_sortie_gestion` |
| `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm` | REF_Logements | `table tbl_REF_Logements colonne date_entree_gestion` | table Excel | oui | oui | Adapter la table avant suppression. | `date_entree_gestion` |
| `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm` | REF_Logements | `table tbl_REF_Logements colonne date_sortie_gestion` | table Excel | oui | oui | Adapter la table avant suppression. | `date_sortie_gestion` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `G6` | autre | non | non | Qualifier avant action. | `Logement LOG_0001: premier mois Flux=2025-07 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `L6` | autre | non | non | Qualifier avant action. | `INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier mois de flux reel ; REF non modifie, date contractuelle a co` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `G7` | autre | non | non | Qualifier avant action. | `Logement LOG_0002: premier mois Flux=2025-02 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `L7` | autre | non | non | Qualifier avant action. | `INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier mois de flux reel ; REF non modifie, date contractuelle a co` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `G8` | autre | non | non | Qualifier avant action. | `Logement LOG_0003: premier mois Flux=2025-02 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `L8` | autre | non | non | Qualifier avant action. | `INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier mois de flux reel ; REF non modifie, date contractuelle a co` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `G9` | autre | non | non | Qualifier avant action. | `Logement LOG_0004: premier mois Flux=2025-02 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `L9` | autre | non | non | Qualifier avant action. | `INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier mois de flux reel ; REF non modifie, date contractuelle a co` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `G10` | autre | non | non | Qualifier avant action. | `Logement LOG_0006: premier mois Flux=2025-02 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `L10` | autre | non | non | Qualifier avant action. | `INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier mois de flux reel ; REF non modifie, date contractuelle a co` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `G11` | autre | non | non | Qualifier avant action. | `Logement LOG_0007: premier mois Flux=2025-03 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `L11` | autre | non | non | Qualifier avant action. | `INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier mois de flux reel ; REF non modifie, date contractuelle a co` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `G12` | autre | non | non | Qualifier avant action. | `Logement LOG_0008: premier mois Flux=2025-02 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `L12` | autre | non | non | Qualifier avant action. | `INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier mois de flux reel ; REF non modifie, date contractuelle a co` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `G13` | autre | non | non | Qualifier avant action. | `Logement LOG_0010: premier mois Flux=2025-01 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `L13` | autre | non | non | Qualifier avant action. | `INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier mois de flux reel ; REF non modifie, date contractuelle a co` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `G14` | autre | non | non | Qualifier avant action. | `Logement LOG_0011: premier mois Flux=2025-01 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `L14` | autre | non | non | Qualifier avant action. | `INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier mois de flux reel ; REF non modifie, date contractuelle a co` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `G15` | autre | non | non | Qualifier avant action. | `Logement LOG_0012: premier mois Flux=2025-02 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `L15` | autre | non | non | Qualifier avant action. | `INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier mois de flux reel ; REF non modifie, date contractuelle a co` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `G16` | autre | non | non | Qualifier avant action. | `Logement LOG_0013: premier mois Flux=2025-01 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `L16` | autre | non | non | Qualifier avant action. | `INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier mois de flux reel ; REF non modifie, date contractuelle a co` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `G17` | autre | non | non | Qualifier avant action. | `Logement LOG_0014: premier mois Flux=2025-01 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `L17` | autre | non | non | Qualifier avant action. | `INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier mois de flux reel ; REF non modifie, date contractuelle a co` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `G18` | autre | non | non | Qualifier avant action. | `Logement LOG_0015: premier mois Flux=2025-02 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `L18` | autre | non | non | Qualifier avant action. | `INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier mois de flux reel ; REF non modifie, date contractuelle a co` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `G19` | autre | non | non | Qualifier avant action. | `Logement LOG_0016: premier mois Flux=2025-05 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.` |
| `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | MASTER | `L19` | autre | non | non | Qualifier avant action. | `INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier mois de flux reel ; REF non modifie, date contractuelle a co` |

## 3. Conclusion obligatoire

- ?carts n?cessitant validation humaine : 2
- r?f?rences Excel r?ellement bloquantes : 4
- r?f?rences explicatives seulement : 0
- pr?requis exacts avant OPTION_B_SUPPRIMER : arbitrer le statut des deux codes techniques hors parc, confirmer qu?ils ne doivent pas avoir de ligne historique, adapter scripts consommateurs, adapter tables/formules/Power Query/validations/noms d?finis bloquants, ajouter tests, sauvegarder REF_Setup avec macros, v?rifier keep_vba/vbaProject.bin/absence #REF!/scan r?f?rences.
- aucune modification effectu?e
