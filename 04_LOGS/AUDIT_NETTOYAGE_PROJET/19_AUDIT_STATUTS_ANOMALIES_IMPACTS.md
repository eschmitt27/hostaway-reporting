# 19 - Audit statuts, anomalies et impacts

Date: 2026-06-29

## Synthese

- Le modele cible est pertinent: il separe traitement, cause, gravite, impact facture et impacts resultat.
- Les vrais doublons probables sont `severity` vs `niveau_anomalie` sur l axe gravite, et `statut_controle` vs futur `statut_traitement` sur l axe traitement.
- `code_impact` et `impact_resultat_reel` / `impact_resultat_comptable` sont complementaires dans l etat actuel: `code_impact` porte IC/HC/HR, les impacts explicites sont plus directement exploitables dans les agregations.
- `impact_facture` est distinct des impacts resultat: il pilote la facturation Lot11/Lot12.
- `statut` est le plus risque: nom trop generique, valeurs libres et roles multiples selon les tables.

## statut

- Statut audit: **COMPATIBILITE**
- Axe metier couvert: Etat local heterogene / compatibilite
- Role metier reel: Nom generique utilise pour des vues de synthese, reglements, rapprochements et etats locaux. Role non unique; a eviter comme nouveau champ canonique.
- Fichiers / feuilles / valeurs presentes:
  - `01_SOURCES_BRUTES\ImputationsAirbnb\SAISIE_ImputationsAirbnb.xlsx` / `MASTER` / `statut`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm` / `REF_Statuts` / `statut`: Excel column; valeurs: A_CONTROLER (3), BLOQUANT (2), A_IMPORTER (1), IMPORTE (1), CONTROLE (1), CORRIGE (1), REJETE (1), ARCHIVE (1), NON_RAPPROCHE (1), RAPPROCHE_AUTO (1), RAPPROCHE_MANUEL (1), ECART_A_ANALYSER (1), OUVERTE (1), EN_COURS (1), CORRIGEE (1), IGNOREE_VALIDEE (1), A_EMETTRE (1), EMISE (1), PAYEE (1), IMPAYEE (1)
  - `02_TRAVAIL\Lot11_Controles\MASTER_CTRL_Coherence.xlsx` / `CAISSE_THEORIQUE` / `statut`: Excel column; valeurs: CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES (3), OK (2), HH/Charges/Acomptes vides. Solde = 0 attendu. Structure stable pour recalculs futurs. (1)
  - `02_TRAVAIL\Lot12_Factures\MASTER_FACT_Proprietaires.xlsx` / `CONTROLE_MENSUEL` / `statut`: Excel column; valeurs: RATTACHE_PROPRIETAIRE (270)
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_CTRL_HA_Anomalies.xlsx` / `data` / `statut`: Excel column; valeurs: OUVERT (55)
  - `02_TRAVAIL\Lot8_Banque\BANQUE_LOT8_IMPORT.xlsx` / `CTRL_RAPPROCHEMENT_8C` / `statut`: Excel column; valeurs: EN_ATTENTE (2), A_CONTROLER (2), INFORMATIF (1)
- Scripts producteurs / consommateurs detectes:
  - `02_TRAVAIL\lib_cloture.py` (producteur/consommateur, 6 occurrence(s)): L5: eviter les statuts libres et les validations automatiques de corrections.; L33: "statut_validation",; L64: statut = str(row.get("statut_validation") or "").strip().upper(); L65: if statut not in VALIDATION_ALLOWED:; L67: if statut == "VALIDE":
  - `02_TRAVAIL\lib_parc.py` (producteur/consommateur, 13 occurrence(s)): L9: def normalise_statut_parc(value):; L15: def _get_statut_parc(ref_row):; L19: return getter("statut_parc"); L22: def statut_parc_traitement(ref_row):; L23: statut = normalise_statut_parc(_get_statut_parc(ref_row))
  - `02_TRAVAIL\lib_settlements.py` (producteur/consommateur, 6 occurrence(s)): L24: "statut",; L39: "statut_controle",; L48: statut: str; L69: if str(row.get("statut") or "").strip().upper() not in {"VALIDE", "VALIDEE", "VALIDÉE"}:; L96: statut = str(row.get("statut_controle") or "").strip().upper()
  - `02_TRAVAIL\lot10_calculer_resultats.py` (producteur/consommateur, 21 occurrence(s)): L45: is_statut_parc_a_controler,; L263: invalid_mask = df_j["logement_id_eff"].map(lambda lid: is_statut_parc_a_controler(log_ref.get(lid))); L273: "statut_parc=HORS_PARC_TECHNIQUE - exclu commission/net/facture/flux proprietaire"; L275: else "statut_parc vide ou invalide - A_CONTROLER sans calcul economique"; L360: "reservation_id", "channel_type", "statut_calcul_payout",
  - `02_TRAVAIL\lot11_controles_coherence.py` (producteur/consommateur, 35 occurrence(s)): L40: from lib_parc import A_CONTROLER, STATUT_PARC_INVALIDE, is_gere, is_hors_parc_technique, is_statut_parc_a_controler; L146: "statut_resolution": "OUVERT",; L275: "statut_gestion", "source", "commentaire",; L313: for _, row in df_res[df_res["statut_controle"].astype(str) == "VALIDE"].iterrows():; L781: if is_statut_parc_a_controler(row_log):
  - `02_TRAVAIL\lot12_generer_factures.py` (producteur/consommateur, 23 occurrence(s)): L39: from lib_parc import A_CONTROLER, STATUT_PARC_INVALIDE, is_hors_parc_technique, is_statut_parc_a_controler; L135: "statut_banque": str(r.get("statut_mois_banque")),; L138: transverse = dash_idx.get("TRANSVERSE", {"bloquants": 0, "a_controler": 0, "ok": "NON", "statut_banque": "OUVERT"}); L180: "statut": "CONTROLE_GLOBAL_NON_AFFECTE" if log_id == SENTINEL_GLOBAL else "RATTACHE_PROPRIETAIRE",; L189: rec["statut"] = "EXCLU_HORS_PARC_TECHNIQUE"
  - `02_TRAVAIL\lot12_seed_donnees_fictives.py` (producteur/consommateur, 3 occurrence(s)): L22: - Lignes fictives en statut_controle = VALIDE (pour traverser le pipeline).; L43: "statut_controle": "VALIDE",; L165: "statut_controle": "A_CONTROLER",
  - `02_TRAVAIL\lot13_export_powerbi.py` (producteur/consommateur, 10 occurrence(s)): L48: "statut_controle","niveau_anomalie","code_anomalie"]),; L54: "origine_initiale","statut_controle","niveau_anomalie","code_anomalie"]),; L57: "nuits","channel_type","source_type","statut_calcul_payout","payout_calcule","menage_retenu","assiette_commission",; L68: "statut_ecart","statut_controle"]),; L71: "nb_menages_declares_externe","nb_menages_declares_interne_m04","total_menages_declares","ecart","statut_controle","code_controle"]),
  - `02_TRAVAIL\lot1_hostaway_extract.py` (documentation/usage, 38 occurrence(s)): L776: Returns 5-tuple: (payout, source_payout, statut_calcul_payout, menage_retenu, meta_dict); L908: "statut":          "OUVERT",; L918: "description", "statut", "date_detection", "ROW_HASH",; L1118: Extrait les tâches ménage. Retourne (rows_tasks, statut).; L1119: statut: "OK" | "INCOMPLETE" | "FAILED"
  - `02_TRAVAIL\lot4bis_charger_reservations.py` (producteur/consommateur, 36 occurrence(s)): L49: is_statut_parc_a_controler,; L150: if r.get("statut_controle") == "VALIDE"; L236: f"{logement_id} statut_parc=HORS_PARC_TECHNIQUE - exclu des traitements metier",; L238: if is_statut_parc_a_controler(log_row):; L243: f"{logement_id} statut_parc vide ou invalide - traitement A_CONTROLER",
  - `02_TRAVAIL\lot4quater_resoudre_source_reservations.py` (documentation/usage, 6 occurrence(s)): L12: - mois clôturé (REF_Cloture_Mensuelle.statut_mois == CLOTURE) = HIST_Reservations_Cloturees ;; L49: "statut_controle", "niveau_anomalie", "code_anomalie", "commentaire",; L92: if str(d.get("statut_mois") or "").strip().upper() == "CLOTURE" and d.get("mois"):; L154: "statut_controle": "A_CONTROLER",; L200: "statut_controle": h.get("statut_controle"),
  - `02_TRAVAIL\lot4ter_historiser_reservations_cloturees.py` (documentation/usage, 6 occurrence(s)): L12: - Mois clôturé = REF_Cloture_Mensuelle.statut_mois == "CLOTURE" UNIQUEMENT.; L72: "statut_controle", "niveau_anomalie", "code_anomalie",; L132: if str(d.get("statut_mois") or "").strip().upper() == "CLOTURE" and d.get("mois"):; L248: statut   = r.get("statut_controle"); L258: statut = "VALIDE"
  - `02_TRAVAIL\lot5_master_acomptes_proprietaires.py` (documentation/usage, 11 occurrence(s)): L56: ("statut_controle",           C_STATUT, 18),; L77: "facture_ref null ou vide ET statut_controle <> EXCLU_RESULTAT"),; L186: # DV : statut_controle (col O=15); L195: dv_stat.sqref = f"{get_column_letter(col['statut_controle'])}2:{get_column_letter(col['statut_controle'])}10000"; L339: value="[Power Query] Filtre appliqué : statut_controle = VALIDE")
  - `02_TRAVAIL\lot6a_cleaning_tasks_comptage.py` (documentation/usage, 27 occurrence(s)): L7: MASTER_ENRICHI : enrichi — logement, mois, statut_menage, contrôles (21 cols); L17: QM-L6a-02 — confirmed ≠ réalisé → statut_menage = 'prévu'; L18: QM-L6a-03 — pending → statut_menage = 'A_CONTROLER'; L238: "status", "statut_menage",; L241: "statut_controle", "niveau_anomalie", "code_anomalie",
  - `02_TRAVAIL\lot6b_m04_menages_internes.py` (documentation/usage, 9 occurrence(s)): L138: statut, code = ("VALIDE", ""); L139: if lid is None: statut, code = "A_CONTROLER", "LOGEMENT_NON_MAPPE"; L140: elif iid is None: statut, code = "A_CONTROLER", "INTERVENANT_NON_MAPPE"; L145: "statut_controle": statut, "code_controle": code, "source_url": url, "date_extraction": NOW,; L156: "cout_lavage_attribue","lavage_non_attribuable_mois","statut_controle","code_controle","source_url","date_extraction","ROW_HASH"]
  - `02_TRAVAIL\lot6c_menages_externes.py` (producteur/consommateur, 18 occurrence(s)): L200: "statut_controle","niveau_anomalie","code_anomalie","commentaire",; L284: statut, niveau, code_ano = "BLOQUANT","BLOQUANT", anomalies_b[0]; L287: statut, niveau, code_ano = "A_CONTROLER","A_CONTROLER", anomalies_a[0]; L291: statut, niveau, code_ano, extra = "VALIDE","INFO", date_info, ""; L317: statut, niveau, code_ano, commentaire,
  - `02_TRAVAIL\lot6d_rapprochement_menages.py` (documentation/usage, 21 occurrence(s)): L54: cloture = {str(d["mois"])[:7] for d in ref_clo if str(d.get("statut_mois")).upper() == "CLOTURE" and d.get("mois")}; L78: if d.get("statut_menage") != "réalisé":   # completed uniquement; L157: cnt_statut = collections.Counter(); L165: statut, code, comm = "VALIDE", "", ""; L168: statut, code, comm = "INFO", "TASK_NON_ASSIGNEE_HISTORIQUE_IGNOREE", "Tasks non assignées (historique), ignorées pour blocage"
  - `02_TRAVAIL\lot6e_gainperte_menages.py` (documentation/usage, 12 occurrence(s)): L98: statut_c, niveau, ctrl = "VALIDE", "INFO", code; L99: if lg is None: statut_c, niveau, ctrl = "A_CONTROLER", "A_CONTROLER", "LOGEMENT_NON_MAPPE"; L100: elif type_id is None: statut_c, niveau, ctrl = "A_CONTROLER", "A_CONTROLER", "TYPE_LOGEMENT_ABSENT"; L101: elif std_u is None: statut_c, niveau, ctrl = "A_CONTROLER", "A_CONTROLER", "COUT_STANDARD_ABSENT"; L104: ecart, statut_e = None, "NON_CALCULABLE"
  - `02_TRAVAIL\lot6f_cout_complet_menages.py` (documentation/usage, 5 occurrence(s)): L253: statut_e = "NON_CALCULABLE" if ecart is None else ("GAIN" if ecart > 0 else "PERTE" if ecart < 0 else "EQUILIBRE"); L255: statut_c, code = "A_CONTROLER", "COUT_INTERNE_A_CONTROLER"; L257: statut_c, code = ("A_CONTROLER", "COUT_STANDARD_ABSENT") if st is None else ("VALIDE", ""); L263: "statut_ecart": statut_e, "statut_controle": statut_c,; L281: "controle_cout_interne","statut_ecart","statut_controle","code_controle","commentaire"]
  - `02_TRAVAIL\lot7_ik_avantages.py` (documentation/usage, 11 occurrence(s)): L169: r = write_section(ws2, r, 'Valeurs statut_controle (Lot 7)',; L170: ['statut_controle', 'description'],; L183: ('BLOQUANT',    'Anomalie bloquante — statut_controle=A_CONTROLER obligatoire'),; L227: 'statut_controle',           # 14  VALIDE/A_CONTROLER/EXCLU_RESULTAT/A_VENTILER; L267: 'statut_controle',                       # 14  VALIDE/A_CONTROLER/EXCLU_RESULTAT/A_VENTILER
- Usage Excel / Power Query / Power BI:
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx::xl/worksheets/sheet3.xml`
  - `01_SOURCES_BRUTES\AirCover\SAISIE_AirCover.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\AjustementsPostCloture\SAISIE_Ajustements_PostCloture.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet3.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet4.xml`
  - `01_SOURCES_BRUTES\ImputationsAirbnb\SAISIE_ImputationsAirbnb.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/tables/table1.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/tables/table10.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/vbaProject.bin`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet10.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet20.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet21.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet23.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet26.xml`
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet3.xml`
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet4.xml`
- Doublon potentiel:
  - Oui, redondant ou ambigu selon contexte; devrait etre remplace par un nom d axe specialise dans les nouvelles tables.

## statut_traitement

- Statut audit: **CANONIQUE**
- Axe metier couvert: Etat de traitement
- Role metier reel: Cible pertinente pour l etat de traitement: VALIDE / A_CONTROLER / EXCLU_RESULTAT / A_VENTILER. Peu ou pas encore deploye.
- Fichiers / feuilles / valeurs presentes:
  - Aucune colonne Excel exacte detectee dans le perimetre lu.
- Scripts producteurs / consommateurs detectes:
  - Aucun usage texte detecte.
- Usage Excel / Power Query / Power BI:
  - Aucun signal XML/PQ detecte dans les classeurs lus.
- Doublon potentiel:
  - Non confirme; champ complementaire justifie dans l etat actuel.

## statut_controle

- Statut audit: **COMPATIBILITE**
- Axe metier couvert: Etat de traitement historique
- Role metier reel: Champ historique tres utilise dans les lots de production pour VALIDE/A_CONTROLER/BLOQUANT/EXCLU_RESULTAT. Recouvre l axe traitement mais nomme controle.
- Fichiers / feuilles / valeurs presentes:
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx` / `SAISIE` / `statut_controle`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\AirCover\SAISIE_AirCover.xlsx` / `MASTER` / `statut_controle`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx` / `SAISIE` / `statut_controle`: Excel column; valeurs: <VIDE> (500)
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx` / `SAISIE` / `statut_controle`: Excel column; valeurs: <VIDE> (499), VALIDE (1)
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_FACT_HA_CleaningTasks_Discovery.xlsx` / `MASTER_ENRICHI` / `statut_controle`: Excel column; valeurs: OK (461), A_CONTROLER (39)
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_FACT_HA_CleaningTasks_Discovery.xlsx` / `VUE_COMPTAGE` / `statut_controle`: Excel column; valeurs: OK (95)
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx` / `MASTER` / `statut_controle`: Excel column; valeurs: <VIDE> (1)
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx` / `VUE_MENAGE` / `statut_controle`: Excel column; valeurs: <VIDE> (1)
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx` / `MASTER` / `statut_controle`: Excel column; valeurs: VALIDE (1322), A_CONTROLER (58), EXCLU_RESULTAT (11)
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx` / `VUE_FLUX` / `statut_controle`: Excel column; valeurs: VALIDE (1322)
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx` / `MASTER` / `statut_controle`: Excel column; valeurs: VALIDE (1349), A_CONTROLER (31), EXCLU_RESULTAT (11)
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx` / `VUE_FLUX` / `statut_controle`: Excel column; valeurs: VALIDE (1349)
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` / `MASTER` / `statut_controle`: Excel column; valeurs: <VIDE> (1), VALIDE (1)
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` / `VUE_ACTIVE` / `statut_controle`: Excel column; valeurs: <VIDE> (1)
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx` / `MASTER` / `statut_controle`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx` / `VUE_ACTIVE` / `statut_controle`: Excel column; valeurs: <VIDE> (1)
  - `02_TRAVAIL\Lot6b_DeclarationsInternes\MASTER_NORM_Declarations_Internes.xlsx` / `MASTER_NORMALISE` / `statut_controle`: Excel column; valeurs: VALIDE (20)
  - `02_TRAVAIL\Lot6c_MenagesExternes\MASTER_FACT_MEN_MenagesExternes.xlsx` / `MASTER` / `statut_controle`: Excel column; valeurs: VALIDE (12), A_CONTROLER (1)
  - `02_TRAVAIL\Lot6c_MenagesExternes\MASTER_FACT_MEN_MenagesExternes.xlsx` / `VUE_ACTIVE` / `statut_controle`: Excel column; valeurs: VALIDE (12)
  - `02_TRAVAIL\Lot6d_Rapprochement_Menages\MASTER_CTRL_Rapprochement_Menages.xlsx` / `TABLEAU_COMPARAISON` / `statut_controle`: Excel column; valeurs: VALIDE (11), A_CONTROLER (6), INFO (1)
  - `02_TRAVAIL\Lot6d_Rapprochement_Menages\MASTER_CTRL_Rapprochement_Menages.xlsx` / `RESUME_APPARTEMENT` / `statut_controle`: Excel column; valeurs: VALIDE (8), A_CONTROLER (6)
  - `02_TRAVAIL\Lot6d_Rapprochement_Menages\MASTER_CTRL_Rapprochement_Menages.xlsx` / `RESUME_INTERVENANT` / `statut_controle`: Excel column; valeurs: A_CONTROLER (3), VALIDE (2)
  - `02_TRAVAIL\Lot6e_GainPerte_Menages\MASTER_CALC_GainPerte_Menages.xlsx` / `DETAIL_ECART_COUT` / `statut_controle`: Excel column; valeurs: VALIDE (16)
  - `02_TRAVAIL\Lot6f_CoutComplet_Menages\MASTER_CALC_CoutComplet_Menages.xlsx` / `DETAIL_COUT_COMPLET` / `statut_controle`: Excel column; valeurs: VALIDE (16)
  - `02_TRAVAIL\Lot7_IK_Avantages\MASTER_FACT_MAN_IK_Avantages.xlsx` / `MASTER_SAISIE` / `statut_controle`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot7_IK_Avantages\MASTER_FACT_MAN_IK_Avantages.xlsx` / `MASTER_CALC_AVANTAGES` / `statut_controle`: Excel column; valeurs: <VIDE> (1)
  - `02_TRAVAIL\Lot8_Banque\BANQUE_LOT8_IMPORT.xlsx` / `NORM_Banque` / `statut_controle`: Excel column; valeurs: A_CONTROLER (124), VALIDE (8)
  - `02_TRAVAIL\Lot8_Banque\BANQUE_LOT8_IMPORT.xlsx` / `CTRL_A_CONTROLER` / `statut_controle`: Excel column; valeurs: A_CONTROLER (17)
  - `02_TRAVAIL\Lot9_FluxUnifie\MASTER_CALC_Flux.xlsx` / `MASTER` / `statut_controle`: Excel column; valeurs: VALIDE (1393)
- Scripts producteurs / consommateurs detectes:
  - `02_TRAVAIL\lib_settlements.py` (producteur/consommateur, 2 occurrence(s)): L39: "statut_controle",; L96: statut = str(row.get("statut_controle") or "").strip().upper()
  - `02_TRAVAIL\lot10_calculer_resultats.py` (producteur/consommateur, 1 occurrence(s)): L741: dfa = dfa[dfa.get("statut_controle", "VALIDE").astype(str) == "VALIDE"]
  - `02_TRAVAIL\lot11_controles_coherence.py` (producteur/consommateur, 1 occurrence(s)): L313: for _, row in df_res[df_res["statut_controle"].astype(str) == "VALIDE"].iterrows():
  - `02_TRAVAIL\lot12_seed_donnees_fictives.py` (producteur/consommateur, 3 occurrence(s)): L22: - Lignes fictives en statut_controle = VALIDE (pour traverser le pipeline).; L43: "statut_controle": "VALIDE",; L165: "statut_controle": "A_CONTROLER",
  - `02_TRAVAIL\lot13_export_powerbi.py` (producteur/consommateur, 4 occurrence(s)): L48: "statut_controle","niveau_anomalie","code_anomalie"]),; L54: "origine_initiale","statut_controle","niveau_anomalie","code_anomalie"]),; L68: "statut_ecart","statut_controle"]),; L71: "nb_menages_declares_externe","nb_menages_declares_interne_m04","total_menages_declares","ecart","statut_controle","code_controle"]),
  - `02_TRAVAIL\lot4bis_charger_reservations.py` (producteur/consommateur, 10 occurrence(s)): L150: if r.get("statut_controle") == "VALIDE"; L266: code_impact, statut_controle, niveau_anomalie, code_anomalie, commentaire,; L277: if statut_controle == "EXCLU_RESULTAT":; L302: "statut_controle":         statut_controle,; L321: statut = hh.get("statut_controle") or "A_CONTROLER"
  - `02_TRAVAIL\lot4quater_resoudre_source_reservations.py` (documentation/usage, 4 occurrence(s)): L49: "statut_controle", "niveau_anomalie", "code_anomalie", "commentaire",; L154: "statut_controle": "A_CONTROLER",; L200: "statut_controle": h.get("statut_controle"),; L222: if r.get("statut_controle") == "VALIDE"
  - `02_TRAVAIL\lot4ter_historiser_reservations_cloturees.py` (documentation/usage, 2 occurrence(s)): L72: "statut_controle", "niveau_anomalie", "code_anomalie",; L248: statut   = r.get("statut_controle")
  - `02_TRAVAIL\lot5_master_acomptes_proprietaires.py` (documentation/usage, 11 occurrence(s)): L56: ("statut_controle",           C_STATUT, 18),; L77: "facture_ref null ou vide ET statut_controle <> EXCLU_RESULTAT"),; L186: # DV : statut_controle (col O=15); L195: dv_stat.sqref = f"{get_column_letter(col['statut_controle'])}2:{get_column_letter(col['statut_controle'])}10000"; L339: value="[Power Query] Filtre appliqué : statut_controle = VALIDE")
  - `02_TRAVAIL\lot6a_cleaning_tasks_comptage.py` (documentation/usage, 4 occurrence(s)): L241: "statut_controle", "niveau_anomalie", "code_anomalie",; L253: "statut_controle": _C_STATUT, "niveau_anomalie": _C_STATUT, "code_anomalie": _C_STATUT,; L262: "statut_controle", "niveau_anomalie", "code_anomalie",; L270: "statut_controle": _C_STATUT, "niveau_anomalie": _C_STATUT, "code_anomalie": _C_STATUT,
  - `02_TRAVAIL\lot6b_m04_menages_internes.py` (documentation/usage, 6 occurrence(s)): L145: "statut_controle": statut, "code_controle": code, "source_url": url, "date_extraction": NOW,; L156: "cout_lavage_attribue","lavage_non_attribuable_mois","statut_controle","code_controle","source_url","date_extraction","ROW_HASH"]; L201: statut_controle = d["statut_controle"]; L204: statut_controle = "A_CONTROLER"; L219: "statut_controle": statut_controle, "niveau_anomalie": ("INFO" if statut_controle == "VALIDE" else "A_CONTROLER"),
  - `02_TRAVAIL\lot6c_menages_externes.py` (producteur/consommateur, 11 occurrence(s)): L200: "statut_controle","niveau_anomalie","code_anomalie","commentaire",; L326: if r[MASTER_COLS.index("statut_controle")] == "VALIDE"; L367: if r[iM("statut_controle")] == "BLOQUANT":; L466: "risque_double_comptage":16,"statut_controle":14,"niveau_anomalie":13,; L473: statut_idx = MASTER_COLS.index("statut_controle")
  - `02_TRAVAIL\lot6d_rapprochement_menages.py` (documentation/usage, 8 occurrence(s)): L188: "statut_controle": statut, "code_controle": code, "commentaire": comm,; L213: "ecart","statut_controle","code_controle","commentaire"]; L219: "ecart_total":v[1]+v[2]-v[0],"statut_controle":"VALIDE" if v[1]+v[2]-v[0]==0 else "A_CONTROLER",; L222: "declares_internes_m04_total","total_declares","ecart_total","statut_controle","commentaire"], r2); L228: "statut_controle":"VALIDE" if v[1]+v[2]-v[0]==0 else "A_CONTROLER","commentaire":""} for k,v in sorted(res_int.items())]
  - `02_TRAVAIL\lot6e_gainperte_menages.py` (documentation/usage, 2 occurrence(s)): L121: "statut_controle": statut_c, "code_controle": ctrl, "commentaire": comm,; L199: "cout_reel_total","ecart_total","statut_ecart","statut_controle","code_controle","commentaire"]
  - `02_TRAVAIL\lot6f_cout_complet_menages.py` (documentation/usage, 2 occurrence(s)): L263: "statut_ecart": statut_e, "statut_controle": statut_c,; L281: "controle_cout_interne","statut_ecart","statut_controle","code_controle","commentaire"]
  - `02_TRAVAIL\lot7_ik_avantages.py` (documentation/usage, 11 occurrence(s)): L169: r = write_section(ws2, r, 'Valeurs statut_controle (Lot 7)',; L170: ['statut_controle', 'description'],; L183: ('BLOQUANT',    'Anomalie bloquante — statut_controle=A_CONTROLER obligatoire'),; L227: 'statut_controle',           # 14  VALIDE/A_CONTROLER/EXCLU_RESULTAT/A_VENTILER; L267: 'statut_controle',                       # 14  VALIDE/A_CONTROLER/EXCLU_RESULTAT/A_VENTILER
  - `02_TRAVAIL\lot8a_banque_import.py` (documentation/usage, 4 occurrence(s)): L299: # ── statut_controle ───────────────────────────────────────────────────; L332: statut,                      # 19 statut_controle; L436: 'statut_controle', 'niveau_risque',; L475: 'code_controle', 'severite', 'description', 'statut_controle',
  - `02_TRAVAIL\lot8b_banque_regles.py` (documentation/usage, 7 occurrence(s)): L84: #          niveau_risque, statut_controle_defaut, statut_classification_defaut,; L93: "niveau_risque", "statut_controle_defaut", "statut_classification_defaut",; L449: COL_STATUT_CTRL      = hdr["statut_controle"]        # 19; L490: statut_ctrl = rule["statut_controle_defaut"]; L542: # statut_controle: fill + value
  - `02_TRAVAIL\lot9_construire_flux.py` (documentation/usage, 15 occurrence(s)): L47: 'statut_controle', 'niveau_anomalie', 'code_anomalie', 'commentaire',; L121: men_valide = [r for r in men_all if r.get('statut_controle') == 'VALIDE']; L126: bnq_valide = [r for r in bnq_all if r.get('type_flux_id') == 'TYPE_FLUX_016' and r.get('statut_controle') == 'VALIDE']; L142: if r.get('statut_controle') == 'VALIDE'; L150: if r.get('statut_controle') == 'VALIDE'
  - `00_CADRAGE\ARCHITECTURE_DONNEES.md` (documentation/usage, 17 occurrence(s)): L485: | `statut_controle` | OK / à contrôler / bloquant |; L524: | `statut_controle` | Validé / à contrôler / bloquant |; L529: - Une réservation VRBO `paymentStatus = Unknown` : tant que le montant n'est pas renseigné manuellement, la ligne est `statut_controle = A_CONTROLER` et n'alimente pas `MASTER_CALC_Flux`.; L591: | `statut_controle` | OK / à contrôler / bloquant |; L631: | 15 | `statut_controle` | Statut | `VALIDE` / `A_CONTROLER` / `EXCLU_RESULTAT` |
- Usage Excel / Power Query / Power BI:
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx::xl/worksheets/sheet3.xml`
  - `01_SOURCES_BRUTES\AirCover\SAISIE_AirCover.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet3.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet4.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet10.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet23.xml`
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet3.xml`
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet4.xml`
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_FACT_HA_CleaningTasks_Discovery.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_FACT_HA_CleaningTasks_Discovery.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet1.xml`
- Doublon potentiel:
  - Oui, doublon de `statut_traitement` dans le modele cible; a migrer prudemment car tres consomme.

## code_anomalie

- Statut audit: **CANONIQUE**
- Axe metier couvert: Cause / code anomalie
- Role metier reel: Code technique/metier expliquant l anomalie. Complementaire du niveau; parfois decline en code_anomalie_lot10/code_controle.
- Fichiers / feuilles / valeurs presentes:
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx` / `SAISIE` / `code_anomalie`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx` / `SAISIE` / `code_anomalie`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx` / `SAISIE` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot12_Factures\MASTER_FACT_Proprietaires.xlsx` / `A_CONTROLER` / `code_anomalie`: Excel column; valeurs: RESERVATION_EXCLUE_A_CONTROLER (59)
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_CTRL_HA_Anomalies.xlsx` / `data` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_FACT_HA_CleaningTasks_Discovery.xlsx` / `MASTER_ENRICHI` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_FACT_HA_CleaningTasks_Discovery.xlsx` / `VUE_COMPTAGE` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx` / `MASTER` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx` / `VUE_MENAGE` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx` / `MASTER` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx` / `VUE_FLUX` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx` / `MASTER` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx` / `VUE_FLUX` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` / `MASTER` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` / `VUE_ACTIVE` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx` / `MASTER` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx` / `VUE_ACTIVE` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot6c_MenagesExternes\MASTER_FACT_MEN_MenagesExternes.xlsx` / `MASTER` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot6c_MenagesExternes\MASTER_FACT_MEN_MenagesExternes.xlsx` / `VUE_ACTIVE` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot7_IK_Avantages\MASTER_FACT_MAN_IK_Avantages.xlsx` / `MASTER_SAISIE` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot7_IK_Avantages\MASTER_FACT_MAN_IK_Avantages.xlsx` / `MASTER_CALC_AVANTAGES` / `code_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot9_FluxUnifie\MASTER_CALC_Flux.xlsx` / `MASTER` / `code_anomalie`: Excel column; valeurs: 
- Scripts producteurs / consommateurs detectes:
  - `02_TRAVAIL\lib_parc.py` (producteur/consommateur, 1 occurrence(s)): L29: def code_anomalie_statut_parc(ref_row):
  - `02_TRAVAIL\lot10_calculer_resultats.py` (producteur/consommateur, 17 occurrence(s)): L282: "code_anomalie_lot10": code,; L313: "code_anomalie": "TAUX_COMMISSION_HISTORIQUE_ABSENT",; L331: "code_anomalie": f"TAUX_COMMISSION_{res.status}",; L349: log.error(f"{c['niveau']} {c['code_anomalie']} - {c['message']}"); L452: "code_anomalie": "COMMISSION_HH_SAISIE_DIFFERE_RECALCUL",
  - `02_TRAVAIL\lot11_controles_coherence.py` (producteur/consommateur, 2 occurrence(s)): L838: if "code_anomalie" in df_ha_ano.columns:; L839: orphelins = df_ha_ano[df_ha_ano["code_anomalie"] == "LISTING_ORPHELIN_A_CONTROLER"]
  - `02_TRAVAIL\lot12_generer_factures.py` (producteur/consommateur, 4 occurrence(s)): L193: rec["code_anomalie"] = STATUT_PARC_INVALIDE; L335: "code_anomalie": r.get("code_anomalie_lot10") or "RESERVATION_A_CONTROLER",; L345: "code_anomalie": "MODE_FACTURATION_A_DEFINIR", "severite": "A_CONTROLER",; L357: "code_anomalie": "TROP_PERÇU / CRÉDIT À TRAITER",
  - `02_TRAVAIL\lot12_seed_donnees_fictives.py` (producteur/consommateur, 3 occurrence(s)): L9: - Tag obligatoire dans une colonne commentaire / code_anomalie :; L96: {  # 1 ménage interne fictif (tag dans code_anomalie : pas de colonne commentaire en M04); L105: "code_anomalie": TAG, "source_pk": "ZZ_TEST_MENAGE_INT_001",
  - `02_TRAVAIL\lot13_export_powerbi.py` (producteur/consommateur, 2 occurrence(s)): L48: "statut_controle","niveau_anomalie","code_anomalie"]),; L54: "origine_initiale","statut_controle","niveau_anomalie","code_anomalie"]),
  - `02_TRAVAIL\lot1_hostaway_extract.py` (documentation/usage, 3 occurrence(s)): L905: "code_anomalie":   code,; L915: return pd.DataFrame(self._rows).drop_duplicates(subset=["code_anomalie", "reservation_id"]); L917: "reservation_id", "code_anomalie", "severite",
  - `02_TRAVAIL\lot4bis_charger_reservations.py` (producteur/consommateur, 10 occurrence(s)): L26: code_anomalie=DIRECT_SANS_SAISIE_HH; L266: code_impact, statut_controle, niveau_anomalie, code_anomalie, commentaire,; L304: "code_anomalie":           code_anomalie,; L332: hh["code_anomalie"] = HORS_PARC_TECHNIQUE; L340: hh["code_anomalie"] = STATUT_PARC_INVALIDE
  - `02_TRAVAIL\lot4quater_resoudre_source_reservations.py` (documentation/usage, 3 occurrence(s)): L49: "statut_controle", "niveau_anomalie", "code_anomalie", "commentaire",; L156: "code_anomalie": "MOIS_CLOTURE_SANS_HISTORIQUE",; L202: "code_anomalie": h.get("code_anomalie"),
  - `02_TRAVAIL\lot4ter_historiser_reservations_cloturees.py` (documentation/usage, 2 occurrence(s)): L72: "statut_controle", "niveau_anomalie", "code_anomalie",; L249: code_ano = r.get("code_anomalie")
  - `02_TRAVAIL\lot5_master_acomptes_proprietaires.py` (documentation/usage, 7 occurrence(s)): L58: ("code_anomalie",             C_STATUT, 34),; L391: {{"code_anomalie",             type text}},; L520: each [code_anomalie],; L521: each if ([code_anomalie] = null or [code_anomalie] = ""); L522: then [_cod_pq] else [code_anomalie],
  - `02_TRAVAIL\lot6a_cleaning_tasks_comptage.py` (documentation/usage, 4 occurrence(s)): L241: "statut_controle", "niveau_anomalie", "code_anomalie",; L253: "statut_controle": _C_STATUT, "niveau_anomalie": _C_STATUT, "code_anomalie": _C_STATUT,; L262: "statut_controle", "niveau_anomalie", "code_anomalie",; L270: "statut_controle": _C_STATUT, "niveau_anomalie": _C_STATUT, "code_anomalie": _C_STATUT,
  - `02_TRAVAIL\lot6b_m04_menages_internes.py` (documentation/usage, 1 occurrence(s)): L220: "code_anomalie": code_controle, "source_module": "lot6b", "source_table": "SOURCE_RAW",
  - `02_TRAVAIL\lot6c_menages_externes.py` (producteur/consommateur, 2 occurrence(s)): L200: "statut_controle","niveau_anomalie","code_anomalie","commentaire",; L467: "code_anomalie":36,"commentaire":55,"source_module":16,
  - `02_TRAVAIL\lot7_ik_avantages.py` (documentation/usage, 8 occurrence(s)): L229: 'code_anomalie',             # 16  code erreur; L268: 'code_anomalie',                         # 15  code erreur; L380: {"_code_anomalie", "_niveau_anomalie"}),; L387: WithCA = Table.AddColumn(WithNA, "code_anomalie", each; L388: if [_code_anomalie] = "" then "" else [_code_anomalie]),
  - `02_TRAVAIL\lot9_construire_flux.py` (documentation/usage, 4 occurrence(s)): L47: 'statut_controle', 'niveau_anomalie', 'code_anomalie', 'commentaire',; L180: niveau_anomalie='INFO', code_anomalie=None):; L215: 'code_anomalie':                code_anomalie,; L489: 'statut_controle': 17, 'niveau_anomalie': 17, 'code_anomalie': 26, 'commentaire': 44,
  - `00_CADRAGE\ARCHITECTURE_DONNEES.md` (documentation/usage, 1 occurrence(s)): L633: | 17 | `code_anomalie` | Statut | Premier code détecté par priorité |
  - `00_CADRAGE\DECISIONS_METIER.md` (documentation/usage, 4 occurrence(s)): L408: `code_anomalie` : code technique du contrôle détecté (ex. `CHARGE_LOGEMENT_SANS_LOGEMENT_ID`); L492: Décision : `MASTER_CALC_Reservations` contient 24 colonnes (22 base + `niveau_anomalie` + `code_anomalie` en bloc statut, après `statut_controle` et avant `commentaire`).; L722: Décision : Option B — date_menage=null si absente sur facture. mois déduit depuis date_facture. precision_date_menage=MOIS_FACTURE. statut_controle=A_CONTROLER. code_anomalie=MENAGE_EXTERNE_DATE_ABSENTE. Interdiction abs; L727: Décision : Ligne Mounir T2-65 (Gabriel) à 0 ménage / 0€ (logement inactif depuis 2026-04-26) conservée dans MASTER pour traçabilité. statut_controle=A_CONTROLER, code_anomalie=MENAGE_EXTERNE_MONTANT_NUL + MENAGE_EXTERNE_
  - `00_CADRAGE\ETAT_AVANCEMENT.md` (documentation/usage, 1 occurrence(s)): L464: - D053 : MASTER_CALC_Reservations 24 cols ; source 7 valeurs ; source_montant 5 valeurs ; +niveau_anomalie +code_anomalie ; contrôle RESERVATION_HH_NON_VALIDE — VERROUILLÉ (QM-L4b-02)
  - `00_CADRAGE\JOURNAL_CONTROLES.md` (documentation/usage, 1 occurrence(s)): L779: + 3 impact + 4 statut (incl. niveau_anomalie + code_anomalie) + 4 système PQ.
- Usage Excel / Power Query / Power BI:
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot10_Resultats\MASTER_CALC_Commissions.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot12_Factures\MASTER_FACT_Proprietaires.xlsx::xl/worksheets/sheet5.xml`
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_CTRL_HA_Anomalies.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_FACT_HA_CleaningTasks_Discovery.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_FACT_HA_CleaningTasks_Discovery.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx::xl/worksheets/sheet1.xml`
- Doublon potentiel:
  - Non confirme; champ complementaire justifie dans l etat actuel.

## niveau_anomalie

- Statut audit: **CANONIQUE**
- Axe metier couvert: Gravite
- Role metier reel: Gravite de l anomalie dans les tables de reservations/menages: INFO/A_CONTROLER/BLOQUANT. A normaliser vers INFO/WARNING/BLOQUANT si decision humaine.
- Fichiers / feuilles / valeurs presentes:
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx` / `SAISIE` / `niveau_anomalie`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx` / `SAISIE` / `niveau_anomalie`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx` / `SAISIE` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_FACT_HA_CleaningTasks_Discovery.xlsx` / `MASTER_ENRICHI` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_FACT_HA_CleaningTasks_Discovery.xlsx` / `VUE_COMPTAGE` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx` / `MASTER` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx` / `VUE_MENAGE` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx` / `MASTER` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx` / `VUE_FLUX` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx` / `MASTER` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx` / `VUE_FLUX` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` / `MASTER` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` / `VUE_ACTIVE` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx` / `MASTER` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx` / `VUE_ACTIVE` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot6c_MenagesExternes\MASTER_FACT_MEN_MenagesExternes.xlsx` / `MASTER` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot6c_MenagesExternes\MASTER_FACT_MEN_MenagesExternes.xlsx` / `VUE_ACTIVE` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot7_IK_Avantages\MASTER_FACT_MAN_IK_Avantages.xlsx` / `MASTER_SAISIE` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot8_Banque\BANQUE_LOT8_IMPORT.xlsx` / `NORM_Banque` / `niveau_anomalie`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot9_FluxUnifie\MASTER_CALC_Flux.xlsx` / `MASTER` / `niveau_anomalie`: Excel column; valeurs: 
- Scripts producteurs / consommateurs detectes:
  - `02_TRAVAIL\lot12_seed_donnees_fictives.py` (producteur/consommateur, 1 occurrence(s)): L44: "niveau_anomalie": "INFO",
  - `02_TRAVAIL\lot13_export_powerbi.py` (producteur/consommateur, 2 occurrence(s)): L48: "statut_controle","niveau_anomalie","code_anomalie"]),; L54: "origine_initiale","statut_controle","niveau_anomalie","code_anomalie"]),
  - `02_TRAVAIL\lot4bis_charger_reservations.py` (producteur/consommateur, 5 occurrence(s)): L266: code_impact, statut_controle, niveau_anomalie, code_anomalie, commentaire,; L303: "niveau_anomalie":         niveau_anomalie,; L322: niveau = hh.get("niveau_anomalie") or "INFO"; L389: "niveau_anomalie":         niveau,; L591: "statut_controle", "niveau_anomalie", "code_anomalie", "commentaire",
  - `02_TRAVAIL\lot4quater_resoudre_source_reservations.py` (documentation/usage, 3 occurrence(s)): L49: "statut_controle", "niveau_anomalie", "code_anomalie", "commentaire",; L155: "niveau_anomalie": "A_CONTROLER",; L201: "niveau_anomalie": h.get("niveau_anomalie"),
  - `02_TRAVAIL\lot4ter_historiser_reservations_cloturees.py` (documentation/usage, 2 occurrence(s)): L72: "statut_controle", "niveau_anomalie", "code_anomalie",; L270: statut, r.get("niveau_anomalie"), code_ano,
  - `02_TRAVAIL\lot5_master_acomptes_proprietaires.py` (documentation/usage, 7 occurrence(s)): L57: ("niveau_anomalie",           C_STATUT, 16),; L390: {{"niveau_anomalie",           type text}},; L515: each [niveau_anomalie],; L516: each if ([niveau_anomalie] = null or [niveau_anomalie] = ""); L517: then [_niv_pq] else [niveau_anomalie],
  - `02_TRAVAIL\lot6a_cleaning_tasks_comptage.py` (documentation/usage, 7 occurrence(s)): L241: "statut_controle", "niveau_anomalie", "code_anomalie",; L253: "statut_controle": _C_STATUT, "niveau_anomalie": _C_STATUT, "code_anomalie": _C_STATUT,; L262: "statut_controle", "niveau_anomalie", "code_anomalie",; L270: "statut_controle": _C_STATUT, "niveau_anomalie": _C_STATUT, "code_anomalie": _C_STATUT,; L598: n_blq = sum(1 for r in enrichi_rows if r[I["niveau_anomalie"]] == "BLOQUANT")
  - `02_TRAVAIL\lot6b_m04_menages_internes.py` (documentation/usage, 1 occurrence(s)): L219: "statut_controle": statut_controle, "niveau_anomalie": ("INFO" if statut_controle == "VALIDE" else "A_CONTROLER"),
  - `02_TRAVAIL\lot6c_menages_externes.py` (producteur/consommateur, 5 occurrence(s)): L200: "statut_controle","niveau_anomalie","code_anomalie","commentaire",; L327: and r[MASTER_COLS.index("niveau_anomalie")] in ("","INFO"); L466: "risque_double_comptage":16,"statut_controle":14,"niveau_anomalie":13,; L577: and ([niveau_anomalie]    = "" or [niveau_anomalie] = "INFO"); L666: "  AND niveau_anomalie IN ('','INFO')",
  - `02_TRAVAIL\lot7_ik_avantages.py` (documentation/usage, 11 occurrence(s)): L178: r = write_section(ws2, r, 'Valeurs niveau_anomalie',; L179: ['niveau_anomalie', 'description'],; L228: 'niveau_anomalie',           # 15  INFO/A_CONTROLER/BLOQUANT; L380: {"_code_anomalie", "_niveau_anomalie"}),; L382: if [_niveau_anomalie] <> "" then "A_CONTROLER" else "VALIDE"),
  - `02_TRAVAIL\lot8b_banque_regles.py` (documentation/usage, 2 occurrence(s)): L458: new_headers = ["statut_classification", "niveau_anomalie", "regle_id_appliquee"]; L498: # niveau_anomalie
  - `02_TRAVAIL\lot9_construire_flux.py` (documentation/usage, 4 occurrence(s)): L47: 'statut_controle', 'niveau_anomalie', 'code_anomalie', 'commentaire',; L180: niveau_anomalie='INFO', code_anomalie=None):; L214: 'niveau_anomalie':              niveau_anomalie,; L489: 'statut_controle': 17, 'niveau_anomalie': 17, 'code_anomalie': 26, 'commentaire': 44,
  - `00_CADRAGE\ARCHITECTURE_DONNEES.md` (documentation/usage, 3 occurrence(s)): L632: | 16 | `niveau_anomalie` | Statut | `BLOQUANT` / `A_CONTROLER` / `INFO` |; L1453: > Nouvelle colonne `niveau_anomalie` (famille `niveau_anomalie` dans `REF_Statuts`) = `INFO` / `A_CONTROLER` / `BLOQUANT`.; L1455: > `REF_Statuts` étendu : STAT_022 désactivé ; STAT_024-026 (statut_controle) + STAT_027-029 (niveau_anomalie) ajoutés.
  - `00_CADRAGE\DECISIONS_METIER.md` (documentation/usage, 4 occurrence(s)): L403: ### D044 — Séparation statut_controle / niveau_anomalie (DM-L3-01); L407: `niveau_anomalie` (sévérité de l'anomalie) : `INFO` / `A_CONTROLER` / `BLOQUANT`; L415: - STAT_027 (INFO / niveau_anomalie) + STAT_028 (A_CONTROLER) + STAT_029 (BLOQUANT) → ajoutés; L492: Décision : `MASTER_CALC_Reservations` contient 24 colonnes (22 base + `niveau_anomalie` + `code_anomalie` en bloc statut, après `statut_controle` et avant `commentaire`).
  - `00_CADRAGE\ETAT_AVANCEMENT.md` (documentation/usage, 4 occurrence(s)): L69: - SAISIE_Charges_Flux.xlsx créé (01_SOURCES_BRUTES/Charges/) : 31 colonnes, 18 listes déroulantes, formules calculées, 13 contrôles, MFC niveau_anomalie.; L71: - Décisions verrouillées : D044 (séparation statut_controle/niveau_anomalie), D045 (REF_Charges_Recurrentes paramétrable). REFACTURATION→sens=CHARGE validé.; L461: - D044 : Séparation statut_controle / niveau_anomalie — `statut_controle` : VALIDE/A_CONTROLER/EXCLU_RESULTAT/A_VENTILER (Lot 3+) ; `niveau_anomalie` : INFO/A_CONTROLER/BLOQUANT — VERROUILLÉ (DM-L3-01); L464: - D053 : MASTER_CALC_Reservations 24 cols ; source 7 valeurs ; source_montant 5 valeurs ; +niveau_anomalie +code_anomalie ; contrôle RESERVATION_HH_NON_VALIDE — VERROUILLÉ (QM-L4b-02)
  - `00_CADRAGE\JOURNAL_CONTROLES.md` (documentation/usage, 4 occurrence(s)): L748: - 11 DV (10 plan + niveau_anomalie pour cohérence Lot 3).; L752: - MFC : rouge = BLOQUANT / orange = A_CONTROLER (sur colonne AB niveau_anomalie).; L779: + 3 impact + 4 statut (incl. niveau_anomalie + code_anomalie) + 4 système PQ.; L1085: valeurs fermées statuts / niveau_anomalie / type_remboursement
  - `00_CADRAGE\REGLES_METIER.md` (documentation/usage, 2 occurrence(s)): L270: ### niveau_anomalie — sévérité; L272: Valeurs fermées (famille `niveau_anomalie` dans `REF_Statuts`) : `INFO` / `A_CONTROLER` / `BLOQUANT`
- Usage Excel / Power Query / Power BI:
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet3.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet10.xml`
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet3.xml`
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet4.xml`
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_FACT_HA_CleaningTasks_Discovery.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot1_Hostaway\MASTER_FACT_HA_CleaningTasks_Discovery.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx::xl/worksheets/sheet2.xml`
- Doublon potentiel:
  - Oui, doublon probable de `severity` si les controles et tables metier convergent.

## severity

- Statut audit: **COMPATIBILITE**
- Axe metier couvert: Gravite controle
- Role metier reel: Gravite des controles Lot11. Doublonne probablement niveau_anomalie sur l axe gravite, mais dans une table de controles distincte.
- Fichiers / feuilles / valeurs presentes:
  - `02_TRAVAIL\Lot11_Controles\MASTER_CTRL_Coherence.xlsx` / `MASTER` / `severity`: Excel column; valeurs: INFO (20), A_CONTROLER (7)
  - `02_TRAVAIL\Lot11_Controles\MASTER_CTRL_Coherence.xlsx` / `BLOQUANTS_OUVERTS` / `severity`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot11_Controles\MASTER_CTRL_Coherence.xlsx` / `A_CONTROLER_OUVERTS` / `severity`: Excel column; valeurs: A_CONTROLER (7)
- Scripts producteurs / consommateurs detectes:
  - `02_TRAVAIL\lib_controls.py` (producteur/consommateur, 8 occurrence(s)): L19: def default_impact_facture(severity, impact_facture=None):; L27: sev = str(severity or "").strip().upper(); L45: impact = default_impact_facture(control.get("severity"), control.get("impact_facture")); L71: if str(c.get("severity")).upper() == "BLOQUANT"; L72: and default_impact_facture(c.get("severity"), c.get("impact_facture")) == IMPACT_BLOQUANT_FACTURE
  - `02_TRAVAIL\lot11_controles_coherence.py` (producteur/consommateur, 9 occurrence(s)): L127: code_controle, severity, message,; L139: "severity":        severity,; L140: "impact_facture":   default_impact_facture(severity, impact_facture),; L1043: (subset["severity"] == "BLOQUANT"); L1047: (subset["severity"] == "A_CONTROLER")
  - `02_TRAVAIL\lot13_export_powerbi.py` (producteur/consommateur, 1 occurrence(s)): L73: ["ctrl_pk","code_controle","severity","mois","logement_id","proprietaire_id","message","statut_resolution"]),
  - `00_CADRAGE\ARCHITECTURE_DONNEES.md` (documentation/usage, 1 occurrence(s)): L1332: Colonnes : `PK` (`source_pk + code_controle`), `source_module`, `source_pk`, `code_controle`, `severity` (bloquant/à contrôler/information), `message`, `statut_resolution` (ouvert/corrigé/ignoré justifié), `commentaire`.
- Usage Excel / Power Query / Power BI:
  - `02_TRAVAIL\Lot11_Controles\MASTER_CTRL_Coherence.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot11_Controles\MASTER_CTRL_Coherence.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot11_Controles\MASTER_CTRL_Coherence.xlsx::xl/worksheets/sheet3.xml`
- Doublon potentiel:
  - Oui, doublon probable de `niveau_anomalie` sur l axe gravite, mais dans le sous-systeme controles Lot11.

## code_impact

- Statut audit: **CANONIQUE**
- Axe metier couvert: Impact resultat/comptabilite compact
- Role metier reel: Axe resultat/comptabilite compact: IC/HC/HR. Complementaire des booleens impact_resultat_* mais peut etre derive d eux.
- Fichiers / feuilles / valeurs presentes:
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx` / `SAISIE` / `code_impact`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx` / `SAISIE` / `code_impact`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm` / `REF_Codes_Impact` / `code_impact`: Excel column; valeurs: IC (1), HC (1), HR (1)
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm` / `REF_Banque_Regles` / `code_impact`: Excel column; valeurs: <VIDE> (22), IC (8)
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx` / `SAISIE` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx` / `MASTER` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx` / `VUE_MENAGE` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx` / `MASTER` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx` / `VUE_FLUX` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx` / `MASTER` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx` / `VUE_FLUX` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` / `MASTER` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` / `VUE_ACTIVE` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx` / `MASTER` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx` / `VUE_ACTIVE` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot6c_MenagesExternes\MASTER_FACT_MEN_MenagesExternes.xlsx` / `MASTER` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot6c_MenagesExternes\MASTER_FACT_MEN_MenagesExternes.xlsx` / `VUE_ACTIVE` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot7_IK_Avantages\MASTER_FACT_MAN_IK_Avantages.xlsx` / `MASTER_SAISIE` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot8_Banque\BANQUE_LOT8_IMPORT.xlsx` / `NORM_Banque` / `code_impact`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot9_FluxUnifie\MASTER_CALC_Flux.xlsx` / `MASTER` / `code_impact`: Excel column; valeurs: 
- Scripts producteurs / consommateurs detectes:
  - `02_TRAVAIL\lot12_seed_donnees_fictives.py` (producteur/consommateur, 11 occurrence(s)): L56: "categorie_charge_id": "CHG_001", "type_flux_id": "TYPE_FLUX_011", "code_impact": "IC",; L64: "categorie_charge_id": "CHG_001", "type_flux_id": "TYPE_FLUX_009", "code_impact": "HC",; L72: "categorie_charge_id": "CHG_015", "type_flux_id": "TYPE_FLUX_011", "code_impact": "IC",; L80: "categorie_charge_id": "CHG_015", "type_flux_id": "TYPE_FLUX_010", "code_impact": "HC",; L88: "categorie_charge_id": "CHG_001", "type_flux_id": "TYPE_FLUX_011", "code_impact": "IC",
  - `02_TRAVAIL\lot13_export_powerbi.py` (producteur/consommateur, 2 occurrence(s)): L47: "code_impact","inclure_resultat_reel","inclure_resultat_comptable","inclure_resultat_hors_compta",; L53: "proprietaire_id","date_arrivee","date_depart","nuits","montant_retenu","code_impact","etat_mois",
  - `02_TRAVAIL\lot4bis_charger_reservations.py` (producteur/consommateur, 15 occurrence(s)): L266: code_impact, statut_controle, niveau_anomalie, code_anomalie, commentaire,; L275: impact_reel  = "A_CONTROLER" if code_impact not in ("IC", "HC", "HR") else ("NON" if code_impact == "HR" else "OUI"); L276: impact_compta = "A_CONTROLER" if code_impact not in ("IC", "HC", "HR") else ("OUI" if code_impact == "IC" else "NON"); L282: logement_id, montant_retenu, code_impact]; L299: "code_impact":             code_impact,
  - `02_TRAVAIL\lot4quater_resoudre_source_reservations.py` (documentation/usage, 2 occurrence(s)): L48: "code_impact", "impact_resultat_reel", "impact_resultat_comptable",; L197: "code_impact": h.get("code_impact"),
  - `02_TRAVAIL\lot4ter_historiser_reservations_cloturees.py` (documentation/usage, 2 occurrence(s)): L71: "code_impact", "impact_resultat_reel", "impact_resultat_comptable",; L269: r.get("code_impact"), r.get("impact_resultat_reel"), r.get("impact_resultat_comptable"),
  - `02_TRAVAIL\lot5_master_acomptes_proprietaires.py` (documentation/usage, 4 occurrence(s)): L53: ("code_impact",               C_IMPACT, 12),; L298: ("7. code_impact / impact_resultat_reel / impact_resultat_comptable : HC / OUI / NON (fixes)", False, 11),; L386: {{"code_impact",               type text}},; L542: "code_impact", "impact_resultat_reel", "impact_resultat_comptable",
  - `02_TRAVAIL\lot6b_m04_menages_internes.py` (documentation/usage, 1 occurrence(s)): L217: "type_flux_id": "TYPE_FLUX_013", "sens": "CHARGE", "code_impact": "HC",
  - `02_TRAVAIL\lot6c_menages_externes.py` (producteur/consommateur, 8 occurrence(s)): L196: "mode_paiement","associe_payeur","code_impact","prise_en_compta",; L297: # code_impact / compta selon D-6c-03; L298: code_impact     = "IC"; L313: mode_paie, None, code_impact, prise_en_compta,; L328: and r[MASTER_COLS.index("code_impact")] in ("IC","HC")
  - `02_TRAVAIL\lot7_ik_avantages.py` (documentation/usage, 9 occurrence(s)): L221: 'code_impact',               #  9  IC/HC/HR — defaut REF_Types_Flux, override controle; L344: {"code_impact_defaut","avantage_brut_defaut","deduit_avantage_defaut"},; L345: {"code_impact_defaut","avantage_brut_defaut","deduit_avantage_defaut"}),; L346: WithCI = Table.AddColumn(Exp, "code_impact", each; L347: [code_impact_defaut] ?? "A_CONTROLER"),
  - `02_TRAVAIL\lot8a_banque_import.py` (documentation/usage, 2 occurrence(s)): L329: None,                        # 16 code_impact; L434: 'tiers_detecte', 'categorie', 'type_flux_id', 'code_impact',
  - `02_TRAVAIL\lot8b_banque_regles.py` (documentation/usage, 4 occurrence(s)): L82: #          tiers_detecte, categorie, type_flux_id, code_impact,; L91: "tiers_detecte", "categorie", "type_flux_id", "code_impact",; L446: COL_CI               = hdr["code_impact"]     # 16; L536: set_cell(COL_CI,          _none_or(rule["code_impact"]))
  - `02_TRAVAIL\lot9_construire_flux.py` (documentation/usage, 21 occurrence(s)): L45: 'type_flux_id', 'sens', 'montant', 'code_impact',; L55: def impact_flags(code_impact):; L60: }.get(code_impact, ('NON', 'NON', 'NON')); L178: type_flux_id, sens, montant, code_impact,; L193: reel, compta, hors = impact_flags(code_impact)
  - `00_CADRAGE\ARCHITECTURE_DONNEES.md` (documentation/usage, 15 occurrence(s)): L483: | `code_impact` | `HC` par défaut |; L523: | `code_impact` | `IC` / `HC` selon la source |; L585: | `code_impact` | `IC` / `HC` / `HR` |; L628: | 12 | `code_impact` | Impact | `HC` fixe |; L676: | `code_impact` | `IC` / `HC` / `HR` |
  - `00_CADRAGE\CLAUDE.md` (documentation/usage, 1 occurrence(s)): L10: Système de pilotage financier et opérationnel d'une conciergerie courte durée (~16 logements, Toulouse / Blagnac). Concilie Hostaway, réservations hors Hostaway, banque, ménages internes / externes, charges perso / liqui
  - `00_CADRAGE\DECISIONS_METIER.md` (documentation/usage, 11 occurrence(s)): L81: Tables : MASTER_CALC_Flux (colonne code_impact); L367: - `code_impact` selon les règles standard (`IC` / `HC` / `HR`).; L388: - Selon le `code_impact` de la ligne saisie (`IC` / `HC` / `HR`).; L505: S3 Direct HA + HH liée → HOSTAWAY_DIRECT_HH, MANUEL_HH, code_impact HH, statut HH — ligne HA exclue.; L508: S6 HH pure → MANUEL_HORS_HOSTAWAY, MANUEL_HH, code_impact HH.
  - `00_CADRAGE\ETAT_AVANCEMENT.md` (documentation/usage, 1 occurrence(s)): L206: - Ingestion Charges (Lot 3, VALIDE) — sens/code_impact/type portés par la ligne
  - `00_CADRAGE\JOURNAL_CONTROLES.md` (documentation/usage, 5 occurrence(s)): L157: sens / code_impact / type_flux portés par la ligne; L159: CHARGE, code_impact = HC (M2 verrouillé), TYPE_FLUX_013; L430: CTR-9-006 : tous code_impact valides (IC uniquement) [OK]; L1028: Répartition par code_impact :; L1179: | `MENAGE_INTERNE_CODE_IMPACT_NON_HC` | M04 | Ligne M04 avec code_impact ≠ HC |
  - `00_CADRAGE\PLAN_CONSTRUCTION.md` (documentation/usage, 8 occurrence(s)): L20: | Code impact | Chaque ligne à effet financier porte un `code_impact` (`IC` / `HC` / `HR`). |; L188: - Empiler les sources sous le schéma commun (`source`, `reservation_id_hostaway`, `reservation_hh_id`, `montant_retenu`, `source_montant`, `code_impact`).; L195: **Contrôles de validation.** Aucune réservation comptée deux fois ; cohérence `source_montant` ↔ `code_impact` ; toute réservation déversée dans `MASTER_CALC_Flux` (Lot 9) passe par cette table.; L268: - Alimenter `MASTER_CALC_Flux` (Lot 9) avec `type_flux_id = COUT_EXECUTION_MENAGE_INTERNE`, `sens = CHARGE`, `code_impact = HC`.; L291: - Schéma minimal cible : `menage_externe_id`, `facture_id`, `date_facture`, `date_menage`, `mois`, `annee`, `prestataire_id`, `nom_prestataire`, `type_intervenant`, `logement_id`, `hostaway_listing_id`, `appartement_sour
  - `00_CADRAGE\README_PROJET.md` (documentation/usage, 1 occurrence(s)): L113: | 9 | Table de flux unifiée `MASTER_CALC_Flux` = colonne vertébrale ; les trois résultats sont des filtres sur `code_impact`. |
  - `00_CADRAGE\REGLES_METIER.md` (documentation/usage, 4 occurrence(s)): L21: | R2 | La table de flux unifiée `MASTER_CALC_Flux` est la colonne vertébrale. Les trois résultats (réel / comptable / hors compta) en sont des filtres sur `code_impact`. | Archi §2.3, §14, §15 |; L69: | SC3 | Toute ligne de `SAISIE_Charges_Flux.xlsx` porte : `charge_id` (nomenclature §16.2), `date_charge`, `mois`, `associe_id` si applicable, `categorie_charge_id`, `code_impact`, `statut_controle` (valeur fermée REF_St; L106: | IV4 | Le `code_impact` (`IC` / `HC` / `HR`) se décide ligne par ligne selon la nature de la dépense. |; L118: | AC4 | **Flux (c) — Impact sur le résultat conciergerie** : selon le `code_impact` (`IC` / `HC` / `HR`) de la ligne saisie. Décidé ligne par ligne. |
- Usage Excel / Power Query / Power BI:
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx::xl/worksheets/sheet4.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet3.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet4.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/vbaProject.bin`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet17.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet18.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet22.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet23.xml`
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet3.xml`
- Doublon potentiel:
  - Partiel: peut deriver les impacts resultat, mais conserve une valeur metier compacte IC/HC/HR.

## impact_facture

- Statut audit: **CANONIQUE**
- Axe metier couvert: Impact facture
- Role metier reel: Axe blocage facturation dans controles Lot11, derive par defaut de severity via lib_controls mais surchargeable.
- Fichiers / feuilles / valeurs presentes:
  - Aucune colonne Excel exacte detectee dans le perimetre lu.
- Scripts producteurs / consommateurs detectes:
  - `02_TRAVAIL\lib_controls.py` (producteur/consommateur, 6 occurrence(s)): L19: def default_impact_facture(severity, impact_facture=None):; L21: if impact_facture:; L22: value = str(impact_facture).strip().upper(); L45: impact = default_impact_facture(control.get("severity"), control.get("impact_facture")); L72: and default_impact_facture(c.get("severity"), c.get("impact_facture")) == IMPACT_BLOQUANT_FACTURE
  - `02_TRAVAIL\lot11_controles_coherence.py` (producteur/consommateur, 6 occurrence(s)): L33: from lib_controls import default_impact_facture, facture_control_counts; L130: impact_facture=None, commentaire=None):; L140: "impact_facture":   default_impact_facture(severity, impact_facture),; L410: impact_facture="A_DECIDER"); L1044: & (subset["impact_facture"] == "BLOQUANT_FACTURE")
- Usage Excel / Power Query / Power BI:
  - Aucun signal XML/PQ detecte dans les classeurs lus.
- Doublon potentiel:
  - Non confirme; champ complementaire justifie dans l etat actuel.

## impact_resultat_reel

- Statut audit: **CANONIQUE**
- Axe metier couvert: Impact resultat reel
- Role metier reel: Indicateur explicite d inclusion dans le resultat reel. Complementaire de code_impact ou derivable selon convention.
- Fichiers / feuilles / valeurs presentes:
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx` / `SAISIE` / `impact_resultat_reel`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx` / `SAISIE` / `impact_resultat_reel`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm` / `REF_Codes_Impact` / `impact_resultat_reel`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx` / `SAISIE` / `impact_resultat_reel`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx` / `MASTER` / `impact_resultat_reel`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx` / `VUE_MENAGE` / `impact_resultat_reel`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx` / `MASTER` / `impact_resultat_reel`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx` / `VUE_FLUX` / `impact_resultat_reel`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx` / `MASTER` / `impact_resultat_reel`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx` / `VUE_FLUX` / `impact_resultat_reel`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` / `MASTER` / `impact_resultat_reel`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` / `VUE_ACTIVE` / `impact_resultat_reel`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx` / `MASTER` / `impact_resultat_reel`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx` / `VUE_ACTIVE` / `impact_resultat_reel`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot6c_MenagesExternes\MASTER_FACT_MEN_MenagesExternes.xlsx` / `MASTER` / `impact_resultat_reel`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot6c_MenagesExternes\MASTER_FACT_MEN_MenagesExternes.xlsx` / `VUE_ACTIVE` / `impact_resultat_reel`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot7_IK_Avantages\MASTER_FACT_MAN_IK_Avantages.xlsx` / `MASTER_SAISIE` / `impact_resultat_reel`: Excel column; valeurs: 
- Scripts producteurs / consommateurs detectes:
  - `02_TRAVAIL\lot12_seed_donnees_fictives.py` (producteur/consommateur, 11 occurrence(s)): L57: "impact_resultat_reel": "OUI", "impact_resultat_comptable": "OUI", "prise_en_compta": "OUI",; L65: "impact_resultat_reel": "OUI", "impact_resultat_comptable": "NON", "prise_en_compta": "NON",; L73: "impact_resultat_reel": "OUI", "impact_resultat_comptable": "OUI", "prise_en_compta": "OUI",; L81: "impact_resultat_reel": "OUI", "impact_resultat_comptable": "NON", "prise_en_compta": "NON",; L89: "impact_resultat_reel": "OUI", "impact_resultat_comptable": "OUI", "prise_en_compta": "OUI",
  - `02_TRAVAIL\lot4bis_charger_reservations.py` (producteur/consommateur, 4 occurrence(s)): L300: "impact_resultat_reel":    impact_reel,; L386: "impact_resultat_reel":    impact_reel,; L578: and r["impact_resultat_reel"] == "OUI"; L590: "code_impact", "impact_resultat_reel", "impact_resultat_comptable",
  - `02_TRAVAIL\lot4quater_resoudre_source_reservations.py` (documentation/usage, 3 occurrence(s)): L48: "code_impact", "impact_resultat_reel", "impact_resultat_comptable",; L198: "impact_resultat_reel": h.get("impact_resultat_reel"),; L223: and r.get("impact_resultat_reel") == "OUI"
  - `02_TRAVAIL\lot4ter_historiser_reservations_cloturees.py` (documentation/usage, 2 occurrence(s)): L71: "code_impact", "impact_resultat_reel", "impact_resultat_comptable",; L269: r.get("code_impact"), r.get("impact_resultat_reel"), r.get("impact_resultat_comptable"),
  - `02_TRAVAIL\lot5_master_acomptes_proprietaires.py` (documentation/usage, 4 occurrence(s)): L54: ("impact_resultat_reel",      C_IMPACT, 18),; L298: ("7. code_impact / impact_resultat_reel / impact_resultat_comptable : HC / OUI / NON (fixes)", False, 11),; L387: {{"impact_resultat_reel",      type text}},; L542: "code_impact", "impact_resultat_reel", "impact_resultat_comptable",
  - `02_TRAVAIL\lot6b_m04_menages_internes.py` (documentation/usage, 1 occurrence(s)): L218: "impact_resultat_reel": "OUI", "impact_resultat_comptable": "NON",
  - `02_TRAVAIL\lot6c_menages_externes.py` (producteur/consommateur, 2 occurrence(s)): L197: "impact_resultat_reel","impact_resultat_comptable",; L464: "prise_en_compta":13,"impact_resultat_reel":14,"impact_resultat_comptable":16,
  - `02_TRAVAIL\lot7_ik_avantages.py` (documentation/usage, 3 occurrence(s)): L222: 'impact_resultat_reel',      # 10  OUI/NON — defaut REF_Types_Flux; L348: WithIR = Table.AddColumn(WithCI, "impact_resultat_reel", each; L396: "nature","montant","code_impact","impact_resultat_reel","impact_resultat_comptable",
  - `00_CADRAGE\ARCHITECTURE_DONNEES.md` (documentation/usage, 2 occurrence(s)): L629: | 13 | `impact_resultat_reel` | Impact | `OUI` fixe (HC → OUI) |; L920: | `impact_resultat_reel` | OUI / NON |
  - `00_CADRAGE\DECISIONS_METIER.md` (documentation/usage, 4 occurrence(s)): L316: impact_resultat_reel, impact_resultat_comptable.; L495: `impact_resultat_reel` calculé : IC/HC→OUI, HR→NON, vide→A_CONTROLER.; L656: Décision : Le type flux M04 est `TYPE_FLUX_013 = COUT_MO_INTERNE_MENAGE`. Révise D028 qui mentionnait `COUT_EXECUTION_MENAGE_INTERNE` comme libellé (ce libellé était provisoire). Clé technique : `TYPE_FLUX_013`. Colonnes; L867: - Injection en **deux niveaux** (mois × logement × intervenant), HC analytique, `impact_resultat_reel=OUI`, `impact_resultat_comptable=NON` :
  - `00_CADRAGE\JOURNAL_CONTROLES.md` (documentation/usage, 4 occurrence(s)): L714: - Formules calculées sur 500 lignes : mois, impact_resultat_reel,; L751: commission, acompte_facture, impact_resultat_reel, impact_resultat_comptable, ROW_HASH.; L790: VUE_FLUX : filtre VALIDE + impact_resultat_reel=OUI + montant_retenu≠0 + non nul.; L1013: VUE_FLUX : 1 321 lignes (VALIDE + impact_resultat_reel=OUI + montant≠0)
  - `00_CADRAGE\PLAN_CONSTRUCTION.md` (documentation/usage, 1 occurrence(s)): L317: - IK en montant direct (D036). Schéma minimal obligatoire : `associe_id`, `mois`, `type_flux`, `nature`, `montant`, `commentaire`, `statut_controle`, `impact_resultat_reel`, `impact_resultat_comptable`.
- Usage Excel / Power Query / Power BI:
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx::xl/worksheets/sheet4.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet4.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/vbaProject.bin`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet17.xml`
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx::xl/worksheets/sheet2.xml`
- Doublon potentiel:
  - Non confirme; champ complementaire justifie dans l etat actuel.

## impact_resultat_comptable

- Statut audit: **CANONIQUE**
- Axe metier couvert: Impact resultat comptable
- Role metier reel: Indicateur explicite d inclusion dans le resultat comptable. Complementaire de code_impact ou derivable selon convention.
- Fichiers / feuilles / valeurs presentes:
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx` / `SAISIE` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx` / `SAISIE` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm` / `REF_Codes_Impact` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx` / `SAISIE` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx` / `MASTER` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx` / `VUE_MENAGE` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx` / `MASTER` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx` / `VUE_FLUX` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx` / `MASTER` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx` / `VUE_FLUX` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` / `MASTER` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` / `VUE_ACTIVE` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx` / `MASTER` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx` / `VUE_ACTIVE` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot6c_MenagesExternes\MASTER_FACT_MEN_MenagesExternes.xlsx` / `MASTER` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot6c_MenagesExternes\MASTER_FACT_MEN_MenagesExternes.xlsx` / `VUE_ACTIVE` / `impact_resultat_comptable`: Excel column; valeurs: 
  - `02_TRAVAIL\Lot7_IK_Avantages\MASTER_FACT_MAN_IK_Avantages.xlsx` / `MASTER_SAISIE` / `impact_resultat_comptable`: Excel column; valeurs: 
- Scripts producteurs / consommateurs detectes:
  - `02_TRAVAIL\lot12_seed_donnees_fictives.py` (producteur/consommateur, 11 occurrence(s)): L57: "impact_resultat_reel": "OUI", "impact_resultat_comptable": "OUI", "prise_en_compta": "OUI",; L65: "impact_resultat_reel": "OUI", "impact_resultat_comptable": "NON", "prise_en_compta": "NON",; L73: "impact_resultat_reel": "OUI", "impact_resultat_comptable": "OUI", "prise_en_compta": "OUI",; L81: "impact_resultat_reel": "OUI", "impact_resultat_comptable": "NON", "prise_en_compta": "NON",; L89: "impact_resultat_reel": "OUI", "impact_resultat_comptable": "OUI", "prise_en_compta": "OUI",
  - `02_TRAVAIL\lot4bis_charger_reservations.py` (producteur/consommateur, 3 occurrence(s)): L301: "impact_resultat_comptable": impact_compta,; L387: "impact_resultat_comptable": impact_compta,; L590: "code_impact", "impact_resultat_reel", "impact_resultat_comptable",
  - `02_TRAVAIL\lot4quater_resoudre_source_reservations.py` (documentation/usage, 2 occurrence(s)): L48: "code_impact", "impact_resultat_reel", "impact_resultat_comptable",; L199: "impact_resultat_comptable": h.get("impact_resultat_comptable"),
  - `02_TRAVAIL\lot4ter_historiser_reservations_cloturees.py` (documentation/usage, 2 occurrence(s)): L71: "code_impact", "impact_resultat_reel", "impact_resultat_comptable",; L269: r.get("code_impact"), r.get("impact_resultat_reel"), r.get("impact_resultat_comptable"),
  - `02_TRAVAIL\lot5_master_acomptes_proprietaires.py` (documentation/usage, 4 occurrence(s)): L55: ("impact_resultat_comptable", C_IMPACT, 22),; L298: ("7. code_impact / impact_resultat_reel / impact_resultat_comptable : HC / OUI / NON (fixes)", False, 11),; L388: {{"impact_resultat_comptable", type text}},; L542: "code_impact", "impact_resultat_reel", "impact_resultat_comptable",
  - `02_TRAVAIL\lot6b_m04_menages_internes.py` (documentation/usage, 1 occurrence(s)): L218: "impact_resultat_reel": "OUI", "impact_resultat_comptable": "NON",
  - `02_TRAVAIL\lot6c_menages_externes.py` (producteur/consommateur, 2 occurrence(s)): L197: "impact_resultat_reel","impact_resultat_comptable",; L464: "prise_en_compta":13,"impact_resultat_reel":14,"impact_resultat_comptable":16,
  - `02_TRAVAIL\lot7_ik_avantages.py` (documentation/usage, 3 occurrence(s)): L223: 'impact_resultat_comptable', # 11  OUI/NON — defaut REF_Types_Flux; L350: WithIC = Table.AddColumn(WithIR, "impact_resultat_comptable", each; L396: "nature","montant","code_impact","impact_resultat_reel","impact_resultat_comptable",
  - `00_CADRAGE\ARCHITECTURE_DONNEES.md` (documentation/usage, 2 occurrence(s)): L630: | 14 | `impact_resultat_comptable` | Impact | `NON` fixe (HC → NON) |; L921: | `impact_resultat_comptable` | OUI / NON |
  - `00_CADRAGE\DECISIONS_METIER.md` (documentation/usage, 4 occurrence(s)): L316: impact_resultat_reel, impact_resultat_comptable.; L496: `impact_resultat_comptable` calculé : IC→OUI, HC/HR→NON, vide→A_CONTROLER.; L656: Décision : Le type flux M04 est `TYPE_FLUX_013 = COUT_MO_INTERNE_MENAGE`. Révise D028 qui mentionnait `COUT_EXECUTION_MENAGE_INTERNE` comme libellé (ce libellé était provisoire). Clé technique : `TYPE_FLUX_013`. Colonnes; L867: - Injection en **deux niveaux** (mois × logement × intervenant), HC analytique, `impact_resultat_reel=OUI`, `impact_resultat_comptable=NON` :
  - `00_CADRAGE\JOURNAL_CONTROLES.md` (documentation/usage, 2 occurrence(s)): L715: impact_resultat_comptable, ROW_HASH.; L751: commission, acompte_facture, impact_resultat_reel, impact_resultat_comptable, ROW_HASH.
  - `00_CADRAGE\PLAN_CONSTRUCTION.md` (documentation/usage, 1 occurrence(s)): L317: - IK en montant direct (D036). Schéma minimal obligatoire : `associe_id`, `mois`, `type_flux`, `nature`, `montant`, `commentaire`, `statut_controle`, `impact_resultat_reel`, `impact_resultat_comptable`.
- Usage Excel / Power Query / Power BI:
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\AcomptesProprietaires\SAISIE_AcomptesProprietaires.xlsx::xl/worksheets/sheet4.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet1.xml`
  - `01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx::xl/worksheets/sheet4.xml`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/vbaProject.bin`
  - `01_SOURCES_BRUTES\REF_Setup\REF_Setup.xlsm::xl/worksheets/sheet17.xml`
  - `01_SOURCES_BRUTES\ReservationsHH\SAISIE_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot4_ReservationsHH\MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot4bis_TableCommune\MASTER_CALC_Reservations.xlsx::xl/worksheets/sheet3.xml`
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx::xl/worksheets/sheet2.xml`
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx::xl/worksheets/sheet1.xml`
  - `02_TRAVAIL\Lot5_AcomptesProprietaires\MASTER_FACT_MAN_AcomptesProprietaires.xlsx::xl/worksheets/sheet2.xml`
- Doublon potentiel:
  - Non confirme; champ complementaire justifie dans l etat actuel.

## Valeurs libres ou incoherentes a surveiller

- `statut` contient des valeurs heterogenes selon les tables: etats de reglement, rapprochement, synthese, controles globaux.
- `statut_controle` porte parfois un etat de traitement et parfois une exclusion (`EXCLU_RESULTAT`) ou un blocage.
- `niveau_anomalie` utilise `A_CONTROLER` comme gravite intermediaire; le modele cible propose `WARNING`, decision humaine requise avant renommage.
- `severity` et `impact_facture` sont couples par defaut dans `lib_controls.default_impact_facture`, mais `impact_facture` peut etre surcharge: ne pas fusionner sans arbitrage.
- Des colonnes specialisees existent hors liste (`code_anomalie_lot10`, `code_controle`, `statut_resolution`, `statut_generation`, `statut_facture`) et doivent etre incluses dans une future migration.

## Decisions humaines necessaires

1. Choisir si la gravite canonique doit rester `A_CONTROLER` ou devenir `WARNING`.
2. Decider si `statut_controle` est renomme progressivement en `statut_traitement` ou conserve comme compatibilite historique.
3. Decider si `code_impact` reste canonique IC/HC/HR ou devient une colonne calculee depuis les impacts resultat.
4. Decider si `severity` Lot11 doit etre migre vers `niveau_anomalie` ou rester specifique aux controles.
5. Encadrer `statut` par contexte ou interdire son usage dans les nouvelles sorties hors statuts tres specialises.

## Plan de convergence sans modification

1. Inventorier toutes les tables de sortie et figer un dictionnaire de colonnes par axe: traitement, cause, gravite, facture, resultat reel, resultat comptable.
2. Ajouter des colonnes canoniques en parallele des colonnes historiques, sans supprimer au premier lot.
3. Faire produire `statut_traitement`, `code_anomalie`, `niveau_anomalie`, `impact_facture`, `impact_resultat_reel`, `impact_resultat_comptable` par les lots producteurs.
4. Adapter Lot11/Lot12/Power BI pour lire prioritairement les colonnes canoniques, avec fallback temporaire documente.
5. Declarer `statut_controle`, `severity` et certains usages de `statut` comme compatibilite; supprimer seulement apres tests et validation humaine.

