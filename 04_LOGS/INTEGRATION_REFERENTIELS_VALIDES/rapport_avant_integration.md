# Rapport avant int?gration des r?f?rentiels valid?s

Date contr?le post-?criture : 2026-06-28T19:55:24
Source : `04_LOGS\PREPARATION_PREMIERE_CLOTURE\A_COMPLETER_REFERENTIELS.xlsx`
Sauvegarde : `04_LOGS\INTEGRATION_REFERENTIELS_VALIDES\BACKUP_REF_SETUP\REF_Setup_BACKUP_AVANT_INTEGRATION_20260628_195203.xlsm`

## Lignes int?gr?es

| Feuille | Lignes int?gr?es |
|---|---:|
| REF_Taux_Commission | 19 |
| REF_Gestion_Logements_Hist | 17 |
| REF_Taux_Heures_Menage | 2 |
| REF_Couts_Menage_Interne | 0 |

## Lignes refus?es ou encore ambigu?s
- TAUX_HORAIRES_AVANT_JUIN / lignes externes: intervenants externes exclus du r?f?rentiel de taux horaire interne
- COUTS_INTERNES_DEPUIS_JUIN / TYPE_004: co?t fixe non renseign?: non int?gr?, bloquant si m?nage interne applicable apr?s juin
- COUTS_INTERNES_DEPUIS_JUIN / TYPE_005: co?t fixe non renseign?: non int?gr?, bloquant si m?nage interne applicable apr?s juin

## Contr?les techniques
- Classeur lisible avec keep_vba=True : OK
- xl/vbaProject.bin pr?sent : OK
- Chevauchements commission/gestion : aucun
- Helper commission PROP_0001 mai 2026 : OK / 0.18
- Helper gestion LOG_0001 mai 2026 : OK / PROP_0001
- Helper co?t interne TYPE_001 juin 2026 : OK / total=30.0 / unit=30.0