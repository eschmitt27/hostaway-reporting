# 19 — Recette navigateur Charges

Application recette : `http://127.0.0.1:8018`.

## Constaté dans le navigateur (via lecture de page)
Le formulaire `/fournisseurs/nouvelle` sert les **données fictives** : propriétaires PROP_A/PROP_B/
PROP_C, logements A1/A2/B1/C1 (LOG_INACTIF exclu), mois ouvert 2026-06, catégories, codes impact
IC/HC, modes de paiement. Le parcours saisie → prévisualisation → confirmation fonctionne sur le
serveur live : confirmation d'une charge → page résultat **SUCCÈS « Charge enregistrée »**,
`statut-lot3 = OK`, charge visible dans la liste `/fournisseurs`.

## Limite outillage
Les **captures d'écran** de l'extension Chrome échouent (bug de sérialisation `params.clip.scale`,
côté extension, indépendant de l'application). Les contrôles ont donc été faits par lecture de page
(`get_page_text`, données fictives confirmées) et par le chemin navigateur→serveur (requêtes HTTP
réelles sur 8018 : preview 303 → confirm 303 → résultat SUCCÈS). À refaire en captures dès qu'une
session navigateur saine est disponible.

## À finaliser
Bannière visuelle « MODE RECETTE — DONNÉES FICTIVES » à ajouter dans les templates (le badge actuel
« CHARGES_REAL_WRITE_ENABLED = False » est statique et ne reflète pas l'état live — l'écriture
réelle en recette fonctionne, prouvé par les confirmations réussies).
