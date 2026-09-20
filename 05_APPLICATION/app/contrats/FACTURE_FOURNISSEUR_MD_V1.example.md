# FACTURE_FOURNISSEUR_MD_V1

Ce fichier accompagne `08-26-Aissata.pdf`, sous le même nom de base. Le PDF reste la pièce
originale ; ce document en est l'interprétation structurée. Il TRANSCRIT la facture et ne la
corrige jamais : les montants imprimés restent tels quels, et une incohérence du document se
signale par une anomalie — c'est l'application qui recalcule les contrôles.

```json
{
  "schema": "FACTURE_FOURNISSEUR_MD_V1",

  "source": {
    "pdf_filename": "08-26-Aissata.pdf",
    "pdf_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
    "referentiel_version": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"
  },

  "facture": {
    "fournisseur": "Aissata",
    "reference_fournisseur": "2026-41",
    "date_facture": "2026-08-31",
    "mois_concerne": "2026-08",
    "devise": "EUR",

    "total_ht": 2790.00,
    "total_tva": 0.00,
    "total_ttc": 2790.00,
    "net_a_payer": 2790.00,

    "total_document": 2790.00
  },

  "lignes": [
    {
      "numero": 1,
      "page": 1,
      "libelle_source": "1. service de nettoyage studio - cote pavé (François) (6 impasse Duroux) x 8 passages",
      "libelle_metier": "Ménages d'août — Studio Côte Pavée",
      "nature": "MENAGE",
      "dates_prestation": ["2026-08-06", "2026-08-08", "2026-08-10", "2026-08-12"],
      "quantite_source": 8,
      "prix_unitaire_source": 29.00,
      "montant_source": 232.00,
      "montant_calcule": 232.00,
      "logements": [
        {
          "logement_id": "LOG_0012",
          "confiance": "CERTAIN",
          "motif": "adresse « 6 impasse Duroux » et propriétaire concordants"
        }
      ],
      "anomalies": []
    },
    {
      "numero": 14,
      "page": 2,
      "libelle_source": "14. Frais de courses produits consommables",
      "libelle_metier": null,
      "nature": "AUTRE_PRESTATION",
      "dates_prestation": [],
      "quantite_source": 1,
      "prix_unitaire_source": 85.00,
      "montant_source": 85.00,
      "montant_calcule": 85.00,
      "logements": [],
      "anomalies": []
    },
    {
      "numero": 15,
      "page": 2,
      "libelle_source": "15. solde dû suite aux prestations du mois de juillet",
      "libelle_metier": null,
      "nature": "A_CLASSER",
      "dates_prestation": [],
      "quantite_source": 1,
      "prix_unitaire_source": 50.00,
      "montant_source": 50.00,
      "montant_calcule": 50.00,
      "logements": [],
      "anomalies": ["NATURE_A_CLASSER"]
    }
  ],

  "controles": {
    "nombre_lignes": 3,
    "somme_montants_source": 367.00,
    "total_document": 2790.00,
    "ecart_source": -2423.00,
    "anomalies_document": []
  }
}
```

## Rappels

- `nature` vaut exactement `MENAGE`, `REMISE_EN_ETAT`, `AUTRE_PRESTATION` ou `A_CLASSER`.
- `confiance` vaut `CERTAIN`, `A_CONFIRMER`, `AMBIGU`, `NON_TROUVE` ou `NON_APPLICABLE`.
- Dans le doute sur un logement : laisser `"logements": []` et poser une anomalie, plutôt que de
  désigner un `LOG_XXXX` au jugé. L'application ne crée jamais un logement depuis un MD.
- Une incohérence arithmétique du document se transcrit telle quelle :
  `quantite_source: 2`, `prix_unitaire_source: 30.00`, `montant_source: 30.00`,
  `montant_calcule: 60.00`, `anomalies: ["INCOHERENCE_ARITHMETIQUE_DOCUMENT"]`.
- `controles` est indicatif : l'application recalcule la somme, l'écart et les contrôles.
