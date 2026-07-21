# APP-3F — Audit du module bancaire existant (avant tout code)

## Matrice de l'existant

| Fonction | Présente | Partielle | Absente | Source | Réutilisation APP-3F |
|---|:-:|:-:|:-:|---|---|
| Reader bancaire | ✅ | | | `app/readers/banques_reader.py` (`mouvements()`, onglet `NORM_Banque`) | Délégué (jamais dupliqué) |
| Source de vérité mouvements | ✅ | | | `MASTER_BANQUE = BANQUE_LOT8_IMPORT.xlsx` (moteur, lecture seule) | Lue via le reader, jamais écrite |
| Masquage compte/IBAN | ✅ | | | `masquer_compte()` → `CM ••••1603` | Réutilisé |
| Identifiant compte opaque | ✅ | | | `id_opaque_compte()` → `CPT-<hash>` (salt `BANQUE_OPAQUE_SALT`) | Modèle réutilisé pour `MVT-<hash>` |
| Identifiant **mouvement** opaque | | | ✅ | service expose `mouvement_id` brut (clé moteur) | **Créé** : `MVT-<hash>` dans le reader de contrat APP-3F |
| Sens / date / montant / libellé nettoyé | ✅ | | | `banques_service` (libelle nettoyé, jamais `libelle_brut`) | Réutilisé via le contrat APP-3F |
| États source (OK/ABSENT/VIDE/ILLISIBLE) | ✅ | | | `banques_reader` machine d'état | Réutilisé |
| Contrôles bancaires génériques | ✅ | | | `banques_controle_service.py` | Non touché |
| Rapprochement Airbnb (moteur) | ✅ | | | `rappro_airbnb()` onglet `RAPPROCH_AIRBNB_ATTENTE` | Hors périmètre |
| Rapprochement propriétaires **moteur** (attente) | ✅ | | | `rappro_proprietaires()` onglet `RAPPROCH_PROPRIETAIRES_ATTENTE` | Source moteur en lecture ; **distinct** du rapprochement déclaratif APP-3F |
| Rapprochement déclaratif **APP-3E ↔ mouvement** | | | ✅ | — | **Créé par APP-3F** (journal SQLite isolé) |
| Writer réel bancaire | ✅ (gardé) | | | `banques_controle_writer.py`, `BANQUE_REAL_WRITE_ENABLED=False` | Jamais activé, jamais appelé |
| Routes/templates bancaires | ✅ | | | `routes/banques.py`, `templates/banques_*.html` | Non touchés (APP-3F complète `/proprietaires-reglements`) |

## Rapprochements existants — vérification anti-duplication

| Cible | Rapprochement moteur existant ? | APP-3F |
|---|---|---|
| Réservations | oui (`rappro_airbnb`) | ne touche pas |
| Factures | non applicable ici | ne touche pas |
| Charges | via APP-3E affectations | ne touche pas |
| Acomptes / reversements | via moteur (relevé) | ne touche pas |
| **Règlements propriétaires déclarés (APP-3E `MARQUE_COMME_PAYE`)** | **aucun** | **objet d'APP-3F** |

Conclusion : APP-3F ne duplique aucune logique. Il relie une **déclaration humaine de paiement**
(APP-3E, `proprietaires_paiement.statut_paiement = MARQUE_COMME_PAYE`) à un **mouvement bancaire
existant en lecture seule**, via un journal SQLite isolé. La vérité des montants reste le moteur ;
la vérité des mouvements reste le fichier bancaire (jamais modifié).

## Contrat APP-3F (lecture seule, exposé au navigateur)

Exposé : `mouvement_opaque` (`MVT-<hash>`), `date`, `montant`, `sens`, `libelle_masque`,
`reference_normalisee`, `source_logique`, `empreinte`, `etat`.
Jamais exposé : IBAN, RIB, BIC, numéro de compte, `libelle_brut`, nom de fichier, chemin absolu,
id SQLite, coordonnées bénéficiaire.
