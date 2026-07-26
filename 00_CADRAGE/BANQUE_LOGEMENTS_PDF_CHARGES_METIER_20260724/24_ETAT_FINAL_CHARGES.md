# 24 — État final Charges

## Fait et prouvé
- Mode recette isolé (`data_recette/`) : données fictives PROP_A/B/C, LOG_A1..C1, taux 15/19 %,
  2026-06 ouvert. **Aucune PII réelle** (scrub = 0).
- **Reset reproductible** (`recette/reset_data_recette.py`) : idempotent, ne touche que
  data_recette, vérifie que les fichiers réels sensibles sont inchangés (empreintes avant/après).
- Writers charges activés **uniquement en recette** (double verrou) + write-guard barrière ultime.
- **Lot11 corrigé** (fichier Lot7 IK/Avantages fictif ajouté) : Lot3 **et** Lot11 = OK.
- **6 scénarios confirmés réellement** (A, B, C, D, H, I) avec impacts chiffrés distincts (doc 13),
  test automatisé PASS.
- Fichiers source réels **intacts** (`git status` vide) ; master + canonique non touchés.
- Application recette active : **http://127.0.0.1:8018**.

## Reste à faire
- Bannière visuelle « MODE RECETTE » dans les templates (le fonctionnel est actif ; seul le badge
  d'affichage reste statique « False »).
- Pipeline aval net propriétaire (lot9/lot10) : nécessite sources réservations/ménages/banque
  fictives (blocage reproduit `CTR-9-001`). Scénarios E (HR), F/G (ménage), J (PDF), K (annulation).
- Contrôles navigateur visuels : bloqués par un bug de sérialisation des captures de l'extension
  Chrome (le contenu se lit via get_page_text — données fictives confirmées à l'écran — et la
  confirmation réelle passe via le serveur live). À refaire en session navigateur saine.
- Tests des scénarios restants + campagne complète.
