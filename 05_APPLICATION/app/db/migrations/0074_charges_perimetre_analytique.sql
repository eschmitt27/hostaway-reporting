-- Migration 0074 — périmètre analytique d'une charge : les N logements qu'elle concerne.
--
-- POURQUOI CETTE TABLE EXISTE
-- `charges_preview_service` calculait DÉJÀ correctement le périmètre (`logements_finaux`), le
-- propriétaire historisé de chaque logement et la répartition égale du montant. Mais rien ne
-- persistait ce calcul : `charges_confirmation_service.confirmer()` ne conservait que `row_data`,
-- et le périmètre mourait avec le manifest de prévisualisation. Une charge de 700 € affectée à
-- deux logements se retrouvait donc en base avec `affectation_type='GLOBAL'`, `logement_id=NULL`
-- et `proprietaire_id=NULL` — l'écran de détail affichait « logement : vide », et la position de
-- refacturation créée derrière restait `A_TRAITER`, donc jamais proposable sur une facture.
--
-- CE QU'ELLE N'EST PAS
-- Ce n'est PAS `charges_affectations` (migration 0011), qui porte UNE affectation logique par
-- charge (index unique sur `charge_id WHERE actif=1`) et ne peut structurellement pas représenter
-- N logements. Les deux coexistent sans se recouvrir : 0011 = affectation humaine de contrôle,
-- 0074 = périmètre analytique calculé et figé à la création.
--
-- DEUX AXES À NE JAMAIS CONFONDRE
--   ANALYTIQUE     : `quote_part_montant` = montant / N. Alimente le résultat par logement.
--   REFACTURATION  : le montant refacturable reste le montant TOTAL de la charge, porté par
--                    `charges_refacturation_positions`. Cette table ne dit QUE qui est éligible ;
--                    elle n'impose aucune ventilation commerciale. Refacturer 700 € sur une seule
--                    facture reste licite même si l'analytique répartit 350/350.
--
-- La ligne est figée à la création de la charge : `proprietaire_id` est le propriétaire résolu par
-- la gestion ACTIVE DU MOIS de la charge. Le recalculer plus tard ferait glisser une charge d'août
-- vers un propriétaire entré en septembre.

CREATE TABLE IF NOT EXISTS charges_perimetre_analytique (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_id           TEXT    NOT NULL,
    logement_id         TEXT    NOT NULL,
    proprietaire_id     TEXT,                      -- résolu à la création, jamais réinféré
    mois                TEXT,                      -- mois économique de la charge (AAAA-MM)
    quote_part_montant  REAL    NOT NULL DEFAULT 0,-- part ANALYTIQUE : montant / nb_logements
    date_creation       TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur              TEXT,
    UNIQUE (charge_id, logement_id)
);

CREATE INDEX IF NOT EXISTS idx_cpa_charge      ON charges_perimetre_analytique(charge_id);
CREATE INDEX IF NOT EXISTS idx_cpa_logement    ON charges_perimetre_analytique(logement_id, mois);
CREATE INDEX IF NOT EXISTS idx_cpa_proprietaire ON charges_perimetre_analytique(proprietaire_id, mois);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0074');
