-- Validation humaine d'un mouvement Qonto : le lien vers le moteur de rapprochement existant.
--
-- AUCUNE TABLE DE RAPPROCHEMENT N'EST CRÉÉE ICI, et c'est le point important. L'application a
-- déjà `banque_rapprochements` : affectation partielle, refus de dépassement, refus d'un
-- mouvement déjà affecté à 100 %, statuts PROPOSE/CONFIRME/REFUSE/ANNULE, journal d'événements,
-- et même l'affectation multiple (`confirmer_groupe`). En construire un second aurait produit
-- deux vérités concurrentes sur « ce mouvement est-il rapproché ? ».
--
-- Une transaction Qonto y entre donc comme n'importe quel mouvement bancaire, par un identifiant
-- de mouvement stable.

-- ── 1. Identifiant de mouvement stable ────────────────────────────────────────────────────────
-- Dérivé du `transaction_id` de Qonto, PAS du libellé ni de l'UUID technique : quand Qonto
-- corrige un intitulé, la ligne RAW change mais l'identifiant ne bouge pas, et le rapprochement
-- déjà validé reste attaché à sa transaction.
ALTER TABLE qonto_transactions_statut_local ADD COLUMN mouvement_id_opaque TEXT;

CREATE INDEX IF NOT EXISTS idx_qonto_statut_mouvement
    ON qonto_transactions_statut_local(mouvement_id_opaque);

-- ── 2. Idempotence, garantie par le schéma ────────────────────────────────────────────────────
-- Un double clic sur « Valider » ne doit pas produire deux rapprochements. Le service le refuse
-- déjà, mais une garde applicative seule finit toujours par être contournée : deux requêtes
-- concurrentes passent le test avant que l'une écrive. L'index le rend impossible.
--
-- Partiel sur les statuts ACTIFS : un rapprochement annulé ou refusé ne doit pas bloquer une
-- nouvelle tentative correcte — c'est précisément à ça que sert une correction.
CREATE UNIQUE INDEX IF NOT EXISTS idx_rapprochement_unique_actif
    ON banque_rapprochements(mouvement_id_opaque, type_objet, objet_id)
    WHERE statut IN ('PROPOSE', 'CONFIRME') AND objet_id IS NOT NULL;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0098');
