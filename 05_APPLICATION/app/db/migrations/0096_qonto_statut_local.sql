-- Statut APPLICATIF d'un mouvement Qonto — notre jugement, pas celui de la banque.
--
-- Table COMPAGNE de `qonto_transactions_raw`, et non une colonne de celle-ci. La couche RAW dit
-- ce que Qonto a répondu ; ce statut dit ce que NOUS en faisons. Les mélanger aurait un coût
-- concret : une réimportation réécrit la ligne RAW quand la charge utile change, et un statut posé
-- là finirait par être emporté par une mise à jour de la banque.
--
-- AUCUN EFFET COMPTABLE À CE STADE. `A_RAPPROCHER` est un libellé d'écran : il ne crée pas de
-- règlement, ne rapproche rien, ne touche ni dette ni créance. C'est une file d'attente visible,
-- rien d'autre.
--
-- `comptabilisable` sépare ce qui est DÉFINITIF de ce qui ne l'est pas. Une opération `pending`
-- est une autorisation de carte : le montant peut encore changer, ou ne jamais être débité. Elle
-- s'affiche — l'ignorer donnerait une image fausse du compte — mais elle est marquée comme non
-- définitive, pour qu'aucun écran, et plus tard aucun rapprochement, ne la traite comme acquise.
CREATE TABLE IF NOT EXISTS qonto_transactions_statut_local (
    qonto_transaction_uuid     TEXT PRIMARY KEY
        REFERENCES qonto_transactions_raw(qonto_transaction_uuid),
    statut_local               TEXT NOT NULL DEFAULT 'A_RAPPROCHER',
    comptabilisable            INTEGER NOT NULL DEFAULT 0,
    motif_non_comptabilisable  TEXT,
    pose_le                    TEXT NOT NULL,
    maj_le                     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_qonto_statut_local
    ON qonto_transactions_statut_local(statut_local);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0096');
