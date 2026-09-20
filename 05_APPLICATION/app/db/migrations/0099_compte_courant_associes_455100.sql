-- Comptes courants d'associés : `467000` → `455100`.
--
-- POURQUOI MAINTENANT. `467000` avait été retenu en migration `0023` par élimination — son en-tête
-- le dit : « le plan de comptes reste PROVISOIRE […] seulement ce qui est nécessaire pour que
-- CAISSE/OD/VENTES fonctionnent sans improviser un compte ». Le cadrage le confirme (« jamais
-- arbitré »). `467` est un compte fourre-tout (« autres comptes débiteurs ou créditeurs ») ; le
-- compte normatif d'un apport d'associé en compte courant est en `455`.
--
-- AUCUNE REPRISE D'ÉCRITURE N'EST NÉCESSAIRE : `467000` porte 0 ligne (audit du 2026-09-20). C'est
-- exactement pourquoi l'arbitrage a été fait à ce moment-là — chaque écriture passée sur `467000`
-- aurait transformé un changement de référentiel en reprise de données.
--
-- UN SEUL COMPTE, LES ASSOCIÉS EN AUXILIAIRE. Créer `4551xx` par associé dupliquerait ce que la
-- colonne `auxiliaire` fait déjà, et obligerait à toucher le plan comptable à chaque nouvel
-- associé. `455100` + auxiliaire (`PERS_EWAN`, `PERS_WAFA`) dit la même chose sans cette dette.
INSERT OR IGNORE INTO plan_comptable
    (compte, libelle, type_compte, auxiliaire_autorise, commentaire)
VALUES
    ('455100', 'Associés - comptes courants - Principal', 'PASSIF', 1,
     'Auxiliaire = associe_id ; apports, avances, dépenses personnelles, remboursements');

-- `467000` est DÉSACTIVÉ, jamais supprimé. Le supprimer rendrait illisible toute écriture
-- historique qui referait surface — il n'y en a aucune aujourd'hui, mais un plan comptable se juge
-- aussi sur ce qu'il permet de relire. Inactif, il n'est plus proposé ni accepté par
-- `_compte_valide`, ce qui suffit à empêcher tout nouvel usage.
UPDATE plan_comptable
   SET actif = 0,
       commentaire = 'REMPLACÉ par 455100 (migration 0099) — conservé pour relecture, plus utilisé'
 WHERE compte = '467000';

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0099');
