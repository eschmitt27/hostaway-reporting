-- 0119 — Progression lisible d'une actualisation : détail structuré de chaque étape.
--
-- JSON {nature, volume, code} écrit par l'orchestrateur à la fin de chaque étape : permet à
-- l'écran de distinguer « non exécutée car un amont a échoué », « import manuel », « non
-- configurée » et un vrai succès, sans analyser le texte d'erreur. Additive, sans effet sur
-- les lignes existantes (NULL = ancien run, affiché comme avant).
ALTER TABLE moteur_run_etapes ADD COLUMN detail TEXT;
