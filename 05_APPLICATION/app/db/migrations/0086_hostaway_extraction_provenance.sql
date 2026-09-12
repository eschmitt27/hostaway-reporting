-- 0086 — une extraction doit dire D'OÙ elle vient et QUAND sa source a été produite.
--
-- LE MANQUE. `hostaway_extractions` savait dire quand l'application avait importé (`date_debut`,
-- `date_fin`) et par quel moyen (`mode` : API / FIXTURE / REPRISE_EXCEL). Elle ne savait pas dire
-- de QUEL état de la source ces lignes proviennent, ni à quelle heure cette source avait été
-- fabriquée.
--
-- Tant que l'application interrogeait l'API elle-même, la question ne se posait pas : la source
-- était produite au moment de la lecture, les deux dates se confondaient. Ce n'est plus vrai. Le
-- pipeline GitHub extrait Hostaway trois fois par jour et publie le résultat dans le dépôt ; la
-- synchronisation locale, elle, a lieu quand l'utilisateur clique. Entre les deux il peut s'écouler
-- des heures.
--
-- CONFONDRE LES DEUX DATES, C'EST MENTIR SUR LA FRAÎCHEUR
-- Afficher l'heure de l'import ferait passer pour « données de 17h » un jeu produit à 11h40. Et
-- afficher l'heure du pipeline ferait passer pour « à jour » un dépôt jamais synchronisé. Les deux
-- notions sont désormais stockées séparément :
--
--   date_debut / date_fin  — quand CETTE INSTALLATION a importé  (déjà présent)
--   source_horodatage      — quand LA SOURCE a été produite      (nouveau)
--   source_ref             — QUELLE version de la source          (nouveau)
--
-- `source_ref` est l'empreinte exacte de l'état importé (pour le dépôt Git : le SHA du commit).
-- C'est elle qui rend l'import IDEMPOTENT : réimporter le même `source_ref` ne recrée rien, parce
-- qu'on peut constater qu'il est déjà en base. Sans elle, chaque clic sur « Actualiser » aurait
-- fabriqué une extraction de plus, identique à la précédente, et la comparaison ligne à ligne
-- d'une extraction à l'autre aurait perdu son sens.
--
-- Les deux colonnes sont NULL pour les extractions existantes, et cela est correct : une extraction
-- API n'a pas de source externe datée, sa source EST l'appel. On ne réécrit rien rétroactivement.

ALTER TABLE hostaway_extractions ADD COLUMN source_ref TEXT;
ALTER TABLE hostaway_extractions ADD COLUMN source_horodatage TEXT;

CREATE INDEX IF NOT EXISTS idx_hax_source_ref
    ON hostaway_extractions(source_ref);
