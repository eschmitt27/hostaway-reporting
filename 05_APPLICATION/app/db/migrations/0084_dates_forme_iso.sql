-- 0084 — quatre dates écrites à la française, dans des colonnes de date (§76).
--
-- CE QUE LE BALAYAGE A TROUVÉ sur la base réelle :
--
--   ecritures.date_ecriture              11/09/2026
--   factures_proprietaires.date_facture  11/09/2026
--   imputations_airbnb.date_imputation   15/08/2026, 12/08/2026
--
-- CE N'EST PAS UN DÉTAIL D'AFFICHAGE. En SQLite, `'11/09/2026' < '2026-09-12'` est VRAI : une date
-- française se trie AVANT toutes les dates ISO, quelle que soit l'année. Un tri chronologique, un
-- filtre « depuis le 1er septembre », un calcul d'ancienneté, un rapprochement par date : tout se
-- trompe sur ces lignes, et rien ne le signale. Une créance de septembre pouvait ainsi paraître
-- plus ancienne qu'une créance de 2025.
--
-- LA CAUSE était des champs de saisie en texte libre portant « AAAA-MM-JJ » comme simple
-- indication : ils acceptaient tout. Ils sont devenus des sélecteurs de calendrier, et
-- `dates_service.exiger()` ferme les chemins programmatiques.
--
-- LA CONVERSION N'INVENTE RIEN : `JJ/MM/AAAA` → `AAAA-MM-JJ` est une réécriture position par
-- position de la même date. Le filtre `GLOB` ne retient que les valeurs ayant exactement cette
-- forme ; tout le reste est laissé intact.

UPDATE ecritures
   SET date_ecriture = SUBSTR(date_ecriture, 7, 4) || '-' || SUBSTR(date_ecriture, 4, 2)
                       || '-' || SUBSTR(date_ecriture, 1, 2)
 WHERE date_ecriture GLOB '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]';

UPDATE factures_proprietaires
   SET date_facture = SUBSTR(date_facture, 7, 4) || '-' || SUBSTR(date_facture, 4, 2)
                      || '-' || SUBSTR(date_facture, 1, 2)
 WHERE date_facture GLOB '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]';

UPDATE imputations_airbnb
   SET date_imputation = SUBSTR(date_imputation, 7, 4) || '-' || SUBSTR(date_imputation, 4, 2)
                         || '-' || SUBSTR(date_imputation, 1, 2)
 WHERE date_imputation GLOB '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]';

-- `periode` suit `date_ecriture` : elle en est le préfixe, et l'écriture concernée portait
-- « 2026-09 » par coïncidence heureuse. On la réaligne explicitement plutôt que de s'y fier.
UPDATE ecritures
   SET periode = SUBSTR(date_ecriture, 1, 7)
 WHERE date_ecriture GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
   AND periode <> SUBSTR(date_ecriture, 1, 7);
