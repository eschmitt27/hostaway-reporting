-- 0083 — un même numéro s'écrivait de deux façons selon la table (§45).
--
-- LE CONSTAT
--   ref_proprietaires   0610190367     (national, 10 chiffres, collé)
--   ref_intervenants    33775793253    (indicatif pays sans « + », collé)
--
-- Aucune des deux ne se lit, et surtout : deux numéros identiques écrits différemment ne se
-- rapprochent pas. Comparer, dédoublonner ou retrouver un correspondant supposait de savoir de
-- quelle table venait la valeur.
--
-- LA FORME RETENUE : E.164 (`+33610190367`) — canonique, internationale, sans ponctuation, celle
-- qui compare bien. L'affichage en groupes de deux (`06 10 19 03 67`) est rendu par le filtre
-- `telephone`, jamais stocké : le stockage n'est pas l'affichage.
--
-- CETTE MIGRATION NE TRANSFORME QUE CE QU'ELLE SAIT LIRE, et la règle est vérifiable à l'œil :
--   · 10 chiffres commençant par 0        → `+33` + les 9 derniers
--   · 11 chiffres commençant par 33       → `+` + la valeur
-- Tout le reste est laissé INTACT. Aucun indicatif n'est ajouté à un numéro qui n'a pas la bonne
-- longueur : compléter un numéro incomplet produirait un numéro que quelqu'un finirait par appeler.
--
-- Les 17 valeurs réelles (12 propriétaires + 5 intervenants) entrent toutes dans l'un des deux cas.

UPDATE ref_proprietaires
   SET telephone = '+33' || SUBSTR(TRIM(telephone), 2)
 WHERE telephone IS NOT NULL
   AND LENGTH(TRIM(telephone)) = 10
   AND SUBSTR(TRIM(telephone), 1, 1) = '0'
   AND TRIM(telephone) GLOB '[0-9]*'
   AND NOT (TRIM(telephone) GLOB '*[^0-9]*');

UPDATE ref_proprietaires
   SET telephone = '+' || TRIM(telephone)
 WHERE telephone IS NOT NULL
   AND LENGTH(TRIM(telephone)) = 11
   AND SUBSTR(TRIM(telephone), 1, 2) = '33'
   AND NOT (TRIM(telephone) GLOB '*[^0-9]*');

UPDATE ref_intervenants
   SET telephone = '+33' || SUBSTR(TRIM(telephone), 2)
 WHERE telephone IS NOT NULL
   AND LENGTH(TRIM(telephone)) = 10
   AND SUBSTR(TRIM(telephone), 1, 1) = '0'
   AND NOT (TRIM(telephone) GLOB '*[^0-9]*');

UPDATE ref_intervenants
   SET telephone = '+' || TRIM(telephone)
 WHERE telephone IS NOT NULL
   AND LENGTH(TRIM(telephone)) = 11
   AND SUBSTR(TRIM(telephone), 1, 2) = '33'
   AND NOT (TRIM(telephone) GLOB '*[^0-9]*');

UPDATE fournisseur_details
   SET telephone = '+33' || SUBSTR(TRIM(telephone), 2)
 WHERE telephone IS NOT NULL
   AND LENGTH(TRIM(telephone)) = 10
   AND SUBSTR(TRIM(telephone), 1, 1) = '0'
   AND NOT (TRIM(telephone) GLOB '*[^0-9]*');

UPDATE fournisseur_details
   SET telephone = '+' || TRIM(telephone)
 WHERE telephone IS NOT NULL
   AND LENGTH(TRIM(telephone)) = 11
   AND SUBSTR(TRIM(telephone), 1, 2) = '33'
   AND NOT (TRIM(telephone) GLOB '*[^0-9]*');
