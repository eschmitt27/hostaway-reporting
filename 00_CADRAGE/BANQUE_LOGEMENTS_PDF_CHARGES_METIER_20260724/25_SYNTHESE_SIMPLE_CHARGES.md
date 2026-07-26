# 25 — Synthèse simple

Oui : deux charges de 100 € avec des paramètres différents produisent bien des effets différents,
et c'est prouvé avec de vrais chiffres, sur un jeu de données 100 % fictif, sans jamais toucher aux
vraies données.

- Une charge **conciergerie** (A) baisse le résultat de la conciergerie, sans rien changer au
  propriétaire.
- La même charge en **refacturable** (B) crée une **préfacture de 100 €** au propriétaire A : elle
  se déduit de ce qu'on lui reverse.
- En **hors comptabilité** (D), la charge compte pour le réel mais **pas** pour la comptabilité.
- Répartie sur deux logements (H), elle se coupe en **50 / 50**, sans perdre un centime.
- Répartie sur deux propriétaires (I), chacun reçoit sa part **50 / 50**, séparément.
- Payée depuis un compte perso (C), elle est tracée différemment et ne crée pas de remboursement
  injustifié.

Chaque charge a été **vraiment enregistrée** puis **recalculée** dans un bac à sable isolé
(`data_recette/`). Ce qui reste : afficher le net final du propriétaire dans le tableau de bord
(il faut d'abord fabriquer de fausses réservations), et les écrans Banque / création logement /
import PDF.
