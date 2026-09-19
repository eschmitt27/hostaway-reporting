-- Une facture propriétaire peut couvrir une PÉRIODE, pas seulement un mois entier.
--
-- Le cycle mensuel automatique reste inchangé : il facture un mois clos, et `mois` le dit. Mais un
-- propriétaire qui termine son contrat le 10 septembre doit pouvoir être facturé du 01 au 10
-- septembre, sans attendre la fin du mois. La période demandée est alors conservée telle quelle :
-- c'est elle qui explique le contenu de la facture, et elle sert à détecter qu'une autre facture
-- couvre déjà tout ou partie des mêmes jours.
--
-- Colonnes vides sur une facture du cycle mensuel : `mois` reste la vérité pour celles-là.
ALTER TABLE factures_proprietaires ADD COLUMN periode_debut TEXT;
ALTER TABLE factures_proprietaires ADD COLUMN periode_fin TEXT;

CREATE INDEX IF NOT EXISTS idx_fpr_periode
    ON factures_proprietaires(proprietaire_id, periode_debut, periode_fin);
