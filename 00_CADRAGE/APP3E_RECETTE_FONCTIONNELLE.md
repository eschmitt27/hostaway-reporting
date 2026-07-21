# Recette fonctionnelle APP-3E — 26 scénarios

Généré le 2026-07-21T15:57:43 — base isolée `C:\Users\Ewan\AppData\Local\Temp\recette_app3e_uuqwr1rq\recette.db`.
**Résultat : 26/26 OK.**

| N° | Scénario | Données initiales | Action | Attendu | Obtenu | Statut | Preuve |
| -: | -------- | ----------------- | ------ | ------- | ------ | ------ | ------ |
| 1 | propriétaire simple | 1 fournisseur actif, 1 charge | affecter la charge | `CHA- opaque créé` | `CHA- opaque créé` | OK | CHA-2517c70ab0 |
| 2 | propriétaire multi-logements | 2 charges, 2 logements | lister par propriétaire/mois | `2` | `2` | OK | 2 affectations |
| 3 | relevé sans anomalie | charge affectée, fournisseur actif | évaluer contrôles | `True` | `True` | OK | bloquants=[] |
| 4 | charge sans fournisseur | affectation sans fournisseur | évaluer | `True` | `True` | OK | ['CHARGE_FOURNISSEUR_ABSENT'] |
| 5 | fournisseur inactif | fournisseur désactivé puis charge affectée | évaluer | `True` | `True` | OK | ['CHARGE_FOURNISSEUR_INACTIF'] |
| 6 | fournisseur changé sur un logement | association ouverte L9 | changer_fournisseur au 01/07 | `fermé + nouveau ouvert` | `fermé + nouveau ouvert` | OK | ancien.date_fin=2026-07-01, nouveau.date_fin=None |
| 7 | payout incomplet | montant moteur indisponible | contrôle passage prêt à payer | `True` | `True` | OK | ['DONNEE_MOTEUR_INDISPONIBLE'] |
| 8 | annulation indemnisée | ajustement avec motif | évaluer | `True` | `True` | OK | ['CHARGE_AFFECTATION_INCOMPLETE'] |
| 9 | charge refacturable | refacturable sans justificatif | évaluer | `True` | `True` | OK | info=['CHARGE_REFACTURABLE_NON_JUSTIFIEE'], bloq=[] |
| 10 | AirCover / divergence moteur | divergence signalée | évaluer | `True` | `True` | OK | ['CHARGE_DIVERGENCE_MOTEUR'] |
| 11 | acompte ajouté | snapshot figé sans acompte | détecter dérive après ajout acompte | `True` | `True` | OK | ['acomptes'] |
| 12 | reversement ajouté | snapshot figé sans reversement | détecter dérive | `True` | `True` | OK | ['reversements'] |
| 13 | ajustement ajouté | snapshot figé sans ajustement | détecter dérive | `True` | `True` | OK | ['ajustements'] |
| 14 | création du snapshot | relevé validé | lire empreinte snapshot | `True` | `True` | OK | empreinte=0f99b25e15f9… |
| 15 | évolution ultérieure des données | snapshot figé | modifier statut moteur | `True` | `True` | OK | ['statut_moteur', 'acomptes', 'reversements', 'ajustements'] |
| 16 | détection de dérive | statut moteur changé | détecter dérive | `True` | `True` | OK | ['statut_moteur', 'acomptes', 'reversements', 'ajustements'] |
| 17 | réouverture | relevé validé + dérive | rouvrir avec motif | `ROUVERT` | `ROUVERT` | OK | état=ROUVERT |
| 18 | préfacture préparatoire | code du service relevé | chercher un numéro légal définitif | `False` | `False` | OK | aucun champ numero_legal |
| 19 | passage à contrôler | paiement NON_PREPARE | démarrer contrôle | `A_CONTROLER` | `A_CONTROLER` | OK | A_CONTROLER |
| 20 | passage prêt à payer | aucun bloquant | marquer prêt à payer | `PRET_A_PAYER` | `PRET_A_PAYER` | OK | PRET_A_PAYER |
| 21 | marquage comme payé | paiement prêt à payer | marquer payé (déclaratif) | `MARQUE_COMME_PAYE` | `MARQUE_COMME_PAYE` | OK | MARQUE_COMME_PAYE |
| 22 | aucun virement déclenché | service paiement | chercher un appel réseau/bancaire | `False` | `False` | OK | aucun import réseau/SEPA |
| 23 | export préparatoire | référence avec amorce de formule | neutraliser l'injection CSV | `True` | `True` | OK | cellule="'=CMD()" |
| 24 | tentative de doublon | charge CHG-R1 déjà affectée | réaffecter la même charge | `True` | `True` | OK | AffectationRefusee levée |
| 25 | source indisponible | source de charges absente | évaluer | `True` | `True` | OK | ['CHARGE_SOURCE_ABSENTE'] |
| 26 | redémarrage | paiement MARQUE_COMME_PAYE en base | relire depuis la base (redémarrage simulé) | `MARQUE_COMME_PAYE` | `MARQUE_COMME_PAYE` | OK | MARQUE_COMME_PAYE |
