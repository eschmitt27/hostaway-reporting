# ETAT_AVANCEMENT.md
> Fichier de mémoire inter-sessions. À lire en PREMIER à chaque reprise. À mettre à jour en FIN de session.

---

## Dernière mise à jour
Date : 2026-06-04
Session : Session 3 — Intégration retours validation principes (D041-D043, P02/P03/P11/P17/P21/P32)
Agent : Claude (claude.ai Projet)

---

## Lot en cours
Lot : 0 — Fiabiliser REF_Setup.xlsm
Statut : **À PRÉPARER / audit requis / non démarré** (D029)
Objectif immédiat : Auditer REF_Setup.xlsm réel (encodage cassé, dates série Excel, unicité des clés, créer les onglets manquants)

> **Séquence obligatoire avant Lot 2** :
> 1. Uploader REF_Setup.xlsm réel → audit encodage + dates + clés
> 2. Corriger à la source (encodage REF_Associes, REF_Codes_Impact, REF_Types_Flux)
> 3. Créer onglets manquants : `REF_Statuts`, `REF_Statuts_Payout`, `REF_Parametres_Generaux`, `REF_Cloture_Mensuelle` (structure vide)
> 4. Marquer obsolète le paramètre `LOCAL_50_INJECTABLE_DANS_M04` (D016-REV)
> 5. Valider → SEULEMENT ENSUITE démarrer Lot 2

---

## Ce qui est terminé

- **Fichiers de cadrage mis à jour et cohérents (Session 2, pack V2)** :
  - CLAUDE.md, README_PROJET.md, REGLES_METIER.md, ARCHITECTURE_DONNEES.md, PLAN_CONSTRUCTION.md
  - ETAT_AVANCEMENT.md, DECISIONS_METIER.md, JOURNAL_CONTROLES.md, JOURNAL_ANOMALIES.md
- **REF_Setup.xlsm** : 19 onglets opérationnels — contenu non audité encodage/dates (Lot 0 à préparer)
- **Lot 1 — Module Hostaway** : extraction produite (run 20260523_005752 — 1505 réservations, 16 listings, 28 anomalies, 451 tâches ménage) — **NON VALIDÉE sur données réelles** (JOURNAL_CONTROLES vide)

> **Règle D029 — IRRÉVOCABLE** : aucun lot ne peut être marqué FAIT sans entrée dans JOURNAL_CONTROLES.

---

## Ce qui a été modifié (cette session)
- Fichiers : mise à jour de 8 fichiers de cadrage (intégration D026-D040 + patch de lancement)
- Scripts : aucun
- Tables : aucune (construction non commencée)
- Requêtes : aucune

---

## Ce qui a été testé sur données réelles
- Test : aucun
- Statut : —

---

## Anomalies connues (résumé — détail dans JOURNAL_ANOMALIES.md)

| ID | Code | Sévérité | Statut |
|---|---|---|---|
| ANO-001 | LISTING_ORPHELIN | A_CONTROLER | OUVERT |
| ANO-002 | ENCODAGE_CASSE | A_CONTROLER | OUVERT |
| ANO-003 | DATES_SERIE_EXCEL | A_CONTROLER | OUVERT |
| ANO-004 | VRBO_MONTANT_NON_RENSEIGNE (×29) | A_CONTROLER | OUVERT |
| ANO-005 | REFERENTIEL_ORPHELIN (497801) | A_CONTROLER | OUVERT |
| ANO-006 | LISTING_CONFIRME_HORS_HOSTAWAY (480780) | INFO | À CONFIRMER |

> **Les 3 orphelins listingMapId sont 3 cas distincts** — voir JOURNAL_ANOMALIES.md.

---

## Décisions prises (résumé — détail dans DECISIONS_METIER.md)
- D001 à D020 : décisions architecture validées (payout, commission, codes impact, upsert…)
- D021 : Statuts payout fermés — `REF_Statuts_Payout` à créer au Lot 0
- D022 : `REF_Statuts` fermé — à créer au Lot 0
- D023 : obsolète — remplacée par D035 (double seuil 0,10 €/1,00 €)
- D024 : `REF_Cloture_Mensuelle` — structure créée au Lot 0, exploitée au Lot 8
- D025 : Frontière Lot 3 / Lot 7 — IK uniquement dans `MASTER_FACT_MAN_IK_Avantages`
- D026 : `SAISIE_Charges_Flux.xlsx` = source unique achats/charges (exclut IK)
- D027 : Suppression définitive `Courses` / `Coût du lavage` / `achats` de M04 — IRRÉVOCABLE
- D016-REV : Forfait local 50 € quitte M04 → `SAISIE_Charges_Flux.xlsx`
- D028 : Coût complet ménage hors M04 via `VUE_ACHATS_MENAGE_VALIDES`
- D029 : Aucun lot ne peut être FAIT sans contrôle dans JOURNAL_CONTROLES — IRRÉVOCABLE
- D030 : Cancellation payout — `BaseCommission = CancellationPayout`, pas de ménage déduit
- D031 : `revenu_net_exploitation_proprietaire` — indicateur économique pur, formule verrouillée
- D032 : `acompte_conciergerie_recu_via_airbnb` — bloc règlement uniquement, jamais exploitation
- D033 : Séparation exploitation / règlement — deux blocs non communicants
- D034 : `charges_exceptionnelles_refacturees` — bloc règlement uniquement
- D035 : Convention d'arrondi — double seuil 0,10 €/ligne / 1,00 €/cumulé — VERROUILLÉ
- D036 : IK en montant direct (pas de barème auto au démarrage) — VERROUILLÉ
- D037 : REF_Couts_Standards_Menage = exécution seule, valeurs à revalider Lot 0 — VERROUILLÉ
- D038 : Rangement dans M04 = main-d'œuvre uniquement — VERROUILLÉ
- D039 : charge_fixe_mensuelle paramétrable dans REF_Logements, 0 si absent — VERROUILLÉ
- D040 : Structure sortie facture (FACT_FACTURE_ENTETE/LIGNES, Excel contrôle, **aucun PDF au démarrage**) — VERROUILLÉ
- D041 : Incidents voyageurs — catégorie `INCIDENT_VOYAGEUR` dans `SAISIE_Charges_Flux.xlsx`, `reservation_id` obligatoire — VERROUILLÉ (P02)
- D042 : AirCover — 3 flux distincts (remboursement propriétaire hors comptes / prestation facturée bloc règlement / impact résultat ligne par ligne) — VERROUILLÉ (P03)
- D043 : Priorité Excel avant Power BI — aucun dashboard `.pbix` livré par les lots — VERROUILLÉ (P32)

---

## Prochaine action obligatoire
```
1. Uploader REF_Setup.xlsm réel
2. Exécuter audit Lot 0 (encodage, dates, clés, onglets manquants)
3. Valider Lot 0 → entrée dans JOURNAL_CONTROLES obligatoire
4. Démarrer Lot 2 — Réconciliation logements
```

---

## Lecture prochaine session (discipline contextuelle)

Pour la prochaine session, l'assistant doit ouvrir uniquement :

```text
À OUVRIR :
- CLAUDE.md (intégral, court)
- ETAT_AVANCEMENT.md (ce fichier)
- PLAN_CONSTRUCTION.md → uniquement Lot 0
- REGLES_METIER.md → §1 et §13
- ARCHITECTURE_DONNEES.md → §2, §4, §16, §23 (via la table des matières)
- DECISIONS_METIER.md → uniquement les décisions liées au Lot 0

À NE PAS OUVRIR (économie de contexte) :
- README_PROJET.md (sauf onboarding)
- OBJECTIF_PROJET_PILOTAGE_CONCIERGERIE_V3.md (vision générale, déjà lue)
- JOURNAL_ANOMALIES.md (sauf anomalie touchée par le Lot 0)
- Les sections d'ARCHITECTURE_DONNEES.md hors §2/§4/§16/§23
- Tous les autres lots du PLAN
```

Cette discipline est appliquée à chaque nouveau lot, en s'appuyant sur la matrice `CLAUDE.md §5.bis`.

---

## Interdictions / points sensibles
- Ne pas modifier : sources brutes (Banque, Hostaway, PDF)
- Ne pas utiliser : REF_Setup.xlsx (nom incorrect — le fichier est REF_Setup.xlsm)
- Ne pas passer au Lot 2 sans audit Lot 0 validé sur fichier réel
- **Ne jamais marquer un lot FAIT sans entrée dans JOURNAL_CONTROLES (D029)**
- Ne pas écraser : tables master existantes sans sauvegarde
- Ne pas fusionner ménages internes / externes / tâches Hostaway
- Ne pas saisir IK/virements associés dans SAISIE_Charges_Flux.xlsx (appartient au Lot 7)
- **Ne jamais réintroduire Courses, Coût du lavage, onglet achats dans M04 (D027 — irrévocable)**
- Ne pas mettre achats/consommables/linge/matériel dans M04 (appartient à SAISIE_Charges_Flux)
- Ne pas modifier revenu_net_exploitation via des acomptes ou paiements reçus (D031/D033)
- Ne jamais intégrer une charge exceptionnelle dans charge_fixe_mensuelle (D039/EP3)
- Ne jamais comparer un coût standard complet à un coût d'exécution M04 (D037)
- Ne jamais inclure une charge exceptionnelle refacturée dans `revenu_net_exploitation_proprietaire` (D034/EP7/P21)
- Ne jamais traiter un remboursement AirCover perçu par le propriétaire comme un payout (D042/AC5)
- Ne jamais livrer un dashboard Power BI dans un lot (D043/PBI2)
