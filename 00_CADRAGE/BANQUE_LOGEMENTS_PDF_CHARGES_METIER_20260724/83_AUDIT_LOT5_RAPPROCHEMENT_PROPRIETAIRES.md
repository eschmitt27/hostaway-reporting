# 83 — Audit Lot 5 et rapprochement des 56 mouvements propriétaires (2026-08-13)

Objectif : déterminer combien des 56 mouvements Banque propriétaires peuvent être résolus par une
**preuve métier déjà existante** (Lot 5 / trésorerie propriétaires), et non par supposition.

**Réponse courte : 0.** La source métier est totalement vide. Détail et conséquences ci-dessous.

## 1. Architecture Lot 5 — identifiée

| Élément | Chemin / valeur |
|---|---|
| Script | `02_TRAVAIL/lot5_master_acomptes_proprietaires.py` |
| Source de saisie | `01_SOURCES_BRUTES/AcomptesProprietaires/SAISIE_AcomptesProprietaires.xlsx` (onglets `SAISIE`, `REF_LOCALE`, `CONTROLES_SAISIE`, `README`) |
| Sortie | `02_TRAVAIL/Lot5_AcomptesProprietaires/MASTER_FACT_MAN_AcomptesProprietaires.xlsx` (onglets `MASTER`, `VUE_ACTIVE`, `POWER_QUERY_CODE`) |
| Clé | `acompte_id` (+ `ROW_HASH`) |
| Rattachement propriétaire | `proprietaire_id`, `logement_id`, `facture_ref` |
| Montant / sens | `montant_acompte`, `report_mois_precedent`, `mode_paiement_id`, `code_impact` |
| Contrôle propre au lot | `ACOMPTE_NON_RATTACHE_FACTURE` (BLOQUANT si `facture_ref` vide hors `EXCLU_RESULTAT`) |

**Objet distinct** : la migration `0025` (`mouvements_tresorerie_proprietaires`) est explicitement
documentée comme un objet **différent** de Lot 5 (« Lot5 conserve son rôle historique, non modifié,
non renommé »). Elle porte les natures `ACOMPTE_/REMBOURSEMENT_/REGULARISATION_/COMPENSATION_/
AVANCE_/RESTITUTION_PROPRIETAIRE|AUTRE_A_CONTROLER`, les sens `PROPRIETAIRE_VERS_SOCIETE` /
`SOCIETE_VERS_PROPRIETAIRE`, les statuts `BROUILLON|A_CONTROLER|VALIDE|ANNULE`, avec historique
append-only et rattachement au rapprochement bancaire générique (`banque_rapprochements`,
migration `0015`) via `type_objet = REVERSEMENT_PROPRIETAIRE`. **Ce moteur n'a pas été reconstruit.**

## 2. Source Lot 5 disponible — mesure exacte

| Source | Contenu |
|---|---:|
| `SAISIE_AcomptesProprietaires.xlsx` onglet `SAISIE` (source réelle, lue en read-only) | **0 ligne** |
| `MASTER_FACT_MAN_AcomptesProprietaires.xlsx` onglet `MASTER` (sortie réelle) | **0 ligne** |
| `mouvements_tresorerie_proprietaires` (base de recette, migration 26) | **0 ligne** |
| `mouvements_tresorerie_proprietaires` (base réelle `05_APPLICATION/data/app.db`) | **table absente** — la base réelle est en migration `0016`, la table est créée en `0025` |
| `proprietaires_releves`, `proprietaires_paiement`, `proprietaires_releve_cycle`, `proprietaires_releve_evenements` (base réelle) | 0 ligne chacune |

`REF_LOCALE` du classeur de saisie contient bien 16 lignes de référentiel (propriétaires, logements,
modes de paiement) — mais c'est du référentiel, **pas un objet économique**.

**Conclusion factuelle : aucun objet métier propriétaire n'existe nulle part dans le dépôt.** Le
contrôle moteur `LOT5_PREREQUIS_MANQUANT` émis par Lot 8c disait déjà exactement cela ; il est
confirmé, pas contourné.

## 3. Les 56 mouvements — structure mesurée

Tous issus de `BANQUE_LOT8_IMPORT.xlsx` onglet `RAPPROCH_PROPRIETAIRES_ATTENTE`, simulation
canonique fraîche (Banque réelle reconstruite sur copies).

| Caractéristique | Valeur |
|---|---|
| Nombre | 56 |
| Sens | **100 % CREDIT** (entrants, propriétaire → société) |
| Montant total | 27 069,18 € |
| Période | 2025-11-10 → 2026-07-06 |
| `nature_presumee` moteur | `ENCAISSEMENT_PROPRIETAIRE_A_VENTILER` (56/56) — un libellé d'attente, **pas** une nature établie |
| Statut moteur | `EN_ATTENTE_SAISIE_ACOMPTE` (56/56) |
| Montants répétés à l'identique | **0** — chaque montant est unique, aucune récurrence exploitable |

### Ventilation par identité candidate

| Identité (opaque) | Nb | Montant cumulé | Période |
|---|---:|---:|---|
| PROP_0009 | 16 | 8 839,74 € | 2025-11 → 2026-07 |
| PROP_0002 | 9 | 5 702,02 € | 2025-11 → 2026-07 |
| PROP_0006 | 9 | 4 040,24 € | 2025-11 → 2026-07 |
| PROP_0008 | 8 | 4 577,88 € | 2025-11 → 2026-07 |
| `FAMILLE_UZON_A_CONTROLER` | 7 | 1 161,69 € | 2025-11 → 2026-05 |
| PROP_0010 | 5 | 1 547,61 € | 2025-11 → 2026-06 |
| PROP_0005 | 2 | 1 200,00 € | 2026-04 → 2026-05 |
| **TOTAL** | **56** | **27 069,18 €** | |

## 4. Identité : candidate ≠ prouvée

Les `proprietaire_id` ci-dessus proviennent de **règles de libellé bancaire** (`lot8b_banque_regles.py`,
règles `R_071`+ : « CONTIENT <nom> → PROP_xxxx »). C'est une **IDENTITÉ_CANDIDATE**, obtenue par
correspondance de chaîne sur le libellé du virement — **pas une IDENTITÉ_PROUVÉE** par un objet
métier. Le moteur lui-même ne les valide pas : il les laisse en `RAPPROCHEMENT_REQUIS` /
`A_CONTROLER`, jamais en `VALIDE`.

Le groupe `FAMILLE_UZON_A_CONTROLER` (7 mouvements) est explicitement marqué à contrôler par le
moteur : le libellé ne permet pas de trancher entre plusieurs personnes d'un même foyer. **Identité
non résolue, à confirmer par l'utilisateur.**

## 5. Niveaux de preuve — résultat

| Statut de preuve | Nombre | Justification |
|---|---:|---|
| PREUVE_A (objet métier existant, compatible, sans contradiction) | **0** | Aucun objet Lot 5 / trésorerie n'existe (§2) |
| PREUVE_B (présomption forte, reconstruction indirecte) | **0** | Aucune reconstruction possible sans objet source ; le montant, la date, la récurrence et le nom sont explicitement exclus comme preuves de nature (§9 du cadrage) |
| AMBIGU (plusieurs objets/interprétations) | **0** | Il faudrait au moins deux objets candidats ; il y en a zéro |
| **ABSENT (aucun objet disponible)** | **56** | |
| **TOTAL** | **56** | |

| Résultat de rapprochement | Nombre |
|---|---:|
| EXACT | 0 |
| PARTIEL | 0 |
| GROUPE | 0 |
| AMBIGU | 0 |
| **AUCUN** | **56** |

Somme cohérente : 0+0+0+0+56 = 56.

**Aucun test EXACT/PARTIEL/GROUPE/AMBIGU n'a pu être exercé sur données réelles** — non par échec du
moteur, mais parce qu'il n'existe aucun objet à rapprocher. Le moteur de rapprochement (exact,
partiel, groupé, ambigu, annulation, historique append-only) reste couvert par sa suite de tests sur
fixtures, exécutée cette mission (§8).

## 6. Effet sur les 56

```
56 décisions initiales
→ X résolubles par PREUVE_A : 0
→ Y restant réellement humaines : 56
X + Y = 56 ✔
```

**Aucune réduction déterministe n'est possible.** Chercher à réduire ce chiffre reviendrait à
inventer la nature économique d'un encaissement, ce qui est explicitement interdit.

### Ce que l'utilisateur doit faire (par ordre de levier)

Le travail réel n'est **pas** 56 enquêtes indépendantes. Il se décompose en :

1. **7 confirmations d'identité** (une par ligne du tableau §3), dont une seule réellement ouverte
   (`FAMILLE_UZON_A_CONTROLER`, 7 mouvements, 1 161,69 €) — les 6 autres sont des identités
   candidates à confirmer ou infirmer en bloc.
2. **Décision de nature** : pour chaque identité confirmée, indiquer la nature applicable
   (`ACOMPTE_PROPRIETAIRE`, `REMBOURSEMENT_`, `AVANCE_`, etc.). Si une même nature s'applique à tous
   les mouvements d'un propriétaire, **une seule décision couvre le bloc entier** — mais cette
   uniformité doit être affirmée par l'utilisateur, elle n'est pas déduite ici.
3. **Création des objets** dans Lot 5 (`SAISIE_AcomptesProprietaires.xlsx`) ou en trésorerie
   propriétaires, puis relance de Lot 8c pour le rapprochement automatique.

**Plancher réaliste : 7 décisions** (1 par identité, si nature uniforme par propriétaire).
**Plafond : 56** (si chaque mouvement a une nature propre). L'utilisateur tranche ; aucune
hypothèse n'est prise ici.

## 7. Prérequis technique découvert

La base applicative réelle (`05_APPLICATION/data/app.db`) est en migration **`0016`** ; la table
`mouvements_tresorerie_proprietaires` est créée en migration **`0025`**. La base réelle ne contient
d'ailleurs aucune donnée métier (seulement 49 événements d'audit et quelques artefacts de recette).

**Ce n'est pas un bug** : le mode réel n'a jamais été activé, la base réelle n'a jamais servi. Mais
c'est un **prérequis explicite** : avant toute exploitation réelle de la trésorerie propriétaires,
les migrations `0017`→`0026` devront être appliquées à la base réelle (opération standard, gatée,
non entreprise ici). Consigné pour la checklist mode réel.

## 8. Tests

0 code modifié. Tests métier ciblés sur fixtures (aucune donnée réelle touchée) :
`test_proprietaires_tresorerie_service.py`, `test_proprietaires_tresorerie_routes.py`,
`test_banques_rapprochement.py`, `test_banques_rapprochement_groupe_routes.py`,
`test_rapprochement_contrat_candidats.py`, `test_banques_candidats_service.py` — couvrent création
d'objet, validation, immutabilité du VALIDE, annulation justifiée, historique append-only,
rapprochement exact/partiel/groupé/ambigu, exclusion des candidats `PAYOUT_PLATEFORME`.

## 9. Idempotence

Pipeline Banque (`lot8a` → `lot8b` → `lot8c`) relancé une seconde fois sur la copie : sortie
**strictement identique** — 236 `CLASSE`, 222 `RAPPROCHEMENT_REQUIS`, 83 `A_ENVOYER_IA`,
166 `PAYOUT_PLATEFORME`, 56 `VIREMENT_PROPRIETAIRE_A_RAPPROCHER`. Aucun doublon créé, mêmes statuts,
mêmes montants, même file humaine.

## 10. Question comptable

Les 56 encaissements propriétaires **ne créent aucune question comptable nouvelle**. La trésorerie
propriétaires était déjà identifiée comme un mapping non arbitré (réserve connue, non bloquante), et
la migration `0025` précise qu'un mouvement **ne crée jamais automatiquement une écriture
comptable** — il devient seulement candidat au rapprochement bancaire. La question du compte
définitif se posera au moment de l'arbitrage comptable global, pas ici.

## 11. Verdict de cette partie

- **LOT 5 : FONCTIONNEL** (script, source, sortie, contrôles présents et cohérents) mais
  **NON ALIMENTÉ** (0 ligne).
- **MOUVEMENTS PROPRIÉTAIRES : 56 → 56 décisions humaines restantes** (0 résoluble par preuve),
  compressibles à **7 décisions** si l'utilisateur confirme une nature uniforme par propriétaire.
- **Aucune donnée réelle modifiée. Aucun objet créé. Aucune nature inférée.**

## 12. Chaine Lot 5 -> Banque : deux gaps techniques trouves (2026-08-13)

L'audit du 2026-08-12 concluait « Lot 5 FONCTIONNEL mais NON ALIMENTE ». La repetition generale a
teste la chaine complete et trouve **deux gaps qui auraient fait perdre son travail a
l'utilisateur** apres avoir rempli Lot 5.

### Gap 1 — lot8c n'ouvrait jamais le master Lot 5 (CORRIGE, commit `1759ce0`)

Le prerequis "MASTER_FACT_MAN_AcomptesProprietaires vide - attendre saisie Lot 5" et le controle
`LOT5_PREREQUIS_MANQUANT` etaient des **chaines codees en dur**. `lot8c` ne lit que
`BANQUE_LOT8_IMPORT.xlsx`.

Preuve empirique sur copie : 5 acomptes synthetiques injectes dans SAISIE (montants et
proprietaires identiques a de vrais mouvements), relance lot5 + lot8c -> **sortie strictement
identique**, 56 lignes `EN_ATTENTE_SAISIE_ACOMPTE`, 27 069,18 EUR. L'action demandee a
l'utilisateur ("alimenter Lot 5 puis relancer") n'avait donc aucun effet observable.

Corrige : lecture reelle de l'onglet MASTER. 0 objet -> message et controle inchanges
(non-regression). >0 objet -> `LOT5_REGLES_RAPPROCHEMENT_A_ARBITRER` avec le nombre reel.

### Gap 2 — lot5 ne peuple pas MASTER depuis SAISIE (documente, non corrige)

`lot5_master_acomptes_proprietaires.py` est un **generateur de template**. `build_master()` cree un
onglet MASTER vide avec ses en-tetes et une feuille `POWER_QUERY_CODE`. Il ne lit pas SAISIE.

Mesure : SAISIE 5 lignes -> lot5 -> **MASTER 0 ligne**.

Le peuplement se fait par **refresh Power Query dans Excel**. Le M-code embarque n'est pas trivial :
5 controles BLOQUANT (`ACOMPTE_CALC_ID_DUPLIQUE`, `ACOMPTE_PROPRIETAIRE_ABSENT`,
`ACOMPTE_MONTANT_INVALIDE`, `ACOMPTE_NON_RATTACHE_FACTURE`, `ACOMPTE_HH_INCOHERENT`), 5
A_CONTROLER, validation croisee avec le master HH (Lot 4), detection de doublons `acompte_id`.

**Non reimplemente volontairement** : reproduire cette logique de controle en Python est une vraie
fonctionnalite, avec un risque reel de diverger subtilement des regles metier existantes. Ce n'est
pas un bug — c'est l'architecture prevue, coherente avec le fait que la saisie est humaine et se
fait dans Excel.

**Consequence operationnelle a connaitre** : apres avoir rempli
`SAISIE_AcomptesProprietaires.xlsx`, il faut **ouvrir le classeur MASTER dans Excel et actualiser
les requetes** avant de relancer lot8c. Sans cette etape, MASTER reste vide et lot8c le signalera
correctement (depuis le gap 1 corrige).

### Etat de la chaine apres correction

| Etape | Etat |
|---|---|
| Saisie humaine dans `SAISIE_AcomptesProprietaires.xlsx` | disponible |
| SAISIE -> MASTER | **manuel (refresh Power Query dans Excel)** |
| MASTER lu par lot8c | **OUI depuis le fix** (etait : jamais) |
| Rapprochement MASTER <-> mouvements Banque | **regles metier non arbitrees** — aucun rapprochement automatique |

Le dernier point est le seul reste, et c'est une **decision metier**, pas un manque technique.

## 13. Decisions utilisateur enregistrees (2026-08-13)

### 13.1 Historique proprietaires : granularite non differenciee

Decision utilisateur, verbatim resume : « Pour l'historique, ca n'a pas d'interet de differencier
finement les differents types de reglements proprietaires. En revanche, A L'AVENIR, les futurs
reglements devront etre differencies correctement selon leur vraie nature au moment de leur
saisie/validation. »

Portee exacte :
- **Historique existant (les 56 mouvements)** : ne pas reconstruire une granularite metier qui
  n'existait pas dans les donnees sources. Aucune nature precise n'est pour autant inventee ici.
- **Futur** : chaque nouveau reglement proprietaire doit conserver sa vraie nature au moment de la
  saisie (acompte, remboursement, avance, restitution, regularisation, compensation, ou autre
  nature existante appropriee).

**Le traitement simplifie de l'historique ne doit JAMAIS devenir le comportement futur.** Voir
§13.4.

### 13.2 Valeur technique neutre : elle existe deja, rien a inventer

Le contrat `mouvements_tresorerie_proprietaires` (migration 0025) impose une nature parmi 7 valeurs
fermees. La 7e est explicitement neutre :

| Valeur | Sens | Neutre ? |
|---|---|---|
| `ACOMPTE_PROPRIETAIRE` | avance sur facture a venir | non — affirme un rattachement facture |
| `REMBOURSEMENT_PROPRIETAIRE` | remboursement d'une depense avancee | non |
| `REGULARISATION_PROPRIETAIRE` | correction d'un ecart anterieur | non |
| `COMPENSATION_PROPRIETAIRE` | compensation entre creances/dettes | non |
| `AVANCE_PROPRIETAIRE` | avance de tresorerie | non |
| `RESTITUTION_PROPRIETAIRE` | restitution de fonds | non |
| **`AUTRE_A_CONTROLER`** | **nature non tranchee, signalee comme telle** | **OUI** |

`AUTRE_A_CONTROLER` fait deja partie du contrat, n'affirme aucune realite economique et reste
visible comme non tranchee. **Le choix reste a l'utilisateur** — il n'est pas fait ici.

Consequences de chaque option, pour information :
- **`AUTRE_A_CONTROLER`** : les 56 entrent, la tresorerie proprietaire devient exploitable, aucune
  affirmation economique fausse n'est ecrite. Contrepartie : les mouvements resteront signales
  comme non tranches dans les controles, ce qui est exact.
- **Une nature precise appliquee en bloc** (ex. tout en `ACOMPTE_PROPRIETAIRE`) : les controles
  seraient silencieux, mais le systeme affirmerait une nature qui n'a pas ete etablie —
  contradictoire avec la decision §13.1 qui dit seulement que la distinction n'a pas d'interet,
  pas qu'une nature particuliere est vraie.

### 13.3 FAMILLE_UZON dissocie : deux proprietaires distincts

Decision utilisateur : le groupe `FAMILLE_UZON_A_CONTROLER` doit etre **entierement dissocie**.

| Proprietaire | Logement | Preuve referentiel |
|---|---|---|
| **Maryline UZON** = `PROP_0011` | `LOG_0015` « Studio - 97 » (nom officiel « Studio - 97 (Maryline) ») | `REF_Proprietaires` + `REF_Gestion_Logements_Hist` |
| **Didier UZON** = `PROP_0001` | `LOG_0001` « Studio - 46 », adresse **46 allee Charles de Fitte** | idem |

**INTERDIT, definitivement** : creer un proprietaire « Famille Uzon », fusionner les deux,
partager une tresorerie, partager une identite metier, rattacher automatiquement un mouvement de
l'un a l'autre. Ils restent separes partout : referentiel, tresorerie, Lot 5, Banque, releves,
comptabilite auxiliaire, historique, controles.

### 13.4 Regle permanente pour les futurs reglements

**A compter de la mise en exploitation reelle, tout nouveau mouvement de tresorerie proprietaire
doit etre saisi avec sa nature reelle.** Le traitement simplifie applique a l'historique
(§13.1-13.2) est une mesure de rattrapage ponctuelle, liee a l'absence de granularite dans les
donnees sources d'avant l'exploitation. Il **ne constitue pas** une regle generale et ne doit pas
etre repris comme comportement par defaut.

Note a l'attention d'un futur developpeur ou d'une future session : si vous trouvez un lot
historique de mouvements proprietaires portant tous la meme nature, **ce n'est pas un modele a
imiter**. C'est la trace d'une decision explicite et datee, limitee aux mouvements anterieurs a la
mise en exploitation reelle.

## 14. Audit des 7 mouvements UZON — identite etablie (2026-08-13)

Les 7 mouvements portent **tous le meme libelle bancaire explicite** : `VIR MLLE MARIE-LINE UZON`
(deux d'entre eux avec une reference de virement en suffixe).

| Mouvement (opaque) | Date | Sens | Montant | Maryline | Didier | Ambigu | Preuve |
|---|---|---|---:|:---:|:---:|:---:|---|
| …78964C | 2025-11-11 | CREDIT | 100,46 € | X | | | prenom explicite au libelle |
| …4EB02B | 2025-12-09 | CREDIT | 230,47 € | X | | | idem |
| …FCC52E | 2026-02-03 | CREDIT | 169,45 € | X | | | idem |
| …591EB1 | 2026-02-03 | CREDIT | 138,08 € | X | | | idem |
| …D5204A | 2026-03-19 | CREDIT | 304,49 € | X | | | idem |
| …2A1C9F | 2026-05-05 | CREDIT | 35,00 € | X | | | idem + reference de virement |
| …709CAB | 2026-05-05 | CREDIT | 183,74 € | X | | | idem + reference de virement |

**MARYLINE (`PROP_0011`) : 7 — DIDIER (`PROP_0001`) : 0 — AMBIGU : 0.** Total 7 ✔
Montant concerne : **1 161,69 €**.

### Pourquoi ces 7 etaient marques ambigus

La regle de classification `R_074` cherche uniquement la chaine **« UZON »** dans le libelle,
c'est-a-dire le seul nom de famille — partage par les deux proprietaires. Elle ne regarde pas le
prenom. Tout virement Uzon tombait donc mecaniquement dans un groupe « a controler ».

Or le prenom **est present** dans les 7 libelles, et il ne designe qu'une seule des deux personnes :
`MARIE-LINE` (variante d'ecriture de `Maryline` au referentiel) — jamais `DIDIER`. L'identite
etait donc etablissable sans rien deviner.

Variante d'ecriture signalee par honnetete : la banque ecrit `MARIE-LINE`, le referentiel
`Maryline`. L'ecart est purement orthographique et ne cree aucune ambiguite entre les deux
proprietaires Uzon, dont les prenoms (Maryline / Didier) sont sans rapport.

### Consequence

Aucun des 7 mouvements ne necessite d'arbitrage d'identite. **Zero mouvement ne doit etre rattache
a Didier UZON.** Reste a trancher pour ces 7, comme pour les 49 autres : uniquement la **nature**
(§13.1-13.2), pas l'identite.

**Aucune ecriture reelle n'a ete faite** : ni Lot 5, ni Banque, ni referentiel, ni base applicative.
La regle `R_074` n'a pas ete modifiee — l'affiner (distinguer les prenoms) serait une correction
possible, mais elle touche la classification Banque, gelee jusqu'a la reponse utilisateur.
