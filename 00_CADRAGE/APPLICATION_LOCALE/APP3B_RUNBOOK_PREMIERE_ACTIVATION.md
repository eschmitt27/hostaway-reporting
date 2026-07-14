# APP-3b — Runbook de la PREMIÈRE écriture réelle d'une charge

> **CE RUNBOOK N'A PAS ÉTÉ EXÉCUTÉ.** Les flags sont à `False` et aucune charge réelle n'a jamais
> été écrite. Ce document décrit la recette humaine à mener, une fois, sous supervision.
>
> Il ne s'agit pas d'une procédure routinière : c'est la **première** mise en écriture d'une chaîne
> qui n'a jamais touché un fichier réel. Traiter chaque étape comme un point d'arrêt.

---

## Avant de commencer

- Prévoir **une heure calme**, sans risque d'interruption.
- Choisir une charge **réelle, simple et contrôlable** : montant modeste, mois **ouvert**,
  catégorie standard, **sans** ménage, **sans** réserve de refacturation, **sans** avantage associé.
  On ajoutera les cas complexes plus tard, une fois le socle éprouvé.
- Ne **jamais** enchaîner deux charges lors de cette première recette.

---

## Phase 1 — Préparation (aucune écriture)

1. **Fermer Excel** entièrement (vérifier le gestionnaire des tâches : aucun `EXCEL.EXE`).
2. **Vérifier l'absence de fichiers `~$`** dans `01_SOURCES_BRUTES/Charges/` et
   `02_TRAVAIL/Lot3_Charges/`, `02_TRAVAIL/Lot7_IK_Avantages/`.
3. **Mettre OneDrive en pause** (icône OneDrive → Pause la synchronisation → 2 heures).
   *C'est la seule protection contre une resynchronisation pendant l'`os.replace` ; aucun verrou
   applicatif ne couvre ce cas.*
4. **Worktree propre** : `git status --short` doit être vide.
5. **Relever les SHA256** des quatre classeurs et de `app.db`, et les **noter** :
   ```powershell
   Get-FileHash '01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx',
                '01_SOURCES_BRUTES\Charges\SAISIE_Charges_Impacts.xlsx',
                '02_TRAVAIL\Lot3_Charges\MASTER_FACT_MAN_Charges.xlsx',
                '02_TRAVAIL\Lot7_IK_Avantages\MASTER_FACT_MAN_IK_Avantages.xlsx',
                '05_APPLICATION\data\app.db' -Algorithm SHA256
   ```
6. **Sauvegarder les deux SAISIE** hors dépôt (copie horodatée dans
   `99_ARCHIVES/APP3B_PREMIERE_ECRITURE_<TS>/`). Vérifier que le dossier est bien ignoré par Git.
7. **Vérifier l'absence de verrou** : `05_APPLICATION/data/.saisie_charges_write.lock` ne doit pas
   exister. S'il existe → §10.1 de `APP3B_CHARGES_CLOTURE.md` **avant** de continuer.
8. **Vérifier que le mois visé est OUVERT** dans `REF_Cloture_Mensuelle`.

## Phase 2 — Activation temporaire des flags

9. Éditer `05_APPLICATION/app/config.py` :
   ```python
   CHARGES_REAL_WRITE_ENABLED = True
   CHARGES_REAL_WRITE_CONFIRMATION_ENABLED = True
   ```
   **Ne pas committer cette modification.** Elle sera annulée en phase 5.
10. Redémarrer l'application.

## Phase 3 — Écriture (le point de non-retour)

11. Saisir la charge dans « Nouvelle charge », puis **prévisualiser**.
12. **Contrôler l'écran de prévisualisation, ligne à ligne** : montant, catégorie, mois, mode de
    paiement, code impact, affectations, effet annoncé sur les fichiers. En cas de doute →
    « Revenir modifier ». Ne jamais confirmer une prévisualisation qu'on ne comprend pas entièrement.
13. **Confirmer UNE SEULE FOIS.** Attendre la page de résultat. Ne pas rafraîchir, ne pas recliquer.

## Phase 4 — Vérifications (selon le dénouement)

### 4.A — Succès complet (`SUCCES`, Lot3/Lot7/Lot11 `OK`)

14. **Journal** : une ligne `SUCCES` dans `saisie_charges_writes`, avec le bon `charge_id`, le token,
    les SHA256 avant/après, et les deux fichiers dans `fichiers_remplaces`.
15. **`SAISIE_Charges_Flux`** : ouvrir dans Excel. **Exactement une** ligne pour ce `charge_id`.
    Vérifier que les colonnes `C` (mois), `I`, `J` (impacts) et `AD` (ROW_HASH) se sont
    **recalculées** à l'ouverture — ce sont des formules.
16. **`SAISIE_Charges_Impacts`** : les affectations/ménage/réserve attendus, et **rien d'autre**.
17. **Lot3** : `MASTER_FACT_MAN_Charges` contient la charge, avec `mois` correct.
18. **Lot7** : seulement si la charge porte un avantage → `code_impact = HR`.
19. **Lot11** : contrôles verts, ou anomalies comprises et justifiées.
20. **Lot9 / Lot10 / Lot12** : relancer si la charge doit impacter le résultat ou le net
    propriétaire, et vérifier que le montant apparaît là où il doit apparaître — et **nulle part
    ailleurs**.

### 4.B — Écriture réussie, recalcul aval en échec

La charge **est** écrite. **Ne pas la ressaisir.** Relancer manuellement Lot3, puis Lot7 si
applicable, puis Lot11. Reprendre au point 14.

### 4.C — `ROLLBACK` (écriture annulée, tout restauré)

Les deux SAISIE doivent avoir **retrouvé leurs SHA256 de l'étape 5**. Le vérifier. Aucune charge
n'a été écrite : comprendre la cause (page de résultat + journal) avant de retenter.

### 4.D — `ROLLBACK_CRITIQUE` / `ETAT_INCOHERENT`

**Arrêter tout.** Ne rien relancer, ne rien ressaisir, ne supprimer aucun `.bak`.
Appliquer §10.2 de `APP3B_CHARGES_CLOTURE.md`.

### 4.E — Refus (flags, verrou, token, validation…)

Aucun fichier n'a été touché : les SHA256 de l'étape 5 sont inchangés. Le vérifier, lire le message,
corriger la cause.

### 4.F — Verrou périmé

Voir §10.1 de `APP3B_CHARGES_CLOTURE.md`. Ne jamais supprimer un verrou sans diagnostic.

## Phase 5 — Retour à l'état sûr (obligatoire, quel que soit le dénouement)

21. **Remettre les deux flags à `False`** dans `config.py`. Redémarrer l'app. Vérifier que le bouton
    « Confirmer » redevient désactivé.
22. **Vérifier l'absence de résidus** : aucun `.lock`, aucun `.bak`, aucun `~$`, aucun temporaire
    dans les dossiers métier.
23. **Réactiver la synchronisation OneDrive** et attendre qu'elle se termine.
24. **Recalculer les SHA256** et les comparer à ceux de l'étape 5 : seuls les fichiers censés avoir
    changé doivent avoir changé.

## Phase 6 — Décision humaine

25. **Conserver ou restaurer.** Si la charge est correcte et cohérente partout : la conserver.
    Sinon, restaurer les deux SAISIE depuis la sauvegarde de l'étape 6, puis régénérer Lot3/Lot7 et
    relancer Lot11.
26. **Consigner** au `JOURNAL_CONTROLES.md` : date, `charge_id`, `transaction_id`, dénouement,
    anomalies, décision (conserver / restaurer).
27. Ce n'est qu'après une première charge réelle conservée et vérifiée que l'on pourra envisager
    d'activer les flags durablement — et ce sera **encore** une décision explicite.
