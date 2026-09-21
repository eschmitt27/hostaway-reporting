"""Les gardes bancaires tiennent, et la suite ne dépend d'aucun classeur du poste.

DEUX CHOSES DIFFÉRENTES, LONGTEMPS CONFONDUES.

1. LA GARDE. Six tests touchent des données bancaires ou de contrôle réelles. Sans marqueur
   `skipif`, ils ÉCHOUENT au lieu de SAUTER dans un environnement qui n'a pas ces données — c'est
   arrivé, et c'est ce que `test_les_6_tests_bancaires_portent_une_garde_active` protège.

2. L'ENVIRONNEMENT. Un second test affirmait que `BANQUE_LOT8_IMPORT.xlsx` EXISTE. Il ne protégeait
   aucun comportement : il documentait le poste de celui qui l'avait écrit. Dans un worktree propre
   — donc chez n'importe qui d'autre, et en intégration continue — il tombait au rouge sans qu'une
   seule ligne de code soit en cause. Une suite de tests ne doit jamais dire « rouge » parce qu'un
   fichier personnel manque.

CE QUE L'AUDIT A MONTRÉ. `cfg.MASTER_BANQUE` n'est lu par AUCUN service : dans `app/`, il n'existe
que sa définition, dans `config.py`. L'import bancaire, lui, est bien vivant — mais il travaille
sur les OCTETS d'un fichier déposé par l'utilisateur (`banques_import_service.previsualiser`),
jamais sur un chemin du disque. La fonctionnalité est donc supportée, et le classeur du poste n'en
est pas la source : c'est exactement ce que les deux tests ci-dessous vérifient, à la place de
l'ancienne assertion d'existence.
"""
from pathlib import Path

import openpyxl

import app.config as cfg
from app.services import banques_import_service as imp


def _porte_une_garde_skipif(func) -> bool:
    """Vrai si `func` porte un marqueur skipif, quel que soit l'état actuel de sa condition.

    La version précédente exigeait que la garde soit ARMÉE, c'est-à-dire que le fichier bancaire
    soit absent. Elle ne pouvait donc passer que dans un environnement dépourvu de classeur — et
    tombait au rouge dès que celui-ci était régénéré, alors que la protection recherchée était
    intacte.

    Ce qu'on veut protéger, c'est la PRÉSENCE du décorateur : sans lui, ces tests échoueraient au
    lieu de sauter dans un environnement sans données bancaires. Que la condition soit vraie ou
    fausse ici ne change rien à cette garantie.
    """
    marks = getattr(func, "pytestmark", [])
    return any(m.name == "skipif" and m.args for m in marks)


def test_les_6_tests_bancaires_portent_une_garde_active():
    import test_controles_actionnable as tca
    import test_controles_runner_gardes as tcrg

    cibles = [
        tca.test_05_grain_detaille_correct,
        tca.test_19_lien_mvt_opaque,
        tca.test_20_decision_app4b_visible,
        tca.test_21_22_lot8c_lot11_reellement_executes_reel_intact,
        tca.test_23_24_controle_maintenu_sans_classification,
        tcrg.test_06_reel_intact_apres_gardes,
    ]
    for f in cibles:
        assert _porte_une_garde_skipif(f), (
            f"{f.__name__} a perdu sa garde skipif : dans un environnement sans "
            f"BANQUE_LOT8_IMPORT.xlsx, il échouerait au lieu de sauter.")


def test_aucun_service_ne_lit_le_classeur_bancaire_du_poste():
    """`MASTER_BANQUE` est un chemin de configuration, jamais une source de données applicative.

    C'est l'invariant qui rend la suite indépendante de la machine. Le jour où un service se
    remettrait à ouvrir ce fichier, l'application recommencerait à dépendre d'un classeur que
    personne d'autre n'a — et la moitié des tests bancaires redeviendraient ingérables.

    La définition dans `config.py` est la seule référence admise : le chemin peut exister, il ne
    doit simplement être lu par personne.
    """
    racine = Path(cfg.APP_ROOT) / "app"
    coupables = [
        chemin.relative_to(racine).as_posix()
        for chemin in racine.rglob("*.py")
        if chemin.name != "config.py"
        and "MASTER_BANQUE" in chemin.read_text(encoding="utf-8")
    ]
    assert not coupables, (
        "ces modules lisent le classeur bancaire du poste, ce qui rend l'application et sa suite "
        f"de tests dépendantes d'un fichier non versionné : {coupables}")


def test_l_import_bancaire_ne_depend_pas_d_un_classeur_present_sur_le_poste(
        tmp_db, tmp_path, monkeypatch):
    """Le comportement que l'ancienne assertion d'existence prétendait couvrir, vraiment testé.

    Un relevé au format Excel est FABRIQUÉ ici, sous `tmp_path`, puis importé. Le chemin du
    classeur réel est pointé sur un fichier inexistant pendant toute la durée du test : si quoi
    que ce soit dans la chaîne d'import allait encore le chercher, ce test le dirait.

    `tmp_db` n'est pas décoratif. `previsualiser` compare les lignes lues aux lignes DÉJÀ connues,
    donc il ouvre la base : sans base isolée, le test emprunterait celle du poste — exactement le
    défaut qu'il est censé corriger. Premier jet fait sans elle, et vérifié : il tombait sur un
    worktree propre.
    """
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_path / "CLASSEUR_ABSENT.xlsx")
    assert not Path(cfg.MASTER_BANQUE).exists()

    releve = tmp_path / "releve_fabrique.xlsx"
    classeur = openpyxl.Workbook()
    feuille = classeur.active
    feuille.append(["Date operation", "Libelle", "Debit", "Credit"])
    feuille.append(["05/06/2026", "VIR HOSTAWAY PAYOUT", None, 850.00])
    feuille.append(["06/06/2026", "PRLV ASSURANCE", 400.00, None])
    classeur.save(releve)
    classeur.close()

    resultat = imp.previsualiser(releve.read_bytes(), releve.name, "CM_TEST",
                                 dryruns_root=tmp_path / "dryruns")
    assert resultat["ok"], resultat
    compteurs = resultat["compteurs"]
    assert compteurs["valides"] == 2
    assert compteurs["debits"] == 1 and compteurs["credits"] == 1
    assert compteurs["total_debit"] == 400.0 and compteurs["total_credit"] == 850.0
