"""Adaptateur TEMPORAIRE : SQLite → classeur attendu par les moteurs Lot8c / Lot11.

CE QUE CE MODULE EST, ET SURTOUT CE QU'IL N'EST PAS
Les moteurs `lot8c_rapprochement_banque.py` et `lot11_controles_coherence.py` lisent encore un
classeur. Les migrer relève d'un chantier distinct (Lot9/10/11), et les réécrire ici recréerait leurs
règles dans l'application — c'est-à-dire deux moteurs de contrôle divergents. En attendant, ce module
FABRIQUE le classeur qu'ils attendent, à partir de la base.

Ce fichier fabriqué :
  · vit dans un workspace runtime jetable, jamais dans l'arbre du projet ;
  · n'est source de vérité pour rien — SQLite l'est ;
  · n'est lu par AUCUN service applicatif, seulement par le sous-processus moteur ;
  · est supprimable sans perte.

Il ne doit jamais devenir un MASTER. S'il se met à être lu ailleurs que par le moteur, c'est que la
frontière a bougé et qu'il faut la remettre en place, pas l'élargir.

LA CLASSIFICATION SIMULÉE
`mouvement_classe` permet de produire la même donnée avec un mouvement donné comme classé plutôt que
`RAPPROCHEMENT_REQUIS`. C'est ce qui permet de faire dire au MOTEUR — et non à l'application — si une
décision humaine fait réellement disparaître un contrôle. La base n'est jamais modifiée pour cela.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from app.services import banque_classification_service as cls
from app.services import banque_vues_service as vues

# Onglets réellement lus par les moteurs, relevés un par un dans leur source (`BNQ_FILE` pour lot11,
# `BANQUE_PATH` pour lot8c) — jamais devinés :
#   lot8c  : NORM_Banque, LOG_Traitement   (il crée lui-même les RAPPROCH_* et CTRL_RAPPROCHEMENT_8C)
#   lot11  : NORM_Banque, CTRL_A_CONTROLER, REF_Cloture_Mensuelle, RAPPROCH_AIRBNB_ATTENTE
#
# La liste doit être COMPLÈTE : lot11 lit ses onglets dans un seul bloc protégé et, s'il en manque un,
# conclut « Banque non disponible » — donc zéro anomalie bancaire, avec un code retour 0. Un onglet
# oublié ne casse rien visiblement ; il fait simplement disparaître les contrôles.
# En écrire davantage, en revanche, donnerait l'illusion d'un classeur complet, qu'il n'est pas.
ONGLET_NORM = "NORM_Banque"
ONGLET_CTRL = "CTRL_A_CONTROLER"
ONGLET_LOG = "LOG_Traitement"
ONGLET_CLOTURE = "REF_Cloture_Mensuelle"
ONGLET_RAPPRO_PLATEFORME = "RAPPROCH_AIRBNB_ATTENTE"

# Le statut de clôture vit dans le référentiel, pas dans la Banque ; le classeur bancaire le portait
# par commodité de fabrication. On le reproduit à l'identique pour ne rien changer au moteur.
COLONNES_CLOTURE = ("mois", "statut_mois", "date_passage_controle", "date_cloture",
                    "nb_lignes_bancaires_non_classees", "nb_controles_bloquants_ouverts",
                    "commentaire")

# Statut que prend un mouvement dont on simule la classification.
STATUT_CLASSE = cls.CLASS_CLASSE


def _lignes_norm(mouvement_classe: str = "", db_path=None) -> list[dict[str, Any]]:
    lignes = vues.mouvements_normalises(db_path=db_path)
    if not mouvement_classe:
        return lignes
    return [{**l, "statut_classification": STATUT_CLASSE, "statut_controle": cls.ST_VALIDE}
            if l["mouvement_id"] == mouvement_classe else l
            for l in lignes]


def ecrire_classeur_moteur(chemin: Path, *, mouvement_classe: str = "",
                           db_path=None) -> dict[str, Any]:
    """Écrit le classeur d'entrée des moteurs. Retourne de quoi tracer ce qui a été produit.

    Refuse si la Banque n'est pas initialisée : produire un classeur vide ferait conclure au moteur
    « aucune anomalie bancaire », ce qui est faux et indétectable en aval.
    """
    import openpyxl

    if not vues.initialisee(db_path=db_path):
        return {"ok": False, "code": vues.ETAT_NON_INITIALISEE,
                "message": vues.MESSAGES[vues.ETAT_NON_INITIALISEE]}

    from app.services import referentiel_service as ref

    norm = _lignes_norm(mouvement_classe, db_path=db_path)
    ctrl = vues.controles_a_controler(db_path=db_path)
    log = vues.journal_traitement(db_path=db_path)
    cloture = ref.cloture_mensuelle(db_path=db_path)
    rappro = vues.attentes_plateforme(db_path=db_path)

    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for nom, colonnes, lignes in (
        (ONGLET_NORM, vues.COLONNES_NORM, norm),
        (ONGLET_CTRL, vues.COLONNES_CTRL, ctrl),
        (ONGLET_LOG, vues.COLONNES_LOG, log),
        (ONGLET_CLOTURE, COLONNES_CLOTURE, cloture),
        (ONGLET_RAPPRO_PLATEFORME, vues.COLONNES_RAPPRO_PLATEFORME, rappro),
    ):
        ws = wb.create_sheet(nom)
        ws.append(list(colonnes))
        for ligne in lignes:
            ws.append([ligne.get(c) for c in colonnes])

    chemin = Path(chemin)
    chemin.parent.mkdir(parents=True, exist_ok=True)
    wb.save(str(chemin))
    wb.close()

    return {"ok": True, "chemin": str(chemin), "nb_mouvements": len(norm),
            "nb_controles": len(ctrl), "nb_mois_clotures": len(cloture),
            "nb_attentes_plateforme": len(rappro),
            "mouvement_classe": mouvement_classe or None}
