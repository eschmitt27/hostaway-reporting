"""Écriture de classeurs jetables pour les moteurs legacy — mécanique commune.

À QUOI CELA SERT
Certains lots moteur (Lot 8c, Lot 11) lisent encore des classeurs. Les migrer relève d'un chantier
distinct, et réécrire leurs règles dans l'application produirait deux moteurs de contrôle divergents.
En attendant, on FABRIQUE le classeur qu'ils attendent, depuis la base, dans le workspace du run.

RÈGLES COMMUNES À TOUS LES ADAPTATEURS
Un fichier produit ici :
  · vit dans un workspace de run, jamais dans l'arbre du projet ni dans un dossier permanent ;
  · n'est source de vérité pour rien — SQLite l'est ;
  · n'est lu que par le sous-processus moteur, jamais par un service applicatif ;
  · disparaît avec le workspace.

Il ne doit jamais devenir un MASTER. Écrire un `MASTER_TEMP.xlsx` dans un dossier permanent serait un
faux progrès : on aurait déplacé le fichier sans supprimer la dépendance.

POURQUOI UNE MÉCANIQUE PARTAGÉE
Deux adaptateurs existent (Banque, réservations) et d'autres suivront. Ce qu'ils ont en commun — créer
le dossier, écrire des onglets colonne par colonne, refuser de produire un classeur vide — n'a aucune
raison d'être recopié : une copie qui dérive donnerait deux comportements pour la même garantie.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Sequence

# Un onglet à écrire : son nom, ses colonnes, ses lignes.
Onglet = tuple[str, Sequence[str], Iterable[dict[str, Any]]]


def ecrire_classeur(chemin: Path, onglets: Sequence[Onglet]) -> dict[str, Any]:
    """Écrit un classeur jetable. Retourne de quoi tracer ce qui a été produit.

    Les colonnes sont explicites et l'ordre est celui du moteur : une colonne manquante ou déplacée
    ferait échouer une lecture par index, et certaines de ces lectures échouent SANS erreur — elles
    concluent simplement « source non disponible ».
    """
    import openpyxl

    chemin = Path(chemin)
    chemin.parent.mkdir(parents=True, exist_ok=True)

    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    compte: dict[str, int] = {}
    for nom, colonnes, lignes in onglets:
        ws = wb.create_sheet(nom)
        ws.append(list(colonnes))
        n = 0
        for ligne in lignes:
            ws.append([ligne.get(c) for c in colonnes])
            n += 1
        compte[nom] = n
    wb.save(str(chemin))
    wb.close()

    return {"ok": True, "chemin": str(chemin), "onglets": compte}


def refus(code: str, message: str) -> dict[str, Any]:
    """Refus normalisé d'un adaptateur.

    Produire un classeur vide plutôt que refuser ferait conclure au moteur « aucune anomalie », ce
    qui est faux et indétectable en aval : le code retour resterait 0.
    """
    return {"ok": False, "code": code, "message": message}
