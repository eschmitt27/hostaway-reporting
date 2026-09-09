"""Applique les 2 estimations de payout VRBO (mois clos) sur la base applicative désignée.

Contexte : contrôle VRBO_MONTANT_NON_RENSEIGNE sur LOG_0008 pour 2026-06 et 2026-08. Le payout réel
est indisponible (l'export VRBO `IMPORT_UNIQUE` s'arrête en mai 2026) et la population de payouts VRBO
fiables présente dans app.db est vide : toutes les lignes VRBO de LOG_0008 ont `montant_retenu = 0`.

MÉTHODE (validée par le métier le 2026-09-09) : moyenne du payout PAR NUIT des réservations VRBO
réelles du MÊME logement, à durée comparable (<= 7 nuits, ce qui écarte les séjours de 20 et 35 nuits)
et sur la période la plus proche (comparables 2026), puis multipliée par le nombre de nuits du séjour.

Ce script n'introduit aucun mécanisme de stockage : il appelle `regularisation_hh_service`, déjà utilisé
par l'écran « Régulariser (saisie HH) », puis réenchaîne le DAG moteur existant. Le montant écrit est
tracé comme ESTIMATION MÉTIER dans le commentaire de la saisie, et porte `source_montant = MANUEL_HH`
côté `reservations_resolues` — jamais présenté comme un payout plateforme réel.

Usage :
    python tools/appliquer_estimations_vrbo_20260909.py <chemin_app.db> [--dry-run]
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services import regularisation_hh_service as reg  # noqa: E402

# Payout moyen par nuit retenu — moyenne des 3 comparables VRBO réels 2026 de LOG_0008
# (HA-1LH1V9 3 nuits 305.00 ; HA-L48JXH 7 nuits 558.69 ; HA-L4DM3B 4 nuits 390.00), montants bruts.
PAYOUT_MOYEN_PAR_NUIT = 92.9932
POPULATION = "HA-1LH1V9 3n 305.00 EUR / HA-L48JXH 7n 558.69 EUR / HA-L4DM3B 4n 390.00 EUR"

# Les deux réservations à estimer. `ctrl_opaque` n'est PAS codé en dur : il n'est pas portable d'une
# base à l'autre et se résout au moment de l'exécution sur la base ciblée (cf. `_ctrl_opaque`).
# Ménage : une saisie HH ne déclenche pas la reprise automatique du coût standard
# (`menage_retenu_source = SAISIE_HH` au lieu de `REF_COUT_STANDARD_MENAGE`). Laissé vide, le ménage
# vaut 0 et l'assiette de commission est alors surévaluée du montant du ménage. On réapplique donc
# explicitement le coût standard du référentiel : `LOG_0008` est de type `TYPE_003` (T3), dont le
# coût standard est 55 € (`COUT_MEN_003`, valide depuis le 2026-01-01) — exactement la valeur que le
# moteur applique de lui-même aux 12 autres réservations du même logement sur ces deux mois.
# Ce n'est donc pas un montant décidé ici, mais la règle existante restaurée.
MENAGE_STANDARD_LOG_0008 = "55"

CAS = [
    {"reservation": "56388919", "mois": "2026-06", "nuits": 2, "montant": "185.99"},
    {"reservation": "57780060", "mois": "2026-08", "nuits": 4, "montant": "371.97"},
]


def _ctrl_opaque(db: Path, reservation_id: str) -> str | None:
    """Retrouve l'élément de contrôle régularisable (VRBO_SANS_MONTANT) de cette réservation.

    Résolution sur la base ciblée : l'identifiant opaque dépend du contenu de la base et ne peut pas
    être repris d'un autre environnement.
    """
    from app.services import controles_actionnable_service as act

    page = 1
    while True:
        tableau = act.load_dashboard(page=page, db_path=db)
        for ligne in tableau["rows"]:
            donnees = ligne.get("donnees") or {}
            if str(donnees.get("reservation_id") or "") != reservation_id:
                continue
            if ligne.get("classification") != "VRBO_SANS_MONTANT":
                continue
            return ligne.get("ctrl_opaque")
        if page >= tableau.get("pages", 1):
            return None
        page += 1


def _commentaire(nuits: int) -> str:
    return (
        "PAYOUT VRBO ESTIME (ESTIMATION METIER - PAS un payout plateforme reel, PAS un montant "
        "Hostaway/VRBO recupere). Methode : moyenne payout/nuit des reservations VRBO REELLES du MEME "
        "logement LOG_0008, durees comparables (<=7 nuits), periode la plus proche (comparables 2026) "
        f"= {PAYOUT_MOYEN_PAR_NUIT} EUR/nuit x {nuits} nuits. Population n=3 : {POPULATION}. "
        "Source : export VRBO IMPORT_UNIQUE (montants bruts, meme convention que RESHH-2025-02-001). "
        "Estime le 2026-09-09."
    )


def corriger_menage(db: Path) -> int:
    """Réapplique le coût standard de ménage sur les deux saisies déjà créées.

    Nécessaire quand les estimations ont été écrites sans `menage` : le contrôle d'origine est alors
    résolu, donc `regulariser()` n'est plus atteignable (il exige un contrôle ouvert). On repasse par
    le service de saisie existant `reservations_hh_saisie_service.modifier()`, qui remplace la
    totalité des champs — la ligne existante est donc relue et réécrite à l'identique, `menage` en
    plus.
    """
    from app.services import reservations_hh_saisie_service as saisie

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    lignes = {
        row["reservation_id_hostaway"]: dict(row)
        for row in conn.execute(
            "SELECT * FROM reservations_hors_hostaway WHERE acteur = 'ESTIMATION_VRBO'")
    }
    conn.close()

    for cas in CAS:
        ligne = lignes.get(cas["reservation"])
        if ligne is None:
            print(f"[BLOQUANT] saisie ESTIMATION_VRBO introuvable pour {cas['reservation']}.")
            return 1
        donnees = {champ: ligne.get(champ) for champ in (
            "mois", "canal_id", "source_financiere", "proprietaire_id", "logement_id",
            "reservation_id_hostaway", "date_arrivee", "date_depart", "montant_percu",
            "code_impact", "impact_resultat_comptable", "statut_controle", "niveau_anomalie",
            "code_anomalie", "commentaire")}
        donnees["menage"] = MENAGE_STANDARD_LOG_0008
        resultat = saisie.modifier(
            ligne["reservation_hh_id"], donnees, acteur="ESTIMATION_VRBO",
            motif=("Application du cout standard de menage du referentiel "
                   f"(COUT_MEN_003, TYPE_003, {MENAGE_STANDARD_LOG_0008} EUR) — une saisie HH ne "
                   "reprend pas automatiquement REF_COUT_STANDARD_MENAGE, l'assiette de commission "
                   "etait donc surevaluee."),
            db_path=db)
        print(f"  menage {MENAGE_STANDARD_LOG_0008} EUR sur {ligne['reservation_hh_id']} "
              f"-> ok={resultat.get('ok')} {resultat.get('message','')}")
        if not resultat.get("ok"):
            return 1
    return 0


def etat(db: Path, tag: str) -> None:
    conn = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    dataset = conn.execute(
        "SELECT dataset_id FROM reservations_datasets WHERE etape='RESOLUES' AND actif=1"
    ).fetchone()[0]
    print(f"\n[{tag}] dataset actif = {dataset}")
    total = 0
    for row in conn.execute(
        "SELECT COALESCE(code_anomalie,'(sans anomalie)') AS code, COUNT(*) AS n "
        "FROM reservations_resolues WHERE dataset_id=? GROUP BY 1 ORDER BY n DESC",
        (dataset,),
    ):
        print(f"    {row['code']:35} {row['n']}")
        total += row["n"]
    print(f"    {'TOTAL LIGNES':35} {total}")
    conn.close()


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    db = Path(sys.argv[1]).resolve()
    dry_run = "--dry-run" in sys.argv
    if not db.exists():
        print(f"[BLOQUANT] base introuvable : {db}")
        return 1

    print(f"base ciblee : {db}")
    etat(db, "AVANT")
    if dry_run:
        print("\n--dry-run : aucune ecriture.")
        return 0

    # Rattrapage : les saisies existent déjà mais sans ménage (contrôle d'origine déjà résolu).
    if "--corriger-menage" in sys.argv:
        if corriger_menage(db) != 0:
            return 1
        print("\n--- recalcul du DAG moteur existant ---")
        recalcul = reg.recalculer(db_path=db)
        for etape in recalcul.get("etapes", []):
            print(f"    {etape['etape']:14} ok={etape['resultat'].get('ok')}")
        if not recalcul.get("ok"):
            print("[BLOQUANT] recalcul incomplet.")
            return 1
        etat(db, "APRES")
        return 0

    for cas in CAS:
        ctrl = _ctrl_opaque(db, cas["reservation"])
        if ctrl is None:
            print(f"[BLOQUANT] aucun controle VRBO_SANS_MONTANT pour {cas['reservation']} — arret.")
            return 1
        print(f"  controle resolu pour {cas['reservation']} : {ctrl}")
        resultat = reg.regulariser(
            ctrl,
            montant_percu=cas["montant"],
            menage=MENAGE_STANDARD_LOG_0008,
            code_impact="HC",
            commentaire=_commentaire(cas["nuits"]),
            acteur="ESTIMATION_VRBO",
            db_path=db,
        )
        libelle = resultat.get("reservation_hh_id") or resultat.get("message", "")
        print(f"  regularisation {cas['reservation']} ({cas['mois']}) {cas['montant']} EUR "
              f"-> ok={resultat.get('ok')} {libelle}")
        if not resultat.get("ok"):
            print("[BLOQUANT] regularisation refusee — arret avant recalcul.")
            return 1

    print("\n--- recalcul du DAG moteur existant ---")
    recalcul = reg.recalculer(db_path=db)
    for etape in recalcul.get("etapes", []):
        message = str(etape["resultat"].get("message", ""))[:80]
        print(f"    {etape['etape']:14} ok={etape['resultat'].get('ok')} {message}")
    if not recalcul.get("ok"):
        print("[BLOQUANT] recalcul incomplet.")
        return 1

    etat(db, "APRES")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
