"""Contrôles métier des mouvements de trésorerie propriétaires — portage des contrôles Lot 5.

CE QUE CE MODULE REMPLACE
`lot5_master_acomptes_proprietaires.py` déclarait dix contrôles, exécutés par cinq requêtes Power
Query dans `SAISIE_AcomptesProprietaires.xlsx` puis figés dans
`MASTER_FACT_MAN_AcomptesProprietaires.xlsx`. Les dix sont repris ici, sur les mouvements de
trésorerie en base. Aucun n'est simplifié ni « amélioré » : les niveaux (BLOQUANT / A_CONTROLER) et
les codes sont ceux du Lot 5, pour que deux exécutions restent comparables.

TABLE DE CORRESPONDANCE — contrôle Lot 5 → fonction Python
    ACOMPTE_NON_RATTACHE_FACTURE       BLOQUANT      _reference_metier_absente
    ACOMPTE_HH_INCOHERENT              BLOQUANT      _source_non_verifiable
    ACOMPTE_CALC_ID_DUPLIQUE           BLOQUANT      _doublon_metier
    ACOMPTE_PROPRIETAIRE_ABSENT        BLOQUANT      _proprietaire_absent
    ACOMPTE_MONTANT_INVALIDE           BLOQUANT      _montant_invalide
    ACOMPTE_LOGEMENT_ABSENT            A_CONTROLER   _logement_absent
    ACOMPTE_SOURCE_HH_INTROUVABLE      A_CONTROLER   _source_introuvable
    ACOMPTE_REPORT_INCOHERENT          A_CONTROLER   _justification_absente
    ACOMPTE_SOURCE_A_CONTROLER         A_CONTROLER   _nature_a_controler
    ACOMPTE_FACTURE_REF_FORMAT_INVALIDE A_CONTROLER  _reference_format_invalide

TROIS TRADUCTIONS QUI MÉRITENT D'ÊTRE DITES

1. `ACOMPTE_CALC_ID_DUPLIQUE` cherchait un `acompte_id` présent deux fois — un risque propre à une
   feuille de calcul, où rien n'empêche de recopier une ligne. En base, `mouvement_opaque` est
   UNIQUE : le doublon d'identifiant ne peut littéralement plus exister. Le risque RÉEL qui subsiste
   est le doublon MÉTIER — le même versement saisi deux fois sous deux identifiants. C'est celui-là
   qui est contrôlé, car c'est celui qui fausse un compte propriétaire.

2. `ACOMPTE_HH_INCOHERENT` comparait le montant à celui de la réservation hors Hostaway du Lot 4.
   Ces réservations ne sont pas encore en base. Plutôt que de conclure « cohérent » faute de pouvoir
   comparer — ce qui transformerait une vérification absente en validation —, le contrôle signale
   explicitement que la source n'est pas vérifiable. Il redeviendra une vraie comparaison quand le
   Lot 4 sera migré.

3. `ACOMPTE_REPORT_INCOHERENT` visait un report de mois exigeant une justification. Le mouvement de
   trésorerie n'a pas de colonne « report » ; ce qu'il porte, c'est une NATURE. Le contrôle vise donc
   les natures qui traduisent un ajustement — régularisation, compensation — laissées sans
   justification : même exigence, exprimée dans le vocabulaire du modèle.

CE QUE CE MODULE NE FAIT PAS
Il ne bloque rien et ne modifie aucun mouvement : il rend des constats. La décision de corriger, de
justifier ou d'ignorer appartient à l'utilisateur, comme dans le classeur.
"""
from __future__ import annotations

from typing import Any

from app.services import proprietaires_tresorerie_service as tresorerie

NIVEAU_BLOQUANT = "BLOQUANT"
NIVEAU_A_CONTROLER = "A_CONTROLER"

C_REFERENCE_ABSENTE = "ACOMPTE_NON_RATTACHE_FACTURE"
C_SOURCE_INCOHERENTE = "ACOMPTE_HH_INCOHERENT"
C_DOUBLON = "ACOMPTE_CALC_ID_DUPLIQUE"
C_PROPRIETAIRE_ABSENT = "ACOMPTE_PROPRIETAIRE_ABSENT"
C_MONTANT_INVALIDE = "ACOMPTE_MONTANT_INVALIDE"
C_LOGEMENT_ABSENT = "ACOMPTE_LOGEMENT_ABSENT"
C_SOURCE_INTROUVABLE = "ACOMPTE_SOURCE_HH_INTROUVABLE"
C_JUSTIFICATION_ABSENTE = "ACOMPTE_REPORT_INCOHERENT"
C_NATURE_A_CONTROLER = "ACOMPTE_SOURCE_A_CONTROLER"
C_REFERENCE_FORMAT = "ACOMPTE_FACTURE_REF_FORMAT_INVALIDE"

NIVEAUX = {
    C_REFERENCE_ABSENTE: NIVEAU_BLOQUANT,
    C_SOURCE_INCOHERENTE: NIVEAU_BLOQUANT,
    C_DOUBLON: NIVEAU_BLOQUANT,
    C_PROPRIETAIRE_ABSENT: NIVEAU_BLOQUANT,
    C_MONTANT_INVALIDE: NIVEAU_BLOQUANT,
    C_LOGEMENT_ABSENT: NIVEAU_A_CONTROLER,
    C_SOURCE_INTROUVABLE: NIVEAU_A_CONTROLER,
    C_JUSTIFICATION_ABSENTE: NIVEAU_A_CONTROLER,
    C_NATURE_A_CONTROLER: NIVEAU_A_CONTROLER,
    C_REFERENCE_FORMAT: NIVEAU_A_CONTROLER,
}

# Format de référence attendu, repris du Lot 5 : FAC-AAAA-MM-PROP-NNN. Le contrôle Lot 5 ne
# vérifiait que le préfixe ; on s'en tient là plutôt que de durcir une règle que personne n'a décidée.
PREFIXE_REFERENCE = "FAC-"

# Natures traduisant un ajustement, qui appellent une justification écrite.
NATURES_AJUSTEMENT = ("REGULARISATION_PROPRIETAIRE", "COMPENSATION_PROPRIETAIRE")

# Un mouvement annulé n'est plus soumis aux contrôles : il ne produit plus d'effet, et le signaler
# encombrerait la liste de constats sans objet.
STATUTS_CONTROLES = (tresorerie.ST_BROUILLON, tresorerie.ST_A_CONTROLER, tresorerie.ST_VALIDE)


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _constat(mouvement: dict[str, Any], code: str, detail: str) -> dict[str, Any]:
    return {
        "mouvement_opaque": mouvement.get("mouvement_opaque"),
        "proprietaire_id": mouvement.get("proprietaire_id"),
        "date_mouvement": mouvement.get("date_mouvement"),
        "montant": mouvement.get("montant"),
        "code_controle": code,
        "niveau": NIVEAUX[code],
        "detail": detail,
    }


# ── Les dix contrôles ───────────────────────────────────────────────────────────────────────────

def _reference_metier_absente(m: dict[str, Any]) -> str:
    if not _txt(m.get("reference_metier")):
        return "Aucune référence de facture : le mouvement n'est rattaché à rien."
    return ""


def _source_non_verifiable(m: dict[str, Any]) -> str:
    """Le montant vient d'un objet source qu'on ne sait pas encore relire.

    Signaler l'impossibilité de vérifier, plutôt que de laisser croire à une vérification faite.
    """
    if _txt(m.get("source_type")) == "OBJET_VALIDE" and _txt(m.get("source_id")):
        return (f"Montant issu de l'objet {_txt(m.get('source_id'))}, non relisible en base : "
                "cohérence du montant non vérifiée.")
    return ""


def _proprietaire_absent(m: dict[str, Any]) -> str:
    if not _txt(m.get("proprietaire_id")):
        return "Propriétaire non renseigné."
    return ""


def _montant_invalide(m: dict[str, Any]) -> str:
    montant = m.get("montant")
    try:
        valeur = float(montant)
    except (TypeError, ValueError):
        return f"Montant non numérique : {montant!r}."
    if valeur <= 0:
        return f"Montant nul ou négatif : {valeur:.2f}."
    return ""


def _logement_absent(m: dict[str, Any]) -> str:
    if not _txt(m.get("logement_id")):
        return "Aucun logement : la ventilation par logement ne pourra pas être faite."
    return ""


def _source_introuvable(m: dict[str, Any]) -> str:
    if _txt(m.get("source_id")) and _txt(m.get("source_type")) != "OBJET_VALIDE":
        return (f"Source {_txt(m.get('source_id'))} renseignée alors que le mouvement est déclaré "
                f"{_txt(m.get('source_type')) or 'sans type'}.")
    return ""


def _justification_absente(m: dict[str, Any]) -> str:
    if _txt(m.get("nature")) in NATURES_AJUSTEMENT and not _txt(m.get("justification")):
        return f"Nature {_txt(m.get('nature'))} sans justification écrite."
    return ""


def _nature_a_controler(m: dict[str, Any]) -> str:
    if _txt(m.get("nature")) == "AUTRE_A_CONTROLER":
        return "Nature AUTRE_A_CONTROLER : qualification à confirmer."
    return ""


def _reference_format_invalide(m: dict[str, Any]) -> str:
    ref = _txt(m.get("reference_metier"))
    if ref and not ref.startswith(PREFIXE_REFERENCE):
        return f"Référence « {ref} » hors format attendu ({PREFIXE_REFERENCE}AAAA-MM-PROP-NNN)."
    return ""


# Ordre d'évaluation : bloquants d'abord, comme l'onglet CONTROLES_SAISIE les présentait.
_CONTROLES_LIGNE = (
    (C_REFERENCE_ABSENTE, _reference_metier_absente),
    (C_SOURCE_INCOHERENTE, _source_non_verifiable),
    (C_PROPRIETAIRE_ABSENT, _proprietaire_absent),
    (C_MONTANT_INVALIDE, _montant_invalide),
    (C_LOGEMENT_ABSENT, _logement_absent),
    (C_SOURCE_INTROUVABLE, _source_introuvable),
    (C_JUSTIFICATION_ABSENTE, _justification_absente),
    (C_NATURE_A_CONTROLER, _nature_a_controler),
    (C_REFERENCE_FORMAT, _reference_format_invalide),
)


def _cle_doublon(m: dict[str, Any]) -> tuple:
    """Ce qui fait qu'un versement est « le même » : même propriétaire, jour, montant, nature, sens.

    La référence de facture est volontairement EXCLUE : deux saisies du même versement portent
    souvent la même référence, et l'inclure ne changerait rien ; en revanche, deux versements
    légitimes du même montant le même jour se distinguent rarement autrement — d'où un constat à
    contrôler, jamais une fusion.
    """
    return (_txt(m.get("proprietaire_id")), _txt(m.get("date_mouvement"))[:10],
            round(float(m.get("montant") or 0), 2), _txt(m.get("nature")), _txt(m.get("sens")))


def _doublon_metier(mouvements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groupes: dict[tuple, list[dict[str, Any]]] = {}
    for m in mouvements:
        groupes.setdefault(_cle_doublon(m), []).append(m)
    constats = []
    for groupe in groupes.values():
        if len(groupe) < 2:
            continue
        autres = ", ".join(_txt(x.get("mouvement_opaque")) for x in groupe)
        # Toutes les occurrences sont signalées, pas seulement les suivantes : on ne sait pas
        # laquelle est la bonne, et désigner arbitrairement la première comme légitime orienterait
        # la correction.
        for m in groupe:
            constats.append(_constat(m, C_DOUBLON,
                                     f"Mouvement identique à {len(groupe) - 1} autre(s) : {autres}."))
    return constats


# ── Exécution ───────────────────────────────────────────────────────────────────────────────────

def controler(*, proprietaire_id: str = "", db_path=None) -> dict[str, Any]:
    """Applique les dix contrôles et rend les constats. N'écrit rien, ne bloque rien."""
    mouvements = [m for m in tresorerie.lister(proprietaire_id=proprietaire_id, db_path=db_path)
                  if _txt(m.get("statut")) in STATUTS_CONTROLES]

    constats: list[dict[str, Any]] = []
    for m in mouvements:
        for code, controle in _CONTROLES_LIGNE:
            detail = controle(m)
            if detail:
                constats.append(_constat(m, code, detail))
    constats.extend(_doublon_metier(mouvements))

    par_code: dict[str, int] = {}
    for c in constats:
        par_code[c["code_controle"]] = par_code.get(c["code_controle"], 0) + 1

    return {
        "nb_mouvements": len(mouvements),
        "nb_constats": len(constats),
        "nb_bloquants": sum(1 for c in constats if c["niveau"] == NIVEAU_BLOQUANT),
        "nb_a_controler": sum(1 for c in constats if c["niveau"] == NIVEAU_A_CONTROLER),
        "par_code": par_code,
        "constats": constats,
    }


def codes_connus() -> tuple[str, ...]:
    """Les dix codes du Lot 5, dans l'ordre où le classeur les déclarait."""
    return tuple(NIVEAUX)
