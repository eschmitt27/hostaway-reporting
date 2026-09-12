"""Couche métier du référentiel — lecture SQLite, jamais Excel.

POSITION DANS L'ARCHITECTURE
    route → service métier → CE MODULE → ref_setup_repo → SQLite

Les écrans n'interrogent jamais une table directement. Ce module expose des accès nommés par le
métier (« les logements du parc », « les taux d'un propriétaire ») et laisse `ref_setup_repo` faire
la lecture brute.

PARITÉ AVEC L'ANCIEN CHEMIN
Chaque fonction reproduit exactement la sémantique du lecteur Excel qu'elle remplace
(`readers/ref_setup_reader.py`) : mêmes clés, mêmes champs retournés, même absence de tri et de
calcul. Aucune règle économique n'est introduite ici — c'est une migration d'interface de données,
pas une réécriture métier.

FAIL-CLOSED
Si le référentiel n'a jamais été importé, ces fonctions ne se rabattent PAS sur le classeur : elles
signalent l'absence. Un repli silencieux ferait croire à un référentiel vide alors qu'il n'est
qu'absent — deux situations qui appellent des actions opposées.
"""
from __future__ import annotations

from typing import Any

from app.services import ref_setup_repo as repo

# Message unique, pour que tous les écrans disent la même chose.
REFERENTIEL_ABSENT = "REFERENTIEL_NON_INITIALISE"
MESSAGE_ABSENT = (
    "Référentiel non initialisé : les données de paramétrage n'ont pas encore été importées. "
    "Ouvrez « Référentiel Setup » pour les prévisualiser puis les importer."
)


def disponible(*, db_path=None) -> bool:
    return repo.est_disponible(db_path=db_path)


def _txt(v: Any) -> str:
    return str(v or "").strip()


# ── Logements ───────────────────────────────────────────────────────────────────────────────────

def logements(*, db_path=None) -> list[dict[str, str]]:
    """Toutes les lignes de REF_Logements, techniques comprises — le tri du parc se fait plus haut."""
    return repo.lire_table("ref_logements", db_path=db_path)


def logement(logement_id: str, *, db_path=None) -> dict[str, str] | None:
    if not logement_id:
        return None
    return repo.lire_par_cle("ref_logements", _txt(logement_id), db_path=db_path)


# ── Rattachement de gestion ─────────────────────────────────────────────────────────────────────

def gestion_par_logement(*, db_path=None) -> dict[str, list[dict[str, str]]]:
    """{logement_id: [rattachements]}. L'historique est rendu tel quel, jamais arbitré ici."""
    out: dict[str, list[dict[str, str]]] = {}
    for r in repo.lire_table("ref_gestion_logements_hist", db_path=db_path):
        out.setdefault(_txt(r.get("logement_id")), []).append(r)
    return out


def gestion_du_logement(logement_id: str, *, db_path=None) -> list[dict[str, str]]:
    return gestion_par_logement(db_path=db_path).get(_txt(logement_id), [])


# ── Propriétaires ───────────────────────────────────────────────────────────────────────────────

def proprietaires(*, db_path=None) -> list[dict[str, str]]:
    return repo.lire_table("ref_proprietaires", db_path=db_path)


def proprietaire(proprietaire_id: str, *, db_path=None) -> dict[str, str] | None:
    if not proprietaire_id:
        return None
    return repo.lire_par_cle("ref_proprietaires", _txt(proprietaire_id), db_path=db_path)


def nom_proprietaire(proprietaire_id: str, *, db_path=None) -> str:
    """Libellé d'affichage. Chaîne vide si inconnu — jamais un identifiant maquillé en nom."""
    p = proprietaire(proprietaire_id, db_path=db_path)
    return _txt(p.get("nom_proprietaire")) if p else ""


def nom_complet_proprietaire(proprietaire_id: str, *, db_path=None) -> str:
    """« Prénom Nom ». Chaîne vide si inconnu — jamais un identifiant maquillé en nom."""
    p = proprietaire(proprietaire_id, db_path=db_path)
    if not p:
        return ""
    return " ".join(x for x in (_txt(p.get("prenom_proprietaire")), _txt(p.get("nom_proprietaire"))) if x)


def nom_logement(logement_id: str, *, db_path=None) -> str:
    """Nom court d'affichage du logement. Chaîne vide si inconnu.

    Le nom court est préféré — c'est celui que l'exploitant emploie (« T3 - 18 Cugnaux »). Mais les
    deux bacs techniques hors parc ont pour nom court leur propre identifiant
    (`LOGEMENT_DIVERS`, `APPARTEMENT_DIVERS`) : l'afficher reviendrait à montrer un code à
    l'utilisateur, alors que leur nom officiel (« Logement divers (hors parc) ») se lit très bien.
    Le départage ne cite aucun identifiant en dur : si le nom court EST l'identifiant, ce n'est pas
    un nom.
    """
    l = logement(logement_id, db_path=db_path)
    if not l:
        return ""
    court = _txt(l.get("nom_court"))
    officiel = _txt(l.get("nom_logement_officiel"))
    if court and court == _txt(logement_id):
        return officiel or court
    return court or officiel


def libelle_proprietaire(proprietaire_id: str, *, db_path=None) -> str:
    """Libellé sûr pour l'UI : vrai nom si résolu, sinon un texte explicite — jamais un ID brut
    silencieux (mission « vrais noms dans les factures »)."""
    nom = nom_complet_proprietaire(proprietaire_id, db_path=db_path)
    return nom if nom else f"Propriétaire non résolu ({proprietaire_id})"


def libelle_logement(logement_id: str, *, db_path=None) -> str:
    """Libellé sûr pour l'UI : vrai nom si résolu, sinon un texte explicite — jamais un ID brut
    silencieux (mission « vrais noms dans les factures »)."""
    nom = nom_logement(logement_id, db_path=db_path)
    return nom if nom else f"Logement non résolu ({logement_id})"


# ── Canaux de réservation ───────────────────────────────────────────────────────────────────────

def canal(canal_id: str, *, db_path=None) -> dict[str, str] | None:
    if not canal_id:
        return None
    return repo.lire_par_cle("ref_canaux_reservation", _txt(canal_id), db_path=db_path)


def libelle_canal(canal_id: str, *, db_path=None) -> str:
    """« VRBO » plutôt que « CANAL_003 » (recette utilisateur n°3, §5).

    Le code reste stocké en base et utilisable comme `value` HTML ; seul l'affichage change.
    """
    if not _txt(canal_id):
        return ""
    c = canal(canal_id, db_path=db_path)
    nom = _txt(c.get("canal")) if c else ""
    return nom if nom else f"Canal non résolu ({canal_id})"


# ── Types de logement ───────────────────────────────────────────────────────────────────────────

def type_label(type_logement_id: str, *, db_path=None) -> str | None:
    """Décodage code → libellé. Même contrat que `ref_setup_reader.get_type_label`."""
    if not type_logement_id:
        return None
    row = repo.lire_par_cle("ref_types_logements", _txt(type_logement_id), db_path=db_path)
    return row.get("type_logement") if row else None


def libelle_type_logement(type_logement_id: str, *, db_path=None) -> str:
    """Libellé sûr pour l'UI (« Studio », « T2»…) : jamais un ID brut silencieux."""
    if not _txt(type_logement_id):
        return ""
    nom = type_label(type_logement_id, db_path=db_path)
    return nom if nom else f"Type non résolu ({type_logement_id})"


# ── Intervenants (ménage) ───────────────────────────────────────────────────────────────────────

def intervenant(intervenant_id: str, *, db_path=None) -> dict[str, str] | None:
    if not intervenant_id:
        return None
    return repo.lire_par_cle("ref_intervenants", _txt(intervenant_id), db_path=db_path)


def libelle_intervenant(intervenant_id: str, *, db_path=None) -> str:
    """Libellé sûr pour l'UI : vrai nom si résolu, sinon un texte explicite."""
    if not _txt(intervenant_id):
        return ""
    i = intervenant(intervenant_id, db_path=db_path)
    nom = _txt(i.get("nom_intervenant")) if i else ""
    return nom if nom else f"Intervenant non résolu ({intervenant_id})"


# ── Associés ────────────────────────────────────────────────────────────────────────────────────

def associe(personne_id: str, *, db_path=None) -> dict[str, str] | None:
    if not personne_id:
        return None
    return repo.lire_par_cle("ref_associes", _txt(personne_id), db_path=db_path)


def libelle_associe(personne_id: str, *, db_path=None) -> str:
    if not _txt(personne_id):
        return ""
    a = associe(personne_id, db_path=db_path)
    nom = _txt(a.get("nom_personne")) if a else ""
    return nom if nom else f"Associé non résolu ({personne_id})"


# ── Fournisseurs / prestataires (table `fournisseurs`, hors catalogue REF_Setup) ─────────────────

def libelle_fournisseur(fournisseur_id: str, *, db_path=None) -> str:
    """Libellé sûr pour l'UI. `fournisseurs` couvre aussi les prestataires (même table, `type`
    distingue) — un seul resolver pour éviter deux jointures différentes du même concept."""
    if not _txt(fournisseur_id):
        return ""
    from app.services import fournisseurs_referentiel_service as fourn_svc
    f = fourn_svc.charger_par_opaque(_txt(fournisseur_id), db_path=db_path)
    nom = _txt(f.get("nom")) if f else ""
    return nom if nom else f"Fournisseur non résolu ({fournisseur_id})"


libelle_prestataire = libelle_fournisseur


# ── Modes de paiement et catégories de charge ───────────────────────────────────────────────────
# Ces deux référentiels stockent des codes ENUM (`BANQUE_PRO`, `MENAGE`…) : lisibles par une
# machine, pas par un utilisateur. `humaniser_code` les rend présentables sans inventer de
# traduction — il ne fait que retirer les soulignés et poser une majuscule.

def humaniser_code(code: str) -> str:
    """`BANQUE_PRO` → `Banque pro`. Transformation purement typographique, jamais un dictionnaire
    de traductions : un code inconnu reste donc lisible plutôt que d'être remplacé par un libellé
    inventé."""
    texte = _txt(code).replace("_", " ").strip()
    if not texte:
        return ""
    return texte[:1].upper() + texte[1:].lower() if texte.isupper() else texte


#: §51 — libellés canoniques des statuts, là où la transformation mécanique ne suffit pas.
#
# `humaniser_code` retire les soulignés et met une capitale. C'est honnête pour `BANQUE_PRO`, et
# incapable pour `PARTIELLEMENT_REGLEE` : aucune règle typographique ne remet les accents d'un code
# qui n'en porte pas. L'écran Créances montrait donc « Partiellement reglee » dans son filtre, à
# côté de « Partiellement réglée » dans son tableau — le même statut, écrit de deux façons sur le
# même écran, parce que le tableau lisait le libellé du service et le filtre passait par la règle
# mécanique.
#
# Ce registre est la source unique. Il ne traduit QUE ce qui a un libellé métier arrêté ; tout le
# reste continue de passer par la transformation mécanique, qui ne ment jamais.
LIBELLES_STATUT: dict[str, str] = {
    # Règlement d'une créance propriétaire (`creances_dettes_service`).
    "NON_REGLEE": "À régler",
    "PARTIELLEMENT_REGLEE": "Partiellement réglée",
    "REGLEE": "Soldée",
    "TROP_PERCU_A_CONTROLER": "À reverser au propriétaire",
    # Cycle de vie d'une facture fournisseur (`factures_service`).
    "A_CONTROLER": "À contrôler",
    "VALIDEE": "Validée",
    "PARTIELLEMENT_REGLEE_FOURNISSEUR": "Partiellement réglée",
    "LITIGE": "En litige",
    "ANNULEE": "Annulée",
    # Facture propriétaire (`factures_proprietaires_service`).
    "BROUILLON": "Brouillon",
    "VALIDE": "Validée",
    "EMIS": "Émise",
    "ANNULE": "Annulée",
    # Parc et gestion.
    "GERE": "Géré",
    "RETIRE": "Retiré du parc",
    "HORS_PARC_TECHNIQUE": "Hors parc (technique)",
    "ACTIF": "Actif",
    "INACTIF": "Inactif",
    # Opérations diverses (§77) et caisse (§72).
    "AJUSTEMENT": "Ajustement",
    "OUVERTURE": "À-nouveau d'ouverture",
    "REMBOURSEMENT_ASSOCIE": "Remboursement à un associé",
    "PAIEMENT_PERSONNEL_ASSOCIE": "Dépense payée personnellement par un associé",
    "HORS_COMPTA_REGULARISE": "Régularisation d'un flux hors comptabilité",
    "CORRECTION": "Correction",
    "RECLASSEMENT": "Reclassement entre comptes",
    "ENCAISSEMENT": "Encaissement",
    "AUTRE": "Autre",
    "PROPOSEE": "Proposée",
    "VALIDEE": "Validée",
    "CONTREPASSEE": "Contrepassée",
    # Clôture mensuelle.
    "OUVERT": "Ouvert",
    "CLOTURE": "Clôturé",
    "PROVISOIRE": "Provisoire",
}


def libelle_statut(code: str) -> str:
    """Libellé humain d'un statut. Le code STOCKÉ ne change jamais, seul son affichage.

    Un statut absent du registre retombe sur `humaniser_code` : mieux vaut « Truc machin » qu'un
    libellé inventé, et mieux vaut ça qu'une page en erreur.
    """
    texte = _txt(code)
    if not texte:
        return ""
    return LIBELLES_STATUT.get(texte.upper(), humaniser_code(texte))


def mode_paiement(mode_paiement_id: str, *, db_path=None) -> dict[str, str] | None:
    if not mode_paiement_id:
        return None
    return repo.lire_par_cle("ref_modes_paiement", _txt(mode_paiement_id), db_path=db_path)


def libelle_mode_paiement(mode_paiement_id: str, *, db_path=None) -> str:
    """« Banque pro » plutôt que « PAY_001 ». Vide si l'identifiant l'est."""
    if not _txt(mode_paiement_id):
        return ""
    m = mode_paiement(mode_paiement_id, db_path=db_path)
    nom = humaniser_code(m.get("mode_paiement")) if m else ""
    return nom if nom else f"Mode de paiement non résolu ({mode_paiement_id})"


def categorie_charge(categorie_charge_id: str, *, db_path=None) -> dict[str, str] | None:
    if not categorie_charge_id:
        return None
    return repo.lire_par_cle("ref_categories_charges", _txt(categorie_charge_id), db_path=db_path)


def libelle_categorie_charge(categorie_charge_id: str, *, db_path=None) -> str:
    """« Maintenance · Réparation logement » plutôt que « CHG_008 ».

    Les deux niveaux sont joints parce qu'aucun des deux ne suffit : le niveau 1 seul regroupe des
    dépenses très différentes, le niveau 2 seul est ambigu d'une famille à l'autre.
    """
    if not _txt(categorie_charge_id):
        return ""
    c = categorie_charge(categorie_charge_id, db_path=db_path)
    if not c:
        return f"Catégorie non résolue ({categorie_charge_id})"
    n1, n2 = _txt(c.get("categorie_niveau_1")), _txt(c.get("categorie_niveau_2"))
    return " · ".join(x for x in (n1, n2) if x) or f"Catégorie non résolue ({categorie_charge_id})"


def refacturable_par_defaut(categorie_charge_id: str, *, db_path=None) -> bool:
    """Valeur PROPOSÉE par le référentiel pour une catégorie (`refacturable_defaut`).

    C'est une proposition, jamais une décision : la saisie reste maîtresse. Sans elle, l'utilisateur
    devait deviner que « Réparation logement » est refacturable par défaut — et le formulaire, qui
    partait systématiquement de « Non », lui faisait perdre l'information du référentiel.
    """
    c = categorie_charge(categorie_charge_id, db_path=db_path)
    return bool(c) and _txt(c.get("refacturable_defaut")).upper() == "OUI"


def libelle_type_flux(type_flux_id: str, *, db_path=None) -> str:
    if not _txt(type_flux_id):
        return ""
    t = repo.lire_par_cle("ref_types_flux", _txt(type_flux_id), db_path=db_path)
    nom = humaniser_code(t.get("type_flux")) if t else ""
    return nom if nom else f"Type de flux non résolu ({type_flux_id})"


# ── Taux de commission ──────────────────────────────────────────────────────────────────────────

def taux_commission(proprietaire_id: str, *, db_path=None) -> list[dict[str, Any]]:
    """Lignes BRUTES rattachées au propriétaire.

    Aucun tri par date, aucune sélection, aucune notion d'« actuel » — reproduit à l'identique
    `ref_setup_reader.get_commission_rows`, y compris les quatre champs retournés.
    """
    if not proprietaire_id:
        return []
    cible = _txt(proprietaire_id)
    return [{
        "taux_commission": r.get("taux_commission"),
        "date_debut": r.get("date_debut"),
        "date_fin": r.get("date_fin"),
        "actif": r.get("actif"),
    } for r in repo.lire_table("ref_taux_commission", db_path=db_path)
        if _txt(r.get("proprietaire_id")) == cible]


# ── Coûts de ménage ─────────────────────────────────────────────────────────────────────────────

def couts_menage_interne(logement_id: str, *, db_path=None) -> list[dict[str, str]]:
    """Lignes brutes rattachées par clé directe logement_id."""
    if not logement_id:
        return []
    cible = _txt(logement_id)
    return [r for r in repo.lire_table("ref_couts_menage_interne", db_path=db_path)
            if _txt(r.get("logement_id")) == cible]


def couts_standards_menage(type_logement_id: str, *, db_path=None) -> list[dict[str, str]]:
    """Lignes brutes pour le type propre du logement. Décodage de référence, aucun calcul."""
    if not type_logement_id:
        return []
    cible = _txt(type_logement_id)
    return [r for r in repo.lire_table("ref_couts_standards_menage", db_path=db_path)
            if _txt(r.get("type_logement_id")) == cible]


# ── Clôture mensuelle ───────────────────────────────────────────────────────────────────────────

def cloture_mensuelle(*, db_path=None) -> list[dict[str, str]]:
    """Statut officiel de clôture par mois, tel que le référentiel le déclare.

    Rendu brut et non trié : l'ordre d'affichage appartient aux écrans, et la notion de « mois
    courant » n'est pas décidée ici.
    """
    return repo.lire_table("ref_cloture_mensuelle", db_path=db_path)
