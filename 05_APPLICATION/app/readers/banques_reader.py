"""Reader Banques & Caisse (APP-4A) — LECTURE SEULE, SQLITE UNIQUEMENT.

Lit la base : `banque_mouvements`, `banque_classifications`, `banque_controles`,
`banque_rapprochements`. Ne transforme aucune valeur métier, n'écrit jamais, n'invente aucun
rapprochement : les statuts (`statut_controle`, `statut_rapprochement`, `statut_classification`)
viennent du moteur de classification.

AUCUNE LECTURE EXCEL
Ce reader ouvrait `BANQUE_LOT8_IMPORT.xlsx`. Il ne l'ouvre plus, et ne s'y replie pas non plus quand
la base est vide : après un changement de banque, un repli afficherait les mouvements de l'ancienne
comme s'ils étaient courants. L'absence de données est un état nommé, pas un écran vide.

États de source distincts — jamais confondus avec « zéro mouvement » :
  OK · NON_INITIALISEE · NON_CLASSEE · VIDE · NON_ALIMENTE (caisse).

Les numéros de compte sont masqués à la source (jamais d'IBAN complet exposé).
"""
from __future__ import annotations

import datetime as _dt
import hashlib
from dataclasses import dataclass, field
from typing import Any

import app.config as cfg
from app.services import banque_vues_service as vues

# ── Vues de lecture (anciennement les onglets du classeur) ───────────────────
# Les noms d'onglet restent le vocabulaire du moteur et servent d'étiquette d'origine à l'écran :
# c'est ce que l'utilisateur reconnaît, et ce qui permet de comparer avec l'historique.
ONGLET_MOUVEMENTS = "NORM_Banque"
ONGLET_CONTROLES = "CTRL_A_CONTROLER"
ONGLET_IA = "IA_Classification"
ONGLET_RAPPRO_AIRBNB = "RAPPROCH_AIRBNB_ATTENTE"
ONGLET_RAPPRO_PROPRIO = "RAPPROCH_PROPRIETAIRES_ATTENTE"
ONGLET_CTRL_8C = "CTRL_RAPPROCHEMENT_8C"
ONGLET_LOG = "LOG_Traitement"

# ── Libellés de source (jamais de chemin absolu, jamais de nom de fichier) ────
SOURCE_BANQUE = "Base de pilotage (SQLite)"
SOURCE_CAISSE = "— (aucune source caisse)"

# ── États ────────────────────────────────────────────────────────────────────
ETAT_OK = "OK"
ETAT_NON_INITIALISEE = vues.ETAT_NON_INITIALISEE
ETAT_NON_CLASSEE = vues.ETAT_NON_CLASSEE
ETAT_VIDE = "VIDE"
ETAT_ILLISIBLE = "ILLISIBLE"
ETAT_NON_ALIMENTE = "NON_ALIMENTE"

# Conservés pour les appelants qui les nomment encore ; la lecture SQLite ne les produit plus.
ETAT_FICHIER_ABSENT = ETAT_NON_INITIALISEE
ETAT_ONGLET_ABSENT = ETAT_NON_INITIALISEE

_ETAT_LIBELLE = {
    ETAT_OK: "Alimentée",
    ETAT_NON_INITIALISEE: "Aucun mouvement en base",
    ETAT_NON_CLASSEE: "Mouvements importés, non classés",
    ETAT_VIDE: "Source vide",
    ETAT_ILLISIBLE: "Source illisible",
    ETAT_NON_ALIMENTE: "Non alimentée par le moteur",
}


@dataclass(frozen=True)
class EtatSource:
    cle: str
    libelle: str
    fichier: str
    onglet: str
    etat: str
    nb_lignes: int = 0
    derniere_maj: str | None = None

    @property
    def disponible(self) -> bool:
        return self.etat == ETAT_OK

    @property
    def etat_libelle(self) -> str:
        return _ETAT_LIBELLE.get(self.etat, self.etat)


@dataclass(frozen=True)
class SourceBanque:
    etat: EtatSource
    lignes: list[dict[str, Any]] = field(default_factory=list)


# ── Cache (invalidé par le compteur de mouvements et l'exécution de classification) ──
# Une exécution de classification change TOUTES les vues d'un coup : la clé de cache est donc
# l'état de la base, pas un fichier. Un import ou un reclassement suffit à la faire changer.
_CACHE: dict[tuple, list[dict[str, Any]]] = {}


def vider_cache() -> None:
    global _CACHE_OPAQUE_COMPTES
    _CACHE.clear()
    _CACHE_OPAQUE_COMPTES = None


def _version_donnees() -> tuple:
    """Signature de l'état courant de la base Banque."""
    from app.services import banque_classification_service as cls
    from app.services import banque_mouvements_service as bq
    try:
        return (str(cfg.DB_PATH), bq.compter(), cls.derniere_execution())
    except Exception:
        return (str(cfg.DB_PATH), -1, "")


_VUES = {
    ONGLET_MOUVEMENTS: vues.mouvements_normalises,
    ONGLET_CONTROLES: vues.controles_a_controler,
    ONGLET_IA: vues.a_classer_par_ia,
    ONGLET_RAPPRO_AIRBNB: vues.attentes_plateforme,
    ONGLET_RAPPRO_PROPRIO: vues.attentes_proprietaires,
    ONGLET_CTRL_8C: vues.controles_rapprochement,
    ONGLET_LOG: vues.journal_traitement,
}


def _source(cle: str, libelle: str, vue: str) -> SourceBanque:
    """Construit une source à partir d'une vue SQLite. Ne lève jamais.

    L'état distingue trois absences que l'affichage doit traiter différemment : pas de mouvement du
    tout, des mouvements non classés, et une vue légitimement vide (aucun contrôle ouvert, par
    exemple — ce qui est une bonne nouvelle, pas une panne).
    """
    version = _version_donnees()
    etat_base = vues.etat()
    if etat_base != ETAT_OK and vue != ONGLET_LOG:
        # Le journal des imports reste lisible même sans classification : il dit justement ce qui a
        # été importé.
        return SourceBanque(
            etat=EtatSource(cle=cle, libelle=libelle, fichier=SOURCE_BANQUE, onglet=vue,
                            etat=etat_base, nb_lignes=0, derniere_maj=None))

    cache_cle = (version, vue)
    if cache_cle in _CACHE:
        lignes = _CACHE[cache_cle]
    else:
        try:
            lignes = _VUES[vue]()
        except Exception:
            return SourceBanque(
                etat=EtatSource(cle=cle, libelle=libelle, fichier=SOURCE_BANQUE, onglet=vue,
                                etat=ETAT_ILLISIBLE, nb_lignes=0, derniere_maj=None))
        _CACHE[cache_cle] = lignes

    return SourceBanque(
        etat=EtatSource(cle=cle, libelle=libelle, fichier=SOURCE_BANQUE, onglet=vue,
                        etat=ETAT_OK if lignes else ETAT_VIDE, nb_lignes=len(lignes),
                        derniere_maj=_derniere_maj()),
        lignes=lignes,
    )


def _derniere_maj() -> str | None:
    """Horodatage du dernier import connu, format court."""
    from app.services import banque_mouvements_service as bq
    try:
        imports = bq.imports()
    except Exception:
        return None
    if not imports:
        return None
    dernier = max((i.get("date_import") or "") for i in imports)
    return dernier.replace("T", " ")[:16] or None


# ── Convertisseurs (jamais None -> 0) ────────────────────────────────────────

def to_texte(v: Any) -> str:
    return "" if v is None else str(v).strip()


def to_nombre(v: Any) -> float | int | None:
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return v
    try:
        return float(str(v).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return None


def to_date(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (_dt.datetime, _dt.date)):
        return v.strftime("%Y-%m-%d")
    return str(v)[:10]


def to_mois(v: Any) -> str:
    d = to_date(v)
    return d[:7] if len(d) >= 7 else ""


def date_affichage(v: Any) -> str:
    """Formate une date ISO/brute en JJ/MM/AAAA pour l'affichage. Interne reste ISO (to_date/to_mois).

    « — » si absente. « Date invalide — <valeur> » si non convertible (jamais une valeur inventée).
    """
    d = to_date(v)
    if not d:
        return "—"
    try:
        return _dt.date.fromisoformat(d[:10]).strftime("%d/%m/%Y")
    except ValueError:
        return f"Date invalide — {to_texte(v)}"


def datetime_affichage(v: Any) -> str:
    """Formate un horodatage ISO (« …T…Z ») en JJ/MM/AAAA HH:MM. Repli identique à date_affichage."""
    s = to_texte(v)
    if not s:
        return "—"
    try:
        s2 = s.replace("Z", "+00:00")
        return _dt.datetime.fromisoformat(s2).strftime("%d/%m/%Y %H:%M")
    except ValueError:
        return date_affichage(v)


def masquer_compte(compte_id: Any) -> str:
    """Masque un identifiant de compte / IBAN : ne garde qu'un préfixe court + 4 derniers caractères.

    Ex. 'CM_02211_00021321603' -> 'CM ••••1603'. Jamais l'identifiant complet en interface.
    """
    s = to_texte(compte_id)
    if not s:
        return ""
    prefixe = s.split("_", 1)[0][:4] if "_" in s else s[:2]
    fin = "".join(ch for ch in s if ch.isalnum())[-4:]
    return f"{prefixe} ••••{fin}"


# ── Identifiant COMPTE opaque (jamais le compte_id brut dans le navigateur) ───
# Le filtre "Compte" exposait la valeur brute (compte_id complet, contenant le numéro de compte)
# en attribut HTML `<option value="...">`. Le navigateur ne doit RECEVOIR aucune donnée bancaire
# complète, même masquée à l'affichage : l'identifiant public est donc opaque, dérivé par hachage
# (même principe que l'identifiant mouvement opaque APP-4B), avec correspondance interne côté serveur.

def id_opaque_compte(compte_id: Any) -> str:
    """« CPT-<hash8> » stable et sans donnée bancaire, dérivé du compte_id réel + sel."""
    base = (cfg.BANQUE_OPAQUE_SALT + "|CPT|" + to_texte(compte_id)).encode("utf-8")
    return "CPT-" + hashlib.sha256(base).hexdigest()[:8]


_CACHE_OPAQUE_COMPTES: dict[str, str] | None = None


def index_opaque_comptes() -> dict[str, str]:
    """{id_opaque: compte_id_reel} pour tous les comptes présents dans NORM_Banque. Mémorisé."""
    global _CACHE_OPAQUE_COMPTES
    if _CACHE_OPAQUE_COMPTES is not None:
        return _CACHE_OPAQUE_COMPTES
    idx: dict[str, str] = {}
    for r in mouvements().lignes:
        cid = to_texte(r.get("compte_id"))
        if cid:
            idx[id_opaque_compte(cid)] = cid
    _CACHE_OPAQUE_COMPTES = idx
    return idx


def resoudre_opaque_compte(opaque: str) -> str | None:
    """id_opaque -> compte_id réel, ou None si inconnu (jamais un compte brut accepté silencieusement)."""
    return index_opaque_comptes().get(to_texte(opaque))


# ── Sources ──────────────────────────────────────────────────────────────────

def mouvements() -> SourceBanque:
    return _source("mouvements", "Mouvements bancaires normalisés", ONGLET_MOUVEMENTS)


def controles() -> SourceBanque:
    return _source("controles", "Contrôles à contrôler (import/classification)", ONGLET_CONTROLES)


def ia_classification() -> SourceBanque:
    return _source("ia", "Propositions de classification IA", ONGLET_IA)


def rappro_airbnb() -> SourceBanque:
    return _source("rappro_airbnb", "Rapprochement Airbnb en attente", ONGLET_RAPPRO_AIRBNB)


def rappro_proprietaires() -> SourceBanque:
    return _source("rappro_proprietaires", "Rapprochement propriétaires en attente", ONGLET_RAPPRO_PROPRIO)


def controles_rappro_8c() -> SourceBanque:
    return _source("controles_rappro", "Contrôles de rapprochement (Lot 8c)", ONGLET_CTRL_8C)


def log_traitement() -> SourceBanque:
    return _source("log", "Journal du traitement", ONGLET_LOG)


def caisse() -> SourceBanque:
    """La caisse n'a pas de module moteur : état NON_ALIMENTE, jamais fabriqué côté application."""
    return SourceBanque(
        etat=EtatSource(cle="caisse", libelle="Mouvements de caisse", fichier=SOURCE_CAISSE,
                        onglet="—", etat=ETAT_NON_ALIMENTE, nb_lignes=0, derniere_maj=None),
        lignes=[],
    )


def etats_sources() -> list[EtatSource]:
    return [
        mouvements().etat,
        controles().etat,
        rappro_airbnb().etat,
        rappro_proprietaires().etat,
        controles_rappro_8c().etat,
        caisse().etat,
    ]
