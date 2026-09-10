"""Type de client de facturation d'un propriétaire — PARTICULIER ou PROFESSIONNEL.

CE QUE CE MODULE NE FAIT PAS : deviner. Ni le nom, ni l'adresse, ni la présence d'un SIREN ne
permettent de conclure qu'un propriétaire est un professionnel. Un particulier peut porter un nom
de société dans son adresse de facturation, et un professionnel peut être facturé sans que son
SIREN soit connu. Le type est donc SAISI, jamais dérivé — et tant qu'il ne l'est pas, il vaut
`A_CONTROLER` et l'émission reste bloquée.

POURQUOI CELA COMPTE : les mentions légales obligatoires diffèrent. Pénalités de retard et
indemnité forfaitaire de recouvrement de 40 € sont dues entre professionnels (art. L441-10 et
D441-5 du code de commerce) ; les imprimer sur la facture d'un particulier serait au mieux
inexact, au pire une menace de recouvrement sans fondement.

STOCKAGE : table compagne `proprietaires_facturation` (migration 0073), et non une colonne de
`ref_proprietaires` — cette dernière est reconstruite à chaque import de `REF_Setup.xlsm`.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.db.connection import get_db
from app.services import facturation_config_service as conf

# Valeurs réellement stockables. `A_CONTROLER` n'en fait pas partie : c'est l'absence de ligne.
TYPES_STOCKABLES = (conf.CLIENT_PARTICULIER, conf.CLIENT_PROFESSIONNEL)

E_TYPE_INVALIDE = "PROPRIETAIRE_TYPE_CLIENT_INVALIDE"
E_PROPRIETAIRE_MANQUANT = "PROPRIETAIRE_MANQUANT"


class TypeClientError(RuntimeError):
    """Refus explicite — jamais un classement par défaut."""


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def lire(proprietaire_id: str, *, db_path=None) -> dict[str, Any] | None:
    """Classement enregistré, ou `None` si le propriétaire n'a jamais été classé."""
    pid = str(proprietaire_id or "").strip()
    if not pid:
        return None
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT * FROM proprietaires_facturation WHERE proprietaire_id = ?",
                         (pid,)).fetchone()
    except Exception:      # noqa: BLE001 — table absente sur une base non migrée : pas un classement
        return None
    finally:
        conn.close()
    return dict(r) if r else None


def type_client(proprietaire_id: str, *, db_path=None) -> str:
    """Type applicable, ou `A_CONTROLER`. Jamais deviné."""
    enregistre = lire(proprietaire_id, db_path=db_path)
    if not enregistre:
        return conf.CLIENT_A_CONTROLER
    valeur = str(enregistre.get("type_client_facturation") or "").upper()
    return valeur if valeur in TYPES_STOCKABLES else conf.CLIENT_A_CONTROLER


def definir(proprietaire_id: str, type_client_facturation: str, *, siren_client: str = "",
            tva_intra_client: str = "", motif: str = "", acteur: str = "",
            db_path=None) -> dict[str, Any]:
    """Classe un propriétaire. Le changement est journalisé avec sa valeur précédente.

    Reclasser reste possible : une société peut cesser son activité, un particulier peut en créer
    une. Ce qui ne doit pas se perdre, c'est le fait qu'on a changé d'avis — d'où le journal.
    Les factures DÉJÀ ÉMISES ne bougent pas : leur bloc de conformité est figé dans leur snapshot.
    """
    pid = str(proprietaire_id or "").strip()
    if not pid:
        raise TypeClientError(f"{E_PROPRIETAIRE_MANQUANT}: identifiant propriétaire obligatoire")
    valeur = str(type_client_facturation or "").strip().upper()
    if valeur not in TYPES_STOCKABLES:
        raise TypeClientError(
            f"{E_TYPE_INVALIDE}: {valeur!r} — attendu {' ou '.join(TYPES_STOCKABLES)}")

    avant = type_client(pid, db_path=db_path)
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO proprietaires_facturation "
            "(proprietaire_id, type_client_facturation, siren_client, tva_intra_client, acteur) "
            "VALUES (?,?,?,?,?) "
            "ON CONFLICT(proprietaire_id) DO UPDATE SET "
            "  type_client_facturation = excluded.type_client_facturation, "
            "  siren_client = excluded.siren_client, "
            "  tva_intra_client = excluded.tva_intra_client, "
            "  date_modification = ?, acteur = excluded.acteur",
            (pid, valeur, str(siren_client or "").strip() or None,
             str(tva_intra_client or "").strip() or None, acteur or "interface", _maintenant()))
        conn.execute(
            "INSERT INTO proprietaires_facturation_evenements "
            "(proprietaire_id, valeur_avant, valeur_apres, motif, acteur) VALUES (?,?,?,?,?)",
            (pid, avant, valeur, motif or None, acteur or "interface"))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"ok": True, "proprietaire_id": pid, "type_client_facturation": valeur,
            "valeur_avant": avant}


def historique(proprietaire_id: str, *, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM proprietaires_facturation_evenements WHERE proprietaire_id = ? "
            "ORDER BY id DESC", (str(proprietaire_id or "").strip(),))]
    except Exception:      # noqa: BLE001
        return []
    finally:
        conn.close()
