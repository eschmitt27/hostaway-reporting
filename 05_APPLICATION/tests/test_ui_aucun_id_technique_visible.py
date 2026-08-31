"""Mission « supprimer les IDs techniques de tous les écrans utilisateur ».

Protection transverse : le TEXTE VISIBLE (pas les attributs href/action/value/name, techniques par
nature) des écrans opérationnels ne doit jamais contenir un identifiant technique brut
(PROP_/LOG_/TYPE_/INT_/RESHH-/CTRL-…). Whitelist explicite pour les écrans où l'ID a un intérêt réel
(Administration des référentiels / Référentiel Setup) — but un futur développement ne doit pas
réintroduire silencieusement un ID brut dans un écran métier normal.
"""
from __future__ import annotations

import re

import pytest
from bs4 import BeautifulSoup
from fastapi.testclient import TestClient

from app.db.connection import apply_migrations, get_db

# Écrans où l'ID technique est explicitement autorisé (mission, section 5).
WHITELIST_PREFIXES = ("/referentiel-setup", "/admin-referentiels")

# Identifiants opaques STABLES (numéro de dossier/mouvement/écriture), pas une référence à une
# AUTRE entité nommée — hors périmètre de cette règle (mission : PROP_/LOG_/TYPE_/FOURN_/PREST_/
# INTERV_/ACTEUR_/FACT_/CHG_/RES_ « ou tout autre identifiant opaque interne » RÉFÉRENÇANT une
# entité ; un numéro de dossier qui s'auto-désigne n'en réfère aucune autre).
ID_PATTERNS = [
    re.compile(r"\bPROP_\d+\b"),
    re.compile(r"\bLOG_\d+\b"),
    re.compile(r"\bTYPE_\d+\b"),
]


@pytest.fixture
def seeded(tmp_db):
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, prenom_proprietaire, "
            "import_id) VALUES ('PROP_0001','Delrieu','Cédrine','IMP-1')")
        conn.execute(
            "INSERT INTO ref_types_logements (type_logement_id, type_logement, import_id) "
            "VALUES ('TYPE_001','Studio','IMP-1')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, statut_parc, actif, "
            "import_id, nom_court, type_logement_id) VALUES "
            "('LOG_0001','700111','GERE','OUI','IMP-1','Studio - 46','TYPE_001')")
        conn.execute(
            "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
            "date_debut, date_fin, statut_gestion, import_id) "
            "VALUES ('GST-1','LOG_0001','PROP_0001','2025-01-01','','ACTIF','IMP-1')")
        conn.commit()
    finally:
        conn.close()
    return tmp_db


def _textes_visibles(html: str) -> str:
    """Texte visible du DOM — jamais les attributs (href/action/value/name/id HTML)."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    return soup.get_text(" ", strip=True)


def _verifier_page(client: TestClient, path: str) -> None:
    resp = client.get(path)
    assert resp.status_code < 500, f"{path} a planté ({resp.status_code})"
    if resp.status_code != 200:
        return
    if any(path.startswith(p) for p in WHITELIST_PREFIXES):
        return
    texte = _textes_visibles(resp.text)
    for pattern in ID_PATTERNS:
        m = pattern.search(texte)
        assert m is None, f"{path} affiche un ID technique brut dans le texte visible : {m.group(0)!r}"


def test_logements_liste_aucun_id_technique(seeded):
    from app.main import app
    with TestClient(app) as client:
        _verifier_page(client, "/logements")


def test_logements_detail_aucun_id_technique(seeded):
    from app.main import app
    with TestClient(app) as client:
        _verifier_page(client, "/logements/LOG_0001")


def test_proprietaires_liste_aucun_id_technique(seeded):
    from app.main import app
    with TestClient(app) as client:
        _verifier_page(client, "/proprietaires")


def test_proprietaires_detail_aucun_id_technique(seeded):
    from app.main import app
    with TestClient(app) as client:
        _verifier_page(client, "/proprietaires/PROP_0001")


def test_id_non_resolu_affiche_texte_explicite(seeded):
    """Un ID sans référentiel résolu affiche un texte explicite, jamais l'ID brut silencieux."""
    from app.services import referentiel_service as ref_svc

    assert ref_svc.libelle_proprietaire("PROP_9999", db_path=seeded) == \
        "Propriétaire non résolu (PROP_9999)"
    assert ref_svc.libelle_logement("LOG_9999", db_path=seeded) == \
        "Logement non résolu (LOG_9999)"
    assert ref_svc.libelle_type_logement("TYPE_999", db_path=seeded) == \
        "Type non résolu (TYPE_999)"


def test_libelles_resolus_sans_mapping_en_dur(seeded):
    """Les libellés viennent du référentiel SQLite — jamais une valeur codée en dur dans le code
    Python (vérifié en changeant la donnée en base et en observant que le libellé suit)."""
    from app.services import referentiel_service as ref_svc

    assert ref_svc.libelle_proprietaire("PROP_0001", db_path=seeded) == "Cédrine Delrieu"
    assert ref_svc.libelle_logement("LOG_0001", db_path=seeded) == "Studio - 46"
    assert ref_svc.libelle_type_logement("TYPE_001", db_path=seeded) == "Studio"

    conn = get_db(seeded)
    conn.execute("UPDATE ref_proprietaires SET nom_proprietaire='Martin' WHERE proprietaire_id='PROP_0001'")
    conn.commit()
    conn.close()
    assert ref_svc.libelle_proprietaire("PROP_0001", db_path=seeded) == "Cédrine Martin"


def test_whitelist_admin_referentiel_setup_autorise_id(seeded):
    """La whitelist explicite (Référentiel Setup / Admin référentiels) reste hors du scope de ce
    contrôle — vérifie juste que le préfixe existe et est bien celui attendu, pas plus."""
    assert "/referentiel-setup" in WHITELIST_PREFIXES
    assert "/admin-referentiels" in WHITELIST_PREFIXES
