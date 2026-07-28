"""Données réelles de secours — Lot 6c (ménages externes, mai 2026).

Transcription manuelle des factures PDF réelles (Aissata / Mounir), utilisée par
`lot6c_menages_externes.py` uniquement quand `01_SOURCES_BRUTES/MenagesExternes/Factures_PDF`
ne fournit aucun PDF exploitable (repli SAISIE_MANUELLE_SECOURS, décisions D079-D088).

Isolé ici, hors du moteur, pour la même raison que `_data_lot6b_alias_reel.py`
(ANO-2026-07-28-01) : ce module n'est **jamais copié** vers `data_recette` par
`recette/build_data_recette.py`. Son absence est sans effet — le moteur bascule alors
sur un jeu de repli entièrement fictif défini localement, avec zéro donnée réelle.
"""

from datetime import date


def _d(s):
    return date(*map(int, s.split("-"))) if s else None


RAW_MANUEL = [
    # === AISSATA — Facture n°2026-37 — 31/05/2026 — Total TTC 1 439 € ===
    ("FAC-2026-05-AISSATA-001", "2026-37", _d("2026-05-31"), "Facture mai Aissata.pdf",
     "Kandia DIABATE / Rends-moi un service", "studio 76 (Dureuil)",
     _d("2026-05-10"), "DATE_PRECISE", "TLM_001", 1, 29.0, 29.0, 1439.0,
     "Ligne 1 fac.2026-37 — date précise sur facture"),
    ("FAC-2026-05-AISSATA-001", "2026-37", _d("2026-05-31"), "Facture mai Aissata.pdf",
     "Kandia DIABATE / Rends-moi un service", "studio - cote pavé (François)",
     None, "MOIS_FACTURE", "TLM_001", 5, 29.0, 145.0, 1439.0,
     "Ligne 2 fac.2026-37 — 5 passages mai 2026, date absente"),
    ("FAC-2026-05-AISSATA-001", "2026-37", _d("2026-05-31"), "Facture mai Aissata.pdf",
     "Kandia DIABATE / Rends-moi un service", "studio Puits verts (Caroline)",
     None, "MOIS_FACTURE", "TLM_001", 10, 29.0, 290.0, 1439.0,
     "Ligne 3 fac.2026-37 — 10 passages mai 2026, date absente"),
    ("FAC-2026-05-AISSATA-001", "2026-37", _d("2026-05-31"), "Facture mai Aissata.pdf",
     "Kandia DIABATE / Rends-moi un service", "T3 310 muret (David)",
     None, "MOIS_FACTURE", "TLM_001", 8, 55.0, 440.0, 1439.0,
     "Ligne 4 fac.2026-37 — 8 passages mai 2026, date absente"),
    ("FAC-2026-05-AISSATA-001", "2026-37", _d("2026-05-31"), "Facture mai Aissata.pdf",
     "Kandia DIABATE / Rends-moi un service", "studio st Pierre (Florane)",
     None, "MOIS_FACTURE", "TLM_001", 2, 29.0, 58.0, 1439.0,
     "Ligne 5 fac.2026-37 — 2 passages mai 2026, date absente"),
    ("FAC-2026-05-AISSATA-001", "2026-37", _d("2026-05-31"), "Facture mai Aissata.pdf",
     "Kandia DIABATE / Rends-moi un service", "T2 9 rue du Toul",
     None, "MOIS_FACTURE", "TLM_001", 8, 39.0, 312.0, 1439.0,
     "Ligne 6 fac.2026-37 — 8 passages mai 2026, date absente"),
    ("FAC-2026-05-AISSATA-001", "2026-37", _d("2026-05-31"), "Facture mai Aissata.pdf",
     "Kandia DIABATE / Rends-moi un service", "T3 4 rue engalières",
     _d("2026-05-23"), "DATE_PRECISE", "TLM_001", 1, 55.0, 55.0, 1439.0,
     "Ligne 7 fac.2026-37 — date précise sur facture"),
    # Ligne 8 originale splittée (2 dates distinctes : 03/05 et 10/05)
    ("FAC-2026-05-AISSATA-001", "2026-37", _d("2026-05-31"), "Facture mai Aissata.pdf",
     "Kandia DIABATE / Rends-moi un service", "T3 20 rue l'Amiral Galache",
     _d("2026-05-03"), "DATE_PRECISE", "TLM_001", 1, 55.0, 55.0, 1439.0,
     "Ligne 8a — splittée (2 dates). Facture: T3, REF: T2 (LOG_0016). Prix 55€ tarif T3 Aissata."),
    ("FAC-2026-05-AISSATA-001", "2026-37", _d("2026-05-31"), "Facture mai Aissata.pdf",
     "Kandia DIABATE / Rends-moi un service", "T3 20 rue l'Amiral Galache",
     _d("2026-05-10"), "DATE_PRECISE", "TLM_001", 1, 55.0, 55.0, 1439.0,
     "Ligne 8b — splittée (2 dates). Facture: T3, REF: T2 (LOG_0016). Prix 55€ tarif T3 Aissata."),
    # === MOUNIR — Facture n°0003 — 31/05/2026 — Total TTC 942 € ===
    ("FAC-2026-05-MOUNIR-001", "0003", _d("2026-05-31"), "Facture mai Mounir.pdf",
     "MH Entreprise", "T.4-90 Blagnac (Cédrine)",
     None, "MOIS_FACTURE", "TLM_001", 6, 65.0, 390.0, 942.0,
     "Ligne 1 fac.0003 — 6 ménages mai 2026, date absente"),
    ("FAC-2026-05-MOUNIR-001", "0003", _d("2026-05-31"), "Facture mai Mounir.pdf",
     "MH Entreprise", "T.3 Sept Deniers (François)",
     None, "MOIS_FACTURE", "TLM_001", 10, 52.0, 520.0, 942.0,
     "Ligne 2 fac.0003 — 10 ménages mai 2026, date absente"),
    ("FAC-2026-05-MOUNIR-001", "0003", _d("2026-05-31"), "Facture mai Mounir.pdf",
     "MH Entreprise", "T.2-65 (Gabriel)",
     None, "MOIS_FACTURE", "TLM_001", 0, 36.0, 0.0, 942.0,
     "Ligne 3 fac.0003 — 0 ménage, logement inactif depuis 2026-04-26. Présent à 0€."),
    ("FAC-2026-05-MOUNIR-001", "0003", _d("2026-05-31"), "Facture mai Mounir.pdf",
     "MH Entreprise", "Studio Puits vert (Caroline)",
     None, "MOIS_FACTURE", "TLM_001", 1, 32.0, 32.0, 942.0,
     "Ligne 4 fac.0003 — 1 ménage mai 2026, date absente"),
]

PREST_MAP = {
    "kandia diabate / rends-moi un service": "INT_0004",
    "mh entreprise": "INT_0003",
}

LOG_MAP = {
    "studio 76 (dureuil)":           "LOG_0014",
    "studio - cote pavé (françois)": "LOG_0012",
    "studio puits verts (caroline)": "LOG_0006",
    "t3 310 muret (david)":          "LOG_0011",
    "studio st pierre (florane)":    "LOG_0010",
    "t2 9 rue du toul":              "LOG_0007",
    "t3 4 rue engalières":           "LOG_0009",
    "t3 20 rue l'amiral galache":    "LOG_0016",
    "t.4-90 blagnac (cédrine)":      "LOG_0002",
    "t.3 sept deniers (françois)":   "LOG_0013",
    "t.2-65 (gabriel)":              "LOG_0003",
    "studio puits vert (caroline)":  "LOG_0006",
}

PREST_BRUT = {"INT_0004": "Kandia DIABATE / Rends-moi un service", "INT_0003": "MH Entreprise"}

# Code court prestataire (identifiant de facture) et mode de paiement connu, par intervenant_id.
PCODE_MAP = {"INT_0004": "AISSATA", "INT_0003": "MOUNIR"}
MODE_PAIEMENT_MAP = {"INT_0004": "VIREMENT"}
