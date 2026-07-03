from pathlib import Path


def test_tests_app2b_ne_referencent_pas_saisie_reel():
    tests_dir = Path(__file__).parent
    forbidden = [
        "cfg." + "SAISIE_RESERVATIONS_HH",
        "01_SOURCES_BRUTES/ReservationsHH/" + "SAISIE_ReservationsHorsHostaway.xlsx",
    ]
    violations = []
    for path in tests_dir.glob("test_saisie_hh_*.py"):
        if path.name == Path(__file__).name:
            continue
        src = path.read_text(encoding="utf-8")
        for needle in forbidden:
            if needle in src:
                violations.append(f"{path.name}: {needle}")
    assert not violations, "References interdites au SAISIE reel:\n" + "\n".join(violations)
