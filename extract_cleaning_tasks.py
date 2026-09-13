import csv
import os
import time

import requests

# Tâches de ménage Hostaway (/v1/tasks) — même authentification que les autres extractions
# (secret GitHub HOSTAWAY_TOKEN), même API, même format de sortie (TSV utf-8-sig).
#
# Le fichier n'est écrit QUE si l'extraction est complète : une extraction interrompue (403, rate
# limit persistant, réponse incomplète) laisse en place le dernier fichier publié, que le commit
# automatique ne modifie donc pas.

ACCESS_TOKEN = os.getenv("HOSTAWAY_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("HOSTAWAY_TOKEN est manquant")
BASE_URL = "https://api.hostaway.com/v1"

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}

DATE_FROM = os.getenv("HOSTAWAY_TASKS_DATE_FROM", "2026-01-01")
OUTPUT_FILE = "cleaning_tasks_hostaway.tsv"
PAGE_SIZE = 100
MAX_PAGES = 200
MAX_TENTATIVES = 3

# Uniquement les champs lus par l'application (lib_hostaway_depot / extraire_cleaning_tasks).
COLONNES = ["id", "reservationId", "listingMapId", "title", "status", "taskType", "type",
            "canStartFrom", "shouldEndBy", "assigneeUserId"]


def api_get(url, params=None):
    for tentative in range(1, MAX_TENTATIVES + 1):
        response = requests.get(url, headers=HEADERS, params=params, timeout=60)

        if response.status_code == 429:
            attente = 3 * tentative
            print(f"Rate limit atteint, pause {attente} secondes...")
            time.sleep(attente)
            continue

        if response.status_code == 403:
            print("Erreur 403 : token invalide ou expiré.")
            raise SystemExit(1)

        response.raise_for_status()
        return response.json()

    raise RuntimeError("Rate limit persistant sur /tasks : extraction abandonnée.")


def get_all_tasks():
    """`/v1/tasks` peut rendre la totalité du résultat quel que soit `offset` : arrêt dès que
    `count` est atteint, dédoublonnage par `id`, plafond de pages contre toute boucle infinie."""
    vues = {}
    offset = 0
    count = None

    for _ in range(MAX_PAGES):
        print(f"Récupération des tâches, offset {offset}...")
        data = api_get(
            f"{BASE_URL}/tasks",
            params={"dateFrom": DATE_FROM, "limit": PAGE_SIZE, "offset": offset},
        )
        rows = data.get("result", []) or []
        count = data.get("count")

        for t in rows:
            if t.get("id") is not None:
                vues.setdefault(t["id"], t)

        print(f"{len(rows)} tâches reçues | uniques={len(vues)} | count={count}")

        if not rows or len(rows) < PAGE_SIZE or (count is not None and len(vues) >= count):
            break

        offset += PAGE_SIZE
        time.sleep(0.2)
    else:
        raise RuntimeError(f"Plafond de {MAX_PAGES} pages atteint : extraction abandonnée.")

    if count is not None and len(vues) < count:
        raise RuntimeError(f"Extraction incomplète : {len(vues)} tâches uniques pour count={count}.")

    return list(vues.values())


def main():
    print("Début extraction des tâches de ménage Hostaway...")

    tasks = get_all_tasks()
    if not tasks:
        raise RuntimeError("Aucune tâche retournée : le fichier précédent est conservé.")

    temporaire = OUTPUT_FILE + ".partiel"
    with open(temporaire, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES, delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        for t in tasks:
            writer.writerow({c: ("" if t.get(c) is None else t.get(c)) for c in COLONNES})
    os.replace(temporaire, OUTPUT_FILE)

    print(f"Extraction terminée : {len(tasks)} tâches")
    print(f"Fichier généré : {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
