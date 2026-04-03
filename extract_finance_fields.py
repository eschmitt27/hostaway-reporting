import time
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

ACCESS_TOKEN = os.getenv("HOSTAWAY_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("HOSTAWAY_TOKEN est manquant")
BASE_URL = "https://api.hostaway.com/v1"

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}

MAX_WORKERS = 5
RETRY_WAIT = 3

session = requests.Session()
session.headers.update(HEADERS)

def api_get(url, params=None):
    for attempt in range(3):
        response = session.get(url, params=params, timeout=60)

        if response.status_code == 429:
            print("Rate limit atteint, pause...")
            time.sleep(RETRY_WAIT)
            continue

        if response.status_code == 403:
            print("Erreur 403 : token invalide ou expiré.")
            print("Remplace ACCESS_TOKEN par un nouveau token.")
            print("Réponse brute :", response.text)
            raise SystemExit(1)

        response.raise_for_status()
        return response.json()

    raise Exception("Échec après plusieurs tentatives")

def get_finance_fields(reservation_id):
    data = api_get(f"{BASE_URL}/financeField/{reservation_id}")
    return data.get("result", [])

def flatten_finance_fields(reservation_id, finance_fields):
    rows = []

    for f in finance_fields:
        rows.append({
            "reservationId": reservation_id,
            "financeFieldId": f.get("id"),
            "type": f.get("type"),
            "name": f.get("name"),
            "title": f.get("title"),
            "value": f.get("value"),
            "total": f.get("total"),
            "isIncludedInTotalPrice": f.get("isIncludedInTotalPrice"),
            "isOverriddenByUser": f.get("isOverriddenByUser"),
            "isMandatory": f.get("isMandatory"),
            "isDeleted": f.get("isDeleted")
        })

    return rows

def process_reservation(reservation_id):
    finance_fields = get_finance_fields(reservation_id)
    return flatten_finance_fields(reservation_id, finance_fields)

def main():
    print("Lecture du fichier reservations_hostaway.tsv ...")
    reservations_df = pd.read_csv("reservations_hostaway.tsv", sep="\t")

    reservation_ids = reservations_df["reservationId"].dropna().astype(int).tolist()
    total_reservations = len(reservation_ids)

    print(f"{total_reservations} réservations à traiter")

    all_rows = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_reservation, rid): rid for rid in reservation_ids}

        for future in as_completed(futures):
            reservation_id = futures[future]
            try:
                rows = future.result()
                all_rows.extend(rows)
            except Exception as e:
                print(f"Erreur pour reservationId={reservation_id} : {e}")

            completed += 1
            if completed % 20 == 0 or completed == total_reservations:
                print(f"{completed}/{total_reservations} réservations traitées")

    df = pd.DataFrame(all_rows)
    df.to_csv("finance_fields_hostaway.tsv", sep="\t", index=False, encoding="utf-8-sig")

    print("Extraction terminée")
    print("Fichier généré : finance_fields_hostaway.tsv")

if __name__ == "__main__":
    main()