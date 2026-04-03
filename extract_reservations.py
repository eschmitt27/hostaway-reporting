import time
import requests
import pandas as pd
import os

ACCESS_TOKEN = os.getenv("HOSTAWAY_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("HOSTAWAY_TOKEN est manquant")
BASE_URL = "https://api.hostaway.com/v1"

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}

def api_get(url, params=None):
    response = requests.get(url, headers=HEADERS, params=params, timeout=60)

    if response.status_code == 429:
        print("Rate limit atteint, pause 3 secondes...")
        time.sleep(3)
        response = requests.get(url, headers=HEADERS, params=params, timeout=60)

    if response.status_code == 403:
        print("Erreur 403 : token invalide ou expiré.")
        print("Remplace ACCESS_TOKEN par un nouveau token.")
        print("Réponse brute :", response.text)
        raise SystemExit(1)

    response.raise_for_status()
    return response.json()

def get_all_reservations():
    all_reservations = []
    limit = 100
    offset = 0
    count = None

    while True:
        print(f"Récupération offset {offset}...")

        data = api_get(
            f"{BASE_URL}/reservations",
            params={
                "limit": limit,
                "offset": offset,
                "includeResources": 1
            }
        )

        rows = data.get("result", [])
        count = data.get("count")

        print(f"{len(rows)} réservations récupérées | count={count}")

        if not rows:
            print("Aucune réservation retournée, arrêt.")
            break

        all_reservations.extend(rows)
        offset += len(rows)

        if count is not None and offset >= count:
            print("Toutes les réservations ont été récupérées.")
            break

        if len(rows) < limit:
            print("Dernier lot partiel détecté.")
            break

        time.sleep(0.7)

    return all_reservations

def flatten_reservations(reservations):
    output = []

    for r in reservations:
        output.append({
            "reservationId": r.get("id"),
            "listingMapId": r.get("listingMapId"),
            "listingName": r.get("listingName"),
            "channelName": r.get("channelName"),
            "channelId": r.get("channelId"),
            "reservationCode": r.get("reservationId"),
            "guestName": r.get("guestName"),
            "guestFirstName": r.get("guestFirstName"),
            "guestLastName": r.get("guestLastName"),
            "arrivalDate": r.get("arrivalDate"),
            "departureDate": r.get("departureDate"),
            "nights": r.get("nights"),
            "numberOfGuests": r.get("numberOfGuests"),
            "status": r.get("status"),
            "paymentStatus": r.get("paymentStatus"),
            "totalPrice": r.get("totalPrice"),
            "currency": r.get("currency"),
            "reservationDate": r.get("reservationDate"),
            "insertedOn": r.get("insertedOn"),
            "updatedOn": r.get("updatedOn"),
            "latestActivityOn": r.get("latestActivityOn")
        })

    return output

def main():
    print("Début extraction Hostaway...")

    reservations = get_all_reservations()
    print(f"Total récupéré : {len(reservations)} réservations")

    flat_data = flatten_reservations(reservations)
    df = pd.DataFrame(flat_data)

    df.to_csv("reservations_hostaway.tsv", sep="\t", index=False, encoding="utf-8-sig")

    print("Extraction terminée")
    print("Fichier généré : reservations_hostaway.tsv")

if __name__ == "__main__":
    main()