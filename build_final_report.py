import pandas as pd

RESERVATIONS_FILE = "reservations_hostaway.tsv"
FINANCE_FIELDS_FILE = "finance_fields_hostaway.tsv"
LISTING_CONSTANTS_FILE = "listing_constants.csv"

OUTPUT_FILE = "hostaway_reporting_final.tsv"

def clean_numeric(series):
    return pd.to_numeric(series, errors="coerce")

def compute_total_payout(row):
    airbnb_payout = row.get("airbnbPayoutSum")
    total_price_channel = row.get("totalPriceFromChannel")

    if pd.notna(airbnb_payout):
        return airbnb_payout

    if pd.notna(total_price_channel):
        return total_price_channel

    return None

def main():
    print("Lecture des fichiers...")

    reservations = pd.read_csv(RESERVATIONS_FILE, sep="\t")
    finance_fields = pd.read_csv(FINANCE_FIELDS_FILE, sep="\t")
    listing_constants = pd.read_csv(LISTING_CONSTANTS_FILE)

    print("Nettoyage des types...")

    reservations["reservationId"] = pd.to_numeric(reservations["reservationId"], errors="coerce")
    reservations["listingMapId"] = pd.to_numeric(reservations["listingMapId"], errors="coerce")

    finance_fields["reservationId"] = pd.to_numeric(finance_fields["reservationId"], errors="coerce")
    finance_fields["value"] = clean_numeric(finance_fields["value"])

    listing_constants["listingMapId"] = pd.to_numeric(listing_constants["listingMapId"], errors="coerce")
    listing_constants["CoutMenage"] = clean_numeric(listing_constants["CoutMenage"])
    listing_constants["TauxCommission"] = clean_numeric(listing_constants["TauxCommission"])

    print("Préparation des finance fields utiles...")

    payout_fields = finance_fields[
        finance_fields["name"].isin(["airbnbPayoutSum", "totalPriceFromChannel"])
    ].copy()

    print("Pivot des finance fields...")
    payout_pivot = payout_fields.pivot_table(
        index="reservationId",
        columns="name",
        values="value",
        aggfunc="first"
    ).reset_index()

    print("Calcul de TotalPayout...")
    payout_pivot["TotalPayout"] = payout_pivot.apply(compute_total_payout, axis=1)

    payout_by_reservation = payout_pivot[["reservationId", "TotalPayout"]]

    print("Fusion avec les réservations...")
    final_df = reservations.merge(
        payout_by_reservation,
        on="reservationId",
        how="left"
    )

    print("Fusion avec les constantes par annonce...")
    final_df = final_df.merge(
        listing_constants[["listingMapId", "CoutMenage", "TauxCommission"]],
        on="listingMapId",
        how="left"
    )

    print("Création des colonnes finales...")
    final_df["NombreDeNuits"] = pd.to_numeric(final_df["nights"], errors="coerce")

    desired_columns = [
        "reservationId",
        "listingMapId",
        "listingName",
        "channelName",
        "arrivalDate",
        "departureDate",
        "NombreDeNuits",
        "TotalPayout",
        "CoutMenage",
        "TauxCommission",
        "status",
        "paymentStatus",
        "totalPrice",
        "currency",
        "reservationDate",
        "updatedOn",
    ]

    other_columns = [col for col in final_df.columns if col not in desired_columns]
    final_df = final_df[desired_columns + other_columns]

    print(f"Export du fichier final : {OUTPUT_FILE}")
    final_df.to_csv(OUTPUT_FILE, sep="\t", index=False, encoding="utf-8-sig")

    print("Terminé.")
    print(f"Fichier généré : {OUTPUT_FILE}")

if __name__ == "__main__":
    main()