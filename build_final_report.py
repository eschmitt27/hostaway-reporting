import pandas as pd

RESERVATIONS_FILE = "reservations_hostaway.tsv"
FINANCE_FIELDS_FILE = "finance_fields_hostaway.tsv"
LISTING_CONSTANTS_FILE = "listing_constants.csv"

OUTPUT_FILE = "hostaway_reporting_final.tsv"


def clean_numeric(series):
    return pd.to_numeric(series, errors="coerce")


def safe_value(row, column_name):
    value = row.get(column_name)
    return value if pd.notna(value) else 0


def compute_total_payout(row):
    channel = str(row.get("channelName", "")).lower()

    airbnb_payout = row.get("airbnbPayoutSum")
    total_price_channel = row.get("totalPriceFromChannel")

    city_tax = safe_value(row, "cityTax")
    ota_payment_processing_fee = safe_value(row, "otaPaymentProcessingFee")
    host_channel_fee = safe_value(row, "hostChannelFee")

    # Règle spécifique Booking
    if "booking" in channel:
        if pd.notna(total_price_channel):
            return (
                total_price_channel
                - city_tax
                - ota_payment_processing_fee
                - host_channel_fee
            )
        return None

    # Règle Airbnb
    if pd.notna(airbnb_payout):
        return airbnb_payout

    # Règle de secours autres canaux
    if pd.notna(total_price_channel):
        return total_price_channel

    return None


def main():
    print("Lecture des fichiers...")

    reservations = pd.read_csv(RESERVATIONS_FILE, sep="\t")
    finance_fields = pd.read_csv(FINANCE_FIELDS_FILE, sep="\t")
    listing_constants = pd.read_csv(
        LISTING_CONSTANTS_FILE,
        sep=";",
        encoding="utf-8-sig"
    )

    print("Colonnes détectées dans listing_constants.csv :")
    print(listing_constants.columns.tolist())

    print("Nettoyage des types...")

    reservations["reservationId"] = pd.to_numeric(
        reservations["reservationId"], errors="coerce"
    )
    reservations["listingMapId"] = pd.to_numeric(
        reservations["listingMapId"], errors="coerce"
    )

    finance_fields["reservationId"] = pd.to_numeric(
        finance_fields["reservationId"], errors="coerce"
    )
    finance_fields["value"] = clean_numeric(finance_fields["value"])

    listing_constants["listingMapId"] = pd.to_numeric(
        listing_constants["listingMapId"], errors="coerce"
    )
    listing_constants["CoutMenage"] = clean_numeric(
        listing_constants["CoutMenage"]
    )
    listing_constants["TauxCommission"] = clean_numeric(
        listing_constants["TauxCommission"]
    )

    print("Préparation des finance fields utiles...")

    finance_fields_utiles = [
        "airbnbPayoutSum",
        "totalPriceFromChannel",
        "cityTax",
        "otaPaymentProcessingFee",
        "hostChannelFee"
    ]

    payout_fields = finance_fields[
        finance_fields["name"].isin(finance_fields_utiles)
    ].copy()

    print("Pivot des finance fields...")

    payout_pivot = payout_fields.pivot_table(
        index="reservationId",
        columns="name",
        values="value",
        aggfunc="first"
    ).reset_index()

    # Sécurité : si une colonne n'existe pas dans les données, on la crée vide
    for col in finance_fields_utiles:
        if col not in payout_pivot.columns:
            payout_pivot[col] = pd.NA

    print("Fusion avec les réservations...")

    final_df = reservations.merge(
        payout_pivot,
        on="reservationId",
        how="left"
    )

    print("Calcul de TotalPayout...")

    final_df["TotalPayout"] = final_df.apply(compute_total_payout, axis=1)

    print("Fusion avec les constantes par annonce...")

    final_df = final_df.merge(
        listing_constants[["listingMapId", "CoutMenage", "TauxCommission"]],
        on="listingMapId",
        how="left"
    )

    print("Création des colonnes finales...")

    final_df["NombreDeNuits"] = pd.to_numeric(
        final_df["nights"], errors="coerce"
    )

    desired_columns = [
        "reservationId",
        "listingMapId",
        "listingName",
        "channelName",
        "arrivalDate",
        "departureDate",
        "NombreDeNuits",

        "TotalPayout",
        "airbnbPayoutSum",
        "totalPriceFromChannel",
        "cityTax",
        "otaPaymentProcessingFee",
        "hostChannelFee",

        "CoutMenage",
        "TauxCommission",

        "status",
        "paymentStatus",
        "totalPrice",
        "currency",
        "reservationDate",
        "updatedOn",
    ]

    # Sécurité : on garde uniquement les colonnes qui existent vraiment
    desired_columns_existing = [
        col for col in desired_columns if col in final_df.columns
    ]

    other_columns = [
        col for col in final_df.columns if col not in desired_columns_existing
    ]

    final_df = final_df[desired_columns_existing + other_columns]

    print(f"Export du fichier final : {OUTPUT_FILE}")

    final_df.to_csv(
        OUTPUT_FILE,
        sep="\t",
        index=False,
        encoding="utf-8-sig"
    )

    print("Terminé.")
    print(f"Fichier généré : {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
