import pandas as pd
import numpy as np

def clean_metadata_fill_blanks_with_nan(input_path, output_path, duplicate_path):
    # Load data
    df = pd.read_csv(input_path)

    num_rows, num_cols = df.shape
    print(f"Starting off with input shape: {num_rows} rows × {num_cols} columns")


    # Standardize column names
    df.columns = [col.strip() for col in df.columns]

    # Replace all empty strings or pure whitespace with np.nan
    df.replace(r'^\s*$', np.nan, regex=True, inplace=True)

    # 1. Validate key identifiers (EIN2)
    df["EIN2"] = df["EIN2"].astype(str).str.strip()

    # Identify duplicate EIN2 values
    duplicated_eins = df[df.duplicated("EIN2", keep=False)].copy()
    num_duplicates = duplicated_eins["EIN2"].nunique()

    # Print number of EINs with duplicates and sample rows
    print(f"Duplicate EIN2 values found for {num_duplicates} unique EINs.")
    print("Sample rows with duplicate EIN2s:")
    print(duplicated_eins.head(10))
    print("Saving to duplicate file:", duplicate_path)
    duplicated_eins.to_csv(duplicate_path, index=False)

    # Remove all but the first occurrence of each duplicate EIN2
    #df = df.drop_duplicates(subset="EIN2", keep="first")

    # Convert GEOCODER_SCORE to numeric if not already
    df["GEOCODER_SCORE"] = pd.to_numeric(df["GEOCODER_SCORE"], errors='coerce')

    # Sort so highest GEOCODER_SCORE comes first
    df = df.sort_values(by="GEOCODER_SCORE", ascending=False)

    # Keep the row with the highest score for each EIN2
    df = df.drop_duplicates(subset="EIN2", keep="first")

    # 2. Clean ORG_ADDR_FULL
    if "ORG_ADDR_FULL" in df.columns:
        df["ORG_ADDR_FULL"] = (
            df["ORG_ADDR_FULL"]
            .astype(str)
            .str.replace(r'\s+', ' ', regex=True)
            .str.replace(r',+', ',', regex=True)
            .str.strip()
            .str.upper()
        )

    # 3. Clean and check geocoder fields
    df["ORG_ADDR_MATCH"] = df["ORG_ADDR_MATCH"].astype(str).str.strip().str.upper()

    df["GEOCODER_MATCH"] = df["GEOCODER_MATCH"].astype(str).str.strip().str.upper()

    df["GEOCODER_SCORE"] = pd.to_numeric(df["GEOCODER_SCORE"], errors='coerce')

    # 4. Validate geolocation
    df["LATITUDE"] = pd.to_numeric(df["LATITUDE"], errors='coerce')
    df.loc[~df["LATITUDE"].between(-90, 90), "LATITUDE"] = np.nan

    df["LONGITDUE"] = pd.to_numeric(df["LONGITDUE"], errors='coerce')
    df.loc[~df["LONGITDUE"].between(-180, 180), "LONGITDUE"] = np.nan

    # Optional: convert all integer columns to nullable Int64 dtype
    for col in df.select_dtypes(include=["float64", "int64"]).columns:
        if pd.api.types.is_integer_dtype(df[col]):
            df[col] = df[col].astype("Int64")

    # 10. Version control tagging
    df["METADATA_VERSION"] = "0.0"
    df["METADATA_RELEASE_DATE"] = "2024-06-24"

    # Save cleaned file
    df.to_csv(output_path, index=False)
    print(f"Cleaned dataset saved to: {output_path}")
    num_rows, num_cols = df.shape
    print(f"Ended with shape: {num_rows} rows × {num_cols} columns")
    return df.head()

# Execute the updated script
clean_metadata_fill_blanks_with_nan(
    input_path="Sample/METADATA_ADDRESS_GEOCODED_sample.csv",
    output_path="Data/METADATA_ADDRESS_GEOCODED_sample_final.csv",
    duplicate_path = "Data/METADATA_ADDRESS_GEOCODED_sample_duplicates.csv",
)
