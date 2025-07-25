import pandas as pd
import re

# Define dtypes for columns (use only valid pandas/numpy dtypes)
dtype_dict = {
    "EIN2": object,
    "ORG_FISCAL_YEAR": "Int64",
    "ORG_ADDR_FULL": object,
    "ORG_ADDR_MATCH": object,
    "LONGITUDE": "float64",
    "LATITUDE": "float64",
    "ADDR_TYPE": object,
    "GEOCODER_SCORE": "float64",
    "GEOCODER_MATCH": object,
    "CENSUS_BLOCK_FIPS": object,
    "CENSUS_BLOCK_CODE": object,
    "CENSUS_BLOCK_NAME": object,
    "CENSUS_BLOCK_HOUSING_UNIT_COUNT": "Int64",
    "CENSUS_BLOCK_POPULATION_COUNT": "Int64",
    "CENSUS_FUNCTIONAL_STATUS": object,
    "CENSUS_URBAN_AREA_TYPE": object,
    "CENSUS_URBAN_AREA_CODE": object,
    "CENSUS_TRACT_FIPS": object,
    "CENSUS_TRACT_CODE": object,
    "CENSUS_TRACT_NAME": object,
    "CENSUS_COUNTY_FIPS": object,
    "CENSUS_STATE_FIPS": object,
    "CENSUS_COUNTY_NAME": object,
    "CENSUS_CBSA_FIPS": object,
    "CENSUS_STATE_NAME": object,
    "CENSUS_STATE_ABBR": object,
    "CENSUS_URBAN_AREA": object,
    "CENSUS_CBSA_NAME": object,
    "CENSUS_CBSA_LEGAL_NAME": object,
    "CENSUS_CBSA_LEGAL_CODE": object,
    "CENSUS_CSA_FIPS": object
}

RAW_PATH = "../data/metadata-address-geocoded.csv"
RESULTS_PATH = "../data/"

# --- Cleaning Functions ---
def clean_cols(df):
    df.columns = [col.strip() for col in df.columns]
    if "LONGITDUE" in df.columns:
        df.rename(columns={"LONGITDUE": "LONGITUDE"}, inplace=True)
    return df

def clean_special_chars(val):
    if pd.isnull(val):
        return val
    val = str(val)
    val = re.sub(r'\s+', ' ', val)
    val = re.sub(r',+', ',', val)
    val = val.replace("\\", "\\\\")
    val = val.replace("'", "\\'")
    val = val.replace('"', '\\"')
    val = val.replace('\x00', '')
    val = val.replace('\r', ' ').replace('\n', ' ')
    return val

def clean_dataframe(df):
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({"nan": pd.NA, "": pd.NA})
            df[col] = df[col].replace(r'^\s*$', pd.NA, regex=True)
            df[col] = df[col].apply(clean_special_chars)
    return df

def clean_ein2(ein):
    """
    Convert EIN2 from formats like 'EIN-00-0000001' to '000000001'
    """
    if pd.isnull(ein):
        return ein
    # Remove prefix if present
    ein = re.sub(r'^EIN-00-', '', str(ein))
    # Remove any non-digit characters
    ein = re.sub(r'\D', '', ein)
    # Pad to 9 digits
    return ein.zfill(9)

def customize_data(df):
    if 'LATITUDE' in df.columns:
        df.loc[~df["LATITUDE"].between(-90, 90), "LATITUDE"] = pd.NA
    if 'LONGITUDE' in df.columns:
        df.loc[~df["LONGITUDE"].between(-180, 180), "LONGITUDE"] = pd.NA
    # Clean EIN2 format
    df['EIN2'] = df['EIN2'].apply(clean_ein2)
    return df

# --- Normalization Functions ---
def save_table(df, path, name):
    df.to_csv(path, index=False)
    print(f"Saved {name} table of size {df.shape} to {path}")

def normalize_and_save(df):
    # ORG_LOC
    org_loc = df[[
        'EIN2', 'ORG_FISCAL_YEAR', 'ORG_ADDR_FULL', 'ORG_ADDR_MATCH', 'LONGITUDE', 'LATITUDE',
        'ADDR_TYPE', 'GEOCODER_SCORE', 'GEOCODER_MATCH', 'CENSUS_BLOCK_FIPS']].drop_duplicates(subset=['EIN2']) #drop duplicate EINs
    save_table(org_loc, f'{RESULTS_PATH}NCCS_Address_Metadata_org_loc.csv', 'ORG_LOC')

    # CENSUS_BLOCKS
    blocks = df[[
        'CENSUS_BLOCK_FIPS', 'CENSUS_BLOCK_CODE', 'CENSUS_BLOCK_NAME',
        'CENSUS_BLOCK_HOUSING_UNIT_COUNT', 'CENSUS_BLOCK_POPULATION_COUNT',
        'CENSUS_FUNCTIONAL_STATUS', 'CENSUS_URBAN_AREA_CODE', 'CENSUS_TRACT_FIPS']].drop_duplicates()

    no_prefix_match_rows = pd.DataFrame()
    def pick_tract_prefix_match(group):
        nonlocal no_prefix_match_rows
        if len(group) == 1:
            return group.iloc[0]
        block_fips = str(group['CENSUS_BLOCK_FIPS'].iloc[0])
        prefix_matches = group[group['CENSUS_TRACT_FIPS'].apply(
            lambda x: str(block_fips).startswith(str(x)) if pd.notnull(x) else False
        )]
        if len(prefix_matches) > 0:
            return prefix_matches.iloc[0]
        else:
            no_prefix_match_rows = pd.concat([no_prefix_match_rows, group], ignore_index=True)
            tract_counts = group['CENSUS_TRACT_FIPS'].value_counts()
            most_common_tract = tract_counts.index[0]
            return group[group['CENSUS_TRACT_FIPS'] == most_common_tract].iloc[0]

    blocks_deduped = (
        blocks.groupby('CENSUS_BLOCK_FIPS', group_keys=False)
        .apply(pick_tract_prefix_match)
        .reset_index(drop=True)
    )
    save_table(blocks_deduped, f'{RESULTS_PATH}NCCS_Address_Metadata_census_blocks.csv', 'CENSUS_BLOCKS')
    #no_prefix_match_rows.to_csv("{RESULTS_PATH}no_prefix_match_rows.csv", index=False)

    # URBAN_AREAS
    urban_areas = (
        df[['CENSUS_URBAN_AREA_CODE', 'CENSUS_URBAN_AREA']]
        .dropna(subset=['CENSUS_URBAN_AREA_CODE', 'CENSUS_URBAN_AREA'], how='all')
        .drop_duplicates()
    )
    save_table(urban_areas, f'{RESULTS_PATH}NCCS_Address_Metadata_urban_areas.csv', 'URBAN_AREAS')

    # TRACTS
    tracts = df[['CENSUS_TRACT_FIPS', 'CENSUS_TRACT_CODE', 'CENSUS_TRACT_NAME', 'CENSUS_COUNTY_FIPS', 'CENSUS_STATE_FIPS']].drop_duplicates()
    save_table(tracts, f'{RESULTS_PATH}NCCS_Address_Metadata_tracts.csv', 'TRACTS')

    # COUNTIES
    county_cbsa_counts = (
        df.groupby(['CENSUS_STATE_FIPS', 'CENSUS_COUNTY_FIPS', 'CENSUS_CBSA_FIPS'])
        .size().reset_index(name='count')
    )
    top_cbsa_per_county = (
        county_cbsa_counts
        .sort_values(['CENSUS_STATE_FIPS', 'CENSUS_COUNTY_FIPS', 'count'], ascending=[True, True, False])
        .drop_duplicates(subset=['CENSUS_STATE_FIPS', 'CENSUS_COUNTY_FIPS'], keep='first')
        .drop(columns='count')
    )
    county_names = df[['CENSUS_STATE_FIPS', 'CENSUS_COUNTY_FIPS', 'CENSUS_COUNTY_NAME']].drop_duplicates()
    counties = (
        top_cbsa_per_county
        .merge(county_names, on=['CENSUS_STATE_FIPS', 'CENSUS_COUNTY_FIPS'], how='left')
        [['CENSUS_COUNTY_FIPS', 'CENSUS_COUNTY_NAME', 'CENSUS_STATE_FIPS', 'CENSUS_CBSA_FIPS']]
    )
    save_table(counties, f'{RESULTS_PATH}NCCS_Address_Metadata_counties.csv', 'COUNTIES')

    # STATES
    states = df[['CENSUS_STATE_FIPS', 'CENSUS_STATE_NAME', 'CENSUS_STATE_ABBR']].drop_duplicates()
    save_table(states, f'{RESULTS_PATH}NCCS_Address_Metadata_states.csv', 'STATES')

    # CBSA
    cbsa = df[['CENSUS_CBSA_FIPS', 'CENSUS_CBSA_NAME', 'CENSUS_CBSA_LEGAL_NAME', 'CENSUS_CBSA_LEGAL_CODE', 'CENSUS_CSA_FIPS']].drop_duplicates()
    save_table(cbsa, f'{RESULTS_PATH}NCCS_Address_Metadata_cbsa.csv', 'CBSA')

    return org_loc, blocks_deduped, tracts, counties, states, urban_areas, cbsa

def sanity_check(df, org_loc, blocks, tracts, counties, states, urban_areas, cbsa):
    def handle_assertion_failure(assertion_name, error_message, data, output_path):
        """Handle assertion failure by saving problematic data to CSV"""
        print(f"{assertion_name} FAILED: {error_message}")
        print(f"   Saving problematic data to {output_path}")
        data.to_csv(output_path, index=False)
        return False

    # Check ORG_LOC uniqueness
    try:
        assert org_loc['EIN2'].is_unique, "ORG_LOC.EIN2 is not unique"
        print("ORG_LOC.EIN2 uniqueness check passed")
    except AssertionError as e:
        # Find duplicates
        duplicates = org_loc[org_loc['EIN2'].duplicated(keep=False)]
        handle_assertion_failure("ORG_LOC.EIN2 uniqueness", str(e), duplicates, "Raw Data/assertion_failures_org_loc_ein2_duplicates.csv")

    # Check BLOCKS uniqueness
    try:
        assert blocks['CENSUS_BLOCK_FIPS'].is_unique, "CENSUS_BLOCK_FIPS not unique in blocks table"
        print("CENSUS_BLOCK_FIPS uniqueness check passed")
    except AssertionError as e:
        # Find duplicates
        duplicates = blocks[blocks['CENSUS_BLOCK_FIPS'].duplicated(keep=False)]
        handle_assertion_failure("CENSUS_BLOCK_FIPS uniqueness", str(e), duplicates, "Raw Data/assertion_failures_blocks_duplicates.csv")

    # Check TRACTS uniqueness
    try:
        assert tracts['CENSUS_TRACT_FIPS'].is_unique, "CENSUS_TRACT_FIPS not unique in tracts table"
        print("CENSUS_TRACT_FIPS uniqueness check passed")
    except AssertionError as e:
        # Find duplicates
        duplicates = tracts[tracts['CENSUS_TRACT_FIPS'].duplicated(keep=False)]
        handle_assertion_failure("CENSUS_TRACT_FIPS uniqueness", str(e), duplicates, "Raw Data/assertion_failures_tracts_duplicates.csv")

    # Check COUNTIES uniqueness
    try:
        assert not counties.duplicated(subset=['CENSUS_STATE_FIPS', 'CENSUS_COUNTY_FIPS']).any(), \
            "Duplicate (CENSUS_STATE_FIPS, CENSUS_COUNTY_FIPS) in counties table"
        print("COUNTIES composite key uniqueness check passed")
    except AssertionError as e:
        # Find duplicates
        duplicates = counties[counties.duplicated(subset=['CENSUS_STATE_FIPS', 'CENSUS_COUNTY_FIPS'], keep=False)]
        handle_assertion_failure("COUNTIES composite key uniqueness", str(e), duplicates, "Raw Data/assertion_failures_counties_duplicates.csv")

    # Check STATES uniqueness
    try:
        assert states['CENSUS_STATE_FIPS'].is_unique, "CENSUS_STATE_FIPS not unique in states table"
        print("CENSUS_STATE_FIPS uniqueness check passed")
    except AssertionError as e:
        # Find duplicates
        duplicates = states[states['CENSUS_STATE_FIPS'].duplicated(keep=False)]
        handle_assertion_failure("CENSUS_STATE_FIPS uniqueness", str(e), duplicates, "Raw Data/assertion_failures_states_duplicates.csv")

    # Check URBAN_AREAS uniqueness
    try:
        assert urban_areas['CENSUS_URBAN_AREA_CODE'].is_unique, "CENSUS_URBAN_AREA_CODE not unique in urban_areas table"
        print("CENSUS_URBAN_AREA_CODE uniqueness check passed")
    except AssertionError as e:
        # Find duplicates
        duplicates = urban_areas[urban_areas['CENSUS_URBAN_AREA_CODE'].duplicated(keep=False)]
        handle_assertion_failure("CENSUS_URBAN_AREA_CODE uniqueness", str(e), duplicates, "Raw Data/assertion_failures_urban_areas_duplicates.csv")

    # Check CBSA uniqueness
    try:
        assert cbsa['CENSUS_CBSA_FIPS'].is_unique, "CENSUS_CBSA_FIPS not unique in cbsa table"
        print("CENSUS_CBSA_FIPS uniqueness check passed")
    except AssertionError as e:
        # Find duplicates
        duplicates = cbsa[cbsa['CENSUS_CBSA_FIPS'].duplicated(keep=False)]
        handle_assertion_failure("CENSUS_CBSA_FIPS uniqueness", str(e), duplicates, "Raw Data/assertion_failures_cbsa_duplicates.csv")

    # Compare reconstructed with original (filtered to 1 row per EIN2)
    reconstructed = (
        org_loc
        .merge(blocks, on='CENSUS_BLOCK_FIPS', how='left')
        .merge(tracts, on='CENSUS_TRACT_FIPS', how='left')
        .merge(counties, on=[ 'CENSUS_STATE_FIPS', 'CENSUS_COUNTY_FIPS'], how='left')
        .merge(states, on='CENSUS_STATE_FIPS', how='left')
        .merge(urban_areas, on=['CENSUS_URBAN_AREA_CODE'], how='left')
        .merge(cbsa, on='CENSUS_CBSA_FIPS', how='left')
    )
    reconstructed_sorted = reconstructed.sort_values(by='EIN2').reset_index(drop=True)
    orig_filtered = df.drop_duplicates(subset=['EIN2']).sort_values(by='EIN2').reset_index(drop=True)

    print(f"Reconstructed table of size {reconstructed_sorted.shape}")
    print(f"Original table of size {orig_filtered.shape}")
    print(f"Reconstructed table columns: {reconstructed_sorted.dtypes}")
    print(f"Original table columns: {orig_filtered.dtypes}")
    assert orig_filtered.shape[0] == reconstructed_sorted.shape[0], "Reconstructed table does not match the original row count."
    print("Reconstructed table matches the original row count.")

def print_schema_suggestions(tables: dict):
    """
    Print max string length for object columns, min/max for numerics, and nullability for each table.
    """
    for name, df in tables.items():
        print(f"\nSchema suggestion for table: {name}")
        for col in df.columns:
            col_data = df[col]
            dtype = col_data.dtype
            null_count = col_data.isnull().sum()
            null_info = f"NULLABLE ({null_count} nulls)" if null_count > 0 else "NOT NULL"
            if dtype == object or pd.api.types.is_string_dtype(dtype):
                max_len = col_data.dropna().astype(str).map(len).max()
                print(f"  {col}: VARCHAR({max_len}) {null_info}")
            elif pd.api.types.is_integer_dtype(dtype):
                min_val = col_data.min()
                max_val = col_data.max()
                print(f"  {col}: INT ({min_val} to {max_val}) {null_info}")
            elif pd.api.types.is_float_dtype(dtype):
                min_val = col_data.min()
                max_val = col_data.max()
                print(f"  {col}: DOUBLE ({min_val} to {max_val}) {null_info}")
            else:
                print(f"  {col}: {dtype} {null_info}")

# --- Main Script ---
def main():
    df = pd.read_csv(RAW_PATH, dtype=dtype_dict)
    df = clean_cols(df)
    print(f"Read original data of size {df.shape} from {RAW_PATH}")
    df = df.dropna(subset=['EIN2'])
    df = df[df['GEOCODER_SCORE'] > 0]
    print(f"After applying filters: {df.shape}")
    df = customize_data(df)
    print(f"After customizing: {df.shape}")
    df = clean_dataframe(df)
    print(f"After cleaning: {df.shape}")
    org_loc, blocks, tracts, counties, states, urban_areas, cbsa = normalize_and_save(df)
    # Print schema suggestions for MySQL
    print_schema_suggestions({
        "org_loc": org_loc,
        "blocks": blocks,
        "tracts": tracts,
        "counties": counties,
        "states": states,
        "urban_areas": urban_areas,
        "cbsa": cbsa
    })
    sanity_check(df, org_loc, blocks, tracts, counties, states, urban_areas, cbsa)

if __name__ == "__main__":
    main() 