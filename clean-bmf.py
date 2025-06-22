import pandas as pd
import argparse
import os
import sys

def clean_bmf_data(input_file, output_file=None):
    """
    Clean BMF dataset for database compatibility while preserving all columns
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output CSV file (optional)
    """
    
    # Validate input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found!")
        sys.exit(1)
    
    # Set default output filename if not provided
    if output_file is None:
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        output_file = f"{base_name}_cleaned.csv"
    
    print(f"Loading data from: {input_file}")
    print(f"Output will be saved to: {output_file}")
    
    try:
        # Load BMF dataset
        df = pd.read_csv(input_file, dtype=str)
        print(f"Loaded {len(df)} rows and {len(df.columns)} columns")
        
        # -----------------------------
        # STEP 1: Basic string cleaning for database compatibility
        # -----------------------------
        print("Step 1: Basic string cleaning...")
        str_cols = df.select_dtypes(include='object').columns
        df[str_cols] = df[str_cols].apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        
        # -----------------------------
        # STEP 2: EIN and EIN2 cleaning for primary/foreign key compatibility
        # -----------------------------
        print("Step 2: Cleaning EIN and EIN2 for database keys...")
        
        # EIN (Primary Key) - ensure 9-digit format with leading zeros
        if 'EIN' in df.columns:
            df['EIN'] = df['EIN'].str.zfill(9)
            # Remove rows with invalid EIN (non-numeric or missing)
            df = df[df['EIN'].notna() & df['EIN'].str.isnumeric() & (df['EIN'].str.len() == 9)]
            print(f"  - Kept {len(df)} rows after EIN cleaning")
        
        # EIN2 (Foreign Key) - ensure 9-digit format with leading zeros
        if 'EIN2' in df.columns:
            df['EIN2'] = df['EIN2'].str.zfill(9)
            # Keep EIN2 even if invalid (will be NULL in database)
            df.loc[~(df['EIN2'].str.isnumeric() & (df['EIN2'].str.len() == 9)), 'EIN2'] = None
            print(f"  - Cleaned EIN2 for foreign key relationships")
        
        # -----------------------------
        # STEP 3: Rename columns for better intuitiveness
        # -----------------------------
        print("Step 3: Renaming columns for better clarity...")
        
        # Comprehensive column mapping for intuitive names
        column_mapping = {
            # Core identifiers
            'EIN': 'ein_pk',
            'EIN2': 'ein_fk',
            
            # Organization names
            'ORG_NAME_CURRENT': 'org_name',
            'ORG_NAME_SEC': 'org_name_secondary',
            
            # Classification codes
            'NTEE_IRS': 'ntee_irs_code',
            'NTEE_NCCS': 'ntee_nccs_code', 
            'NTEEV2': 'ntee_modern_code',
            'NCCS_LEVEL_1': 'nccs_level_1_category',
            'NCCS_LEVEL_2': 'nccs_level_2_category',
            'NCCS_LEVEL_3': 'nccs_level_3_category',
            
            # Financial data (Form 990)
            'F990_TOTAL_REVENUE_RECENT': 'total_revenue',
            'F990_TOTAL_INCOME_RECENT': 'total_income',
            'F990_TOTAL_ASSETS_RECENT': 'total_assets',
            
            # Address information (Form 990)
            'F990_ORG_ADDR_CITY': 'city',
            'F990_ORG_ADDR_STATE': 'state',
            'F990_ORG_ADDR_ZIP': 'zipcode',
            'F990_ORG_ADDR_STREET': 'street_address',
            
            # Census geographic data
            'CENSUS_CBSA_FIPS': 'census_cbsa_fips_code',
            'CENSUS_CBSA_NAME': 'census_cbsa_name',
            'CENSUS_BLOCK_FIPS': 'census_block_fips_code',
            'CENSUS_URBAN_AREA': 'census_urban_area',
            'CENSUS_STATE_ABBR': 'census_state_abbreviation',
            'CENSUS_COUNTY_NAME': 'census_county_name',
            
            # Geocoded address data
            'ORG_ADDR_FULL': 'address_full',
            'ORG_ADDR_MATCH': 'address_matched',
            'LATITUDE': 'latitude',
            'LONGITUDE': 'longitude',
            'GEOCODER_SCORE': 'geocoder_confidence_score',
            'GEOCODER_MATCH': 'geocoder_match_type',
            
            # BMF (Business Master File) codes
            'BMF_SUBSECTION_CODE': 'bmf_subsection_code',
            'BMF_STATUS_CODE': 'bmf_status_code',
            'BMF_PF_FILING_REQ_CODE': 'bmf_private_foundation_filing_required',
            'BMF_ORGANIZATION_CODE': 'bmf_organization_code',
            'BMF_INCOME_CODE': 'bmf_income_code',
            'BMF_GROUP_EXEMPT_NUM': 'bmf_group_exemption_number',
            'BMF_FOUNDATION_CODE': 'bmf_foundation_code',
            'BMF_FILING_REQ_CODE': 'bmf_filing_requirement_code',
            'BMF_DEDUCTIBILITY_CODE': 'bmf_deductibility_code',
            'BMF_CLASSIFICATION_CODE': 'bmf_classification_code',
            'BMF_ASSET_CODE': 'bmf_asset_code',
            'BMF_AFFILIATION_CODE': 'bmf_affiliation_code',
            
            # Organization dates
            'ORG_RULING_DATE': 'organization_ruling_date',
            'ORG_FISCAL_YEAR': 'organization_fiscal_year',
            'ORG_RULING_YEAR': 'organization_ruling_year',
            'ORG_YEAR_FIRST': 'organization_first_year',
            'ORG_YEAR_LAST': 'organization_last_year',
            'ORG_YEAR_COUNT': 'organization_year_count',
            'ORG_PERS_ICO': 'organization_personal_ico',
            'ORG_FISCAL_PERIOD': 'organization_fiscal_period'
        }
        
        # Apply column renaming (only for columns that exist)
        existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_columns)
        print(f"  - Renamed {len(existing_columns)} columns for better clarity")
        
        # -----------------------------
        # STEP 4: Clean dates for database compatibility
        # -----------------------------
        print("Step 4: Cleaning dates...")
        # Find all potential date columns
        date_patterns = ['DATE', 'DT', 'YEAR', 'MONTH', 'DAY']
        date_cols = [col for col in df.columns if any(pattern in col.upper() for pattern in date_patterns)]
        
        for col in date_cols:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                print(f"  - Converted {col} to datetime")
            except:
                print(f"  - Skipped {col} (not a valid date column)")
        
        # -----------------------------
        # STEP 5: Clean numeric columns for database compatibility
        # -----------------------------
        print("Step 5: Cleaning numeric columns...")
        # Find potential numeric columns (common patterns)
        numeric_patterns = ['AMOUNT', 'REVENUE', 'INCOME', 'ASSETS', 'LIABILITIES', 'EXPENSES', 'GRANTS', 'CONTRIBUTIONS', 'SALARY', 'COMPENSATION', 'BALANCE', 'TOTAL', 'COUNT', 'NUMBER', 'PERCENT', 'RATIO', 'SCORE', 'YEAR_COUNT']
        numeric_cols = [col for col in df.columns if any(pattern in col.upper() for pattern in numeric_patterns)]
        
        for col in numeric_cols:
            try:
                # Remove common non-numeric characters and convert
                df[col] = df[col].astype(str).str.replace('$', '').str.replace(',', '').str.replace('(', '-').str.replace(')', '')
                df[col] = pd.to_numeric(df[col], errors='coerce')
                print(f"  - Converted {col} to numeric")
            except:
                print(f"  - Skipped {col} (not a valid numeric column)")
        
        # -----------------------------
        # STEP 6: Handle NULL values for database compatibility
        # -----------------------------
        print("Step 6: Handling NULL values...")
        # Replace empty strings with None for proper NULL handling in database
        df = df.replace('', None)
        df = df.replace('nan', None)
        df = df.replace('None', None)
        
        # -----------------------------
        # STEP 7: Remove duplicates based on EIN (primary key)
        # -----------------------------
        print("Step 7: Removing duplicates...")
        initial_rows = len(df)
        
        # Step 7a: Drop placeholder EINs
        placeholder_count = len(df[df['ein_pk'] == '000000000'])
        df = df[df['ein_pk'] != '000000000']
        print(f"  - Removed {placeholder_count} placeholder EINs (000000000)")
        
        # Step 7b: Drop exact row duplicates
        exact_duplicates = df.duplicated().sum()
        df = df.drop_duplicates()
        print(f"  - Removed {exact_duplicates} exact row duplicates")
        
        # Step 7c: Handle EINs with multiple valid entries
        print("  - Handling EINs with multiple valid entries...")
        
        # Find EINs that still have duplicates after exact duplicate removal
        duplicate_mask = df['ein_pk'].duplicated(keep=False)
        duplicate_rows = df[duplicate_mask]
        
        if len(duplicate_rows) > 0:
            print(f"    - Found {len(duplicate_rows)} rows with duplicate EINs")
            
            # Save duplicates to a separate file for manual review
            duplicates_file = output_file.replace('.csv', '_duplicates.csv')
            duplicate_rows.to_csv(duplicates_file, index=False)
            print(f"    - Saved duplicates to: {duplicates_file}")
            
            # Strategy: Keep most recent entry based on organization_last_year or organization_ruling_date
            print("    - Keeping most recent entry for each EIN...")
            
            # Create a priority score for each row
            def get_priority_score(row):
                # Priority 1: organization_last_year (most recent)
                if pd.notna(row.get('organization_last_year')):
                    try:
                        return pd.to_datetime(row['organization_last_year']).year
                    except:
                        pass
                
                # Priority 2: organization_ruling_date
                if pd.notna(row.get('organization_ruling_date')):
                    try:
                        return pd.to_datetime(row['organization_ruling_date']).year
                    except:
                        pass
                
                # Priority 3: organization_ruling_year
                if pd.notna(row.get('organization_ruling_year')):
                    try:
                        return pd.to_datetime(row['organization_ruling_year']).year
                    except:
                        pass
                
                # Default: keep first occurrence
                return 1900
            
            # Apply priority scoring
            df['_priority_score'] = df.apply(get_priority_score, axis=1)
            
            # Keep the row with highest priority score for each EIN
            df = df.sort_values('_priority_score', ascending=False).drop_duplicates(subset=['ein_pk'], keep='first')
            
            # Remove the temporary priority column
            df = df.drop('_priority_score', axis=1)
            
            # Create a summary of what was kept
            duplicate_summary = duplicate_rows.groupby('ein_pk').agg({
                'org_name': 'count',
                'total_revenue': ['first', 'last'],
                'total_income': ['first', 'last'],
                'total_assets': ['first', 'last'],
                'organization_ruling_date': ['first', 'last'],
                'organization_last_year': ['first', 'last']
            }).round(2)
            
            summary_file = output_file.replace('.csv', '_duplicates_summary.csv')
            duplicate_summary.to_csv(summary_file)
            print(f"    - Saved duplicate summary to: {summary_file}")
            
            print(f"    - Kept most recent entry for each duplicate EIN")
        else:
            print(f"    - No duplicate EINs found after exact duplicate removal")
        
        final_rows = len(df)
        total_removed = initial_rows - final_rows
        print(f"  - Total rows removed: {total_removed}")
        print(f"  - Final dataset: {final_rows} rows")
        
        # -----------------------------
        # STEP 8: Final validation for database compatibility
        # -----------------------------
        print("Step 8: Final database compatibility checks...")
        
        # Ensure EIN is unique and not null
        if 'ein_pk' in df.columns:
            null_eins = df['ein_pk'].isna().sum()
            if null_eins > 0:
                print(f"  Warning: {null_eins} rows have NULL EIN values")
            
            duplicate_eins = df['ein_pk'].duplicated().sum()
            if duplicate_eins > 0:
                print(f"  Warning: {duplicate_eins} duplicate EIN values found")
        
        # Check for problematic characters in string columns
        problematic_chars = ['\x00', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08', '\x0b', '\x0c', '\x0e', '\x0f']
        for col in df.select_dtypes(include='object').columns:
            for char in problematic_chars:
                if df[col].astype(str).str.contains(char, na=False).any():
                    df[col] = df[col].astype(str).str.replace(char, '')
        
        # -----------------------------
        # STEP 9: Export cleaned dataset
        # -----------------------------
        print("Step 9: Exporting cleaned dataset...")
        df.to_csv(output_file, index=False)
        print(f"Successfully cleaned BMF data for database import!")
        print(f"Final dataset: {len(df)} rows, {len(df.columns)} columns")
        print(f"Saved to: {output_file}")
        print(f"EIN ready as primary key, EIN2 ready as foreign key")
        print(f"Column names improved for better intuitiveness")
        
        return df
        
    except Exception as e:
        print(f"Error during cleaning: {str(e)}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Clean BMF (Business Master File) data for database import')
    parser.add_argument('--input', '-i', 
                       default='Sample/BMF_UNIFIED_sample.csv',
                       help='Input CSV file path (default: Sample/BMF_UNIFIED_sample.csv)')
    parser.add_argument('--output', '-o',
                       help='Output CSV file path (default: auto-generated)')
    parser.add_argument('--sample', action='store_true',
                       help='Use sample data (equivalent to --input Sample/BMF_UNIFIED_sample.csv)')
    parser.add_argument('--full', action='store_true',
                       help='Use full BMF data (equivalent to --input ../BMF_UNIFIED_V1.1.csv)')
    
    args = parser.parse_args()
    
    # Handle convenience flags
    if args.sample:
        input_file = 'Sample/BMF_UNIFIED_sample.csv'
    elif args.full:
        input_file = '../BMF_UNIFIED_V1.1.csv'
    else:
        input_file = args.input
    
    # Clean the data
    clean_bmf_data(input_file, args.output)

if __name__ == "__main__":
    main()
