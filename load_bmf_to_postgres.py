import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, Float, DateTime
from sqlalchemy.dialects.postgresql import UUID
import uuid
import os
import argparse
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BMFPostgresLoader:
    def __init__(self, db_url, schema_name='bmf'):
        """
        Initialize the BMF PostgreSQL loader
        
        Args:
            db_url (str): PostgreSQL connection URL
            schema_name (str): Schema name for BMF tables
        """
        self.db_url = db_url
        self.schema_name = schema_name
        self.engine = create_engine(db_url)
        self.metadata = MetaData(schema=schema_name)
        
    def create_schema(self):
        """Create the BMF schema if it doesn't exist"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}"))
                conn.commit()
                logger.info(f"Schema '{self.schema_name}' created/verified")
        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            raise
    
    def create_main_table(self):
        """Create the main BMF table with all columns"""
        try:
            # Define the main BMF table
            bmf_table = Table(
                'organizations', 
                self.metadata,
                Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
                Column('ein_pk', String(9), unique=True, nullable=False, index=True),
                Column('ein_fk', String(9), nullable=True, index=True),
                Column('org_name', String(500), nullable=True),
                Column('org_name_secondary', String(500), nullable=True),
                
                # Classification codes
                Column('ntee_irs_code', String(10), nullable=True),
                Column('ntee_nccs_code', String(10), nullable=True),
                Column('ntee_modern_code', String(20), nullable=True),
                Column('nccs_level_1_category', String(100), nullable=True),
                Column('nccs_level_2_category', String(100), nullable=True),
                Column('nccs_level_3_category', String(100), nullable=True),
                
                # Financial data
                Column('total_revenue', Float, nullable=True),
                Column('total_income', Float, nullable=True),
                Column('total_assets', Float, nullable=True),
                
                # Address information
                Column('city', String(100), nullable=True),
                Column('state', String(2), nullable=True),
                Column('zipcode', String(10), nullable=True),
                Column('street_address', String(200), nullable=True),
                
                # Census data
                Column('census_cbsa_fips_code', String(10), nullable=True),
                Column('census_cbsa_name', String(200), nullable=True),
                Column('census_block_fips_code', String(15), nullable=True),
                Column('census_urban_area', String(1), nullable=True),
                Column('census_state_abbreviation', String(2), nullable=True),
                Column('census_county_name', String(100), nullable=True),
                
                # Geocoding data
                Column('address_full', String(500), nullable=True),
                Column('address_matched', String(500), nullable=True),
                Column('latitude', Float, nullable=True),
                Column('longitude', Float, nullable=True),
                Column('geocoder_confidence_score', Float, nullable=True),
                Column('geocoder_match_type', String(10), nullable=True),
                
                # BMF codes
                Column('bmf_subsection_code', String(10), nullable=True),
                Column('bmf_status_code', String(10), nullable=True),
                Column('bmf_private_foundation_filing_required', String(10), nullable=True),
                Column('bmf_organization_code', String(10), nullable=True),
                Column('bmf_income_code', String(10), nullable=True),
                Column('bmf_group_exemption_number', String(10), nullable=True),
                Column('bmf_foundation_code', String(10), nullable=True),
                Column('bmf_filing_requirement_code', String(10), nullable=True),
                Column('bmf_deductibility_code', String(10), nullable=True),
                Column('bmf_classification_code', String(10), nullable=True),
                Column('bmf_asset_code', String(10), nullable=True),
                Column('bmf_affiliation_code', String(10), nullable=True),
                
                # Organization dates
                Column('organization_ruling_date', DateTime, nullable=True),
                Column('organization_fiscal_year', DateTime, nullable=True),
                Column('organization_ruling_year', DateTime, nullable=True),
                Column('organization_first_year', DateTime, nullable=True),
                Column('organization_last_year', DateTime, nullable=True),
                Column('organization_year_count', Integer, nullable=True),
                Column('organization_personal_ico', String(10), nullable=True),
                Column('organization_fiscal_period', String(10), nullable=True),
                
                # Metadata
                Column('created_at', DateTime, default=datetime.utcnow),
                Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
                
                schema=self.schema_name
            )
            
            # Create the table
            self.metadata.create_all(self.engine)
            logger.info("✅ Main BMF table 'organizations' created")
            
            # Create indexes for better performance
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{self.schema_name}_organizations_ein_pk 
                    ON {self.schema_name}.organizations(ein_pk);
                """))
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{self.schema_name}_organizations_ein_fk 
                    ON {self.schema_name}.organizations(ein_fk);
                """))
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{self.schema_name}_organizations_org_name 
                    ON {self.schema_name}.organizations(org_name);
                """))
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{self.schema_name}_organizations_state 
                    ON {self.schema_name}.organizations(state);
                """))
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{self.schema_name}_organizations_ntee 
                    ON {self.schema_name}.organizations(ntee_irs_code);
                """))
                conn.commit()
                logger.info("✅ Indexes created for better query performance")
                
        except Exception as e:
            logger.error(f"❌ Error creating main table: {e}")
            raise
    
    def create_normalized_tables(self):
        """Create normalized tables for better data organization"""
        try:
            # NTEE Categories table
            ntee_table = Table(
                'ntee_categories',
                self.metadata,
                Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
                Column('ntee_irs_code', String(10), unique=True, nullable=False),
                Column('ntee_nccs_code', String(10), nullable=True),
                Column('ntee_modern_code', String(20), nullable=True),
                Column('category_name', String(200), nullable=True),
                Column('created_at', DateTime, default=datetime.utcnow),
                schema=self.schema_name
            )
            
            # Geographic locations table
            locations_table = Table(
                'geographic_locations',
                self.metadata,
                Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
                Column('census_cbsa_fips_code', String(10), nullable=True),
                Column('census_cbsa_name', String(200), nullable=True),
                Column('census_state_abbreviation', String(2), nullable=True),
                Column('census_county_name', String(100), nullable=True),
                Column('latitude', Float, nullable=True),
                Column('longitude', Float, nullable=True),
                Column('created_at', DateTime, default=datetime.utcnow),
                schema=self.schema_name
            )
            
            # Create normalized tables
            self.metadata.create_all(self.engine)
            logger.info("Normalized tables created")
            
        except Exception as e:
            logger.error(f"Error creating normalized tables: {e}")
            raise
    
    def load_data(self, csv_file, chunk_size=10000):
        """
        Load data from CSV to PostgreSQL
        
        Args:
            csv_file (str): Path to cleaned CSV file
            chunk_size (int): Number of rows to process at once
        """
        try:
            if not os.path.exists(csv_file):
                raise FileNotFoundError(f"CSV file not found: {csv_file}")
            
            logger.info(f"Loading data from {csv_file}")
            
            # Get total rows for progress tracking
            total_rows = sum(1 for _ in open(csv_file)) - 1  # Subtract header
            logger.info(f"Total rows to process: {total_rows:,}")
            
            # Load data in chunks
            chunks_processed = 0
            rows_processed = 0
            
            for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
                # Clean up any remaining NaN values
                chunk = chunk.where(pd.notnull(chunk), None)
                
                # Load chunk to database
                chunk.to_sql(
                    'organizations', 
                    self.engine, 
                    schema=self.schema_name,
                    if_exists='append', 
                    index=False,
                    method='multi'
                )
                
                chunks_processed += 1
                rows_processed += len(chunk)
                
                logger.info(f"Processed chunk {chunks_processed}: {rows_processed:,}/{total_rows:,} rows ({(rows_processed/total_rows)*100:.1f}%)")
            
            logger.info(f"Successfully loaded {rows_processed:,} rows to PostgreSQL")
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def create_views(self):
        """Create useful views for common queries"""
        try:
            with self.engine.connect() as conn:
                # View for organizations with financial data
                conn.execute(text(f"""
                    CREATE OR REPLACE VIEW {self.schema_name}.organizations_with_financials AS
                    SELECT 
                        ein_pk,
                        org_name,
                        ntee_irs_code,
                        total_revenue,
                        total_income,
                        total_assets,
                        city,
                        state,
                        organization_ruling_date
                    FROM {self.schema_name}.organizations
                    WHERE total_revenue IS NOT NULL 
                       OR total_income IS NOT NULL 
                       OR total_assets IS NOT NULL;
                """))
                
                # View for organizations by state
                conn.execute(text(f"""
                    CREATE OR REPLACE VIEW {self.schema_name}.organizations_by_state AS
                    SELECT 
                        state,
                        COUNT(*) as organization_count,
                        COUNT(total_revenue) as orgs_with_revenue_data,
                        AVG(total_revenue) as avg_revenue,
                        SUM(total_revenue) as total_revenue_sum
                    FROM {self.schema_name}.organizations
                    WHERE state IS NOT NULL
                    GROUP BY state
                    ORDER BY organization_count DESC;
                """))
                
                # View for organizations by NTEE category
                conn.execute(text(f"""
                    CREATE OR REPLACE VIEW {self.schema_name}.organizations_by_ntee AS
                    SELECT 
                        ntee_irs_code,
                        COUNT(*) as organization_count,
                        COUNT(total_revenue) as orgs_with_revenue_data,
                        AVG(total_revenue) as avg_revenue
                    FROM {self.schema_name}.organizations
                    WHERE ntee_irs_code IS NOT NULL
                    GROUP BY ntee_irs_code
                    ORDER BY organization_count DESC;
                """))
                
                conn.commit()
                logger.info("✅ Useful views created for common queries")
                
        except Exception as e:
            logger.error(f"❌ Error creating views: {e}")
            raise
    
    def run_full_load(self, csv_file, create_normalized=True, create_views=True):
        """
        Run the complete loading process
        
        Args:
            csv_file (str): Path to cleaned CSV file
            create_normalized (bool): Whether to create normalized tables
            create_views (bool): Whether to create useful views
        """
        logger.info("🚀 Starting BMF PostgreSQL loading process...")
        
        try:
            # Step 1: Create schema
            self.create_schema()
            
            # Step 2: Create main table
            self.create_main_table()
            
            # Step 3: Create normalized tables (optional)
            if create_normalized:
                self.create_normalized_tables()
            
            # Step 4: Load data
            self.load_data(csv_file)
            
            # Step 5: Create views (optional)
            if create_views:
                self.create_views()
            
            logger.info("🎉 BMF PostgreSQL loading completed successfully!")
            
        except Exception as e:
            logger.error(f"❌ Loading process failed: {e}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Load cleaned BMF data to PostgreSQL')
    parser.add_argument('--csv', required=True, help='Path to cleaned CSV file')
    parser.add_argument('--db-url', required=True, help='PostgreSQL connection URL')
    parser.add_argument('--schema', default='bmf', help='Schema name (default: bmf)')
    parser.add_argument('--chunk-size', type=int, default=10000, help='Chunk size for loading (default: 10000)')
    parser.add_argument('--no-normalized', action='store_true', help='Skip creating normalized tables')
    parser.add_argument('--no-views', action='store_true', help='Skip creating views')
    
    args = parser.parse_args()
    
    # Initialize loader
    loader = BMFPostgresLoader(args.db_url, args.schema)
    
    # Run full load
    loader.run_full_load(
        csv_file=args.csv,
        create_normalized=not args.no_normalized,
        create_views=not args.no_views
    )

if __name__ == "__main__":
    main() 