import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Float, DateTime, Integer
from sqlalchemy.dialects.postgresql import UUID
import uuid
import os
import argparse
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressMetadataLoader:
    def __init__(self, db_url, schema_name='metadata'):
        self.db_url = db_url
        self.schema_name = schema_name
        self.engine = create_engine(db_url)
        self.metadata = MetaData(schema=schema_name)

    def create_schema(self):
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}"))
            conn.commit()
            logger.info(f"Schema '{self.schema_name}' created or already exists")

    def create_address_table(self):
        address_table = Table(
            'address_metadata',
            self.metadata,
            Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
            Column('EIN2', String(9), unique=True, nullable=False),
            Column('CENSUS_CSA_FIPS', Float),
            Column('CENSUS_CBSA_FIPS', Float),
            Column('CENSUS_CBSA_NAME', String(100)),
            Column('CENSUS_CBSA_LEGAL_NAME', String(100)),
            Column('CENSUS_CBSA_LEGAL_CODE', String(5)),
            Column('geometry', String(100)),
            Column('CENSUS_BLOCK_CODE', Float),
            Column('CENSUS_BLOCK_FIPS', Float),
            Column('CENSUS_BLOCK_NAME', String(100)),
            Column('CENSUS_URBAN_AREA', String(5)),
            Column('CENSUS_URBAN_AREA_CODE', Float),
            Column('CENSUS_URBAN_AREA_TYPE', String(5)),
            Column('CENSUS_FUNCTIONAL_STATUS', String(5)),
            Column('CENSUS_BLOCK_HOUSING_UNIT_COUNT', Float),
            Column('CENSUS_BLOCK_POPULATION_COUNT', Float),
            Column('CENSUS_STATE_FIPS', Float),
            Column('CENSUS_COUNTY_FIPS', Float),
            Column('CENSUS_TRACT_CODE', Float),
            Column('CENSUS_TRACT_FIPS', Float),
            Column('CENSUS_TRACT_NAME', String(100)),
            Column('CENSUS_STATE_ABBR', String(2)),
            Column('CENSUS_COUNTY_NAME', String(100)),
            Column('CENSUS_STATE_NAME', String(100)),
            Column('ORG_FISCAL_YEAR', Integer),
            Column('ORG_ADDR_FULL', String(300)),
            Column('ORG_ADDR_MATCH', String(300)),
            Column('LONGITUDE', Float),
            Column('LATITUDE', Float),
            Column('ADDR_TYPE', String(50)),
            Column('GEOCODER_SCORE', Float),
            Column('GEOCODER_MATCH', String(5)),
            Column('METADATA_VERSION', Float),
            Column('METADATA_RELEASE_DATE', String(20)),
            Column('created_at', DateTime, default=datetime.utcnow),
            Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
            schema=self.schema_name
        )
        self.metadata.create_all(self.engine)
        logger.info("Address metadata table created")

    def load_data(self, csv_file, chunk_size=10000):
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        logger.info(f"Loading data from {csv_file}")
        total_rows = sum(1 for _ in open(csv_file)) - 1
        logger.info(f"Total rows to process: {total_rows}")

        chunks_processed = 0
        rows_processed = 0

        for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
            chunk = chunk.rename(columns={'LONGITUDE': 'LONGITUDE'})
            chunk = chunk.where(pd.notnull(chunk), None)
            chunk.to_sql(
                'address_metadata',
                self.engine,
                schema=self.schema_name,
                if_exists='append',
                index=False,
                method='multi'
            )
            chunks_processed += 1
            rows_processed += len(chunk)
            logger.info(f"Chunk {chunks_processed}: {rows_processed}/{total_rows} rows loaded")

        logger.info(f"Finished loading {rows_processed} rows")

    def run_full_load(self, csv_file):
        logger.info("Starting address metadata loading process")
        self.create_schema()
        self.create_address_table()
        self.load_data(csv_file)
        logger.info("Address metadata loading complete")

def main():
    parser = argparse.ArgumentParser(description='Load cleaned address metadata to PostgreSQL')
    parser.add_argument('--csv', required=True, help='Path to cleaned address metadata CSV file')
    parser.add_argument('--db-url', required=True, help='PostgreSQL connection URL')
    parser.add_argument('--schema', default='metadata', help='Target schema name (default: metadata)')

    args = parser.parse_args()
    loader = AddressMetadataLoader(args.db_url, args.schema)
    loader.run_full_load(args.csv)

if __name__ == "__main__":
    main()
