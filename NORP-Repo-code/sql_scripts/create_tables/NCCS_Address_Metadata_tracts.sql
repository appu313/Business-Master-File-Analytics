CREATE TABLE IF NOT EXISTS `NCCS_Address_Metadata_tracts` (
  `CENSUS_TRACT_FIPS` VARCHAR(11),
  `CENSUS_TRACT_CODE` VARCHAR(6),
  `CENSUS_TRACT_NAME` VARCHAR(20),
  `CENSUS_COUNTY_FIPS` VARCHAR(3),
  `CENSUS_STATE_FIPS` VARCHAR(2),
  UNIQUE (`CENSUS_TRACT_FIPS`)
);
