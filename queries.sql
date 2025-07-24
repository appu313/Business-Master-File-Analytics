-- Top-level NTEE Category Distribution
SELECT nccs_level_1_category, COUNT(*) AS count
    FROM nccs_bmf
    WHERE nccs_level_1_category IS NOT NULL
    GROUP BY nccs_level_1_category
    ORDER BY count DESC;

-- Nonprofit Registrations Over Time (Fiscal Year)
SELECT ORG_FISCAL_YEAR, COUNT(*) AS org_count
FROM NCCS_Address_Metadata_org_loc
GROUP BY ORG_FISCAL_YEAR
ORDER BY ORG_FISCAL_YEAR;

-- Nonprofit Registrations Over Time (Ruling Year)
SELECT YEAR(organization_ruling_date) AS ruling_year, COUNT(*) AS org_count
    FROM nccs_bmf
    WHERE organization_ruling_date IS NOT NULL
    GROUP BY ruling_year
    ORDER BY ruling_year;

-- Number of Nonprofit Organizations Across States
SELECT s.CENSUS_STATE_NAME, COUNT(*) AS org_count
FROM NCCS_Address_Metadata_org_loc o
JOIN NCCS_Address_Metadata_census_blocks b ON o.CENSUS_BLOCK_FIPS = b.CENSUS_BLOCK_FIPS
JOIN NCCS_Address_Metadata_tracts t ON b.CENSUS_TRACT_FIPS = t.CENSUS_TRACT_FIPS
JOIN NCCS_Address_Metadata_states s ON t.CENSUS_STATE_FIPS = s.CENSUS_STATE_FIPS
GROUP BY s.CENSUS_STATE_NAME
ORDER BY org_count DESC;

--Top 15 CBSAs by Number of Nonprofit Organizations
SELECT cb.CENSUS_CBSA_NAME, COUNT(*) AS org_count
FROM NCCS_Address_Metadata_org_loc o
JOIN NCCS_Address_Metadata_census_blocks b ON o.CENSUS_BLOCK_FIPS = b.CENSUS_BLOCK_FIPS
JOIN NCCS_Address_Metadata_tracts t ON b.CENSUS_TRACT_FIPS = t.CENSUS_TRACT_FIPS
JOIN NCCS_Address_Metadata_counties c ON t.CENSUS_COUNTY_FIPS = c.CENSUS_COUNTY_FIPS
JOIN NCCS_Address_Metadata_cbsa cb ON c.CENSUS_CBSA_FIPS = cb.CENSUS_CBSA_FIPS
GROUP BY cb.CENSUS_CBSA_NAME
ORDER BY org_count DESC
LIMIT 15;

-- Top 15 Counties by Number of Nonprofit Organizations
SELECT c.CENSUS_COUNTY_NAME, COUNT(*) AS org_count
FROM NCCS_Address_Metadata_org_loc o
JOIN NCCS_Address_Metadata_census_blocks b ON o.CENSUS_BLOCK_FIPS = b.CENSUS_BLOCK_FIPS
JOIN NCCS_Address_Metadata_tracts t ON b.CENSUS_TRACT_FIPS = t.CENSUS_TRACT_FIPS
JOIN NCCS_Address_Metadata_counties c ON t.CENSUS_COUNTY_FIPS = c.CENSUS_COUNTY_FIPS
GROUP BY c.CENSUS_COUNTY_NAME
ORDER BY org_count DESC
LIMIT 15;

-- View top counties for a given state by Number of Nonprofit Organizations
SELECT 
  c.CENSUS_COUNTY_NAME,
  COUNT(*) AS org_count
FROM NCCS_Address_Metadata_org_loc o
JOIN NCCS_Address_Metadata_census_blocks b 
  ON o.CENSUS_BLOCK_FIPS = b.CENSUS_BLOCK_FIPS
JOIN NCCS_Address_Metadata_tracts t 
  ON b.CENSUS_TRACT_FIPS = t.CENSUS_TRACT_FIPS
JOIN NCCS_Address_Metadata_counties c 
  ON t.CENSUS_COUNTY_FIPS = c.CENSUS_COUNTY_FIPS
JOIN NCCS_Address_Metadata_states s 
  ON t.CENSUS_STATE_FIPS = s.CENSUS_STATE_FIPS
WHERE s.CENSUS_STATE_NAME = 'Georgia'
GROUP BY c.CENSUS_COUNTY_NAME
ORDER BY org_count DESC
LIMIT 15;

-- Top 15 Cities by Number of Nonprofit Organizations
SELECT city, COUNT(*) AS org_count
    FROM nccs_bmf
    WHERE city IS NOT NULL AND city != ''
    GROUP BY city
    ORDER BY org_count DESC
    LIMIT 15;

--View top cities for a given state by Number of Nonprofit Organizations
    SELECT city, COUNT(*) AS org_count
    FROM nccs_bmf
    WHERE city IS NOT NULL AND city != ''
      AND state = 'CA'
    GROUP BY city
    ORDER BY org_count DESC
    LIMIT 15;

-- Average Revenue vs. Assets by Top-Level Nonprofit Category
    SELECT nccs_level_1_category,
           AVG(total_revenue) AS avg_revenue,
           AVG(total_assets) AS avg_assets
    FROM nccs_bmf
    WHERE total_revenue IS NOT NULL AND total_assets IS NOT NULL
          AND nccs_level_1_category IS NOT NULL
    GROUP BY nccs_level_1_category
    ORDER BY avg_revenue DESC
    LIMIT 15;

-- Distribution of Total Revenue by Category (Log Scale)
SELECT nccs_level_1_category, total_revenue, total_assets
    FROM nccs_bmf
    WHERE nccs_level_1_category IS NOT NULL
      AND total_revenue IS NOT NULL AND total_assets IS NOT NULL
      AND total_revenue > 0 AND total_assets > 0
    LIMIT 50000;  -- Sample size for performance (adjust as needed)

-- Private Foundation Filing Requirement Status
SELECT 
        bmf_private_foundation_filing_required,
        COUNT(*) AS org_count
    FROM nccs_bmf
    GROUP BY bmf_private_foundation_filing_required
    ORDER BY org_count DESC;

-- 

