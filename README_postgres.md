# BMF PostgreSQL Loading Guide

This guide explains how to load the cleaned BMF (Business Master File) data into PostgreSQL for analysis and reporting.

## **Architecture Overview**

### **Recommended Approach: Hybrid Design**
- **Main Table**: `organizations` (denormalized for analytics)
- **Views**: Pre-built views for common queries
- **Indexes**: Optimized for performance
- **Schema**: Separate `bmf` schema for organization

## **Quick Start**

### **1. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **2. Set Up PostgreSQL Connection**
```bash
# Option A: Direct connection string
export DB_URL="postgresql://username:password@localhost:5432/database_name"

# Option B: Environment variables
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=your_database
export DB_USER=your_username
export DB_PASSWORD=your_password
```

### **3. Load Data**
```bash
# Load sample data
python load_to_postgres.py \
  --csv BMF_UNIFIED_sample_cleaned.csv \
  --db-url "postgresql://username:password@localhost:5432/database_name"

# Load full dataset
python load_to_postgres.py \
  --csv BMF_UNIFIED_V1.1_cleaned.csv \
  --db-url "postgresql://username:password@localhost:5432/database_name" \
  --chunk-size 5000
```

## 📊 **Database Schema**

### **Main Table: `bmf.organizations`**
```sql
-- Core identifiers
ein_pk VARCHAR(9) PRIMARY KEY          -- Primary key for joins
ein_fk VARCHAR(9)                      -- Foreign key for address joins

-- Organization info
org_name VARCHAR(500)                  -- Current organization name
org_name_secondary VARCHAR(500)        -- Secondary/alternative name

-- Classification
ntee_irs_code VARCHAR(10)              -- IRS NTEE classification
ntee_nccs_code VARCHAR(10)             -- NCCS NTEE code
ntee_modern_code VARCHAR(20)           -- Modern NTEE code
nccs_level_1/2/3_category VARCHAR(100) -- NCCS category levels

-- Financial data
total_revenue FLOAT                    -- Total revenue
total_income FLOAT                     -- Total income  
total_assets FLOAT                     -- Total assets

-- Address data
city VARCHAR(100), state VARCHAR(2)    -- Location
zipcode VARCHAR(10), street_address VARCHAR(200)

-- Geographic data
latitude FLOAT, longitude FLOAT        -- Coordinates
census_cbsa_name VARCHAR(200)          -- Metropolitan area
census_county_name VARCHAR(100)        -- County

-- BMF codes (various classification codes)
bmf_status_code, bmf_foundation_code, etc.

-- Dates
organization_ruling_date TIMESTAMP     -- IRS ruling date
organization_fiscal_year TIMESTAMP     -- Fiscal year
organization_year_count INTEGER        -- Years of operation
```

### **Useful Views Created **

#### **1. `bmf.organizations_with_financials`**
```sql
-- Organizations that have financial data
SELECT ein_pk, org_name, total_revenue, total_income, total_assets
FROM bmf.organizations_with_financials;
```

#### **2. `bmf.organizations_by_state`**
```sql
-- Summary by state
SELECT state, organization_count, avg_revenue, total_revenue_sum
FROM bmf.organizations_by_state;
```

#### **3. `bmf.organizations_by_ntee`**
```sql
-- Summary by NTEE category
SELECT ntee_irs_code, organization_count, avg_revenue
FROM bmf.organizations_by_ntee;
```

## 🔍 **Example Queries**

### **Find Large Organizations**
```sql
SELECT org_name, total_revenue, total_assets, state
FROM bmf.organizations
WHERE total_revenue > 10000000
ORDER BY total_revenue DESC
LIMIT 20;
```

### **Organizations by State and Category**
```sql
SELECT 
    state,
    ntee_irs_code,
    COUNT(*) as org_count,
    AVG(total_revenue) as avg_revenue
FROM bmf.organizations
WHERE state IS NOT NULL 
  AND ntee_irs_code IS NOT NULL
GROUP BY state, ntee_irs_code
ORDER BY org_count DESC;
```

### **Join with Address Dataset**
```sql
SELECT 
    o.org_name,
    o.total_revenue,
    a.address_line_1,
    a.city,
    a.state
FROM bmf.organizations o
JOIN address_dataset a ON o.ein_fk = a.ein
WHERE o.total_revenue > 1000000;
```

## **Performance Improvements**

### **Indexes Created**
- `ein_pk` (Primary Key)
- `ein_fk` (Foreign Key)
- `org_name` (Text search)
- `state` (Geographic queries)
- `ntee_irs_code` (Category queries)

### **Query Optimization**
```sql
-- Use indexes effectively
SELECT * FROM bmf.organizations WHERE ein_pk = '123456789';

-- For text search
SELECT * FROM bmf.organizations WHERE org_name ILIKE '%hospital%';

-- For geographic analysis
SELECT * FROM bmf.organizations WHERE state = 'CA';
```

## 🔧 **Advanced Options**

### **Custom Chunk Size**
```bash
python load_to_postgres.py \
  --csv your_file.csv \
  --db-url your_connection_string \
  --chunk-size 5000  # Adjust based on memory
```

### **Skip Views**
```bash
python load_to_postgres.py \
  --csv your_file.csv \
  --db-url your_connection_string \
  --no-views  # Don't create views
```

### **Custom Schema**
```bash
python load_to_postgres.py \
  --csv your_file.csv \
  --db-url your_connection_string \
  --schema nonprofit_data  # Custom schema name
```

## **Troubleshooting**

### **Memory Issues**
- Reduce `--chunk-size` (default: 10000)
- Monitor system memory during loading
- Consider loading during off-peak hours

### **Connection Issues**
- Verify PostgreSQL is running
- Check connection string format
- Ensure user has CREATE SCHEMA permissions

### **Performance Issues**
- Monitor query execution plans
- Consider additional indexes based on usage patterns
- Use views for complex aggregations