# Data Integration and Analysis of U.S. Tax-Exempt Organizations

This project explores trends in nonprofit tax-exempt organizations across the United States using datasets from the National Center for Charitable Statistics (NCCS). The goal is to analyze temporal, geographical, and categorical patterns and deliver interactive visualizations that highlight insights such as urban clustering, category specialization, and growth over time.

## Datasets Used

### 1. NCCS Business Master File (BMF)
- Contains IRS ruling year, NTEE codes, financial metrics, and basic organization information.
- Key Fields: `EIN`, `ORG_RULING_YEAR`, `NTEE_NCCS`, `NCCS_LEVEL_1/2/3`, `F990_TOTAL_REVENUE_RECENT`

### 2. NCCS Metadata-Address File
- Provides geocoded address details, urban/rural indicators, CBSA codes, and latitude/longitude.
- Key Fields: `EIN2`, `LATITUDE`, `LONGITUDE`, `CENSUS_CBSA_NAME`, `CENSUS_URBAN_AREA_TYPE`

Both datasets can be joined using the `EIN` / `EIN2` field.

## Project Goals

- Analyze the growth of tax-exempt organizations over time across states, counties, and metro areas.
- Visualize geographical concentration vs. dispersion, identifying urban clustering trends.
- Explore specialization vs. generalization cycles in organization types using NTEE and NCCS levels.
- Build interactive dashboards for exploring nonprofit distribution, category trends, and per-capita comparisons.

## Deliverables

- Data Integration Pipeline – Scripts for joining, cleaning, and validating BMF and metadata-address datasets.
- Interactive Visualizations – Dashboards showing trends over time and across geography using Plotly/Dash or other tools.
- Analytical Reports – Summaries of findings including heatmaps, bar charts, and trend lines.
- Skill Learning Reflections – Applying advanced reasoning skills to uncover real-world patterns.

## Project Structure

```text
.
├── sample/ # Sample of Original downloaded dataset
│   ├── BMF_UNIFIED_sample.csv                  
│   ├── METADATA_ADDRESS_GEOCODED_sample.csv
│
├── schema analysis/
│   ├── BMF Dataset Schema.xsls     # Initial schema exploration
│   ├── Address Meta Data Schema.xlsx     # Initial schema exploration
├── Data/  # Cleaned sample data
│   ├── BMF_UNIFIED_sample_cleaned.csv    
│   ├── METADATA_ADDRESS_GEOCODED_sample_duplicates.csv
│   ├── METADATA_ADDRESS_GEOCODED_sample_final.csv
│
└── clean-address-metadata.py # script used to clean address data
└── clean-bmf.py # script used to clean bmf data
└── load_bmf_to_postgres.py # script to load data to local postgress server
└── README_postgres.md
└── README.md
└── requirements.txt
