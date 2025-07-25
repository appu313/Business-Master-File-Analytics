#!/usr/bin/python

import sys
import csv
import mysql.connector


def create_table(sql_file, mydb):
    with open(sql_file, 'r') as f:
        cursor = mydb.cursor()
        cursor.execute(f.read())
    mydb.commit()


if __name__ == '__main__':

    tablename = sys.argv[1]
    password = sys.argv[2]

    mydb = mysql.connector.connect(user='norpuser',
                                   password=password,
                                   host='127.0.0.1',
                                   database='norp',
                                   auth_plugin='mysql_native_password')
    base_path = "/home/norp-services/"

    if tablename == 'ngo':
        create_table(f"{base_path}nccs/sql_scripts/create_tables/create_ngo.sql", mydb)
    elif tablename == 'atlanta_crime':
        create_table(f"{base_path}nccs/sql_scripts/create_tables/atlanta_crime.sql", mydb)
    elif tablename == 'Georgia_Crime':
        create_table(f"{base_path}nccs/sql_scripts/create_tables/GeorgiaCrime.sql", mydb)
    elif tablename == 'Uninsured_People':
        create_table(f"{base_path}nccs/sql_scripts/create_tables/UninsuredPeople.sql", mydb)
    elif tablename == 'Healthcare_Diversity':
        create_table(f"{base_path}nccs/sql_scripts/create_tables/HealthcareDiversity.sql", mydb)
    elif tablename == 'education':
        create_table(f"{base_path}nccs/sql_scripts/create_tables/Education.sql", mydb)
    elif tablename == 'Household_Income':
        create_table(f"{base_path}nccs/sql_scripts/create_tables/HousingIncome.sql", mydb)
    elif tablename == 'poverty_diversity':
        create_table(f"{base_path}nccs/sql_scripts/create_tables/Poverty_Diversity.sql", mydb) 
    elif tablename == 'ga_healthcare':
        create_table(f"{base_path}nccs/sql_scripts/create_tables/ga_healthcare.sql", mydb)
    elif tablename == 'bmf':
        create_table(f"{base_path}nccs/sql_scripts/create_tables/create_bmf.sql", mydb)
    elif tablename == 'pub78':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_pub78.sql", mydb)
    elif tablename == 'revoked':
        create_table(f"{base_path}nccs/sql_scripts/create_tablescreate_revoc.sql", mydb)
    elif tablename == 'merged_nccs':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_merged_nccs.sql", mydb)
    elif tablename == 'census':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_census_fields_by_zip_code.sql", mydb)

    elif tablename == 'survey_public_2021':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_survey_public_2021.sql", mydb)

    elif tablename == 'Broadband_Usage_Per_County':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_Broadband_Usage_Per_County.sql", mydb)

    elif tablename == 'Broadband_Speeds_Per_County':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_Broadband_Speeds_Per_County.sql", mydb)

    elif tablename == 'F1023ez_Approvals':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_F1023ez_Approvals.sql", mydb)

    elif tablename == 'irs_990_rev_trends':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_irs_990_rev_trends.sql", mydb)
        
    elif tablename == 'cleaned_census_data':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_census.sql", mydb) 
        
    elif tablename == 'cleaned_charity_data':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_charity.sql", mydb)
    
    elif tablename == 'cleaned_pub78_data':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_pub78dataset.sql", mydb)

    elif tablename == 'economic_data':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_economic_data.sql", mydb)
        
    elif tablename == 'health_inequity':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_health_inequity.sql", mydb)
        
    elif tablename == "homelessness_age":
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/homelessness_age.sql", mydb
        )
    elif tablename == "homelessness_race":
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/homelessness_race.sql", mydb
        )   
    elif tablename == "homelessness_gender":
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/homelessness_gender.sql", mydb
        )
    elif tablename == "homelessness_ethnicity":
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/homelessness_ethnicity.sql", mydb
        )
             
    elif tablename == "CrimeData":
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_crime_2020_present.sql", mydb)
        
    elif tablename == 'atlanta_crime_data':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_atlanta_crime_data.sql", mydb)
        
    elif tablename == 'nyc_crime_data':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_nyc_crime_data.sql", mydb)
    
    elif tablename == 'la_crime_data':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_la_crime_data.sql", mydb)
        
    elif tablename == 'philly_crime_data':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_philly_crime_data.sql", mydb)

    elif tablename == 'housing_value':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_housing_value.sql", mydb)

    elif tablename == 'housing_rent':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_housing_rent.sql", mydb)

    elif tablename == 'housing_year_built':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_housing_year_built.sql", mydb)

    elif tablename == 'housing_heating_fuel':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_housing_heating_fuel.sql", mydb)

    elif tablename == 'housing_mortgage':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_housing_mortgage.sql", mydb)

    elif tablename == 'housing_gross_rent_percent':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_housing_gross_rent_percent.sql", mydb)

    elif tablename == 'household_income_percent_with_mortgage':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_household_income_percent_with_mortgage.sql", mydb)

    elif tablename == 'household_income_percent_without_mortgage':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_household_income_percent_without_mortgage.sql", mydb)

    elif tablename == 'social_base':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_social_base.sql", mydb)

    elif tablename == 'social_citizenship_status':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_social_citizenship_status.sql", mydb)

    elif tablename == 'social_computer_internet_usage':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_social_computer_internet_usage.sql", mydb)

    elif tablename == 'social_education_attainment':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_social_education_attainment.sql", mydb)

    elif tablename == 'social_schooling':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_social_schooling.sql", mydb)
        
    elif tablename == 'economic_commute_to_work':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_economic_commute_to_work.sql", mydb)

    elif tablename == 'economic_health_insurance':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_economic_health_insurance.sql", mydb)

    elif tablename == 'economic_income_and_benefits':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_economic_income_and_benefits.sql", mydb)

    elif tablename == 'demographic_basics':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_demographic_basics.sql", mydb)

    elif tablename == 'demographic_race':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_demographic_race.sql", mydb)

    elif tablename == 'food_access':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/create_food_access.sql', mydb)
        
    elif tablename == 'us_shootings':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/us_shootings.sql', mydb)
        
    elif tablename == 'lead_ami':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_lead_ami.sql", mydb) 

    elif tablename == 'lead_smi':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_lead_smi.sql", mydb)  

    elif tablename == 'lead_fpl':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_lead_fpl.sql", mydb)   
    
    elif tablename == 'life_expectancy':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/life_expectancy.sql', mydb)
        
    elif tablename == 'air_pollution':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/create_air_pollution.sql', mydb)
        
    elif tablename == 'Asian_Nonprofit':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/Asian_Nonprofit.sql', mydb)

    elif tablename == 'disadvantaged_communities':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_disadvantaged_communities.sql", mydb)
    
    elif tablename == 'res_electricity_rates':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_res_electricity_rates.sql", mydb)      
 
    elif tablename == 'population_demographics':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/populationDemographics.sql", mydb) 
    
    elif tablename == 'Candid_NPO_Stats':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_candid_npo_stats.sql", mydb) 

    elif tablename == 'unemployment_rates':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/create_unemployment_rates.sql', mydb)

    elif tablename == 'housing_prices':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/create_housing_prices.sql', mydb)
    
    elif tablename == 'homelessness_rates':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/homelessness_rates.sql', mydb)
        
    elif tablename == 'cost_of_living_us':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/cost_of_living_us.sql', mydb)

    elif tablename == 'population_density':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/population_density.sql', mydb)
    
    elif tablename == 'zipcode_and_coordinates':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/create_zip_and_coord.sql', mydb)
        
    elif tablename == 'ga_county_to_zcta':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/create_ga_county_to_zcta.sql', mydb) 

    elif tablename == 'revocations':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/create_revocations.sql', mydb) 
    elif tablename == 'census_migrations':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/create_census_migration.sql', mydb) 

    elif tablename == 'eitc_county':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/eitc_county.sql', mydb)
    elif tablename == 'eitc_metro':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/eitc_metro.sql', mydb) 
    elif tablename == 'eitc_nonmetro':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/eitc_nonmetro.sql', mydb) 
    elif tablename == 'eitc_state':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/eitc_state.sql', mydb) 
    elif tablename == 'mental_health_cdc':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/mental_health_cdc.sql', mydb)
    elif tablename == 'mental_health_hrsa':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/mental_health_hrsa.sql', mydb)
    elif tablename == 'cleaned_2020_Expenses_file':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/cleaned_2020_Expenses_file.sql', mydb)
    elif tablename == 'cleaned_2021_Expenses_file':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/cleaned_2021_Expenses_file.sql', mydb)
    elif tablename == 'cleaned_2021_SCHOOLS_file':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/SE-P01-T00-SCHOOLS-2021.sql', mydb)
    elif tablename == 'cleaned_2020_SCHOOLS_file':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/SE-P01-T00-SCHOOLS-2020.sql', mydb)
    elif tablename == 'mental_health_irs_990':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/mental_health_irs_990.sql', mydb)
    elif tablename == 'nccs_efilers':
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P00-T00-HEADER-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P01-T00-SUMMARY-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P01-T00-SUMMARY-EZ-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P02-T00-SIGNATURE-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P03-T00-MISSION-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P03-T00-PROGRAMS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P04-T00-REQUIRED-SCHEDULES-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P04-T00-REQUIRED-SCHEDULES-EZ-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P05-T00-OTHER-IRS-FILING-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P06-T00-GOVERNANCE-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P06-T00-GOVERNANCE-EZ-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P07-T00-DIR-TRUST-KEY-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P08-T00-REVENUE-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P09-T00-EXPENSES-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P10-T00-BALANCE-SHEET-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P11-T00-ASSETS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/F9-P12-T00-FINANCIAL-REPORTING-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SA-P00-T00-HEADER-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SA-P01-T00-PUBLIC-CHARITY-STATUS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SA-P02-T00-SUPPORT-SCHEDULE-170-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SA-P03-T00-SUPPORT-SCHEDULE-509-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SA-P04-T00-SUPPORT-ORGS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SA-P05-T00-SUPPORT-ORGS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SA-P99-T00-PUBLIC-CHARITY-STATUS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SC-P01-T00-LOBBY-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SC-P02-T00-LOBBY-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SC-P03-T00-LOBBY-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SCHEDULE-TABLE-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SD-P01-T00-ORGS-DONOR-ADVISED-FUNDS-OTH-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SD-P02-T00-CONSERV-EASEMENTS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SD-P03-T00-ORGS-COLLECT-ART-HIST-TREASURE-OTH-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SD-P04-T00-ESCROW-CUSTODIAL-ARRANGEMENTS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SD-P05-T00-ENDOWMENT-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SD-P06-T00-LAND-BLDG-EQUIP-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SD-P07-T00-INVESTMENTS-OTH-SECURITIES-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SD-P09-T00-OTH-ASSETS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SD-P10-T00-OTH-LIABILITIES-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SD-P11-T00-RECONCILIATION-REVENUE-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SD-P12-T00-RECONCILIATION-EXPENSES-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SD-P99-T00-RECONCILIATION-NETASSETS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SE-P01-T00-SCHOOLS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SF-P01-T00-FRGN-ACTS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SF-P02-T00-FRGN-ORG-GRANTS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SF-P04-T00-FRGN-INTERESTS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SF-P99-T00-FRGN-ORG-GRANTS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SG-P01-T00-FUNDRAISING-ACTS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SG-P02-T00-FUNDRAISING-EVENTS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SG-P03-T00-GAMING-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SH-P01-T00-FAP-COMMUNITY-BENEFIT-POLICY-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SH-P02-T00-FAP-COMMUNITY-BENEFIT-POLICY-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SH-P03-T00-FAP-COMMUNITY-BENEFIT-POLICY-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SH-P05-T00-FAP-COMMUNITY-BENEFIT-POLICY-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SH-P99-T00-FAP-COMMUNITY-BENEFIT-POLICY-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SI-P01-T00-GRANTS-INFO-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SI-P02-T00-GRANTS-US-ORGS-GOVTS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SI-P99-T00-GRANTS-US-ORGS-GOVTS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SJ-P01-T00-COMPENSATION-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SL-P01-T00-EXCESS-BENEFIT-TRANSAC-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SL-P02-T00-LOANS-INTERESTED-PERS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SM-P01-T00-NONCASH-CONTRIBUTIONS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SN-P01-T00-LIQUIDATION-TERMINATION-DISSOLUTION-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SN-P02-T00-DISPOSITION-OF-ASSETS-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SN-P99-T00-LIQUIDATION-TERMINATION-DISSOLUTION-2022.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/990_Efilers/SR-P05-T00-TRANSACTIONS-RLTD-ORGS-2022.sql", mydb)

        
    elif tablename == 'unemployment_usda':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/unemployment_usda.sql', mydb)
    elif tablename == 'education_usda':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/education_usda.sql', mydb)
        


    elif tablename == 'nccs_crosswalk_demographic':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/nccs_crosswalk_demographic.sql', mydb)
    elif tablename == 'nccs_crosswalk_economic':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/nccs_crosswalk_economic.sql', mydb)
    elif tablename == 'nccs_crosswalk_education_social':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/nccs_crosswalk_education_social.sql', mydb)
    elif tablename == 'nccs_crosswalk_housing':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/nccs_crosswalk_housing.sql', mydb)
    elif tablename == 'nccs_geographic_metadata':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/nccs_geographic_metadata.sql', mydb)
        
    elif tablename == 'firearm_laws':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/firearm_laws.sql', mydb)
    elif tablename == 'states_fips':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/states_fips.sql', mydb)
    elif tablename == 'exempt_orgs':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/exempt_orgs.sql', mydb)
    elif tablename == 'demographics_data':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/demographics_data.sql', mydb)
    elif tablename == 'national_data':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/national_data.sql', mydb)
        
    elif tablename == 'city_info':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/create_city_info.sql', mydb)
    elif tablename == 'geocoded_aqi_dataset':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/create_geocoded_aqi_dataset.sql', mydb)
        
    elif tablename == 'charities_2020':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/charities_2020.sql', mydb)
    elif tablename == 'nonprofit_employment_2020':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/nonprofit_employment_2020.sql', mydb)
    elif tablename == 'ngos_categorization':
        create_table(
            f'{base_path}nccs/sql_scripts/create_tables/create_NGO_categories.sql', mydb)
    elif tablename == 'unemployment_rates_by_state':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_unemployment_rates_by_state.sql", mydb)
    elif tablename == 'F9_P09_T00_EXPENSES_2022_Spring2025':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/create_tables_F9_P09_T00_EXPENSES_2022.sql", mydb)
    elif tablename == 'NCCS_Address_Metadata':
        create_table(f"{base_path}nccs/sql_scripts/create_tables/NCCS_Address_Metadata_cbsa.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/NCCS_Address_Metadata_counties.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/NCCS_Address_Metadata_org_loc.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/NCCS_Address_Metadata_tracts.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/NCCS_Address_Metadata_urban_areas.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/NCCS_Address_Metadata_states.sql", mydb)
        create_table(f"{base_path}nccs/sql_scripts/create_tables/NCCS_Address_Metadata_census_blocks.sql", mydb)
    elif tablename == 'nccs_bmf':
        create_table(
            f"{base_path}nccs/sql_scripts/create_tables/bmf.sql", mydb)

        
    mydb.close()
