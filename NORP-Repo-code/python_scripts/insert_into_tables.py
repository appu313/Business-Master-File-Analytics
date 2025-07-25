import sys
import csv
import os
import glob
import mysql.connector
from tqdm import tqdm
import datetime

unemployment_rates_by_state_schema = ['State', 'Rate_2022', 'Rate_2023', 'Change', 'Rank']

ngo_schema = ['EIN', 'NAME', 'CITY', 'STATE', 'ZIP', 'DEDUCTIBILITY', 'ACTIVITY', 'ORGANIZATION',
              'STATUS', 'ASSET_CD', 'INCOME_CD', 'ASSET_AMT', 'INCOME_AMT', 'REVENUE_AMT']

pub78_schema = ['EIN', 'Legal.Name', 'City',
                'State', 'Country', 'Deductibility.Status']
atlanta_crime = ['Types of Crime', 'Reported', 'Percent of Total', 'Rate Per 100,000']

georgia_crime_schema = ['Types of Crime', 'Reported', 'Percent of Total', 'Rate Per 100,000', 'Year']

ga_healthcare_schema = ['GEOID10', 'X', 'Y', 'Coverage', 'CoverageMED', 'CoveragOTH', 'TravelTime'
                        , 'TravelTimeMED', 'TravelTimeOTH', 'Congestion', 'CongestionMED'
                        , 'CongestionOTH']
education_schema = ['Gender', 'IPEDS_Race', 'Year', 'Enrollment']

housing_income_schema = ['Household_Income_Bucket', 'Year', 'Household_Income', 'Household_Income_Moe'
                         , 'Geography', 'share']
poverty_diversity_schema = ['Race', 'Year', 'Poverty_Population', 'Poverty_Population_Moe'
                           , 'Geography', 'share', 'Sex']

uninsured_people_schema = ['ID Kaiser Coverage', 'Kaiser Coverage', 'ID Year', 'Year', 'Health Insurance Policies','Geography', 'ID Geography', 'Slug Geography', 'share']

health_care_diversity_schema = ['ID Year', 'Year', 'ID Health Coverage', 'Health Coverage', 'ID Gender', 'Gender', 'Health Insurance by Gender and Age', 'Geography', 'ID Geography', 'Slug Geography', 'Share']
                           
bmf_schema = ['EIN', 'STREET', 'CITY', 'STATE',
              'ZIP', 'SUBSECTION', 'PF_FILING_REQ_CD', 'DEDUCTIBILITY',
              'FILING_REQ_CD', 'FOUNDATION', 'AFFILIATION', 'REVENUE_AMT']
# merged_nccs_schema = ['ein', 'subseccd']

merged_nccs_schema = ['ein', 'accper', 'activ1', 'activ2', 'activ3', 'address', 'city', 'contact',
                      'subseccd', 'exps', 'ass_eoy', 'fundbal', 'cont', 'progrev', 'totrev', 'ntmaj5',
                      'ntmaj10', 'ntmaj12', 'level2', 'nteecc', 'ntee1', 'majgrpb']

census_fields_by_zip_code_schema = ["ZIP_CODE", "STATE", "ZCTA", "fraction_assisted_income", "fraction_high_school_edu", "median_income", "fraction_no_health_ins", "fraction_poverty", "fraction_vacant_housing", "dep_index", "Region", "Division", "population-total_population","population-male_total_population","percent-male-of-total_population","population-female_total_population","percent-female-of-total_population","race-population-total_population","race-population-one_race_white_total_population","race-percent-one_race_white-of-total_population","race-population-one_race_black_total_population","race-percent-one_race_black-of-total_population","population-american_indian_and_alaska_native_total_population","percent-american_indian_and_alaska_native-of-total_population","race-population-one_race_asian_total_population","race-percent-one_race_asian-of-total_population","hispanic_or_latino_total_population","hispanic_or_latino_of_any_race_total_population","hispanic_or_latino_of_any_race-of-total_population","employment_status-population-population_16_years_and_over","income_and_benefits_in_2016-households","income_and_benefits_in_2016-households-less_than_10000","income_and_benefits_in_2016-percent-less_than_10000","income_and_benefits_in_2016-households-10000_to_14999","income_and_benefits_in_2016-percent-10000_to_14999","income_and_benefits_in_2016-households-15000_to_24999","income_and_benefits_in_2016-percent-15000_to_24999","income_and_benefits_in_2016-households-25000_to_34999","income_and_benefits_in_2016-percent-25000_to_34999","income_and_benefits_in_2016-households-35000_to_49999","income_and_benefits_in_2016-percent-35000_to_49999","income_and_benefits_in_2016-households-50000_to_74999","income_and_benefits_in_2016-percent-50000_to_74999","income_and_benefits_in_2016-households-75000_to_99999","income_and_benefits_in_2016-percent-75000_to_99999","income_and_benefits_in_2016-households-100000_to_149999","income_and_benefits_in_2016-percent-100000_to_149999","income_and_benefits_in_2016-households-150000_to_199999","income_and_benefits_in_2016-percent-150000_to_199999","income_and_benefits_in_2016-households-200000_or_more","income_and_benefits_in_2016-percent-200000_or_more","2016-median_household_income_dollars","2016-mean_household_income_dollars","households_with_food_stamps_in_past_year","percent-households_with_food_stamps_in_the_past_year","percent_people_whose_income_in_past_12m_is_below_poverty_level","total_population","urban_total_population","rural_total_population","percent-urban-population-of-total_population","percent-rural-population-of-total_population","population_3_years_and_over_enrolled_in_school","population_25_years_and_over","population-less_than_9th_grade_and_25_years_and_over","percent-population-less_than_9th_grade-and_25_years_and_over","high_school_no_diploma_population_25_years_and_over","percent-high_school_no_diploma-of-population_25_years_and_over","high_school_equivalent_population_25_years_and_over",
                                    "percent-high_school_equivalent-of-population_25_years_and_over","some_college_no_degree_population_25_years_and_over","percent-some_college_no_degree-of-population_25_years_and_over","associates_degree_population_25_years_and_over","percent-associates_degree-of-population_25_years_and_over","population-bachelors_degree_population_25_years_and_over","percent-bachelors_degree-of-population_25_years_and_over","graduate_or_professional_degree_population_25_years_and_over","percent-graduate_degree-of-population_25_years_and_over","percent-high_school_grad_or_higher-of-population_25_and_up","percent-bachelors_degree_or_higher-of-population_25_and_up"]
cleaned_2020_Expenses_file_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_09_INFO_SCHED_O_X", "F9_09_EXP_GRANT_US_ORG_TOT", "F9_09_EXP_GRANT_US_ORG_PROG", "F9_09_EXP_GRANT_US_INDIV_TOT", "F9_09_EXP_GRANT_US_INDIV_PROG", "F9_09_EXP_GRANT_FRGN_TOT", "F9_09_EXP_GRANT_FRGN_PROG", "F9_09_EXP_BEN_PAID_MEMB_TOT", "F9_09_EXP_BEN_PAID_MEMB_PROG", "F9_09_EXP_COMP_DTK_TOT", "F9_09_EXP_COMP_DTK_PROG", "F9_09_EXP_COMP_DTK_MGMT", "F9_09_EXP_COMP_DTK_FUNDR", "F9_09_EXP_COMP_DSQ_PERS_TOT", "F9_09_EXP_COMP_DSQ_PERS_PROG", "F9_09_EXP_COMP_DSQ_PERS_MGMT", "F9_09_EXP_COMP_DSQ_PERS_FUNDR", "F9_09_EXP_OTH_SAL_WAGE_TOT", "F9_09_EXP_OTH_SAL_WAGE_PROG", "F9_09_EXP_OTH_SAL_WAGE_MGMT", "F9_09_EXP_OTH_SAL_WAGE_FUNDR", "F9_09_EXP_PENSION_CONTR_TOT", "F9_09_EXP_PENSION_CONTR_PROG", "F9_09_EXP_PENSION_CONTR_MGMT", "F9_09_EXP_PENSION_CONTR_FUNDR", "F9_09_EXP_OTH_EMPL_BEN_TOT", "F9_09_EXP_OTH_EMPL_BEN_PROG", "F9_09_EXP_OTH_EMPL_BEN_MGMT", "F9_09_EXP_OTH_EMPL_BEN_FUNDR", "F9_09_EXP_PAYROLL_TAX_TOT", "F9_09_EXP_PAYROLL_TAX_PROG", "F9_09_EXP_PAYROLL_TAX_MGMT", "F9_09_EXP_PAYROLL_TAX_FUNDR", "F9_09_EXP_FEE_SVC_MGMT_TOT", "F9_09_EXP_FEE_SVC_MGMT_PROG", "F9_09_EXP_FEE_SVC_MGMT_MGMT", "F9_09_EXP_FEE_SVC_MGMT_FUNDR", "F9_09_EXP_FEE_SVC_LEGAL_TOT", "F9_09_EXP_FEE_SVC_LEGAL_PROG", "F9_09_EXP_FEE_SVC_LEGAL_MGMT", "F9_09_EXP_FEE_SVC_LEGAL_FUNDR", "F9_09_EXP_FEE_SVC_ACC_TOT", "F9_09_EXP_FEE_SVC_ACC_PROG", "F9_09_EXP_FEE_SVC_ACC_MGMT", "F9_09_EXP_FEE_SVC_ACC_FUNDR", "F9_09_EXP_FEE_SVC_LOB_TOT", "F9_09_EXP_FEE_SVC_LOB_PROG", "F9_09_EXP_FEE_SVC_LOB_MGMT", "F9_09_EXP_FEE_SVC_LOB_FUNDR", "F9_09_EXP_FEE_SVC_FUNDR_TOT", "F9_09_EXP_FEE_SVC_FUNDR_PROG", "F9_09_EXP_FEE_SVC_FUNDR_MGMT", "F9_09_EXP_FEE_SVC_FUNDR_FUNDR", "F9_09_EXP_FEE_SVC_INVEST_TOT", "F9_09_EXP_FEE_SVC_INVEST_PROG", "F9_09_EXP_FEE_SVC_INVEST_MGMT", "F9_09_EXP_FEE_SVC_INVEST_FUNDR", "F9_09_EXP_FEE_SVC_OTH_TOT", "F9_09_EXP_FEE_SVC_OTH_PROG", "F9_09_EXP_FEE_SVC_OTH_MGMT", "F9_09_EXP_FEE_SVC_OTH_FUNDR", "F9_09_EXP_AD_PROMO_TOT", "F9_09_EXP_AD_PROMO_PROG", "F9_09_EXP_AD_PROMO_MGMT", "F9_09_EXP_AD_PROMO_FUNDR", "F9_09_EXP_OFFICE_TOT", "F9_09_EXP_OFFICE_TOT_V2", "F9_09_EXP_OFFICE_PROG", "F9_09_EXP_OFFICE_MGMT", "F9_09_EXP_OFFICE_FUNDR", "F9_09_EXP_INFO_TECH_TOT", "F9_09_EXP_INFO_TECH_PROG", "F9_09_EXP_INFO_TECH_MGMT", "F9_09_EXP_INFO_TECH_FUNDR", "F9_09_EXP_ROY_TOT", "F9_09_EXP_ROY_PROG", "F9_09_EXP_ROY_MGMT", "F9_09_EXP_ROY_FUNDR", "F9_09_EXP_OCCUPANCY_TOT", "F9_09_EXP_OCCUPANCY_PROG", "F9_09_EXP_OCCUPANCY_MGMT", "F9_09_EXP_OCCUPANCY_FUNDR", "F9_09_EXP_TRAVEL_TOT", "F9_09_EXP_TRAVEL_PROG", "F9_09_EXP_TRAVEL_MGMT", "F9_09_EXP_TRAVEL_FUNDR", "F9_09_EXP_TRAVEL_ENTMT_TOT", "F9_09_EXP_TRAVEL_ENTMT_PROG", "F9_09_EXP_TRAVEL_ENTMT_MGMT", "F9_09_EXP_TRAVEL_ENTMT_FUNDR", "F9_09_EXP_CONF_MEETING_TOT", "F9_09_EXP_CONF_MEETING_PROG", "F9_09_EXP_CONF_MEETING_MGMT", "F9_09_EXP_CONF_MEETING_FUNDR", "F9_09_EXP_INT_TOT", "F9_09_EXP_INT_PROG", "F9_09_EXP_INT_MGMT", "F9_09_EXP_INT_FUNDR", "F9_09_EXP_PAY_AFFIL_TOT", "F9_09_EXP_PAY_AFFIL_PROG", "F9_09_EXP_PAY_AFFIL_MGMT", "F9_09_EXP_PAY_AFFIL_FUNDR", "F9_09_EXP_DEPREC_TOT", "F9_09_EXP_DEPREC_PROG", "F9_09_EXP_DEPREC_MGMT", "F9_09_EXP_DEPREC_FUNDR", "F9_09_EXP_INSURANCE_TOT", "F9_09_EXP_INSURANCE_PROG", "F9_09_EXP_INSURANCE_MGMT", "F9_09_EXP_INSURANCE_FUNDR", "F9_09_EXP_OTH_OTH_TOT", "F9_09_EXP_OTH_OTH_PROG", "F9_09_EXP_OTH_OTH_MGMT", "F9_09_EXP_OTH_OTH_FUNDR", "F9_09_EXP_TOT_TOT", "F9_09_EXP_TOT_PROG", "F9_09_EXP_TOT_MGMT", "F9_09_EXP_TOT_FUNDR", "F9_09_EXP_JOINT_COST_X", "F9_09_EXP_JOINT_COST_TOT", "F9_09_EXP_JOINT_COST_PROG", "F9_09_EXP_JOINT_COST_MGMT", "F9_09_EXP_JOINT_COST_FUNDR"]

cleaned_2021_Expenses_file_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_09_INFO_SCHED_O_X", "F9_09_EXP_GRANT_US_ORG_TOT", "F9_09_EXP_GRANT_US_ORG_PROG", "F9_09_EXP_GRANT_US_INDIV_TOT", "F9_09_EXP_GRANT_US_INDIV_PROG", "F9_09_EXP_GRANT_FRGN_TOT", "F9_09_EXP_GRANT_FRGN_PROG", "F9_09_EXP_BEN_PAID_MEMB_TOT", "F9_09_EXP_BEN_PAID_MEMB_PROG", "F9_09_EXP_COMP_DTK_TOT", "F9_09_EXP_COMP_DTK_PROG", "F9_09_EXP_COMP_DTK_MGMT", "F9_09_EXP_COMP_DTK_FUNDR", "F9_09_EXP_COMP_DSQ_PERS_TOT", "F9_09_EXP_COMP_DSQ_PERS_PROG", "F9_09_EXP_COMP_DSQ_PERS_MGMT", "F9_09_EXP_COMP_DSQ_PERS_FUNDR", "F9_09_EXP_OTH_SAL_WAGE_TOT", "F9_09_EXP_OTH_SAL_WAGE_PROG", "F9_09_EXP_OTH_SAL_WAGE_MGMT", "F9_09_EXP_OTH_SAL_WAGE_FUNDR", "F9_09_EXP_PENSION_CONTR_TOT", "F9_09_EXP_PENSION_CONTR_PROG", "F9_09_EXP_PENSION_CONTR_MGMT", "F9_09_EXP_PENSION_CONTR_FUNDR", "F9_09_EXP_OTH_EMPL_BEN_TOT", "F9_09_EXP_OTH_EMPL_BEN_PROG", "F9_09_EXP_OTH_EMPL_BEN_MGMT", "F9_09_EXP_OTH_EMPL_BEN_FUNDR", "F9_09_EXP_PAYROLL_TAX_TOT", "F9_09_EXP_PAYROLL_TAX_PROG", "F9_09_EXP_PAYROLL_TAX_MGMT", "F9_09_EXP_PAYROLL_TAX_FUNDR", "F9_09_EXP_FEE_SVC_MGMT_TOT", "F9_09_EXP_FEE_SVC_MGMT_PROG", "F9_09_EXP_FEE_SVC_MGMT_MGMT", "F9_09_EXP_FEE_SVC_MGMT_FUNDR", "F9_09_EXP_FEE_SVC_LEGAL_TOT", "F9_09_EXP_FEE_SVC_LEGAL_PROG", "F9_09_EXP_FEE_SVC_LEGAL_MGMT", "F9_09_EXP_FEE_SVC_LEGAL_FUNDR", "F9_09_EXP_FEE_SVC_ACC_TOT", "F9_09_EXP_FEE_SVC_ACC_PROG", "F9_09_EXP_FEE_SVC_ACC_MGMT", "F9_09_EXP_FEE_SVC_ACC_FUNDR", "F9_09_EXP_FEE_SVC_LOB_TOT", "F9_09_EXP_FEE_SVC_LOB_PROG", "F9_09_EXP_FEE_SVC_LOB_MGMT", "F9_09_EXP_FEE_SVC_LOB_FUNDR", "F9_09_EXP_FEE_SVC_FUNDR_TOT", "F9_09_EXP_FEE_SVC_FUNDR_PROG", "F9_09_EXP_FEE_SVC_FUNDR_MGMT", "F9_09_EXP_FEE_SVC_FUNDR_FUNDR", "F9_09_EXP_FEE_SVC_INVEST_TOT", "F9_09_EXP_FEE_SVC_INVEST_PROG", "F9_09_EXP_FEE_SVC_INVEST_MGMT", "F9_09_EXP_FEE_SVC_INVEST_FUNDR", "F9_09_EXP_FEE_SVC_OTH_TOT", "F9_09_EXP_FEE_SVC_OTH_PROG", "F9_09_EXP_FEE_SVC_OTH_MGMT", "F9_09_EXP_FEE_SVC_OTH_FUNDR", "F9_09_EXP_AD_PROMO_TOT", "F9_09_EXP_AD_PROMO_PROG", "F9_09_EXP_AD_PROMO_MGMT", "F9_09_EXP_AD_PROMO_FUNDR", "F9_09_EXP_OFFICE_TOT", "F9_09_EXP_OFFICE_TOT_V2", "F9_09_EXP_OFFICE_PROG", "F9_09_EXP_OFFICE_MGMT", "F9_09_EXP_OFFICE_FUNDR", "F9_09_EXP_INFO_TECH_TOT", "F9_09_EXP_INFO_TECH_PROG", "F9_09_EXP_INFO_TECH_MGMT", "F9_09_EXP_INFO_TECH_FUNDR", "F9_09_EXP_ROY_TOT", "F9_09_EXP_ROY_PROG", "F9_09_EXP_ROY_MGMT", "F9_09_EXP_ROY_FUNDR", "F9_09_EXP_OCCUPANCY_TOT", "F9_09_EXP_OCCUPANCY_PROG", "F9_09_EXP_OCCUPANCY_MGMT", "F9_09_EXP_OCCUPANCY_FUNDR", "F9_09_EXP_TRAVEL_TOT", "F9_09_EXP_TRAVEL_PROG", "F9_09_EXP_TRAVEL_MGMT", "F9_09_EXP_TRAVEL_FUNDR", "F9_09_EXP_TRAVEL_ENTMT_TOT", "F9_09_EXP_TRAVEL_ENTMT_PROG", "F9_09_EXP_TRAVEL_ENTMT_MGMT", "F9_09_EXP_TRAVEL_ENTMT_FUNDR", "F9_09_EXP_CONF_MEETING_TOT", "F9_09_EXP_CONF_MEETING_PROG", "F9_09_EXP_CONF_MEETING_MGMT", "F9_09_EXP_CONF_MEETING_FUNDR", "F9_09_EXP_INT_TOT", "F9_09_EXP_INT_PROG", "F9_09_EXP_INT_MGMT", "F9_09_EXP_INT_FUNDR", "F9_09_EXP_PAY_AFFIL_TOT", "F9_09_EXP_PAY_AFFIL_PROG", "F9_09_EXP_PAY_AFFIL_MGMT", "F9_09_EXP_PAY_AFFIL_FUNDR", "F9_09_EXP_DEPREC_TOT", "F9_09_EXP_DEPREC_PROG", "F9_09_EXP_DEPREC_MGMT", "F9_09_EXP_DEPREC_FUNDR", "F9_09_EXP_INSURANCE_TOT", "F9_09_EXP_INSURANCE_PROG", "F9_09_EXP_INSURANCE_MGMT", "F9_09_EXP_INSURANCE_FUNDR", "F9_09_EXP_OTH_OTH_TOT", "F9_09_EXP_OTH_OTH_PROG", "F9_09_EXP_OTH_OTH_MGMT", "F9_09_EXP_OTH_OTH_FUNDR", "F9_09_EXP_TOT_TOT", "F9_09_EXP_TOT_PROG", "F9_09_EXP_TOT_MGMT", "F9_09_EXP_TOT_FUNDR", "F9_09_EXP_JOINT_COST_X", "F9_09_EXP_JOINT_COST_TOT", "F9_09_EXP_JOINT_COST_PROG", "F9_09_EXP_JOINT_COST_MGMT", "F9_09_EXP_JOINT_COST_FUNDR"]

SE_P01_T00_SCHOOLS_2020_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SE_01_POLICY_STMT_NONDISCR_X", "SE_01_POLICY_STMT_BROCHURES_X", "SE_01_POLICY_BCAST_MEDIA_X", "SE_01_POLICY_BCAST_MEDIA_EXPLAIN", "SE_01_MAINT_RACIAL_RECORD_X", "SE_01_MAINT_SCHOLARSHIP_RECORD_X", "SE_01_MAINT_COPY_BROCHURES_X", "SE_01_MAINT_COPY_SOL_X", "SE_01_MAINT_EXPLANATION", "SE_01_DISCR_RACE_STUDENT_RIGHT_X", "SE_01_DISCR_RACE_ADM_POLICY_X", "SE_01_DISCR_RACE_EMPL_FACULTY_X", "SE_01_DISCR_RACE_SCHOLARSHIP_X", "SE_01_DISCR_RACE_EDU_POLICY_X", "SE_01_DISCR_RACE_USE_FACILITY_X", "SE_01_DISCR_RACE_ATHLETIC_PROG_X", "SE_01_DISCR_RACE_OTH_ACT_X", "SE_01_DISCR_RACE_EXPLANATION", "SE_01_GOVT_FIN_AID_REC_X", "SE_01_GOVT_FIN_AID_REVOKED_X", "SE_01_GOVT_FIN_AID_EXPLANATION", "SE_01_COMPLIANCE_REV_PROC_7550_X", "SE_01_COMPLIANCE_EXPLANATION"]

SE_P01_T00_SCHOOLS_2021_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SE_01_POLICY_STMT_NONDISCR_X", "SE_01_POLICY_STMT_BROCHURES_X", "SE_01_POLICY_BCAST_MEDIA_X", "SE_01_POLICY_BCAST_MEDIA_EXPLAIN", "SE_01_MAINT_RACIAL_RECORD_X", "SE_01_MAINT_SCHOLARSHIP_RECORD_X", "SE_01_MAINT_COPY_BROCHURES_X", "SE_01_MAINT_COPY_SOL_X", "SE_01_MAINT_EXPLANATION", "SE_01_DISCR_RACE_STUDENT_RIGHT_X", "SE_01_DISCR_RACE_ADM_POLICY_X", "SE_01_DISCR_RACE_EMPL_FACULTY_X", "SE_01_DISCR_RACE_SCHOLARSHIP_X", "SE_01_DISCR_RACE_EDU_POLICY_X", "SE_01_DISCR_RACE_USE_FACILITY_X", "SE_01_DISCR_RACE_ATHLETIC_PROG_X", "SE_01_DISCR_RACE_OTH_ACT_X", "SE_01_DISCR_RACE_EXPLANATION", "SE_01_GOVT_FIN_AID_REC_X", "SE_01_GOVT_FIN_AID_REVOKED_X", "SE_01_GOVT_FIN_AID_EXPLANATION", "SE_01_COMPLIANCE_REV_PROC_7550_X", "SE_01_COMPLIANCE_EXPLANATION"]


economic_commute_to_work_schema = ["year","id", "zipcode", "total_workers", "car_alone", "car_pool", "public_transport", "walk", "other", "work_from_home", "mean_travel_time"]

economic_income_and_benefits_schema = ["year","id", "zipcode", "total_households", "median_household_income", "mean_household_income"]

economic_health_insurance_schema = ["year","id", "zipcode", "total_population", "with_health_insurance", "no_health_insurance"]

demographic_basics_schema = ['year', 'id', 'zipcode', 'total_population', 'total_population_male', 'total_population_female', 'total_population_sex_ratio', 'total_population_median_age', 'total_housing_units', 'voting_age_population', 'voting_age_population_male', 'voting_age_population_female']

demographic_race_schema = ['year', 'id', 'zipcode', 'race_total_population', 'one_race', 'two_or_more_races', 'white', 'black', 'american_indian_and_alaska_native', 'asian', 'native_hawaiian_and_other_pacific_islander', 'some_other_race', 'hispanic_or_latino_total', 'hispanic_or_latino', 'not_hispanic_or_latino']

Broadband_Speeds_Per_County_schema = ['POSTAL_CODE', 'County', 'ST', 'WiredCount_2020', 'Fwcount_2020', 'AllProviderCount_2020', 'Wired25_3_2020',
    'Wired100_3_2020', 'All25_3_2020', 'All100_3', 'TestCount', 'AverageMbps', 'FastestAverageMbps', 'ACC_TER_BROADBAND', 'LOW_PR_TERR_BROADBAND']

Broadband_Usage_Per_County_schema = [
    'ST', 'COUNTY_NAME', 'COUNTY_ID', 'POSTAL_CODE', 'BROADBAND_USAGE']

F1023ez_Approvals_schema = ['Ein', 'CaseNumber', 'Formrevision', 'Eligibilityworksheet', 'Orgname1', 'Orgname2', 'Address', 'City', 'State', 'Zip', 'Zippl4', 'Accountingperiodend', 'Primarycontactname', 'Primarycontactphone', 'Primarycontactphoneext', 'Primarycontactfax', 'Userfeesubmitted', 'Ofcrdirtrust1firstname', 'Ofcrdirtrust1lastname', 'Ofcrdirtrust1title', 'Ofcrdirtrust1streetaddr', 'Ofcrdirtrust1city', 'Ofcrdirtrust1state', 'Ofcrdirtrust1zip', 'Ofcrdirtrust1zippl4', 'Ofcrdirtrust2firstname', 'Ofcrdirtrust2lastname', 'Ofcrdirtrust2title', 'Ofcrdirtrust2streetaddr', 'Ofcrdirtrust2city', 'Ofcrdirtrust2state', 'Ofcrdirtrust2zip', 'Ofcrdirtrust2zippl4', 'Ofcrdirtrust3firstname', 'Ofcrdirtrust3lastname', 'Ofcrdirtrust3title', 'Ofcrdirtrust3streetaddr', 'Ofcrdirtrust3city', 'Ofcrdirtrust3state', 'Ofcrdirtrust3zip', 'Ofcrdirtrust3zippl4', 'Ofcrdirtrust4firstname', 'Ofcrdirtrust4lastname', 'Ofcrdirtrust4title', 'Ofcrdirtrust4streetaddr', 'Ofcrdirtrust4city', 'Ofcrdirtrust4state', 'Ofcrdirtrust4zip', 'Ofcrdirtrust4zippl4', 'Ofcrdirtrust5firstname', 'Ofcrdirtrust5lastname', 'Ofcrdirtrust5title', 'Ofcrdirtrust5streetaddr', 'Ofcrdirtrust5city', 'Ofcrdirtrust5state', 'Ofcrdirtrust5zip', 'Ofcrdirtrust5zippl4', 'Orgurl', 'Orgemail', 'Orgtypecorp', 'Orgtypeunincorp', 'Orgtypetrust', 'Necessaryorgdocs', 'Incorporateddate', 'Incorporatedstate', 'Containslimitation', 'Doesnotexpresslyempower', 'Containsdissolution', 'Nteecode', 'Orgpurposecharitable', 'Orgpurposereligious', 'Orgpurposeeducational', 'Orgpurposescientific', 'Orgpurposeliterary', 'Orgpurposepublicsafety', 'Orgpurposeamateursports', 'Orgpurposecrueltyprevention', 'Qualifyforexemption', 'Leginflno', 'Leginflyes', 'Compofcrdirtrustno', 'Compofcrdirtrustyes', 'Donatefundsno', 'Donatefundsyes', 'Conductactyoutsideusno', 'Conductactyoutsideusyes', 'Financialtransofcrsno', 'Financialtransofcrsyes', 'Unrelgrossincm1000moreno', 'Unrelgrossincm1000moreyes', 'Gamingactyno', 'Gamingactyyes', 'Disasterreliefno', 'Disasterreliefyes', 'Onethirdsupportpublic', 'Onethirdsupportgifts', 'Benefitofcollege', 'Privatefoundation508e', 'Seekingretroreinstatement', 'Seekingsec7reinstatement', 'Correctnessdeclaration', 'Mission', 'Gamingactyno_2', 'Gamingactyyes_2', 'Hospitalorchurchno', 'Hospitalorchurchyes', 'Ezversionnumber', 'Signaturename', 'Signaturetitle', 'Signaturedate', 'FilingYear']

irs_990_rev_trends_schema = ['ein', 'name', 'state', 'city', 'zip', 'rev2012', 'rev2013', 'rev2014', 'rev2015', 'rev2016', 'rev2017', 'prgmrev2012', 
                             'prgmrev2013', 'prgmrev2014', 'prgmrev2015', 'prgmrev2016', 'prgmrev2017', 'cntrbgfts2012',
                             'cntrbgfts2013', 'cntrbgfts2014', 'cntrbgfts2015', 'cntrbgfts2016', 'cntrbgfts2017']

economic_data_schema = ['Date', 'Inflation',
                        'Real_GDP', 'Total_Employees', 'Unemployment_Rate']

health_inequity_schema = ['FIPS', 'State', 'County', 'Personal_Income_Per_Capita_2019', 'Number_Of_Ranked_Counties', 'Health_Outcomes_Rank', 'Health_Outcomes_Quartile', 'Health_Factors_Rank', 'Health_Factors_Quartile', 'Length_Of_Life_Rank', 'Length_Of_Life_Quartile', 'Quality_Of_Life_Rank', 'Quality_Of_Life_Quartile', 'Health_Behaviors_Rank', 'Health_Behaviors_Quartile', 'Clinical_Care_Rank', 'Clinical_Care_Quartile', 'Social_Economic_Factors_Rank', 'Social_Economic_Factors_Quartile', 'Physical_Environment_Rank', 'Physical_Environment_Quartile', 'Percent_Of_Population_In_Frequent_Physical_Distress', 'Percent_Of_Population_In_Frequent_Mental_Distress', 'Percent_Of_Adults_With_Diabetes', 'Percent_Of_Population_Food_Insecure', 'Percent_Of_Population_Insufficient_Sleep', 'Percent_Of_Population_Homeowners', 'Percent_Of_Population_Fair_Or_Poor_Health', 'Percent_Of_Population_Broadband_Access', 'Population_Additional_Measure_Data', 'Percent_Of_Population_Less_Than_18_Years_Of_Age', 'Percent_Of_Population_65_And_Over', 'Percentage_Smokers', 'Percentage_Some_College', 'Percentage_Adults_With_Obesity', 'Percentage_Severe_Housing_Problems', 'Percent_Long_Drives_Alone', 'Percentage_Physically_Inactive', 'Percentage_Excessive_Drinking', 'Traffic_Volume', 'Average_Number_Of_Physically_Unhealthy_Days', 'Average_Number_Of_Mentally_Unhealthy_Days', 'Primary_Care_Physicians_Ratio', 'Other_Primary_Care_Provider_Ratio', 'Dentist_Ratio', 'Mental_Health_Provider_Ratio', 'Number_Completed_High_School', 'Presence_Of_Water_Violation', 'Severe_Housing_Cost_Burden', 'Overcrowding', 'Inadequate_Facilities', 'Number_Workers_Drive_Alone', 'Mortality_Rate_Cancer_2014', 'Mortality_Rate_Cardiovascular_Disease_2014', 'Mortality_Rate_Infectious_Disease_2014', 'Mortality_Rate_Mental_Health_2014', 'Mortality_Rate_Respiratory_Infection_2014']

pub78dataset_schema = ['legal_name', 'city', 'state', 'deductibility_status']

charity_schema = ['category', 'private_donations', 'total_revenue', 'fundraising_efficiency', 'charitable_commitment', 'total_expense', 'tot_exp_char_service', 'tot_exp_management_general', 'tot_exp_fundraising', 'surplus_loss', 'net_assets', 'donor_dependency', 'highest_compensation', 'city', 'state']

census_schema = ['state', 'total_pop', 'men', 'women', 'hispanic', 'white', 'black', 'native', 'asian', 'pacific', 'voting_age_citizen', 'income', 'income_err', 'income_per_cap', 'income_per_cap_err', 'poverty', 'child_poverty', 'professional', 'service', 'office', 'construction', 'production', 'drive', 'carpool', 'transit', 'walk', 'other_transp', 'work_at_home', 'mean_commute', 'employed', 'private_work', 'public_work', 'self_employed', 'family_work', 'unemployment']

housing_prices_schema = ['state', 'bed', 'bath', 'acre_lot', 'house_size', 'price']
unemployment_rates_schema = ['State', 'Year_Date', 'Total_Civilian_Non_Institutional_Population', 'Total_Civilian_Labor_Force', 'Percent_of_State_Population', 'Total_Employment', 'Percent_of_Labor_Force_Employed', 'Total_Unemployment', 'Percent_of_Labor_Force_Unemployed']
homelessness_rates_schema = ['State', 'Year_date', 'Measures', 'Counts']
cost_of_living_us_schema = ['state', 'case_id', 'isMetro', 'areaname', 'county', 'housing_cost', 'food_cost', 'transportation_cost', 'healthcare_cost', 'other_necessities_cost', 'childcare_cost', 'taxes', 'total_cost', 'median_family_income', 'parents', 'children']

homelessness_race_schema = ['CALENDAR_YEAR', 'LOCATION_ID', 'LOCATION', 'RACE', 'EXPERIENCING_HOMELESSNESS']
homelessness_age_schema = ['CALENDAR_YEAR', 'LOCATION', 'AGE_GROUP_PUBLIC', 'EXPERIENCING_HOMELESSNESS_CNT']
homelessness_gender_schema = ['CALENDAR_YEAR', 'LOCATION', 'GENDER', 'EXPERIENCING_HOMELESSNESS']
homelessness_ethnicity_schema = ['CALENDAR_YEAR', 'LOCATION', 'ETHNICITY', 'EXPERIENCING_HOMELESSNESS']
crimedata_schema = ['Crime_Date', 'Area', 'Area_Name', 'Crime_Numeric_Code', 'Crime_Description', 'Victim_Age', 'Victim_Sex', 'Victim_Descent', 'Weapon_Code', 'Status', 'Status_Description', 'Crm_Cd_1', 'Location']
atlanta_crime_detailed_schema = ['Crime_ID', 'Report_#', 'Day_Occurred', 'Crime_Date', 'Crime_Time', 'Crime_Type', 'Area_Name', 'Address', 'Longitude', 'Latitude']
nyc_crime_schema = ['Crime_ID', 'Report_#', 'Crime_Date', 'Crime_Time', 'Crime_Class', 'Crime_Type', 'Area_Name', 'Latitude', 'Longitude']
la_crime_schema = ['Crime_ID', 'Report_#', 'Date_Reported', 'Crime_Date', 'Crime_Time', 'Crime_Type', 'Area_Name', 'Vict_Age', 'Vict_Sex', 'Weapon_Desc', 'Address', 'Latitude', 'Longitude']
philly_crime_schema = ['Crime_ID', 'Report_#', 'Crime_Date', 'Crime_Time', 'Crime_Type', 'Area_#', 'Address', 'Longitude', 'Latitude']

housing_value_schema = ['year', 'id', 'zipcode', 'number_of_units', 'house_value_min', 'house_value_max']
housing_rent_schema = ['year', 'id', 'zipcode', 'number_of_units', 'rent_value_min', 'rent_value_max']
housing_year_built_schema = ['year', 'id', 'zipcode', 'number_of_units', 'year_built_min', 'year_built_max']
housing_heating_fuel_schema = ['year', 'id', 'zipcode', 'total_heating_units', 'utility_gas', 'bottled_tank_or_LP_gas','electricity','fuel_oil_kerosene','coal_or_coke','wood','solar_energy','other_fuel','no_fuel_used']
housing_mortgage_schema = ['year', 'id', 'zipcode', 'units_with_mortgage', 'units_without_mortgage']
housing_gross_rent_percent_schema = ['year', 'id', 'zipcode', 'number_of_units', 'income_percent_min', 'income_percent_max']
household_income_percent_without_mortgage_schema = ['year', 'id', 'zipcode', 'number_of_units', 'income_percent_min', 'income_percent_max']
household_income_percent_with_mortgage_schema = ['year', 'id', 'zipcode', 'number_of_units', 'income_percent_min', 'income_percent_max']

social_base_schema = ['year', 'id', 'zipcode', 'total_households_by_type', 'avg_household_size', 'avg_family_size', 'population_in_house', 'males_15_years_and_over', 'female_15_years_and_over']
social_citizenship_status_schema = ['year', 'id', 'zipcode', 'foreign_born_population', 'naturalized_us_citizen', 'not_a_us_citizen']
social_computer_internet_usage_schema = ['year', 'id', 'zipcode', 'total_households', 'with_a_computer', 'with_broadband_internet_subscription']
social_education_attainment_schema = ['year', 'id', 'zipcode', 'population_25_years_and_over', 'less_than_9th_grade', '9th_to_12th_grade_no_diploma', 'high_school_graduate', 'some_college_no_degree', 'associate_degree', 'bachelors_degree', 'graduate_or_professional_degree', 'high_school_graduate_or_higher', 'bachelors_degree_or_higher']
social_schooling_schema = ['year', 'id', 'zipcode', 'enrolled_in_school', 'enrolled_in_nursery_school', 'enrolled_in_kindergarten', 'enrolled_in_elementary_school', 'enrolled_in_high_school', 'enrolled_in_college_or_graduate_school']

food_access_schema = ['CensusTract', 'County', 'HUNVFlag', 'LA1and10', 'LA1and20', 'laaian1', 'laaian10', 'laaian20', 'laaianhalf', 'laasian1', 'laasian10', 'laasian20', 'laasianhalf', 'lablack1', 'lablack10', 'lablack20', 'lablackhalf', 'LAhalfand10', 'lahisp1', 'lahisp10', 'lahisp20', 'lahisphalf', 'lahunv1', 'lahunv10', 'lahunv20', 'lahunvhalf', 'lakids1', 'lakids10', 'lakids20', 'lakidshalf', 'LALOWI05_10', 'lalowi1', 'lalowi10', 'LALOWI1_10', 'LALOWI1_20', 'lalowi20', 'lalowihalf', 'lanhopi1', 'lanhopi10', 'lanhopi20', 'lanhopihalf', 'laomultir1', 'laomultir10', 'laomultir20', 'laomultirhalf', 'LAPOP05_10', 'lapop1', 'lapop10', 'LAPOP1_10', 'LAPOP1_20', 'lapop20', 'lapophalf', 'laseniors1', 'laseniors10', 'laseniors20', 'laseniorshalf', 'lasnap1', 'lasnap10', 'lasnap20', 'lasnaphalf', 'LATracts1', 'LATracts10', 'LATracts20', 'LATracts_half', 'LATractsVehicle_20', 'lawhite1', 'lawhite10', 'lawhite20', 'lawhitehalf', 'LILATracts_1And10', 'LILATracts_1And20', 'LILATracts_halfAnd10', 'LILATracts_Vehicle', 'LowIncomeTracts', 'MedianFamilyIncome', 'OHU2010', 'Pop2010', 'PovertyRate', 'State', 'TractAIAN', 'TractAsian', 'TractBlack', 'TractHispanic', 'TractHUNV', 'TractKids', 'TractLOWI', 'TractNHOPI', 'TractOMultir', 'TractSeniors', 'TractSNAP', 'TractWhite', 'Urban']

us_shootings_schema = ['State',	'Address', 'IncidentID', 'IncidentDate', 'CityOrCounty', 'VictimsKilled', 'VictimsInjured', 'SuspectsInjured', 'SuspectsKilled', 'SuspectsArrested']

lead_ami_schema = [	'TEN', 'YBL6', 'BLD', 'HFL', 'AMI68', 'UNITS', 'HINCPxUNITS', 'ELEPxUNITS', 'HCOUNT', 'ECOUNT', 'GCOUNT', 'FCOUNT', 'HINCP', 'ELEP', 'GASP', 'FULP', 'STATE']
lead_smi_schema = [	'TEN', 'YBL6', 'BLD', 'HFL', 'SMI68', 'UNITS', 'HINCPxUNITS', 'ELEPxUNITS', 'HCOUNT', 'ECOUNT', 'GCOUNT', 'FCOUNT', 'HINCP', 'ELEP', 'GASP', 'FULP', 'STATE']
lead_fpl_schema = [	'TEN', 'YBL6', 'BLD', 'HFL', 'FPL15', 'UNITS', 'HINCPxUNITS', 'ELEPxUNITS', 'HCOUNT', 'ECOUNT', 'GCOUNT', 'FCOUNT', 'HINCP', 'ELEP', 'GASP', 'FULP', 'STATE']
disadvantaged_communities_schema = ['GEOID','city','county_name','county_fips','state_code','state_fips','state','DAC_status','population','lowincome_ami_pct','avg_energy_burden','grid_outages_county','grid_outage_duration','renters_pct','single_parent_pct','no_internet_pct','lowincome_fpl_pct','avg_housing_burden','food_desert_pct','DAC_score','avg_energy_burden_natl_pctile']
res_electricity_rates_schema = ['zip','state','res_rate']
ga_county_to_zcta_schema = ['COUNTY','ZCTA']

life_expectancy_schema = ['Country', 'Year', 'Air_Quality', 'Status', 'Life_expectancy', 'Adult_Mortality', 'infant_deaths', 'Alcohol', 'percentage_expenditure', 'Hepatitis_B', 'Measles', 'BMI', 'under-five_deaths', 'Polio', 'Total_expenditure', 'Diphtheria', 'HIV/AIDS', 'GDP', 'Population', 'thinness_1-19_years', 'thinness_5-9_years', 'Income_composition_of_resources', 'Schooling']

air_pollution_schema = [ "State", "City", "Population", "Latitude", "Longitude", "MHNG", "DDAA", "CAAA", "CHAA", "PM2_5" ]
pop_den_schema = ['Lat','Lon','Population']

zip_and_coord_schema = ['ZipCode', 'Latitude','Longitude']


asian_nonprofit_schema = ['Ein', 'Name', 'Zip', 'Date_Filed']

population_demographics_schema = ["ID", "communityName", "State", "countyName", "FIPS", "Population", "householdSize", "racePctBlack", "racePctWhite", "racePctAsian", "racePctHisp", "agePct12t21", "agePct12t29", "agePct16t24", "agePct65up", "numbUrban", "pctUrban", "medIncome", "pctWWage", "pctWFarmSelf", "pctWInvInc", "pctWSocSec", "pctWPubAsst", "pctWRetire", "medFamInc", "perCapInc", "whitePerCap", "blackPerCap", "indianPerCap", "AsianPerCap", "OtherPerCap", "HispPerCap", "NumUnderPov", "PctPopUnderPov", "PctLess9thGrade", "PctNotHSGrad", "PctBSorMore", "PctUnemployed", "PctEmploy", "PctEmplManu", "PctEmplProfServ", "PctOccupManu", "PctOccupMgmtProf", "MalePctDivorce", "MalePctNevMarr", "FemalePctDiv", "TotalPctDiv", "PersPerFam", "PctFam2Par", "PctKids2Par", "PctYoungKids2Par", "PctTeen2Par", "PctWorkMomYoungKids", "PctWorkMom", "NumKidsBornNeverMar", "PctKidsBornNeverMar", "NumImmig", "PctImmigRecent", "PctImmigRec5", "PctImmigRec8", "PctImmigRec10", "PctRecentImmig", "PctRecImmig5", "PctRecImmig8", "PctRecImmig10", "PctSpeakEnglOnly", "PctNotSpeakEnglWell", "PctLargHouseFam", "PctLargHouseOccup", "PersPerOccupHous", "PersPerOwnOccHous", "PersPerRentOccHous", "PctPersOwnOccup", "PctPersDenseHous", "PctHousLess3BR", "MedNumBR", "HousVacant", "PctHousOccup", "PctHousOwnOcc", "PctVacantBoarded", "PctVacMore6Mos", "MedYrHousBuilt", "PctHousNoPhone", "PctWOFullPlumb", "OwnOccLowQuart", "OwnOccMedVal", "OwnOccHiQuart", "OwnOccQrange", "RentLowQ", "RentMedian", "RentHighQ", "RentQrange", "MedRent", "MedRentPctHousInc", "MedOwnCostPctInc", "MedOwnCostPctIncNoMtg", "NumInShelters", "NumStreet", "PctForeignBorn", "PctBornSameState", "PctSameHouse85", "PctSameCity85", "PctSameState85", "LemasSwornFT", "LemasSwFTPerPop", "LemasSwFTFieldOps", "LemasSwFTFieldPerPop", "LemasTotalReq", "LemasTotReqPerPop", "PolicReqPerOffic", "PolicPerPop", "RacialMatchCommPol", "PctPolicWhite", "PctPolicBlack", "PctPolicHisp", "PctPolicAsian", "PctPolicMinor", "OfficAssgnDrugUnits", "NumKindsDrugsSeiz", "PolicAveOTWorked", "LandArea", "PopDens", "PctUsePubTrans", "PolicCars", "PolicOperBudg", "LemasPctPolicOnPatr", "LemasGangUnitDeploy", "LemasPctOfficDrugUn", "PolicBudgPerPop", "Murders", "murdPerPop", "Rapes", "rapesPerPop", "Robberies", "robbbPerPop", "Assaults", "assaultPerPop", "Burglaries", "burglPerPop", "Larcenies", "larcPerPop", "autoTheft", "autoTheftPerPop", "Arsons", "arsonsPerPop", "ViolentCrimesPerPop", "nonViolPerPop"]

candid_npo_stats_schema = ['record_id', 'city', 'state_code', 'RECEIPT_TOT', 'TYPE', 'FORMATION_YEAR', 'TOT_VOLUNTEERS', 'TOT_REV_CURRENT', 'TOTAL_COMP_ORG',
                    'CONTRIB_ALL', 'SERVICE_REVENUE', 'INCOME_INVESTMENT', 'NET_SALES', 'NET_SPECIAL',
                    'GROSS_PROFIT', 'REVENUE_OTHER', 'REVENUE_TOTAL', 'GRANTS_ORGS_US', 'EXPENSES_TOTAL',
                    'TOTAL_PROGRAM_SERVICES', 'TOTAL_MANAGEMENT', 'TOTAL_FUNDRAISING', 'CASH_EOY', 'SAVINGS_EOY', 'PLEDGES_GRANTS_EOY', 'ASSETS_TOTAL_EOY',
                    'UNRESTRICTED_EOY', 'RETAINED_EOY', 'NET_TOTAL_EOY', 'BALANCES_EOY', 'FINS_REVIEWED_CBX',
                    'FINS_AUDITED_CBX']

revocations_schema = ['ein', 'legal_name', 'doing_business_as_name', 'organization_address', 'city', 'state', 'zip_code', 'country', 'exemption_type', 'revocation_date', 'revocation_posting_date', 'exemption_reinstatement_date', 'RYEAR', 'RDATE', 'ID']

census_migration_schema = ["source_state", "destination_state", "quantity", "margin_of_error"]


eitc_county_schema = ["county_id", "state_name", "county_name", "estimated_tax_units", "EITC_eligible_units", "total_population_in_EITC_eligible_units", "total_qualifying_children_in_EITC_eligible_units", "married_filing_jointly_pct", "head_of_household_pct", "single_pct", "same_sex_married_filing_jointly_pct", "white_pct", "black_pct", "asian_native_hawaiian_other_pacific_islander_pct", "hispanic_pct", "other_pct", "married_couple_EITC_qualifying_kids_pct", "married_couple_no_EITC_qualifying_kids_pct", "same_sex_married_couple_EITC_qualifying_kids_pct", "same_sex_married_couple_no_EITC_qualifying_kids_pct", "unmarried_woman_EITC_qualifying_kids_pct", "unmarried_woman_no_EITC_qualifying_kids_pct", "unmarried_man_EITC_qualifying_kids_pct", "unmarried_man_no_EITC_qualifying_kids_pct", "high_school_or_less_pct", "some_college_or_associate_pct", "BA_or_higher_pct", "head_or_spouse_is_veteran_pct", "households_receiving_SNAP_pct", "median_AGI", "language_1", "language_1_pct", "language_2", "language_2_pct", "language_3", "language_3_pct", "industry_1", "industry_1_pct", "industry_2", "industry_2_pct", "industry_3", "industry_3_pct", "industry_4", "industry_4_pct", "industry_5", "industry_5_pct", "occupation_1", "occupation_1_pct", "occupation_2", "occupation_2_pct", "occupation_3", "occupation_3_pct", "occupation_4", "occupation_4_pct", "occupation_5", "occupation_5_pct"]

eitc_metro_schema = ["metro_id", "state_name", "metro_name", "estimated_tax_units", "EITC_eligible_units", "total_population_in_EITC_eligible_units", "total_qualifying_children_in_EITC_eligible_units", "married_filing_jointly_pct", "head_of_household_pct", "single_pct", "same_sex_married_filing_jointly_pct", "white_pct", "black_pct", "asian_native_hawaiian_other_pacific_islander_pct", "hispanic_pct", "other_pct", "married_couple_EITC_qualifying_kids_pct", "married_couple_no_EITC_qualifying_kids_pct", "same_sex_married_couple_EITC_qualifying_kids_pct", "same_sex_married_couple_no_EITC_qualifying_kids_pct", "unmarried_woman_EITC_qualifying_kids_pct", "unmarried_woman_no_EITC_qualifying_kids_pct", "unmarried_man_EITC_qualifying_kids_pct", "unmarried_man_no_EITC_qualifying_kids_pct", "high_school_or_less_pct", "some_college_or_associate_pct", "BA_or_higher_pct", "head_or_spouse_is_veteran_pct", "households_receiving_SNAP_pct", "median_AGI", "language_1", "language_1_pct", "language_2", "language_2_pct", "language_3", "language_3_pct", "industry_1", "industry_1_pct", "industry_2", "industry_2_pct", "industry_3", "industry_3_pct", "industry_4", "industry_4_pct", "industry_5", "industry_5_pct", "occupation_1", "occupation_1_pct", "occupation_2", "occupation_2_pct", "occupation_3", "occupation_3_pct", "occupation_4", "occupation_4_pct", "occupation_5", "occupation_5_pct"]

eitc_nonmetro_schema = ["nonmetro_id", "state_name", "nonmetro_name", "unweight_EITC_units", "estimated_tax_units", "EITC_eligible_units", "share_of_EITC_units_in_exclusively_nonmetro_PUMAs", "total_population_in_EITC_eligible_units", "total_qualifying_children_in_EITC_eligible_units", "married_filing_jointly_pct", "head_of_household_pct", "single_pct", "same_sex_married_filing_jointly_pct", "white_pct", "black_pct", "asian_native_hawaiian_other_pacific_islander_pct", "hispanic_pct", "other_pct", "married_couple_EITC_qualifying_kids_pct", "married_couple_no_EITC_qualifying_kids_pct", "same_sex_married_couple_EITC_qualifying_kids_pct", "same_sex_married_couple_no_EITC_qualifying_kids_pct", "unmarried_woman_EITC_qualifying_kids_pct", "unmarried_woman_no_EITC_qualifying_kids_pct", "unmarried_man_EITC_qualifying_kids_pct", "unmarried_man_no_EITC_qualifying_kids_pct", "high_school_or_less_pct", "some_college_or_associate_pct", "BA_or_higher_pct", "head_or_spouse_is_veteran_pct", "households_receiving_SNAP_pct", "median_AGI", "language_1", "language_1_pct", "language_2", "language_2_pct", "language_3", "language_3_pct", "industry_1", "industry_1_pct", "industry_2", "industry_2_pct", "industry_3", "industry_3_pct", "industry_4", "industry_4_pct", "industry_5", "industry_5_pct", "occupation_1", "occupation_1_pct", "occupation_2", "occupation_2_pct", "occupation_3", "occupation_3_pct", "occupation_4", "occupation_4_pct", "occupation_5", "occupation_5_pct"]

eitc_state_schema = ["state_id", "state_name", "estimated_tax_units", "EITC_eligible_units", "total_population_in_EITC_eligible_units", "total_qualifying_children_in_EITC_eligible_units", "married_filing_jointly_pct", "head_of_household_pct", "single_pct", "same_sex_married_filing_jointly_pct", "white_pct", "black_pct", "asian_native_hawaiian_other_pacific_islander_pct", "hispanic_pct", "other_pct", "married_couple_EITC_qualifying_kids_pct", "married_couple_no_EITC_qualifying_kids_pct", "same_sex_married_couple_EITC_qualifying_kids_pct", "same_sex_married_couple_no_EITC_qualifying_kids_pct", "unmarried_woman_EITC_qualifying_kids_pct", "unmarried_woman_no_EITC_qualifying_kids_pct", "unmarried_man_EITC_qualifying_kids_pct", "unmarried_man_no_EITC_qualifying_kids_pct", "high_school_or_less_pct", "some_college_or_associate_pct", "BA_or_higher_pct", "head_or_spouse_is_veteran_pct", "households_receiving_SNAP_pct", "median_AGI", "language_1", "language_1_pct", "language_2", "language_2_pct", "language_3", "language_3_pct", "industry_1", "industry_1_pct", "industry_2", "industry_2_pct", "industry_3", "industry_3_pct", "industry_4", "industry_4_pct", "industry_5", "industry_5_pct", "occupation_1", "occupation_1_pct", "occupation_2", "occupation_2_pct", "occupation_3", "occupation_3_pct", "occupation_4", "occupation_4_pct", "occupation_5", "occupation_5_pct"]

F9_P00_T00_HEADER_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_00_NAME_ORG_CTRL", "F9_00_BUILD_TIME_STAMP", "F9_00_RETURN_TIME_STAMP", "F9_00_SPECIAL_COND_DESC", "F9_00_TAX_PERIOD_BEGIN_DATE", "F9_00_TAX_PERIOD_END_DATE", "F9_00_FORM_ADDR_CHANGE_X", "F9_00_FORM_AMENDED_RETURN_X", "F9_00_FORM_FINAL_RETURN_X", "F9_00_FORM_INITIAL_RETURN_X", "F9_00_ORG_ADDR_CITY", "F9_00_ORG_ADDR_CNTR", "F9_00_ORG_ADDR_IN_CARE_OF", "F9_00_ORG_ADDR_L1", "F9_00_ORG_ADDR_L2", "F9_00_ORG_ADDR_STATE", "F9_00_ORG_ADDR_ZIP", "F9_00_ORG_NAME_DBA_L1", "F9_00_ORG_NAME_DBA_L2", "F9_00_ORG_PHONE", "F9_00_PRIN_OFF_ADDR_CITY", "F9_00_PRIN_OFF_ADDR_CNTR", "F9_00_PRIN_OFF_ADDR_L1", "F9_00_PRIN_OFF_ADDR_L2", "F9_00_PRIN_OFF_ADDR_STATE", "F9_00_PRIN_OFF_ADDR_ZIP", "F9_00_PRIN_OFF_NAME_ORG_L1", "F9_00_PRIN_OFF_NAME_ORG_L2", "F9_00_PRIN_OFF_NAME_PERS", "F9_00_GRO_RCPT", "F9_00_GROUP_RETURN_AFFIL_X", "F9_00_ALL_AFFIL_INCL_X", "F9_00_GROUP_EXEMPT_NUM", "F9_00_EXEMPT_STAT_4947A1_X", "F9_00_EXEMPT_STAT_501C_X", "F9_00_EXEMPT_STAT_501C3_X", "F9_00_EXEMPT_STAT_527_X", "F9_00_ORG_WEBSITE", "F9_00_TYPE_ORG_ASSOC_X", "F9_00_TYPE_ORG_CORP_X", "F9_00_TYPE_ORG_OTH_DESC", "F9_00_TYPE_ORG_OTH_X", "F9_00_TYPE_ORG_TRUST_X", "F9_00_YEAR_FORMATION", "F9_00_LEGAL_DMCL_CNTR", "F9_00_LEGAL_DMCL_STATE"]

F9_P01_T00_SUMMARY_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_01_ACT_GVRN_ACT_MISSION", "F9_01_ACT_GVRN_TERMIN_KONTR_X", "F9_01_ACT_GVRN_NUM_VOTE_MEMB", "F9_01_ACT_GVRN_NUM_VOTE_MEMB_IND", "F9_01_ACT_GVRN_EMPL_TOT", "F9_01_ACT_GVRN_VOL_TOT", "F9_01_ACT_GVRN_UBIZ_REV_TOT", "F9_01_ACT_GVRN_UBIZ_TAXABLE_NET", "F9_01_REV_CONTR_TOT_CY", "F9_01_REV_CONTR_TOT_CY_2", "F9_01_REV_CONTR_TOT_PY", "F9_01_REV_PROG_TOT_CY", "F9_01_REV_PROG_TOT_PY", "F9_01_REV_INVEST_TOT_CY", "F9_01_REV_INVEST_TOT_PY", "F9_01_REV_OTH_CY", "F9_01_REV_OTH_PY", "F9_01_REV_TOT_CY", "F9_01_REV_TOT_PY", "F9_01_EXP_GRANT_SIMILAR_CY", "F9_01_EXP_GRANT_SIMILAR_PY", "F9_01_EXP_BEN_PAID_MEMB_CY", "F9_01_EXP_BEN_PAID_MEMB_PY", "F9_01_EXP_SAL_ETC_CY", "F9_01_EXP_SAL_ETC_PY", "F9_01_EXP_PROF_FUNDR_TOT_CY", "F9_01_EXP_PROF_FUNDR_TOT_PY", "F9_01_EXP_FUNDR_TOT_CY", "F9_01_EXP_OTH_CY", "F9_01_EXP_OTH_CY_V2", "F9_01_EXP_OTH_PY", "F9_01_EXP_TOT_CY", "F9_01_EXP_TOT_PY", "F9_01_EXP_REV_LESS_EXP_CY", "F9_01_EXP_REV_LESS_EXP_PY", "F9_01_NAFB_ASSET_TOT_BOY", "F9_01_NAFB_ASSET_TOT_EOY", "F9_01_NAFB_LIAB_TOT_BOY", "F9_01_NAFB_LIAB_TOT_EOY", "F9_01_NAFB_TOT_BOY", "F9_01_NAFB_TOT_EOY"]

F9_P01_T00_SUMMARY_EZ_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_01_INFO_SCHED_O_X", "F9_01_EXP_FEE_OTH_PAY_KONTR"]

F9_P02_T00_SIGNATURE_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_02_DISC_PREP_THIRD_PARTY_X", "F9_02_PREP_FIRM_ADDR_CITY", "F9_02_PREP_FIRM_ADDR_CNTR", "F9_02_PREP_FIRM_ADDR_L1", "F9_02_PREP_FIRM_ADDR_L2", "F9_02_PREP_FIRM_ADDR_STATE", "F9_02_PREP_FIRM_ADDR_ZIP", "F9_02_PREP_FIRM_EIN", "F9_02_PREP_FIRM_NAME_L1", "F9_02_PREP_FIRM_NAME_L2", "F9_02_PREP_NAME", "F9_02_PREP_PHONE", "F9_02_PREP_PTIN", "F9_02_PREP_SELF_EMPL_X", "F9_02_PREP_SIGNTR_DATE", "F9_02_SIGNING_OFF_NAME", "F9_02_SIGNING_OFF_PHONE", "F9_02_SIGNING_OFF_SIGNTR_DATE", "F9_02_SIGNING_OFF_TITLE"]

F9_P03_T00_MISSION_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_03_ORG_MISSION_PURPOSE", "F9_03_PROG_NEW_X", "F9_03_PROG_CHANGE_X"]

F9_P03_T00_PROGRAMS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_03_INFO_SCHED_O_X", "F9_03_PROG_EXP_TOT"]

F9_P04_T00_REQUIRED_SCHEDULES_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_04_DESC_IN_501C3_X", "F9_04_SCHED_B_REQ_X", "F9_04_POLI_ACT_X", "F9_04_LOB_ACT_X", "F9_04_PROXY_TAX_X", "F9_04_DAF_X", "F9_04_CONSERV_EMT_X", "F9_04_COLLEC_ART_X", "F9_04_CREDIT_COUNS_X", "F9_04_ENDOW_X", "F9_04_REP_BALANCE_SHEET_AMT_X", "F9_04_REP_LAND_BLDG_EQUIP_X", "F9_04_REP_INVEST_OTH_SEC_X", "F9_04_REP_PROG_RLTD_X", "F9_04_REP_OTH_ASSET_X", "F9_04_REP_OTH_LIAB_X", "F9_04_REP_FOOTNOTE_FIN48_X", "F9_04_AFS_IND_X", "F9_04_AFS_CONSOL_X", "F9_04_SCHOOL_X", "F9_04_FRGN_OFFICE_X", "F9_04_FRGN_ACT_X", "F9_04_GRANT_MT_5K_FRGN_ORG_X", "F9_04_GRANT_MT_5K_FRGN_INDIV_X", "F9_04_PROF_FUNDR_X", "F9_04_FUNDR_ACT_X", "F9_04_GAMING_X", "F9_04_HOSPITAL_X", "F9_04_HOSPITAL_AFS_X", "F9_04_GRANT_MT_5K_US_ORG_X", "F9_04_GRANT_MT_5K_US_INDIV_X", "F9_04_COMP_DTK_YES_X", "F9_04_TAX_EXEMPT_BOND_X", "F9_04_TAX_EXEMPT_BOND_INVEST_X", "F9_04_TAX_EXEMPT_BOND_ISSUER_X", "F9_04_TAX_EXEMPT_BOND_ESCROW_X", "F9_04_TRANSAC_ENGAGED_X", "F9_04_TRANSAC_PY_X", "F9_04_LOAN_DTK_X", "F9_04_GRANT_RLTD_PERS_X", "F9_04_BIZ_TRANSAC_DTK_X", "F9_04_BIZ_TRANSAC_DTK_FAM_X", "F9_04_BIZ_TRANSAC_DTK_ENTITY_X", "F9_04_CONTR_NONCSH_MT_25K_X", "F9_04_CONTR_ART_HIST_X", "F9_04_LTD_X", "F9_04_LTD_DOA_X", "F9_04_DOA_X", "F9_04_ENTITY_DISREG_X", "F9_04_ENTITY_RLTD_X", "F9_04_ENTITY_CTRL_X", "F9_04_ENTITY_CTRL_TRANSAC_X", "F9_04_TRANSFER_EXEMPT_ORG_X", "F9_04_PTR_ACT_X", "F9_04_SCHED_O_REQ_X"]

F9_P04_T00_REQUIRED_SCHEDULES_EZ_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_04_LOAN_DTK_AMT", "F9_04_FRGN_OFFICE_CNTR", "F9_04_RLTD_ORG_527_X"]

F9_P05_T00_OTHER_IRS_FILING_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_05_INFO_SCHED_O_X", "F9_05_NUM_FORM_1096_BOX_3", "F9_05_NUM_FORM_W2G", "F9_05_COMPLY_BACKUP_WITHHOLD_X", "F9_05_NUM_EMPL", "F9_05_EMPL_TAX_RETURN_FILED_X", "F9_05_UBIZ_IMCOME_OVER_LIMIT_X", "F9_05_UBIZ_FORM_990T_FILED_X", "F9_05_FRGN_FIN_ACC_X", "F9_05_FRGN_FIN_ACC_CNTR", "F9_05_PTST_X", "F9_05_PTST_PARTY_NOTIFY_X", "F9_05_PTST_FORM_8886T_FILED_X", "F9_05_NONDEDUCT_CONTR_X", "F9_05_NONDEDUCT_CONTR_NOTIFY_X", "F9_05_170C_QUID_PRO_QUO_CONTR_X", "F9_05_170C_QUID_PRO_QUO_NOTIFY_X", "F9_05_170C_FORM_8282_REQ_X", "F9_05_170C_NUM_FORM_8282_FILED_X", "F9_05_170C_FUNDS_FOR_PREMIUM_X", "F9_05_170C_PREMIUM_PAID_X", "F9_05_170C_FORM_8899_FILED_X", "F9_05_170C_FORM_1098C_FILED_X", "F9_05_DAF_EXCESS_BIZ_HOLDING_X", "F9_05_DAF_TAXABLE_DIST_X", "F9_05_DAF_DIST_DONOR_X", "F9_05_501C7_INITIATION_FEES", "F9_05_501C7_GRO_RCPT_PUB_USE", "F9_05_501C12_GRO_INCOME_MEMB", "F9_05_501C12_GRO_INCOME_OTH", "F9_05_4947_FORM_990_FILED_1041_X", "F9_05_4947_TAX_EXEMPT_INT_AMT", "F9_05_501C29_LIC_HEALTH_PLAN_X", "F9_05_501C29_STATE_RESERVE_AMT", "F9_05_501C29_RESERVE_MAINT_AMT", "F9_05_TANNING_SVC_PROV_X", "F9_05_TANNING_FORM_720_FILED_X", "F9_05_SUBJ_TO_4960_TAX_X", "F9_05_SUBJ_TO_4968_TAX_X"]

F9_P06_T00_GOVERNANCE_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_06_INFO_SCHED_O_X", "F9_06_GVRN_NUM_VOTING_MEMB", "F9_06_GVRN_NUM_VOTING_MEMB_IND", "F9_06_GVRN_DTK_FAMBIZ_RELATION_X", "F9_06_GVRN_DELEGATE_MGMT_DUTY_X", "F9_06_GVRN_CHANGE_DOC_X", "F9_06_GVRN_ASSET_DIVERSION_X", "F9_06_GVRN_MEMB_STCKHLDR_X", "F9_06_GVRN_PERS_ELECT_MEMB_X", "F9_06_GVRN_DECISION_APPROV_X", "F9_06_GVRN_DOC_GVRN_BODY_X", "F9_06_GVRN_DOC_COMMITTEE_X", "F9_06_GVRN_DTK_NO_MAILING_ADDR_X", "F9_06_POLICY_CHPTR_AFFIL_X", "F9_06_POLICY_CHPTR_REFERENCE_X", "F9_06_POLICY_FORM990_GVRN_BODY_X", "F9_06_POLICY_COI_X", "F9_06_POLICY_COI_DISCLOSURE_X", "F9_06_POLICY_COI_MONITOR_X", "F9_06_POLICY_WHSTLBLWR_X", "F9_06_POLICY_DOC_RETENTION_X", "F9_06_POLICY_COMP_PROCESS_CEO_X", "F9_06_POLICY_COMP_PROCESS_OTH_X", "F9_06_POLICY_JV_X", "F9_06_POLICY_JV_PROC_X", "F9_06_DISCLOSURE_STATES_FILED", "F9_06_DISCLOSURE_AVBL_OTH_WEB_X", "F9_06_DISCLOSURE_AVBL_OTH_X", "F9_06_DISCLOSURE_AVBL_OWN_WEB_X", "F9_06_DISCLOSURE_AVBL_REQUEST_X", "F9_06_DISCLOSURE_BOOK_ADDR_CITY", "F9_06_DISCLOSURE_BOOK_ADDR_CNTR", "F9_06_DISCLOSURE_BOOK_ADDR_L1", "F9_06_DISCLOSURE_BOOK_ADDR_L2", "F9_06_DISCLOSURE_BOOK_ADDR_STATE", "F9_06_DISCLOSURE_BOOK_ADDR_ZIP", "F9_06_DISCLOSURE_BOOK_NAME_L1", "F9_06_DISCLOSURE_BOOK_NAME_L2", "F9_06_DISCLOSURE_BOOK_NAME_PERS", "F9_06_DISCLOSURE_BOOK_PHONE"]

F9_P06_T00_GOVERNANCE_EZ_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_06_ACT_NOT_PREVIOUSLY_REP_X", "F9_06_POLI_EXP", "F9_06_POLI_FORM1120_POL_FILED_X", "F9_06_TAX_IMPOSED_IRC_4911", "F9_06_TAX_IMPOSED_IRC_4912", "F9_06_TAX_IMPOSED_IRC_4955", "F9_06_TAX_IMPOSED_ORG_MGR", "F9_06_TAX_REIMBURSED_ORG"]

F9_P07_T00_DIR_TRUST_KEY_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_07_COMP_KONTR_NUM_GT_100K", "F9_07_COMP_DTK_COMP_OTH_SOURCE_X", "F9_07_COMP_DTK_FORMER_LISTED_X", "F9_07_COMP_DTK_COMP_GT_150K_X", "F9_07_COMP_DTK_NO_LISTED_X", "F9_07_COMP_DTK_NUM_GT_100K", "F9_07_COMP_DTK_COMP_ORG_SUBTOT", "F9_07_COMP_DTK_COMP_RLTD_SUBTOT", "F9_07_COMP_DTK_COMP_OTH_SUBTOT", "F9_07_INFO_SCHED_O_X", "F9_07_COMP_DTK_COMP_OTH_TOT", "F9_07_COMP_DTK_COMP_RLTD_TOT", "F9_07_COMP_DTK_COMP_ORG_TOT", "F9_07_SCHED_A_FILED_X", "F9_07_COMP_DTK_NUM_GT_100K_HCE"]

F9_P08_T00_REVENUE_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_08_INFO_SCHED_O_X", "F9_08_REV_CONTR_FED_CAMP", "F9_08_REV_CONTR_MEMBSHIP_DUE", "F9_08_REV_CONTR_FUNDR_EVNT", "F9_08_REV_CONTR_RLTD_ORG", "F9_08_REV_CONTR_GOVT_GRANT", "F9_08_REV_CONTR_OTH", "F9_08_REV_CONTR_NONCSH", "F9_08_REV_CONTR_TOT", "F9_08_REV_PROG_OTH_TOT", "F9_08_REV_PROG_OTH_RLTD", "F9_08_REV_PROG_OTH_UBIZ", "F9_08_REV_PROG_OTH_EXCL", "F9_08_REV_PROG_TOT_TOT", "F9_08_REV_OTH_INVEST_INCOME_TOT", "F9_08_REV_OTH_INVEST_INCOME_RLTD", "F9_08_REV_OTH_INVEST_INCOME_UBIZ", "F9_08_REV_OTH_INVEST_INCOME_EXCL", "F9_08_REV_OTH_INVEST_BOND_TOT", "F9_08_REV_OTH_INVEST_BOND_RLTD", "F9_08_REV_OTH_INVEST_BOND_UBIZ", "F9_08_REV_OTH_INVEST_BOND_EXCL", "F9_08_REV_OTH_ROY_TOT", "F9_08_REV_OTH_ROY_RLTD", "F9_08_REV_OTH_ROY_UBIZ", "F9_08_REV_OTH_ROY_EXCL", "F9_08_REV_OTH_RENT_GRO_REAL", "F9_08_REV_OTH_RENT_GRO_PERS", "F9_08_REV_OTH_RENT_LESS_EXP_REAL", "F9_08_REV_OTH_RENT_LESS_EXP_PERS", "F9_08_REV_OTH_RENT_INCOME_REAL", "F9_08_REV_OTH_RENT_INCOME_PERS", "F9_08_REV_OTH_RENT_NET_TOT", "F9_08_REV_OTH_RENT_NET_RLTD", "F9_08_REV_OTH_RENT_NET_UBIZ", "F9_08_REV_OTH_RENT_NET_EXCL", "F9_08_REV_OTH_SALE_ASSET_SEC", "F9_08_REV_OTH_SALE_ASSET_OTH", "F9_08_REV_OTH_SALE_ASSET", "F9_08_REV_OTH_SALE_LESS_COST_SEC", "F9_08_REV_OTH_SALE_LESS_COST_OTH", "F9_08_REV_OTH_SALE_LESS_COST", "F9_08_REV_OTH_SALE_GAIN_SEC", "F9_08_REV_OTH_SALE_GAIN_OTH", "F9_08_REV_OTH_SALE_GAIN_NET_TOT", "F9_08_REV_OTH_SALE_GAIN_NET_RLTD", "F9_08_REV_OTH_SALE_GAIN_NET_UBIZ", "F9_08_REV_OTH_SALE_GAIN_NET_EXCL", "F9_08_REV_OTH_FUNDR_EVNT_0", "F9_08_REV_OTH_FUNDR_EVNT_1", "F9_08_REV_OTH_FUNDR_DIRECT_EXP", "F9_08_REV_OTH_EVNT_DIRECT_EXP", "F9_08_REV_OTH_EVNT_NET_TOT", "F9_08_REV_OTH_FUNDR_NET_TOT", "F9_08_REV_OTH_FUNDR_NET_RLTD", "F9_08_REV_OTH_FUNDR_NET_UBIZ", "F9_08_REV_OTH_FUNDR_NET_EXCL", "F9_08_REV_OTH_GAMING", "F9_08_REV_OTH_GAMING_DIRECT_EXP", "F9_08_REV_OTH_GAMING_NET_TOT", "F9_08_REV_OTH_GAMING_NET_RLTD", "F9_08_REV_OTH_GAMING_NET_UBIZ", "F9_08_REV_OTH_GAMING_NET_EXCL", "F9_08_REV_OTH_INV_GRO_SALE", "F9_08_REV_OTH_INV_COST_GOODS", "F9_08_REV_OTH_INV_NET_TOT", "F9_08_REV_OTH_INV_NET_RLTD", "F9_08_REV_OTH_INV_NET_UBIZ", "F9_08_REV_OTH_INV_NET_EXCL", "F9_08_REV_MISC_OTH_TOT", "F9_08_REV_MISC_OTH_RLTD", "F9_08_REV_MISC_OTH_UBIZ", "F9_08_REV_MISC_OTH_EXCL", "F9_08_REV_MISC_TOT_TOT", "F9_08_REV_TOT_TOT", "F9_08_REV_TOT_RLTD", "F9_08_REV_TOT_UBIZ", "F9_08_REV_TOT_EXCL"]

F9_P09_T00_EXPENSES_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_09_INFO_SCHED_O_X", "F9_09_EXP_GRANT_US_ORG_TOT", "F9_09_EXP_GRANT_US_ORG_PROG", "F9_09_EXP_GRANT_US_INDIV_TOT", "F9_09_EXP_GRANT_US_INDIV_PROG", "F9_09_EXP_GRANT_FRGN_TOT", "F9_09_EXP_GRANT_FRGN_PROG", "F9_09_EXP_BEN_PAID_MEMB_TOT", "F9_09_EXP_BEN_PAID_MEMB_PROG", "F9_09_EXP_COMP_DTK_TOT", "F9_09_EXP_COMP_DTK_PROG", "F9_09_EXP_COMP_DTK_MGMT", "F9_09_EXP_COMP_DTK_FUNDR", "F9_09_EXP_COMP_DSQ_PERS_TOT", "F9_09_EXP_COMP_DSQ_PERS_PROG", "F9_09_EXP_COMP_DSQ_PERS_MGMT", "F9_09_EXP_COMP_DSQ_PERS_FUNDR", "F9_09_EXP_OTH_SAL_WAGE_TOT", "F9_09_EXP_OTH_SAL_WAGE_PROG", "F9_09_EXP_OTH_SAL_WAGE_MGMT", "F9_09_EXP_OTH_SAL_WAGE_FUNDR", "F9_09_EXP_PENSION_CONTR_TOT", "F9_09_EXP_PENSION_CONTR_PROG", "F9_09_EXP_PENSION_CONTR_MGMT", "F9_09_EXP_PENSION_CONTR_FUNDR", "F9_09_EXP_OTH_EMPL_BEN_TOT", "F9_09_EXP_OTH_EMPL_BEN_PROG", "F9_09_EXP_OTH_EMPL_BEN_MGMT", "F9_09_EXP_OTH_EMPL_BEN_FUNDR", "F9_09_EXP_PAYROLL_TAX_TOT", "F9_09_EXP_PAYROLL_TAX_PROG", "F9_09_EXP_PAYROLL_TAX_MGMT", "F9_09_EXP_PAYROLL_TAX_FUNDR", "F9_09_EXP_FEE_SVC_MGMT_TOT", "F9_09_EXP_FEE_SVC_MGMT_PROG", "F9_09_EXP_FEE_SVC_MGMT_MGMT", "F9_09_EXP_FEE_SVC_MGMT_FUNDR", "F9_09_EXP_FEE_SVC_LEGAL_TOT", "F9_09_EXP_FEE_SVC_LEGAL_PROG", "F9_09_EXP_FEE_SVC_LEGAL_MGMT", "F9_09_EXP_FEE_SVC_LEGAL_FUNDR", "F9_09_EXP_FEE_SVC_ACC_TOT", "F9_09_EXP_FEE_SVC_ACC_PROG", "F9_09_EXP_FEE_SVC_ACC_MGMT", "F9_09_EXP_FEE_SVC_ACC_FUNDR", "F9_09_EXP_FEE_SVC_LOB_TOT", "F9_09_EXP_FEE_SVC_LOB_PROG", "F9_09_EXP_FEE_SVC_LOB_MGMT", "F9_09_EXP_FEE_SVC_LOB_FUNDR", "F9_09_EXP_FEE_SVC_FUNDR_TOT", "F9_09_EXP_FEE_SVC_FUNDR_PROG", "F9_09_EXP_FEE_SVC_FUNDR_MGMT", "F9_09_EXP_FEE_SVC_FUNDR_FUNDR", "F9_09_EXP_FEE_SVC_INVEST_TOT", "F9_09_EXP_FEE_SVC_INVEST_PROG", "F9_09_EXP_FEE_SVC_INVEST_MGMT", "F9_09_EXP_FEE_SVC_INVEST_FUNDR", "F9_09_EXP_FEE_SVC_OTH_TOT", "F9_09_EXP_FEE_SVC_OTH_PROG", "F9_09_EXP_FEE_SVC_OTH_MGMT", "F9_09_EXP_FEE_SVC_OTH_FUNDR", "F9_09_EXP_AD_PROMO_TOT", "F9_09_EXP_AD_PROMO_PROG", "F9_09_EXP_AD_PROMO_MGMT", "F9_09_EXP_AD_PROMO_FUNDR", "F9_09_EXP_OFFICE_TOT", "F9_09_EXP_OFFICE_TOT_V2", "F9_09_EXP_OFFICE_PROG", "F9_09_EXP_OFFICE_MGMT", "F9_09_EXP_OFFICE_FUNDR", "F9_09_EXP_INFO_TECH_TOT", "F9_09_EXP_INFO_TECH_PROG", "F9_09_EXP_INFO_TECH_MGMT", "F9_09_EXP_INFO_TECH_FUNDR", "F9_09_EXP_ROY_TOT", "F9_09_EXP_ROY_PROG", "F9_09_EXP_ROY_MGMT", "F9_09_EXP_ROY_FUNDR", "F9_09_EXP_OCCUPANCY_TOT", "F9_09_EXP_OCCUPANCY_PROG", "F9_09_EXP_OCCUPANCY_MGMT", "F9_09_EXP_OCCUPANCY_FUNDR", "F9_09_EXP_TRAVEL_TOT", "F9_09_EXP_TRAVEL_PROG", "F9_09_EXP_TRAVEL_MGMT", "F9_09_EXP_TRAVEL_FUNDR", "F9_09_EXP_TRAVEL_ENTMT_TOT", "F9_09_EXP_TRAVEL_ENTMT_PROG", "F9_09_EXP_TRAVEL_ENTMT_MGMT", "F9_09_EXP_TRAVEL_ENTMT_FUNDR", "F9_09_EXP_CONF_MEETING_TOT", "F9_09_EXP_CONF_MEETING_PROG", "F9_09_EXP_CONF_MEETING_MGMT", "F9_09_EXP_CONF_MEETING_FUNDR", "F9_09_EXP_INT_TOT", "F9_09_EXP_INT_PROG", "F9_09_EXP_INT_MGMT", "F9_09_EXP_INT_FUNDR", "F9_09_EXP_PAY_AFFIL_TOT", "F9_09_EXP_PAY_AFFIL_PROG", "F9_09_EXP_PAY_AFFIL_MGMT", "F9_09_EXP_PAY_AFFIL_FUNDR", "F9_09_EXP_DEPREC_TOT", "F9_09_EXP_DEPREC_PROG", "F9_09_EXP_DEPREC_MGMT", "F9_09_EXP_DEPREC_FUNDR", "F9_09_EXP_INSURANCE_TOT", "F9_09_EXP_INSURANCE_PROG", "F9_09_EXP_INSURANCE_MGMT", "F9_09_EXP_INSURANCE_FUNDR", "F9_09_EXP_OTH_OTH_TOT", "F9_09_EXP_OTH_OTH_PROG", "F9_09_EXP_OTH_OTH_MGMT", "F9_09_EXP_OTH_OTH_FUNDR", "F9_09_EXP_TOT_TOT", "F9_09_EXP_TOT_PROG", "F9_09_EXP_TOT_MGMT", "F9_09_EXP_TOT_FUNDR", "F9_09_EXP_JOINT_COST_X", "F9_09_EXP_JOINT_COST_TOT", "F9_09_EXP_JOINT_COST_PROG", "F9_09_EXP_JOINT_COST_MGMT", "F9_09_EXP_JOINT_COST_FUNDR"]

F9_P10_T00_BALANCE_SHEET_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_10_INFO_SCHED_O_X", "F9_10_ASSET_CASH_SAVING_BOY", "F9_10_ASSET_CASH_SAVING_EOY", "F9_10_ASSET_CASH_BOY", "F9_10_ASSET_CASH_EOY", "F9_10_ASSET_SAVING_BOY", "F9_10_ASSET_SAVING_EOY", "F9_10_ASSET_PLEDGE_NET_BOY", "F9_10_ASSET_PLEDGE_NET_EOY", "F9_10_ASSET_ACC_NET_BOY", "F9_10_ASSET_ACC_NET_EOY", "F9_10_ASSET_LOAN_OFF_BOY", "F9_10_ASSET_LOAN_OFF_EOY", "F9_10_ASSET_LOAN_DSQ_PERS_BOY", "F9_10_ASSET_LOAN_DSQ_PERS_EOY", "F9_10_ASSET_NOTE_LOAN_NET_BOY", "F9_10_ASSET_NOTE_LOAN_NET_EOY", "F9_10_ASSET_INV_SALE_BOY", "F9_10_ASSET_INV_SALE_EOY", "F9_10_ASSET_EXP_PREPAID_BOY", "F9_10_ASSET_EXP_PREPAID_EOY", "F9_10_ASSET_LAND_BLDG", "F9_10_ASSET_LAND_BLDG_BOY", "F9_10_ASSET_LAND_BLDG_EOY", "F9_10_ASSET_LAND_BLDG_DEPREC", "F9_10_ASSET_LAND_BLDG_NET_BOY", "F9_10_ASSET_LAND_BLDG_NET_EOY", "F9_10_ASSET_INVEST_SEC_BOY", "F9_10_ASSET_INVEST_SEC_EOY", "F9_10_ASSET_INVEST_SEC_OTH_BOY", "F9_10_ASSET_INVEST_SEC_OTH_EOY", "F9_10_ASSET_INVEST_PROG_RLTD_BOY", "F9_10_ASSET_INVEST_PROG_RLTD_EOY", "F9_10_ASSET_INTANGIBLE_BOY", "F9_10_ASSET_INTANGIBLE_EOY", "F9_10_ASSET_OTH_BOY", "F9_10_ASSET_OTH_BOY_V2", "F9_10_ASSET_OTH_EOY", "F9_10_ASSET_OTH_EOY_V2", "F9_10_ASSET_TOT_BOY", "F9_10_ASSET_TOT_EOY", "F9_10_LIAB_ACC_PAYABLE_BOY", "F9_10_LIAB_ACC_PAYABLE_EOY", "F9_10_LIAB_GRANT_PAYABLE_BOY", "F9_10_LIAB_GRANT_PAYABLE_EOY", "F9_10_LIAB_REV_DEFERRED_BOY", "F9_10_LIAB_REV_DEFERRED_EOY", "F9_10_LIAB_TAX_EXEMPT_BOND_BOY", "F9_10_LIAB_TAX_EXEMPT_BOND_EOY", "F9_10_LIAB_ESCROW_ACC_BOY", "F9_10_LIAB_ESCROW_ACC_EOY", "F9_10_LIAB_LOAN_OFF_BOY", "F9_10_LIAB_LOAN_OFF_EOY", "F9_10_LIAB_MTG_NOTE_BOY", "F9_10_LIAB_MTG_NOTE_EOY", "F9_10_LIAB_NOTE_UNSEC_BOY", "F9_10_LIAB_NOTE_UNSEC_EOY", "F9_10_LIAB_OTH_BOY", "F9_10_LIAB_OTH_EOY", "F9_10_LIAB_TOT_BOY", "F9_10_NAFB_FOLLOW_SFAS117_X", "F9_10_LIAB_TOT_EOY", "F9_10_NAFB_UNRESTRICT_BOY", "F9_10_NAFB_UNRESTRICT_EOY", "F9_10_NAFB_RESTRICT_TEMP_BOY", "F9_10_NAFB_RESTRICT_TEMP_EOY", "F9_10_NAFB_RESTRICT_PERM_BOY", "F9_10_NAFB_NO_FOLLOW_SFAS117_X", "F9_10_NAFB_RESTRICT_PERM_EOY", "F9_10_NAFB_CAP_STCK_BOY", "F9_10_NAFB_CAP_STCK_EOY", "F9_10_NAFB_CAP_SURPLUS_BOY", "F9_10_NAFB_CAP_SURPLUS_EOY", "F9_10_NAFB_EARNING_RETAINED_BOY", "F9_10_NAFB_EARNING_RETAINED_EOY", "F9_10_NAFB_TOT_BOY", "F9_10_NAFB_TOT_EOY", "F9_10_NAFB_TOT_LIAB_NAFB_BOY", "F9_10_NAFB_TOT_LIAB_NAFB_EOY"]

F9_P11_T00_ASSETS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_11_INFO_SCHED_O_X", "F9_11_RECO_REV_LESS_EXP", "F9_11_RECO_GAIN_INVEST_NET", "F9_11_RECO_SVC_DONATED", "F9_11_RECO_INVEST_EXP", "F9_11_RECO_ADJ_PRIOR", "F9_11_RECO_OTH_CHANGE_NAFB"]

F9_P12_T00_FINANCIAL_REPORTING_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "F9_12_INFO_SCHED_O_X", "F9_12_FINSTAT_METHOD_ACC_ACCRU_X", "F9_12_FINSTAT_METHOD_ACC_CASH_X", "F9_12_FINSTAT_METHOD_ACC_OTH", "F9_12_FINSTAT_METHOD_ACC_OTH_X", "F9_12_FINSTAT_COMPILE_BOTH_X", "F9_12_FINSTAT_COMPILE_CONSOL_X", "F9_12_FINSTAT_COMPILE_SEP_X", "F9_12_FINSTAT_COMPILE_X", "F9_12_FINSTAT_AUDIT_BOTH_X", "F9_12_FINSTAT_AUDIT_CONSOL_X", "F9_12_FINSTAT_AUDIT_SEP_X", "F9_12_FINSTAT_AUDIT_X", "F9_12_FINSTAT_AUDIT_COMMITTEE_X", "F9_12_FINSTAT_FED_AUDIT_REQ_X", "F9_12_FINSTAT_FED_AUDIT_PERF_X"]

SA_P00_T00_HEADER_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SA_00_NAME_ORG_L1"]

SA_P01_T00_PUBLIC_CHARITY_STATUS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SA_01_PCSTAT_CHURCH_X", "SA_01_PCSTAT_SCHOOL_X", "SA_01_PCSTAT_HOSPITAL_X", "SA_01_PCSTAT_HOSPITAL_ADDR_CITY", "SA_01_PCSTAT_HOSPITAL_ADDR_CNTR", "SA_01_PCSTAT_HOSPITAL_ADDR_STATE", "SA_01_PCSTAT_HOSPITAL_NAME_L1", "SA_01_PCSTAT_HOSPITAL_NAME_L2", "SA_01_PCSTAT_MEDICAL_RSRCH_X", "SA_01_PCSTAT_COLLEGE_BEN_X", "SA_01_PCSTAT_GOVT_X", "SA_01_PCSTAT_GOVT_PUB_SUPPORT_X", "SA_01_PCSTAT_COM_TRUST_X", "SA_01_PCSTAT_PUB_SUPPORT_X", "SA_01_PCSTAT_PUB_SAFETY_TEST_X", "SA_01_PCSTAT_SUPPORT_ORG_X", "SA_01_PCSTAT_SUPPORT_T1_X", "SA_01_PCSTAT_SUPPORT_T2_X", "SA_01_PCSTAT_SUPPORT_T3_FUNC_X", "SA_01_PCSTAT_SUPPORT_T3_NOFUNC_X", "SA_01_PCSTAT_DETERMIN_IRS_X", "SA_01_PCSTAT_SUPPORT_ORG_NUM"]

SA_P02_T00_SUPPORT_SCHEDULE_170_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SA_02_PUB_GIFT_GRANT_CONTR_CY_M4", "SA_02_PUB_GIFT_GRANT_CONTR_CY_M3", "SA_02_PUB_GIFT_GRANT_CONTR_CY_M2", "SA_02_PUB_GIFT_GRANT_CONTR_CY_M1", "SA_02_PUB_GIFT_GRANT_CONTR_CY", "SA_02_PUB_GIFT_GRANT_CONTR_TOT", "SA_02_PUB_TAXREV_LEVIED_CY_M4", "SA_02_PUB_TAXREV_LEVIED_CY_M3", "SA_02_PUB_TAXREV_LEVIED_CY_M2", "SA_02_PUB_TAXREV_LEVIED_CY_M1", "SA_02_PUB_TAXREV_LEVIED_CY", "SA_02_PUB_TAXREV_LEVIED_TOT", "SA_02_PUB_VALUE_SVC_GOVT_CY_M4", "SA_02_PUB_VALUE_SVC_GOVT_CY_M3", "SA_02_PUB_VALUE_SVC_GOVT_CY_M2", "SA_02_PUB_VALUE_SVC_GOVT_CY_M1", "SA_02_PUB_VALUE_SVC_GOVT_CY", "SA_02_PUB_VALUE_SVC_GOVT_TOT", "SA_02_PUB_TOT_L123_CY_M4", "SA_02_PUB_TOT_L123_CY_M3", "SA_02_PUB_TOT_L123_CY_M2", "SA_02_PUB_TOT_L123_CY_M1", "SA_02_PUB_TOT_L123_CY", "SA_02_PUB_TOT_L123_TOT", "SA_02_PUB_CONTR_SBST_TOT", "SA_02_PUB_SUPPORT_TOT", "SA_02_TOT_AMT_L4_CY_M4", "SA_02_TOT_AMT_L4_CY_M3", "SA_02_TOT_AMT_L4_CY_M2", "SA_02_TOT_AMT_L4_CY_M1", "SA_02_TOT_AMT_L4_CY", "SA_02_TOT_AMT_L4_CY_TOT", "SA_02_TOT_INCOME_GRO_CY_M4", "SA_02_TOT_INCOME_GRO_CY_M3", "SA_02_TOT_INCOME_GRO_CY_M2", "SA_02_TOT_INCOME_GRO_CY_M1", "SA_02_TOT_INCOME_GRO_CY", "SA_02_TOT_INCOME_GRO_TOT", "SA_02_TOT_INCOME_NET_UBIZ_CY_M4", "SA_02_TOT_INCOME_NET_UBIZ_CY_M3", "SA_02_TOT_INCOME_NET_UBIZ_CY_M2", "SA_02_TOT_INCOME_NET_UBIZ_CY_M1", "SA_02_TOT_INCOME_NET_UBIZ_CY", "SA_02_TOT_INCOME_NET_UBIZ_TOT", "SA_02_TOT_INCOME_OTH_CY_M4", "SA_02_TOT_INCOME_OTH_CY_M3", "SA_02_TOT_INCOME_OTH_CY_M2", "SA_02_TOT_INCOME_OTH_CY_M1", "SA_02_TOT_INCOME_OTH_CY", "SA_02_TOT_INCOME_OTH_TOT", "SA_02_TOT_SUPPORT_TOT", "SA_02_TOT_GRO_RCPT_RLTD_ACTS", "SA_02_TOT_FIRST_5Y_X", "SA_02_PCT_PUB_SUPPORT_CY", "SA_02_PCT_PUB_SUPPORT_PY", "SA_02_PCT_33_SUPPORT_TEST_CY_X", "SA_02_PCT_33_SUPPORT_TEST_PY_X", "SA_02_PCT_10_SUPPORT_TEST_CY_X", "SA_02_PCT_10_SUPPORT_TEST_PY_X", "SA_02_PCT_PRIVATE_FOUNDATION_X"]

SA_P03_T00_SUPPORT_SCHEDULE_509_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SA_03_PUB_GIFT_GRANT_CONTR_CY_M4", "SA_03_PUB_GIFT_GRANT_CONTR_CY_M3", "SA_03_PUB_GIFT_GRANT_CONTR_CY_M2", "SA_03_PUB_GIFT_GRANT_CONTR_CY_M1", "SA_03_PUB_GIFT_GRANT_CONTR_CY", "SA_03_PUB_GIFT_GRANT_CONTR_TOT", "SA_03_PUB_GRO_RCPT_ADM_CY_M4", "SA_03_PUB_GRO_RCPT_ADM_CY_M3", "SA_03_PUB_GRO_RCPT_ADM_CY_M2", "SA_03_PUB_GRO_RCPT_ADM_CY_M1", "SA_03_PUB_GRO_RCPT_ADM_CY", "SA_03_PUB_GRO_RCPT_ADM_TOT", "SA_03_PUB_GRO_RCPT_NONUBIZ_CY_M4", "SA_03_PUB_GRO_RCPT_NONUBIZ_CY_M3", "SA_03_PUB_GRO_RCPT_NONUBIZ_CY_M2", "SA_03_PUB_GRO_RCPT_NONUBIZ_CY_M1", "SA_03_PUB_GRO_RCPT_NONUBIZ_CY", "SA_03_PUB_GRO_RCPT_NONUBIZ_TOT", "SA_03_PUB_TAXREV_LEVIED_CY_M4", "SA_03_PUB_TAXREV_LEVIED_CY_M3", "SA_03_PUB_TAXREV_LEVIED_CY_M2", "SA_03_PUB_TAXREV_LEVIED_CY_M1", "SA_03_PUB_TAXREV_LEVIED_CY", "SA_03_PUB_TAXREV_LEVIED_TOT", "SA_03_PUB_VALUE_SVC_GOVT_CY_M4", "SA_03_PUB_VALUE_SVC_GOVT_CY_M3", "SA_03_PUB_VALUE_SVC_GOVT_CY_M2", "SA_03_PUB_VALUE_SVC_GOVT_CY_M1", "SA_03_PUB_VALUE_SVC_GOVT_CY", "SA_03_PUB_VALUE_SVC_GOVT_TOT", "SA_03_PUB_TOT_L1_5_CY_M4", "SA_03_PUB_TOT_L1_5_CY_M3", "SA_03_PUB_TOT_L1_5_CY_M2", "SA_03_PUB_TOT_L1_5_CY_M1", "SA_03_PUB_TOT_L1_5_CY", "SA_03_PUB_TOT_L1_5_TOT", "SA_03_PUB_AMT_PERS_DSQ_CY_M4", "SA_03_PUB_AMT_PERS_DSQ_CY_M3", "SA_03_PUB_AMT_PERS_DSQ_CY_M2", "SA_03_PUB_AMT_PERS_DSQ_CY_M1", "SA_03_PUB_AMT_PERS_DSQ_CY", "SA_03_PUB_AMT_PERS_DSQ_TOT", "SA_03_PUB_AMT_CONTR_SBST_CY_M4", "SA_03_PUB_AMT_CONTR_SBST_CY_M3", "SA_03_PUB_AMT_CONTR_SBST_CY_M2", "SA_03_PUB_AMT_CONTR_SBST_CY_M1", "SA_03_PUB_AMT_CONTR_SBST_CY", "SA_03_PUB_AMT_CONTR_SBST_TOT", "SA_03_PUB_ADD_L7AB_CY_M4", "SA_03_PUB_ADD_L7AB_CY_M3", "SA_03_PUB_ADD_L7AB_CY_M2", "SA_03_PUB_ADD_L7AB_CY_M1", "SA_03_PUB_ADD_L7AB_CY", "SA_03_PUB_ADD_L7AB_TOT", "SA_03_PUB_SUPPORT_TOT", "SA_03_TOT_AMT_L6_CY_M4", "SA_03_TOT_AMT_L6_CY_M3", "SA_03_TOT_AMT_L6_CY_M2", "SA_03_TOT_AMT_L6_CY_M1", "SA_03_TOT_AMT_L6_CY", "SA_03_TOT_AMT_L6_TOT", "SA_03_TOT_INCOME_GRO_CY_M4", "SA_03_TOT_INCOME_GRO_CY_M3", "SA_03_TOT_INCOME_GRO_CY_M2", "SA_03_TOT_INCOME_GRO_CY_M1", "SA_03_TOT_INCOME_GRO_CY", "SA_03_TOT_INCOME_GRO_TOT", "SA_03_TOT_INCOME_UBIZ_CY_M4", "SA_03_TOT_INCOME_UBIZ_CY_M3", "SA_03_TOT_INCOME_UBIZ_CY_M2", "SA_03_TOT_INCOME_UBIZ_CY_M1", "SA_03_TOT_INCOME_UBIZ_CY", "SA_03_TOT_INCOME_UBIZ_TOT", "SA_03_TOT_ADD_L10AB_CY_M4", "SA_03_TOT_ADD_L10AB_CY_M3", "SA_03_TOT_ADD_L10AB_CY_M2", "SA_03_TOT_ADD_L10AB_CY_M1", "SA_03_TOT_ADD_L10AB_CY", "SA_03_TOT_ADD_L10AB_TOT", "SA_03_TOT_INCOME_NET_UBIZ_CY_M4", "SA_03_TOT_INCOME_NET_UBIZ_CY_M3", "SA_03_TOT_INCOME_NET_UBIZ_CY_M2", "SA_03_TOT_INCOME_NET_UBIZ_CY_M1", "SA_03_TOT_INCOME_NET_UBIZ_CY", "SA_03_TOT_INCOME_NET_UBIZ_TOT", "SA_03_TOT_INCOME_OTH_CY_M4", "SA_03_TOT_INCOME_OTH_CY_M3", "SA_03_TOT_INCOME_OTH_CY_M2", "SA_03_TOT_INCOME_OTH_CY_M1", "SA_03_TOT_INCOME_OTH_CY", "SA_03_TOT_INCOME_OTH_TOT", "SA_03_TOT_SUPPORT_CY_M4", "SA_03_TOT_SUPPORT_CY_M3", "SA_03_TOT_SUPPORT_CY_M2", "SA_03_TOT_SUPPORT_CY_M1", "SA_03_TOT_SUPPORT_CY", "SA_03_TOT_SUPPORT_TOT", "SA_03_TOT_FIRST_5Y_X", "SA_03_PCT_PUB_SUPPORT_CY", "SA_03_PCT_PUB_SUPPORT_PY", "SA_03_PCT_INVEST_INCOME_CY", "SA_03_PCT_INVEST_INCOME_PY", "SA_03_PCT_33_SUPPORT_TEST_CY_X", "SA_03_PCT_33_SUPPORT_TEST_PY_X", "SA_03_PCT_PRIVATE_FOUNDATION_X"]

SA_P04_T00_SUPPORT_ORGS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SA_04_ALL_ORG_NAME_DOC_LISTED_X", "SA_04_ALL_ORG_NONDETERMIN_IRS_X", "SA_04_ALL_ORG_501C456_X", "SA_04_ALL_ORG_QUAL_X", "SA_04_ALL_ORG_170C2B_PURPOSE_X", "SA_04_ALL_ORG_FRGN_X", "SA_04_ALL_ORG_CTRL_GRANT_FRGN_X", "SA_04_ALL_ORG_SUPPORT_FRGN_X", "SA_04_ALL_ORG_CHANGE_ORG_X", "SA_04_ALL_ORG_T12_ONLY_X", "SA_04_ALL_ORG_SUBSTITUT_ONLY_X", "SA_04_ALL_ORG_SUPPORT_OTH_X", "SA_04_ALL_ORG_PAY_CONTRIBUTOR_X", "SA_04_ALL_ORG_LOAN_PERS_DSQ_X", "SA_04_ALL_ORG_CTRL_BY_PERS_DSQ_X", "SA_04_ALL_ORG_PERS_DSQ_CTRL_X", "SA_04_ALL_ORG_PERS_DSQ_OWN_X", "SA_04_ALL_ORG_EXCESS_BIZ_RULES_X", "SA_04_ALL_ORG_EXCESS_BIZ_HOLD_X", "SA_04_ALL_ORG_CONTR_PERS_X", "SA_04_ALL_ORG_CONTR_FAM_X", "SA_04_ALL_ORG_CONTR_35PCT_CE_X", "SA_04_T1_ORG_DIR_TRUST_APPONT_X", "SA_04_T1_ORG_BEN_OTH_X", "SA_04_T2_ORG_MAJORITY_DIRTRUST_X", "SA_04_T3_ORG_DOC_PROV_TIMELY_X", "SA_04_T3_ORG_OFFS_RELATIONSHIP_X", "SA_04_T3_ORG_VOICE_INVEST_X", "SA_04_T3FUNC_ORG_ACT_TEST_X", "SA_04_T3FUNC_ORG_PARENT_X", "SA_04_T3FUNC_ORG_SUPPORT_GOVT_X", "SA_04_T3FUNC_ORG_ACT_EXEMPT_X", "SA_04_T3FUNC_ORG_ACT_ENGAGED_X", "SA_04_T3FUNC_ORG_APOINT_OFF_X", "SA_04_T3FUNC_ORG_DIRECT_POLICY_X"]

SA_P05_T00_SUPPORT_ORGS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SA_05_TRUST_INTEGRAL_PART_TEST_X", "SA_05_NETINCOME_ADJ_CAP_GAIN_PY", "SA_05_NETINCOME_ADJ_CAP_GAIN_CY", "SA_05_NETINCOME_ADJ_RECOVERY_PY", "SA_05_NETINCOME_ADJ_RECOVERY_CY", "SA_05_NETINCOME_ADJ_OTH_GRO_PY", "SA_05_NETINCOME_ADJ_OTH_GRO_CY", "SA_05_NETINCOME_ADJ_ADD_L123_PY", "SA_05_NETINCOME_ADJ_ADD_L123_CY", "SA_05_NETINCOME_ADJ_DEPREC_PY", "SA_05_NETINCOME_ADJ_DEPREC_CY", "SA_05_NETINCOME_ADJ_PRODUCT_PY", "SA_05_NETINCOME_ADJ_PRODUCT_CY", "SA_05_NETINCOME_ADJ_EXP_OTH_PY", "SA_05_NETINCOME_ADJ_EXP_OTH_CY", "SA_05_NETINCOME_ADJ_TOT_PY", "SA_05_NETINCOME_ADJ_TOT_CY", "SA_05_ASSET_MINIM_FMV_SEC_PY", "SA_05_ASSET_MINIM_FMV_SEC_CY", "SA_05_ASSET_MINIM_FMV_CASH_PY", "SA_05_ASSET_MINIM_FMV_CASH_CY", "SA_05_ASSET_MINIM_FMV_OTH_PY", "SA_05_ASSET_MINIM_FMV_OTH_CY", "SA_05_ASSET_MINIM_FMV_TOT_PY", "SA_05_ASSET_MINIM_FMV_TOT_CY", "SA_05_ASSET_MINIM_FMV_DISCOUNT", "SA_05_ASSET_MINIM_INDEPTED_CY", "SA_05_ASSET_MINIM_L2_L1D_PY", "SA_05_ASSET_MINIM_L2_L1D_CY", "SA_05_ASSET_MINIM_CASH_EXEMPT_PY", "SA_05_ASSET_MINIM_CASH_EXEMPT_CY", "SA_05_ASSET_MINIM_NON_EXEMPT_PY", "SA_05_ASSET_MINIM_NON_EXEMPT_CY", "SA_05_ASSET_MINIM_MULTIPLY_L5_PY", "SA_05_ASSET_MINIM_MULTIPLY_L5_CY", "SA_05_ASSET_MINIM_RECOVERIES_PY", "SA_05_ASSET_MINIM_RECOVERIES_CY", "SA_05_ASSET_MINIM_TOT_PY", "SA_05_ASSET_MINIM_TOT_CY", "SA_05_DIST_AMT_NETINCOME_ADJ_CY", "SA_05_DIST_AMT_85PCT_L1_CY", "SA_05_DIST_AMT_ASSET_MINIM_CY", "SA_05_DIST_AMT_GREATER_L23_CY", "SA_05_DIST_AMT_IMCOME_TAX_CY", "SA_05_DIST_AMT_ADJ_CY", "SA_05_DIST_AMT_FIRST_Y_T3_X", "SA_05_DIST_PAID_ORG_EXEMPT_CY", "SA_05_DIST_PAID_EXCESS_INCOME_CY", "SA_05_DIST_PAID_EXP_ADMIN_CY", "SA_05_DIST_PAID_EXEMPT_ASSET_CY", "SA_05_DIST_PAID_SET_ASIDE_CY", "SA_05_DIST_PAID_OTH_CY", "SA_05_DIST_PAID_TOT_CY", "SA_05_DIST_PAID_ATTENTIVE_CY", "SA_05_DIST_PAID_ADJ_CY", "SA_05_DIST_PAID_DIVIDE_L89_CY", "SA_05_DIST_ALLOC_ADJ_DA", "SA_05_DIST_ALLOC_UNDERDIST_UD", "SA_05_DIST_ALLOC_EXCESS_CY_M2", "SA_05_DIST_ALLOC_EXCESS_CY_M1", "SA_05_DIST_ALLOC_EXCESS_TOT_ED", "SA_05_DIST_ALLOC_EXCESS_APPLY_UD", "SA_05_DIST_ALLOC_EXCESS_APPLY_DA", "SA_05_DIST_ALLOC_EXCESS_REMAIN", "SA_05_DIST_ALLOC_DIST_TOT", "SA_05_DIST_ALLOC_DIST_APPLY_UD", "SA_05_DIST_ALLOC_DIST_APPLY_DA", "SA_05_DIST_ALLOC_DIST_REMAIN", "SA_05_DIST_ALLOC_REMAINING_UD", "SA_05_DIST_ALLOC_REMAINING_DA", "SA_05_DIST_ALLOC_EXCESS_NY_TOT", "SA_05_DIST_ALLOC_EXCESS_NY_M3", "SA_05_DIST_ALLOC_EXCESS_NY_M2", "SA_05_DIST_ALLOC_EXCESS_NY_M1"]

SA_P99_T00_PUBLIC_CHARITY_STATUS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SA_99_PCSTAT_SUPPORT_T3_OTH_X", "SA_99_PCSTAT_CERTIFICATION_X", "SA_99_PCSTAT_CONTR_PERS_X", "SA_99_PCSTAT_CONTR_FAM_PARTY_X", "SA_99_PCSTAT_CONTR_35PCT_CE_X"]

SC_P01_T00_LOBBY_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SC_01_POLI_EXP", "SC_01_POLI_VOL_HOURS", "SC_01_AMT_EXCISE_TAX_4955_ORG", "SC_01_AMT_EXCISE_TAX_4955_MGR", "SC_01_4955_FORM4720_FILED_X", "SC_01_4955_FORM4720_CORRECTED_X", "SC_01_ORG_AMT_EXPENDED_527_ACT", "SC_01_ORG_AMT_FUND_CONTR_527_ACT", "SC_01_ORG_TOT_EXEMPT_FUNC_EXP", "SC_01_ORG_FORM1120_FILED_X"]

SC_P02_T00_LOBBY_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SC_02_EXP_GRASS_LOB_FILEORG", "SC_02_EXP_GRASS_LOB_AFFGROUP", "SC_02_EXP_DIRECT_LOB_FILEORG", "SC_02_EXP_DIRECT_LOB_AFFGROUP", "SC_02_EXP_TOT_LOB_FILEORG", "SC_02_EXP_TOT_LOB_AFFGROUP", "SC_02_EXP_OTH_EXEMPT_FILEORG", "SC_02_EXP_OTH_EXEMPT_AFFGROUP", "SC_02_EXP_TOT_EXEMPT_FILEORG", "SC_02_EXP_TOT_EXEMPT_AFFGROUP", "SC_02_EXP_LOB_NONTAX_FILEORG", "SC_02_EXP_LOB_NONTAX_AFFGROUP", "SC_02_EXP_GRASS_NONTAX_FILEORG", "SC_02_EXP_GRASS_NONTAX_AFFGROUP", "SC_02_EXP_GRASS_M_NONTAX_FILEORG", "SC_02_EXP_GRASS_M_NONTAX_AFFGROU", "SC_02_EXP_LOB_M_NONTAX_FILEORG", "SC_02_EXP_LOB_M_NONTAX_AFFGROUP", "SC_02_4911TAX_FORM4720_FILED_X", "SC_02_AVE_EXP_LOB_NONTAX_CY_M3", "SC_02_AVE_EXP_LOB_NONTAX_CY_M2", "SC_02_AVE_EXP_LOB_NONTAX_CY_M1", "SC_02_AVE_EXP_LOB_NONTAX_CY", "SC_02_AVE_EXP_LOB_NONTAX_TOT", "SC_02_AVE_EXP_LOB_CEILING_TOT", "SC_02_AVE_EXP_TOT_LOB_CY_M3", "SC_02_AVE_EXP_TOT_LOB_CY_M2", "SC_02_AVE_EXP_TOT_LOB_CY_M1", "SC_02_AVE_EXP_TOT_LOB_CY", "SC_02_AVE_EXP_TOT_LOB_TOT", "SC_02_AVE_EXP_GRASS_NONTAX_CY_M3", "SC_02_AVE_EXP_GRASS_NONTAX_CY_M2", "SC_02_AVE_EXP_GRASS_NONTAX_CY_M1", "SC_02_AVE_EXP_GRASS_NONTAX_CY", "SC_02_AVE_EXP_GRASS_NONTAX_TOT", "SC_02_AVE_EXP_GRASS_CEILING_TOT", "SC_02_AVE_EXP_GRASS_LOB_CY_M3", "SC_02_AVE_EXP_GRASS_LOB_CY_M2", "SC_02_AVE_EXP_GRASS_LOB_CY_M1", "SC_02_AVE_EXP_GRASS_LOB_CY", "SC_02_AVE_EXP_GRASS_LOB_TOT", "SC_02_ORG_BELONG_AFFGROUP_X", "SC_02_ORG_LIMIT_CTRL_PROV_X", "SC_02_LOB_ACT_VOL_X", "SC_02_LOB_ACT_PAID_STAFF_X", "SC_02_LOB_ACT_PAID_STAFF_AMT", "SC_02_LOB_ACT_MEDIA_AD_X", "SC_02_LOB_ACT_MEDIA_AD_AMT", "SC_02_LOB_ACT_MAIL_MEMB_X", "SC_02_LOB_ACT_MAIL_MEMB_AMT", "SC_02_LOB_ACT_PUBLICA_BCAST_X", "SC_02_LOB_ACT_PUBLICA_BCAST_AMT", "SC_02_LOB_ACT_GRANT_OTH_ORG_X", "SC_02_LOB_ACT_GRANT_OTH_ORG_AMT", "SC_02_LOB_ACT_CONTACT_LEGIS_X", "SC_02_LOB_ACT_CONTACT_LEGIS_AMT", "SC_02_LOB_ACT_RALLIES_X", "SC_02_LOB_ACT_RALLIES_AMT", "SC_02_LOB_ACT_OTH_ACT_X", "SC_02_LOB_ACT_OTH_ACT_AMT", "SC_02_LOB_ACT_TOT_L1C_1I", "SC_02_LOB_ACT_NO_DESC_501C3_X", "SC_02_LOB_ACT_4912TAX_ORG", "SC_02_LOB_ACT_4912TAX_MGR", "SC_02_LOB_ACT_4912_FILE_4720_X"]

SC_P03_T00_LOBBY_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SC_03_DUES_NONDEDUCT_X", "SC_03_INHOUSE_LOB_EXP_LT_2K_X", "SC_03_AGREE_CARRYOVER_LOB_PY_X", "SC_03_AMT_DUE_ASSESS", "SC_03_NONDEDUCT_LOB_CY", "SC_03_NONDEDUCT_LOB_CARRYOVER", "SC_03_NONDEDUCT_LOB_TOT", "SC_03_AMT_AGGREGATE_REPORTED", "SC_03_AMT_CARRYOVER_NY", "SC_03_AMT_TAXABLE_LOB"]

SCHEDULE_TABLE_2022_schema = ["OBJECTID", "URL", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SCHEDA", "SCHEDB", "SCHEDC", "SCHEDD", "SCHEDE", "SCHEDF", "SCHEDG", "SCHEDH", "SCHEDI", "SCHEDJ", "SCHEDK", "SCHEDL", "SCHEDM", "SCHEDN", "SCHEDO", "SCHEDR"]

SD_P01_T00_ORGS_DONOR_ADVISED_FUNDS_OTH_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SD_01_TOT_NUM_EOY_DAF", "SD_01_TOT_NUM_EOY_OTH", "SD_01_AGGREGATE_CONTR_DAF", "SD_01_AGGREGATE_CONTR_OTH", "SD_01_AGGREGATE_GRANT_DAF", "SD_01_AGGREGATE_GRANT_OTH", "SD_01_AGGREGATE_VALUE_EOY_DAF", "SD_01_AGGREGATE_VALUE_EOY_OTH", "SD_01_DISCLOSE_LEGAL_CTRL_DAF_X", "SD_01_DISCLOSE_CHARIT_PURPOSE_X"]

SD_P02_T00_CONSERV_EASEMENTS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SD_02_EMT_HIST_LAND_AREA_X", "SD_02_EMT_HIST_STR_X", "SD_02_EMT_NATURAL_HABITAT_X", "SD_02_EMT_OPEN_SPACE_X", "SD_02_EMT_PUB_USE_X", "SD_02_EMT_TOT_NUM", "SD_02_EMT_TOT_ACREAGE", "SD_02_EMT_NUM_HIST_STR", "SD_02_EMT_NUM_HIST_STR_AFTER_06", "SD_02_EMT_NUM_MODIFIED", "SD_02_EMT_NUM_STATES", "SD_02_EMT_WRITTEN_POLICY_X", "SD_02_EMT_STAFF_HOURS_ENFORCE", "SD_02_EMT_EXP_INCURRED_ENFORCE", "SD_02_EMT_170H_SATISFIED_X"]

SD_P03_T00_ORGS_COLLECT_ART_HIST_TREASURE_OTH_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SD_03_ART_PUB_EXHIBITION_REV", "SD_03_ART_PUB_EXHIBITION_ASSET", "SD_03_WORKS_ART_REV", "SD_03_WORKS_ART_ASSET", "SD_03_COLLEC_PUB_EXHIBITION_X", "SD_03_COLLEC_SCHOLAR_RSRCH_X", "SD_03_COLLEC_PRESERVATION_X", "SD_03_COLLEC_LOAN_PROG_X", "SD_03_COLLEC_OTH_DESC", "SD_03_COLLEC_OTH_X", "SD_03_SOL_ASSET_SALE_X"]

SD_P04_T00_ESCROW_CUSTODIAL_ARRANGEMENTS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SD_04_AGENT_TRUSTEE_OTH_X", "SD_04_ESCROW_BALANCE_BEG", "SD_04_ESCROW_ADDITION", "SD_04_ESCROW_DIST", "SD_04_ESCROW_BALANCE_END", "SD_04_ESCROW_ACC_LIAB_INCL_P10_X", "SD_04_EXPLANATION_P13_X"]

SD_P05_T00_ENDOWMENT_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SD_05_ENDOW_BALANCE_BOY_CY", "SD_05_ENDOW_BALANCE_BOY_CY_M1", "SD_05_ENDOW_BALANCE_BOY_CY_M2", "SD_05_ENDOW_BALANCE_BOY_CY_M3", "SD_05_ENDOW_BALANCE_BOY_CY_M4", "SD_05_ENDOW_CONTR_CY", "SD_05_ENDOW_CONTR_CY_M1", "SD_05_ENDOW_CONTR_CY_M2", "SD_05_ENDOW_CONTR_CY_M3", "SD_05_ENDOW_CONTR_CY_M4", "SD_05_ENDOW_INVEST_CY", "SD_05_ENDOW_INVEST_CY_M1", "SD_05_ENDOW_INVEST_CY_M2", "SD_05_ENDOW_INVEST_CY_M3", "SD_05_ENDOW_INVEST_CY_M4", "SD_05_ENDOW_GRANT_CY", "SD_05_ENDOW_GRANT_CY_M1", "SD_05_ENDOW_GRANT_CY_M2", "SD_05_ENDOW_GRANT_CY_M3", "SD_05_ENDOW_GRANT_CY_M4", "SD_05_ENDOW_EXP_OTH_CY", "SD_05_ENDOW_EXP_OTH_CY_M1", "SD_05_ENDOW_EXP_OTH_CY_M2", "SD_05_ENDOW_EXP_OTH_CY_M3", "SD_05_ENDOW_EXP_OTH_CY_M4", "SD_05_ENDOW_EXP_ADMIN_CY", "SD_05_ENDOW_EXP_ADMIN_CY_M1", "SD_05_ENDOW_EXP_ADMIN_CY_M2", "SD_05_ENDOW_EXP_ADMIN_CY_M3", "SD_05_ENDOW_EXP_ADMIN_CY_M4", "SD_05_ENDOW_BALANCE_EOY_CY", "SD_05_ENDOW_BALANCE_EOY_CY_M1", "SD_05_ENDOW_BALANCE_EOY_CY_M2", "SD_05_ENDOW_BALANCE_EOY_CY_M3", "SD_05_ENDOW_BALANCE_EOY_CY_M4", "SD_05_PCT_L1G_BOARD_DESIGNATED", "SD_05_PCT_L1G_PERMANENT_ENDOW", "SD_05_PCT_L1G_TERM_ENDOW", "SD_05_ENDOW_HELD_ORG_UNRLTD_X", "SD_05_ENDOW_HELD_ORG_RLTD_X", "SD_05_RLTD_ORG_LISTED_SCHED_R_X"]

SD_P06_T00_LAND_BLDG_EQUIP_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SD_06_ORG_LAND_COST_INVEST", "SD_06_ORG_LAND_COST_OTH", "SD_06_ORG_LAND_BV", "SD_06_ORG_BLDG_COST_INVEST", "SD_06_ORG_BLDG_COST_OTH", "SD_06_ORG_BLDG_DEPREC", "SD_06_ORG_BLDG_BV", "SD_06_ORG_LEASEHOLD_COST_INVEST", "SD_06_ORG_LEASEHOLD_COST_OTH", "SD_06_ORG_LEASEHOLD_DEPREC", "SD_06_ORG_LEASEHOLD_BV", "SD_06_ORG_EQUIP_COST_INVEST", "SD_06_ORG_EQUIP_COST_OTH", "SD_06_ORG_EQUIP_DEPREC", "SD_06_ORG_EQUIP_BV", "SD_06_ORG_OTH_COST_INVEST", "SD_06_ORG_OTH_COST_OTH", "SD_06_ORG_OTH_DEPREC", "SD_06_ORG_OTH_BV", "SD_06_ORG_TOT_BV"]

SD_P07_T00_INVESTMENTS_OTH_SECURITIES_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SD_07_INVEST_SEC_TOT_BV"]

SD_P09_T00_OTH_ASSETS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SD_09_OTH_ASSET_BV_TOT"]

SD_P10_T00_OTH_LIABILITIES_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SD_10_LIAB_TOT_BV", "SD_10_LIAB_FOOTNOTE_PART_08_X"]

SD_P11_T00_RECONCILIATION_REVENUE_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SD_11_RECO_REV_TOT_PER_AFS", "SD_11_RECO_REV_NUGAINS_INVEST", "SD_11_RECO_REV_DONATED_SVC", "SD_11_RECO_REV_GRANT_PY", "SD_11_RECO_REV_OTH", "SD_11_RECO_REV_ADD_L2ABCD", "SD_11_RECO_REV_SUBTRACT_L2E_L1", "SD_11_RECO_REV_INVEST_NO_INCL", "SD_11_RECO_REV_OTH_NO_INCL", "SD_11_RECO_REV_ADD_L4AB", "SD_11_RECO_REV_TOT"]

SD_P12_T00_RECONCILIATION_EXPENSES_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SD_12_RECO_EXP_TOT_PER_AFS", "SD_12_RECO_EXP_DONATED_SVC", "SD_12_RECO_EXP_PY_ADJ", "SD_12_RECO_EXP_LOSSES", "SD_12_RECO_EXP_OTH_INCL", "SD_12_RECO_EXP_ADD_L2A_2D", "SD_12_RECO_EXP_SUBTOT_L1_M_L2E", "SD_12_RECO_EXP_INVEST_NO_INCL", "SD_12_RECO_EXP_OTH_NO_INCL", "SD_12_RECO_EXP_ADD_L4AB", "SD_12_RECO_EXP_TOT"]

SD_P99_T00_RECONCILIATION_NETASSETS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SD_99_RECO_NETASSET_REV_TOT", "SD_99_RECO_NETASSET_EXP_TOT", "SD_99_RECO_NETASSET_EXCESS", "SD_99_RECO_NETASSET_NUGAINS", "SD_99_RECO_NETASSET_DONATED_SVC", "SD_99_RECO_NETASSET_INVEST_EXP", "SD_99_RECO_NETASSET_PRIOR_ADJ", "SD_99_RECO_NETASSET_OTH", "SD_99_RECO_NETASSET_TOT_ADJ", "SD_99_RECO_NETASSET_EXCESS_AFS"]

SE_P01_T00_SCHOOLS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SE_01_POLICY_STMT_NONDISCR_X", "SE_01_POLICY_STMT_BROCHURES_X", "SE_01_POLICY_BCAST_MEDIA_X", "SE_01_POLICY_BCAST_MEDIA_EXPLAIN", "SE_01_MAINT_RACIAL_RECORD_X", "SE_01_MAINT_SCHOLARSHIP_RECORD_X", "SE_01_MAINT_COPY_BROCHURES_X", "SE_01_MAINT_COPY_SOL_X", "SE_01_MAINT_EXPLANATION", "SE_01_DISCR_RACE_STUDENT_RIGHT_X", "SE_01_DISCR_RACE_ADM_POLICY_X", "SE_01_DISCR_RACE_EMPL_FACULTY_X", "SE_01_DISCR_RACE_SCHOLARSHIP_X", "SE_01_DISCR_RACE_EDU_POLICY_X", "SE_01_DISCR_RACE_USE_FACILITY_X", "SE_01_DISCR_RACE_ATHLETIC_PROG_X", "SE_01_DISCR_RACE_OTH_ACT_X", "SE_01_DISCR_RACE_EXPLANATION", "SE_01_GOVT_FIN_AID_REC_X", "SE_01_GOVT_FIN_AID_REVOKED_X", "SE_01_GOVT_FIN_AID_EXPLANATION", "SE_01_COMPLIANCE_REV_PROC_7550_X", "SE_01_COMPLIANCE_EXPLANATION"]

SF_P01_T00_FRGN_ACTS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SF_01_GRANT_RECORD_MAINT_X", "SF_01_FRGN_REG_NUM_OFFICE_SUBTOT", "SF_01_FRGN_REG_NUM_EMPL_SUBTOT", "SF_01_FRGN_REG_TOT_EXP_SUBTOT", "SF_01_FRGN_REG_NUM_OFFICE_CONTIN", "SF_01_FRGN_REG_NUM_EMPL_CONTIN", "SF_01_FRGN_REG_TOT_EXP_CONTIN", "SF_01_FRGN_REG_NUM_OFFICE_TOT", "SF_01_FRGN_REG_NUM_EMPL_TOT", "SF_01_FRGN_REG_TOT_EXP_TOT"]

SF_P02_T00_FRGN_ORG_GRANTS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SF_02_FRGN_ORG_GRANT_501C3_TOT", "SF_02_FRGN_ORG_GRANT_OTH_ORG_TOT"]

SF_P04_T00_FRGN_INTERESTS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SF_04_FRGN_CORP_TRANSFEROR_X", "SF_04_FRGN_TRUST_INT_X", "SF_04_FRGN_CORP_OWNERSHIP_INT_X", "SF_04_FRGN_FUND_SHAREHOLDER_X", "SF_04_FRGN_PTR_OWNERSHIP_INT_X", "SF_04_BOYCOTTING_CNTR_ACT_X"]

SF_P99_T00_FRGN_ORG_GRANTS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SF_99_FRGN_ORG_GRANT_LT_5K_X"]

SG_P01_T00_FUNDRAISING_ACTS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SG_01_FUNDR_ACT_SOL_MAIL_X", "SG_01_FUNDR_ACT_SOL_EMAIL_X", "SG_01_FUNDR_ACT_SOL_PHONE_X", "SG_01_FUNDR_ACT_SOL_IN_PERSON_X", "SG_01_FUNDR_ACT_SOL_NONGOVT_X", "SG_01_FUNDR_ACT_SOL_GOVT_GRANT_X", "SG_01_FUNDR_ACT_SPECIAL_EVNT_X", "SG_01_FUNDR_SVC_AGREEMENT_X", "SG_01_FUNDRAISER_ACT_GRORCPT_TOT", "SG_01_FUNDRAISER_AMT_ORG_TOT", "SG_01_FUNDRAISER_AMT_PAID_TOT", "SG_01_LIST_STATES_ORG_LIC"]

SG_P02_T00_FUNDRAISING_EVENTS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SG_02_FUNDR_EVNT_REV_GRORCPT_TOT", "SG_02_FUNDR_EVNT_REV_CONTR_TOT", "SG_02_FUNDR_EVNT_REV_GRO_TOT", "SG_02_FUNDR_EVNT_EXP_CASH_TOT", "SG_02_FUNDR_EVNT_EXP_NONCSH_TOT", "SG_02_FUNDR_EVNT_EXP_RENT_TOT", "SG_02_FUNDR_EVNT_EXP_FOOD_TOT", "SG_02_FUNDR_EVNT_EXP_ENTMT_TOT", "SG_02_FUNDR_EVNT_EXP_OTH_TOT", "SG_02_FUNDR_EVNT_EXP_SUMMARY_TOT", "SG_02_FUNDR_EVNT_NET_INCOME_TOT"]

SG_P03_T00_GAMING_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SG_03_GAMING_REV_GRO_BINGO", "SG_03_GAMING_REV_GRO_PTAB", "SG_03_GAMING_REV_GRO_OTH", "SG_03_GAMING_REV_GRO_TOT", "SG_03_GAMING_EXP_CASH_BINGO", "SG_03_GAMING_EXP_CASH_PTAP", "SG_03_GAMING_EXP_CASH_OTH", "SG_03_GAMING_EXP_CASH_TOT", "SG_03_GAMING_EXP_NONCSH_BINGO", "SG_03_GAMING_EXP_NONCSH_PTAP", "SG_03_GAMING_EXP_NONCSH_OTH", "SG_03_GAMING_EXP_NONCSH_TOT", "SG_03_GAMING_EXP_RENT_BINGO", "SG_03_GAMING_EXP_RENT_PTAP", "SG_03_GAMING_EXP_RENT_OTH", "SG_03_GAMING_EXP_RENT_TOT", "SG_03_GAMING_EXP_OTH_BINGO", "SG_03_GAMING_EXP_OTH_PTAP", "SG_03_GAMING_EXP_OTH_OTH", "SG_03_GAMING_EXP_OTH_TOT", "SG_03_GAMING_VOL_BINGO_X", "SG_03_GAMING_VOL_PCT_BINGO", "SG_03_GAMING_VOL_PCT_PTAP", "SG_03_GAMING_VOL_PTAP_X", "SG_03_GAMING_VOL_OTH_X", "SG_03_GAMING_VOL_PCT_OTH", "SG_03_GAMING_EXP_SUMMARY_TOT", "SG_03_GAMING_NET_INCOME_TOT", "SG_03_STATES_GAMING_CONDUCTED", "SG_03_GAMING_LIC_X", "SG_03_GAMING_NO_LIC_EXPLANATION", "SG_03_GAMING_LIC_SUSPENDED_X", "SG_03_LIC_SUSPENDED_EXPLANATION", "SG_03_GAMING_NONMEMB_X", "SG_03_GAMING_MEMB_OTH_ENTITY_X", "SG_03_PCT_GAMING_OWN_FACILITY", "SG_03_PCT_GAMING_OTH_FACILITY", "SG_03_GRK_ADDR_CITY", "SG_03_GRK_ADDR_CNTR", "SG_03_GRK_ADDR_L1", "SG_03_GRK_ADDR_L2", "SG_03_GRK_ADDR_STATE", "SG_03_GRK_ADDR_ZIP", "SG_03_GRK_NAME_ORG_L1", "SG_03_GRK_NAME_ORG_L2", "SG_03_GRK_NAME_PERS", "SG_03_KONTR_3RD_PARTY_X", "SG_03_AMT_GAMING_REV_3RD_PARTY", "SG_03_AMT_GAMING_REV_ORG", "SG_03_THIRD_PARTY_ADDR_CITY", "SG_03_THIRD_PARTY_ADDR_CNTR", "SG_03_THIRD_PARTY_ADDR_L1", "SG_03_THIRD_PARTY_ADDR_L2", "SG_03_THIRD_PARTY_ADDR_STATE", "SG_03_THIRD_PARTY_ADDR_ZIP", "SG_03_THIRD_PARTY_NAME_ORG_L1", "SG_03_THIRD_PARTY_NAME_ORG_L2", "SG_03_THIRD_PARTY_NAME_PERS", "SG_03_GAMING_MGR_COMP", "SG_03_GAMING_MGR_KONTR_X", "SG_03_GAMING_MGR_SVC_PROV", "SG_03_GAMING_MGR_DIR_OFF_X", "SG_03_GAMING_MGR_EMPL_X", "SG_03_GAMING_MGR_NAME_ORG_L1", "SG_03_GAMING_MGR_NAME_ORG_L2", "SG_03_GAMING_MGR_NAME_PERS", "SG_03_CHARIT_DIST_REQ_X", "SG_03_AMT_DIST"]

SH_P01_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SH_01_FAP_X", "SH_01_FAP_WRITTEN_X", "SH_01_FAP_ALL_HOSPITAL_X", "SH_01_FAP_INDIV_HOSPITAL_X", "SH_01_FAP_MOST_HOSPITAL_X", "SH_01_FPG_FREE_CARE_PCT_100_X", "SH_01_FPG_FREE_CARE_PCT_150_X", "SH_01_FPG_FREE_CARE_PCT_200_X", "SH_01_FPG_FREE_CARE_PCT_OTH", "SH_01_FPG_FREE_CARE_PCT_OTH_X", "SH_01_FPG_FREE_CARE_X", "SH_01_FPG_DCNT_CARE_PCT_200_X", "SH_01_FPG_DCNT_CARE_PCT_250_X", "SH_01_FPG_DCNT_CARE_PCT_300_X", "SH_01_FPG_DCNT_CARE_PCT_350_X", "SH_01_FPG_DCNT_CARE_PCT_400_X", "SH_01_FPG_DCNT_CARE_PCT_OTH", "SH_01_FPG_DCNT_CARE_PCT_OTH_X", "SH_01_FPG_DCNT_CARE_X", "SH_01_FAP_FREE_DCNT_CARE_X", "SH_01_FA_AMT_BUDGETED_CARE_X", "SH_01_FA_EXP_EXCEED_BUDGET_X", "SH_01_FA_UNAVBL_PROV_CARE_X", "SH_01_REP_ANNUAL_COM_BEN_X", "SH_01_REP_AVBL_PUB_X", "SH_01_FA_NUM_ACT_PROG", "SH_01_FA_PERS_SERVED", "SH_01_FA_EXP_TOT_COM_BEN", "SH_01_FA_REV_DIRECT_OFFSET", "SH_01_FA_EXP_NET_COM_BEN", "SH_01_FA_PCT_EXP_TOT", "SH_01_MEDICAID_NUM_ACT_PROG", "SH_01_MEDICAID_PERS_SERVED", "SH_01_MEDICAID_EXP_TOT_COM_BEN", "SH_01_MEDICAID_REV_DIRECT_OFFSET", "SH_01_MEDICAID_EXP_NET_COM_BEN", "SH_01_MEDICAID_PCT_EXP_TOT", "SH_01_MEAN_NUM_ACT_PROG", "SH_01_MEAN_PERS_SERVED", "SH_01_MEAN_EXP_TOT_COM_BEN", "SH_01_MEAN_REV_DIRECT_OFFSET", "SH_01_MEAN_EXP_NET_COM_BEN", "SH_01_MEAN_PCT_EXP_TOT", "SH_01_TOT_FA_NUM_ACT_PROG", "SH_01_TOT_FA_PERS_SERVED", "SH_01_TOT_FA_EXP_TOT_COM_BEN", "SH_01_TOT_FA_REV_DIRECT_OFFSET", "SH_01_TOT_FA_EXP_NET_COM_BEN", "SH_01_TOT_FA_PCT_EXP_TOT", "SH_01_H_SVC_NUM_ACT_PROG", "SH_01_H_SVC_PERS_SERVED", "SH_01_H_SVC_EXP_TOT_COM_BEN", "SH_01_H_SVC_REV_DIRECT_OFFSET", "SH_01_H_SVC_EXP_NET_COM_BEN", "SH_01_H_SVC_PCT_EXP_TOT", "SH_01_H_EDU_NUM_ACT_PROG", "SH_01_H_EDU_PERS_SERVEDU", "SH_01_H_EDU_EXP_TOT_COM_BEN", "SH_01_H_EDU_REV_DIRECT_OFFSET", "SH_01_H_EDU_EXP_NET_COM_BEN", "SH_01_H_EDU_PCT_EXP_TOT", "SH_01_H_SUBSID_NUM_ACT_PROG", "SH_01_H_SUBSID_PERS_SERVED", "SH_01_H_SUBSID_EXP_TOT_COM_BEN", "SH_01_H_SUBSID_REV_DIRECT_OFFSET", "SH_01_H_SUBSID_EXP_NET_COM_BEN", "SH_01_H_SUBSID_PCT_EXP_TOT", "SH_01_RSRCH_NUM_ACT_PROG", "SH_01_RSRCH_PERS_SERVED", "SH_01_RSRCH_EXP_TOT_COM_BEN", "SH_01_RSRCH_REV_DIRECT_OFFSET", "SH_01_RSRCH_EXP_NET_COM_BEN", "SH_01_RSRCH_PCT_EXP_TOT", "SH_01_CASH_NUM_ACT_PROG", "SH_01_CASH_PERS_SERVED", "SH_01_CASH_EXP_TOT_COM_BEN", "SH_01_CASH_REV_DIRECT_OFFSET", "SH_01_CASH_EXP_NET_COM_BEN", "SH_01_CASH_PCT_EXP_TOT", "SH_01_TOT_OTH_NUM_ACT_PROG", "SH_01_TOT_OTH_PERS_SERVED", "SH_01_TOT_OTH_EXP_TOT_COM_BEN", "SH_01_TOT_OTH_REV_DIRECT_OFFSET", "SH_01_TOT_OTH_EXP_NET_COM_BEN", "SH_01_TOT_OTH_PCT_EXP_TOT", "SH_01_TOT_NUM_ACT_PROG", "SH_01_TOT_PERS_SERVED", "SH_01_TOT_EXP_TOT_COM_BEN", "SH_01_TOT_REV_DIRECT_OFFSET", "SH_01_TOT_EXP_NET_COM_BEN", "SH_01_TOT_PCT_EXP_TOT"]

SH_P02_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SH_02_PHY_IMP_NUM_ACT_PROG", "SH_02_PHY_IMP_PERS_SERVED", "SH_02_PHY_IMP_EXP_TOT_COM_BLDG", "SH_02_PHY_IMP_REV_DIRECT_OFFSET", "SH_02_PHY_IMP_EXP_NET_COM_BLDG", "SH_02_PHY_IMP_PCT_EXP_TOT", "SH_02_ECON_DEV_NUM_ACT_PROG", "SH_02_ECON_DEV_PERS_SERVED", "SH_02_ECON_DEV_EXP_TOT_COM_BLDG", "SH_02_ECON_DEV_REV_DIRECT_OFFSET", "SH_02_ECON_DEV_EXP_NET_COM_BLDG", "SH_02_ECON_DEV_PCT_EXP_TOT", "SH_02_COM_SUP_NUM_ACT_PROG", "SH_02_COM_SUP_PERS_SERVED", "SH_02_COM_SUP_EXP_TOT_COM_BLDG", "SH_02_COM_SUP_REV_DIRECT_OFFSET", "SH_02_COM_SUP_EXP_NET_COM_BLDG", "SH_02_COM_SUP_PCT_EXP_TOT", "SH_02_ENVIR_NUM_ACT_PROG", "SH_02_ENVIR_PERS_SERVED", "SH_02_ENVIR_EXP_TOT_COM_BLDG", "SH_02_ENVIR_REV_DIRECT_OFFSET", "SH_02_ENVIR_EXP_NET_COM_BLDG", "SH_02_ENVIR_PCT_EXP_TOT", "SH_02_LDRSP_NUM_ACT_PROG", "SH_02_LDRSP_PERS_SERVED", "SH_02_LDRSP_EXP_TOT_COM_BLDG", "SH_02_LDRSP_REV_DIRECT_OFFSET", "SH_02_LDRSP_EXP_NET_COM_BLDG", "SH_02_LDRSP_PCT_EXP_TOT", "SH_02_COAL_NUM_ACT_PROG", "SH_02_COAL_PERS_SERVED", "SH_02_COAL_EXP_TOT_COM_BLDG", "SH_02_COAL_REV_DIRECT_OFFSET", "SH_02_COAL_EXP_NET_COM_BLDG", "SH_02_COAL_PCT_EXP_TOT", "SH_02_ADVOCACY_NUM_ACT_PROG", "SH_02_ADVOCACY_PERS_SERVED", "SH_02_ADVOCACY_EXP_TOT_COM_BLDG", "SH_02_ADVOCACY_REV_DIRECT_OFFSET", "SH_02_ADVOCACY_EXP_NET_COM_BLDG", "SH_02_ADVOCACY_PCT_EXP_TOT", "SH_02_WORK_NUM_ACT_PROG", "SH_02_WORK_PERS_SERVED", "SH_02_WORK_EXP_TOT_COM_BLDG", "SH_02_WORK_REV_DIRECT_OFFSET", "SH_02_WORK_EXP_NET_COM_BLDG", "SH_02_WORK_PCT_EXP_TOT", "SH_02_OTH_NUM_ACT_PROG", "SH_02_OTH_PERS_SERVED", "SH_02_OTH_EXP_TOT_COM_BLDG", "SH_02_OTH_REV_DIRECT_OFFSET", "SH_02_OTH_EXP_NET_COM_BLDG", "SH_02_OTH_PCT_EXP_TOT", "SH_02_TOT_NUM_ACT_PROG", "SH_02_TOT_PERS_SERVED", "SH_02_TOT_EXP_TOT_COM_BLDG", "SH_02_TOT_REV_DIRECT_OFFSET", "SH_02_TOT_EXP_NET_COM_BLDG", "SH_02_TOT_PCT_EXP_TOT"]

SH_P03_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SH_03_BAD_DEBT_EXP_REPORTED_X", "SH_03_BAD_DEBT_EXP_AMT", "SH_03_BAD_DEBT_CONTR_FAP", "SH_03_MEDICARE_REIMBURSE_AMT", "SH_03_MEDICARE_COST_REIMBURSED", "SH_03_MEDICARE_SURPLUS", "SH_03_MEDICARE_COST_ACC_X", "SH_03_MEDICARE_COST_OTH_X", "SH_03_MEDICARE_COST_RATIO_X", "SH_03_COLLEC_POLICY_WRITTEN_X", "SH_03_COLLEC_POLICY_PROV_FA_X"]

SH_P05_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SH_05_HOSPITAL_NUM", "SH_05_HOSPITAL_FACILITY_LINE_NUM", "SH_05_HOSPITAL_FACILITY_NAME_L1", "SH_05_HOSPITAL_FACILITY_NAME_L2", "SH_05_CHNA_FIRST_LIC_X", "SH_05_CHNA_TAX_EXEMPT_HOSPITAL_X", "SH_05_CHNA_CONDUCTED_X", "SH_05_CHNA_DESC_DEFINITION_COM_X", "SH_05_CHNA_DESC_DEMOGRAPHICS_X", "SH_01_CHNA_DESC_RESOURCES_X", "SH_05_CHNA_DESC_DATA_OBTAINED_X", "SH_05_CHNA_DESC_HEALTH_NEED_X", "SH_05_CHNA_DESC_HEALTH_ISSUE_X", "SH_05_CHNA_DESC_IDENTIFYING_X", "SH_05_CHNA_DESC_CONSULTING_X", "SH_05_CHNA_DESC_INFO_GAP_X", "SH_05_CHNA_DESC_OTH_X", "SH_05_CHNA_YEAR_CONDUCTED", "SH_05_CHNA_TAKE_COM_ACC_X", "SH_05_CHNA_FACILITY_OTH_X", "SH_05_CHNA_FACILITY_NON_X", "SH_05_CHNA_AVBL_PUB_X", "SH_05_CHNA_AVBL_WEBSITE", "SH_05_CHNA_AVBL_WEBSITE_X", "SH_05_CHNA_AVBL_OTH_WEBSITE", "SH_05_CHNA_AVBL_OTH_WEBSITE_X", "SH_05_CHNA_AVBL_PAPER_COPY_X", "SH_05_CHNA_AVBL_OTH_METHOD_X", "SH_05_CHNA_IMPLEMENT_ADOPT_X", "SH_05_CHNA_IMPLEMENT_ADOPT_YEAR", "SH_05_CHNA_STRATEGY_WEBSITE_X", "SH_05_CHNA_STRATEGY_WEBSITE", "SH_05_CHNA_STRATEGY_ATTACHED_X", "SH_05_CHNA_EXCISE_TAX_4959_X", "SH_05_CHNA_4959_FORM4720_FILED_X", "SH_05_CHNA_EXCISE_TAX_4959_AMT", "SH_05_FAP_CRITERIA_EXPLAIN_X", "SH_05_FAP_CRITERIA_PCT_FPG_DCNT", "SH_05_FAP_CRITERIA_PCT_FPG_FREE", "SH_05_FAP_CRITERIA_PCT_FPG_X", "SH_05_FAP_CRITERIA_INCOME_X", "SH_05_FAP_CRITERIA_ASSET_X", "SH_05_FAP_CRITERIA_INDIGENCY_X", "SH_05_FAP_CRITERIA_INSURANCE_X", "SH_05_FAP_CRITERIA_UNINSURED_X", "SH_05_FAP_CRITERIA_RESIDENCY_X", "SH_05_FAP_CRITERIA_OTH_X", "SH_05_FAP_BASIS_EXPLAIN_X", "SH_05_FAP_METHOD_EXPLAIN_X", "SH_05_FAP_METHOD_DESC_INFO_X", "SH_05_FAP_METHOD_DESC_DOC_X", "SH_05_FAP_METHOD_INFO_HOSPITAL_X", "SH_05_FAP_METHOD_INFO_NPO_X", "SH_05_FAP_METHOD_OTH_X", "SH_05_FAP_AVBL_PUBLICITY_X", "SH_05_FAP_AVBL_WEBSITE", "SH_05_FAP_AVBL_WEBSITE_X", "SH_05_FAP_AVBL_FORM_WEBSITE", "SH_05_FAP_AVBL_FORM_WEBSITE_X", "SH_05_FAP_AVBL_SUMMARY_WEBSITE", "SH_05_FAP_AVBL_SUMMARY_WEBSITE_X", "SH_05_FAP_AVBL_REQUEST_X", "SH_05_FAP_AVBL_FORM_REQUEST_X", "SH_05_FAP_AVBL_SUMMARY_REQUEST_X", "SH_05_FAP_AVBL_COM_NOTIFIED_X", "SH_05_FAP_AVBL_OTH_PUBLICITY_X", "SH_05_BILL_EXPLAIN_ACT_NOPAY_X", "SH_05_BILL_PERMIT_AGENCY_X", "SH_05_BILL_PERMIT_SELL_DEBT_X", "SH_05_BILL_PERMIT_LEGAL_X", "SH_05_BILL_PERMIT_OTH_ACT_X", "SH_05_BILL_PERMIT_NO_ACT_X", "SH_05_BILL_3RD_PARTY_COLLEC_X", "SH_05_BILL_3RD_PARTY_AGENCY_X", "SH_05_BILL_3RD_PARTY_SELL_DEBT_X", "SH_05_BILL_3RD_PARTY_LEGAL_X", "SH_05_BILL_3RD_PARTY_OTH_ACT_X", "SH_05_BILL_MADE_OTH_ACT_X", "SH_05_BILL_MADE_NO_ACT_X", "SH_05_EMERGENCY_NODISCR_POLICY_X", "SH_05_EMERGENCY_NO_CARE_X", "SH_05_EMERGENCY_NO_POLICY_X", "SH_05_EMERGENCY_LIMIT_CARE_X", "SH_05_EMERGENCY_OTH_REASON_X", "SH_05_CHARGE_MT_AMT_BILLED_X", "SH_05_CHARGE_FAP_GRO_CHARGE_X", "SH_05_NON_HOSPITAL_NUM"]

SH_P99_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SH_99_CHNA_AVBL_REQUEST_X", "SH_99_CHNA_NEED_ADOPT_IMP_X", "SH_99_CHNA_NEED_EXECUTE_IMP_X", "SH_99_CHNA_NEED_DEVELOP_PLAN_X", "SH_99_CHNA_NEED_PARTICIPA_PLAN_X", "SH_99_CHNA_NEED_INCL_PLAN_X", "SH_99_CHNA_NEED_ADOPT_BUDGET_X", "SH_99_CHNA_NEED_PRIORITIZA_X", "SH_99_CHNA_NEED_PRIORITIZA_SVC_X", "SH_99_CHNA_NEED_OTH_X", "SH_99_CHNA_NEED_ALL_ADDRESSED_X", "SH_99_FAP_USE_FPG_FREE_CARE_PCT", "SH_99_FAP_USE_FPG_FREE_CARE_X", "SH_99_FAP_USE_FPG_DCNT_CARE_PCT", "SH_99_FAP_USE_FPG_DCNT_CARE_X", "SH_99_FAP_FACTOR_INCOME_X", "SH_99_FAP_FACTOR_ASSET_X", "SH_99_FAP_FACTOR_INDIGENCY_X", "SH_99_FAP_FACTOR_INSURANCE_X", "SH_99_FAP_FACTOR_UNINSURED_X", "SH_99_FAP_FACTOR_MEDICAID_X", "SH_99_FAP_FACTOR_STATE_REG_X", "SH_99_FAP_FACTOR_RESIDENCY_X", "SH_99_FAP_FACTOR_OTH_X", "SH_99_FAP_PROV_PUBLICITY_X", "SH_99_FAP_PROV_WEBSITE_X", "SH_99_FAP_PROV_ATTACH_INVOICE_X", "SH_99_FAP_PROV_EMERGENCY_ROOM_X", "SH_99_FAP_PROV_ADM_OFFICE_X", "SH_99_FAP_PROV_ADM_X", "SH_99_FAP_PROV_REQUEST_X", "SH_99_FAP_PROV_OTH_X", "SH_99_BILL_PERMIT_LAWSUIT_X", "SH_99_BILL_PERMIT_LIEN_X", "SH_99_BILL_PERMIT_BODY_ATTACH_X", "SH_99_FAP_AVBL_NOTICE_DISPLAY_X", "SH_99_BILL_3RD_PARTY_LAWSUIT_X", "SH_99_BILL_3RD_PARTY_LIEN_X", "SH_99_BILL_3RD_PARTY_BODY_X", "SH_99_BILL_NOTIFIED_ADM_X", "SH_99_BILL_NOTIFIED_DISCHARGE_X", "SH_99_BILL_NOTIFIED_PATIENT_X", "SH_99_BILL_NOTIFIED_DOC_X", "SH_99_BILL_NOTIFIED_OTH_ACT_X", "SH_99_CHARGE_LOW_RATE_X", "SH_99_CHARGE_AVE_RATE_X", "SH_99_CHARGE_MT_MEDICARE_RATE_X", "SH_99_CHARGE_OTH_METHOD_X"]

SI_P01_T00_GRANTS_INFO_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SI_01_GRANT_RECORD_MAINT_X"]

SI_P02_T00_GRANTS_US_ORGS_GOVTS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SI_02_GRANT_US_ORG_501C3_TOT", "SI_02_GRANT_US_ORG_OTH_TOT"]

SI_P99_T00_GRANTS_US_ORGS_GOVTS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SI_99_GRANT_US_ORG_LT_5K_X"]

SJ_P01_T00_COMPENSATION_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SJ_01_COMP_PROV_CLUB_DUE_FEE_X", "SJ_01_COMP_PROV_DISCRETIONARY_X", "SJ_01_COMP_PROV_FIRST_CLASS_X", "SJ_01_COMP_PROV_HOUSING_ALLOW_X", "SJ_01_COMP_PROV_INDEMNIFY_X", "SJ_01_COMP_PROV_PERSONAL_SVC_X", "SJ_01_COMP_PROV_TRAVEL_X", "SJ_01_COMP_PROV_USE_RESIDENCE_X", "SJ_01_COMP_WRITTEN_POLICY_X", "SJ_01_COMP_SUBSTANTIATION_REQ_X", "SJ_01_COMP_METHOD_BOARD_APPROV_X", "SJ_01_COMP_METHOD_COMMITTEE_X", "SJ_01_COMP_METHOD_CONSULTANT_X", "SJ_01_COMP_METHOD_EMPL_KONTR_X", "SJ_01_COMP_METHOD_OTH_ORG_990_X", "SJ_01_COMP_METHOD_SURVEY_X", "SJ_01_COMP_PAY_SEVERANCE_X", "SJ_01_COMP_PAY_RETIREMENT_PLAN_X", "SJ_01_COMP_PAY_EQUITY_BASED_X", "SJ_01_COMP_CONTINGENT_REV_ORG_X", "SJ_01_COMP_CONTINGENT_REV_RLTD_X", "SJ_01_COMP_CONTINGENT_NET_ORG_X", "SJ_01_COMP_CONTINGENT_NET_RLTD_X", "SJ_01_COMP_NON_FIXED_PAYMENT_X", "SJ_01_COMP_KONTR_EXCEPTION_X", "SJ_01_COMP_REBUTTABLE_PROC_X"]

SL_P01_T00_EXCESS_BENEFIT_TRANSAC_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SL_01_TRANSAC_AMT_TAX_IMPOSED", "SL_01_TRANSAC_AMT_TAX_REIMBURSED"]

SL_P02_T00_LOANS_INTERESTED_PERS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SL_02_LOAN_BALANCE_DUE_TOT"]

SM_P01_T00_NONCASH_CONTRIBUTIONS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SM_01_ART_WORK_NONCSH_X", "SM_01_ART_WORK_NUM_CONTR", "SM_01_ART_WORK_NONCSH_CONTR", "SM_01_ART_WORK_METHOD_REV", "SM_01_ART_HIST_TREASURE_NONCSH_X", "SM_01_ART_HIST_NUM_CONTR", "SM_01_ART_HIST_NONCSH_CONTR", "SM_01_ART_HIST_METHOD_REV", "SM_01_ART_FRACT_INT_NONCSH_X", "SM_01_ART_FRACT_INT_NUM_CONTR", "SM_01_ART_FRACT_INT_NONCSH_CONTR", "SM_01_ART_FRACT_INT_METHOD_REV", "SM_01_BOOK_PUBLICA_NONCSH_X", "SM_01_BOOK_PUBLICA_NONCSH_CONTR", "SM_01_BOOK_PUBLICA_METHOD_REV", "SM_01_CLOTHING_NONCSH_X", "SM_01_CLOTHING_NONCSH_CONTR", "SM_01_CLOTHING_METHOD_REV", "SM_01_CAR_BEHICLE_NONCSH_X", "SM_01_CAR_BEHICLE_NUM_CONTR", "SM_01_CAR_BEHICLE_NONCSH_CONTR", "SM_01_CAR_BEHICLE_METHOD_REV", "SM_01_BOAT_PLANE_NONCSH_X", "SM_01_BOAT_PLANE_NUM_CONTR", "SM_01_BOAT_PLANE_NONCSH_CONTR", "SM_01_BOAT_PLANE_METHOD_REV", "SM_01_IP_NONCSH_X", "SM_01_IP_NUM_CONTR", "SM_01_IP_NONCSH_CONTR", "SM_01_IP_METHOD_REV", "SM_01_SEC_PUB_TRADE_NONCSH_X", "SM_01_SEC_PUB_TRADE_NUM_CONTR", "SM_01_SEC_PUB_TRADE_NONCSH_CONTR", "SM_01_SEC_PUB_TRADE_METHOD_REV", "SM_01_SEC_STCK_NONCSH_X", "SM_01_SEC_STCK_NUM_CONTR", "SM_01_SEC_STCK_NONCSH_CONTR", "SM_01_SEC_STCK_METHOD_REV", "SM_01_SEC_PTR_NONCSH_X", "SM_01_SEC_PTR_NUM_CONTR", "SM_01_SEC_PTR_NONCSH_CONTR", "SM_01_SEC_PTR_METHOD_REV", "SM_01_SEC_MISC_NONCSH_X", "SM_01_SEC_MISC_NUM_CONTR", "SM_01_SEC_MISC_NONCSH_CONTR", "SM_01_SEC_MISC_METHOD_REV", "SM_01_QUAL_HIST_NONCSH_X", "SM_01_QUAL_HIST_NUM_CONTR", "SM_01_QUAL_HIST_NONCSH_CONTR", "SM_01_QUAL_HIST_METHOD_REV", "SM_01_QUAL_OTH_NONCSH_X", "SM_01_QUAL_OTH_NUM_CONTR", "SM_01_QUAL_OTH_NONCSH_CONTR", "SM_01_QUAL_OTH_METHOD_REV", "SM_01_RE_RESID_NONCSH_X", "SM_01_RE_RESID_NUM_CONTR", "SM_01_RE_RESID_NONCSH_CONTR", "SM_01_RE_RESID_METHOD_REV", "SM_01_RE_COMMERCIAL_NONCSH_X", "SM_01_RE_COMMERCIAL_NUM_CONTR", "SM_01_RE_COMMERCIAL_NONCSH_CONTR", "SM_01_RE_COMMERCIAL_METHOD_REV", "SM_01_RE_OTH_NONCSH_X", "SM_01_RE_OTH_NUM_CONTR", "SM_01_RE_OTH_NONCSH_CONTR", "SM_01_RE_OTH_METHOD_REV", "SM_01_COLLECTIBLES_NONCSH_X", "SM_01_COLLECTIBLES_NUM_CONTR", "SM_01_COLLECTIBLES_NONCSH_CONTR", "SM_01_COLLECTIBLES_METHOD_REV", "SM_01_FOOD_INV_NONCSH_X", "SM_01_FOOD_INV_NUM_CONTR", "SM_01_FOOD_INV_NONCSH_CONTR", "SM_01_FOOD_INV_METHOD_REV", "SM_01_DRUG_MEDICAL_NONCSH_X", "SM_01_DRUG_MEDICAL_NUM_CONTR", "SM_01_DRUG_MEDICAL_NONCSH_CONTR", "SM_01_DRUG_MEDICAL_METHOD_REV", "SM_01_TAXIDERMY_NONCSH_X", "SM_01_TAXIDERMY_NUM_CONTR", "SM_01_TAXIDERMY_NONCSH_CONTR", "SM_01_TAXIDERMY_METHOD_REV", "SM_01_HIST_ARTIFACT_NONCSH_X", "SM_01_HIST_ARTIFACT_NUM_CONTR", "SM_01_HIST_ARTIFACT_NONCSH_CONTR", "SM_01_HIST_ARTIFACT_METHOD_REV", "SM_01_SPECIMENS_NONCSH_X", "SM_01_SPECIMENS_NUM_CONTR", "SM_01_SPECIMENS_NONCSH_CONTR", "SM_01_SPECIMENS_METHOD_REV", "SM_01_ARCHAE_NONCSH_X", "SM_01_ARCHAE_NUM_CONTR", "SM_01_ARCHAE_NONCSH_CONTR", "SM_01_ARCHAE_METHOD_REV", "SM_01_NUM_FORM_8283_REC", "SM_01_PROP_HELD_X", "SM_01_REVIEW_PROCESS_UNUSUAL_X", "SM_01_THIRD_PARTIES_USED_X"]

SN_P01_T00_LIQUIDATION_TERMINATION_DISSOLUTION_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SN_01_ORG_DTK_DIR_SUCCESSOR_X", "SN_01_ORG_DTK_EMPL_SUCCESSOR_X", "SN_01_ORG_DTK_OWN_SUCCESSOR_X", "SN_01_ORG_DTK_REC_COMP_X", "SN_01_ORG_ASSET_DIST_GVRN_X", "SN_01_ORG_REQ_NOTIFY_ATTORNEY_X", "SN_01_ORG_ATTORNEY_NOTIFIED_X", "SN_01_ORG_LIAB_PAID_X", "SN_01_ORG_BOND_OUTSTANDING_X", "SN_01_ORG_BOND_LIAB_DISCHARGED_X"]

SN_P02_T00_DISPOSITION_OF_ASSETS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SN_02_ORG_DTK_DIR_SUCCESSOR_X", "SN_02_ORG_DTK_EMPL_SUCCESSOR_X", "SN_02_ORG_DTK_OWN_SUCCESSOR_X", "SN_02_ORG_DTK_REC_COMP_X"]

SN_P99_T00_LIQUIDATION_TERMINATION_DISSOLUTION_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SN_99_ORG_DETERMIN_LETTER_X", "SN_99_ORG_DATE_LETTER"]

SR_P05_T00_TRANSACTIONS_RLTD_ORGS_2022_schema = ["OBJECTID", "URL", "RETURN_VERSION", "ORG_EIN", "ORG_NAME_L1", "ORG_NAME_L2", "RETURN_TYPE", "TAX_YEAR", "SR_05_TRANSAC_RCPT_INT_X", "SR_05_TRANSAC_GIFT_GRANT_TO_X", "SR_05_TRANSAC_GIFT_GRANT_FROM_X", "SR_05_TRANSAC_LOAN_TO_X", "SR_05_TRANSAC_LOAN_BY_X", "SR_05_TRANSAC_DIVIDEND_X", "SR_05_TRANSAC_SALE_ASSET_X", "SR_05_TRANSAC_PURCHASE_ASSET_X", "SR_05_TRANSAC_EXCHANGE_ASSET_X", "SR_05_TRANSAC_LEASE_TO_X", "SR_05_TRANSAC_LEASE_FROM_X", "SR_05_TRANSAC_PERFORM_SVC_FOR_X", "SR_05_TRANSAC_PERFORM_SVC_BY_X", "SR_05_TRANSAC_SHARE_FACILITIES_X", "SR_05_TRANSAC_SHARE_PAID_EMPL_X", "SR_05_TRANSAC_REIMBURSEMENT_TO_X", "SR_05_TRANSAC_REIMBURSEMENT_BY_X", "SR_05_TRANSAC_OTH_CASH_TO_X", "SR_05_TRANSAC_OTH_CASH_FROM_X"]

unemployment_usda_schema = [
    "FIPS_Code", "State", "Area_name", "Rural_urban_continuum_code_2013", "Urban_influence_code_2013", "City/Suburb/Town/Rural", "Metro_2013", 
    "Civilian_labor_force_2000", "Employed_2000", "Unemployed_2000", "Unemployment_rate_2000", "Civilian_labor_force_2001", "Employed_2001", "Unemployed_2001", "Unemployment_rate_2001", "Civilian_labor_force_2002", "Employed_2002", "Unemployed_2002", "Unemployment_rate_2002", "Civilian_labor_force_2003", "Employed_2003", "Unemployed_2003", "Unemployment_rate_2003", "Civilian_labor_force_2004", "Employed_2004", "Unemployed_2004", "Unemployment_rate_2004", "Civilian_labor_force_2005", "Employed_2005", "Unemployed_2005", "Unemployment_rate_2005", "Civilian_labor_force_2006", "Employed_2006", "Unemployed_2006", "Unemployment_rate_2006", "Civilian_labor_force_2007", "Employed_2007", "Unemployed_2007", "Unemployment_rate_2007", "Civilian_labor_force_2008", "Employed_2008", "Unemployed_2008", "Unemployment_rate_2008", "Civilian_labor_force_2009", "Employed_2009", "Unemployed_2009", "Unemployment_rate_2009", "Civilian_labor_force_2010", "Employed_2010", "Unemployed_2010", "Unemployment_rate_2010", "Civilian_labor_force_2011", "Employed_2011", "Unemployed_2011", "Unemployment_rate_2011", "Civilian_labor_force_2012", "Employed_2012", "Unemployed_2012", "Unemployment_rate_2012", "Civilian_labor_force_2013", "Employed_2013", "Unemployed_2013", "Unemployment_rate_2013", "Civilian_labor_force_2014", "Employed_2014", "Unemployed_2014", "Unemployment_rate_2014", "Civilian_labor_force_2015", "Employed_2015", "Unemployed_2015", "Unemployment_rate_2015", "Civilian_labor_force_2016", "Employed_2016", "Unemployed_2016", "Unemployment_rate_2016", "Civilian_labor_force_2017", "Employed_2017", "Unemployed_2017", "Unemployment_rate_2017", "Civilian_labor_force_2018", "Employed_2018", "Unemployed_2018", "Unemployment_rate_2018", "Civilian_labor_force_2019", "Employed_2019", "Unemployed_2019", "Unemployment_rate_2019", "Civilian_labor_force_2020", "Employed_2020", "Unemployed_2020", "Unemployment_rate_2020", "Median_Household_Income_2019", "Med_HH_Income_Percent_of_State_Total_2019"
]


education_usda_schema = ["fips_code", "state", "area_name", "rural_urban_code_2003", "urban_influence_code_2003", "rural_urban_code_2013", "urban_influence_code_2013", "area_type_2013", "lt_hs_1970", "hs_1970", "sc_1970", "bc_1970", "pct_lt_hs_1970", "pct_hs_1970", "pct_sc_1970", "pct_bc_1970", "lt_hs_1980", "hs_1980", "sc_1980", "bc_1980", "pct_lt_hs_1980", "pct_hs_1980", "pct_sc_1980", "pct_bc_1980", "lt_hs_1990", "hs_1990", "sc_1990", "bc_1990", "pct_lt_hs_1990", "pct_hs_1990", "pct_sc_1990", "pct_bc_1990", "lt_hs_2000", "hs_2000", "sc_2000", "bc_2000", "pct_lt_hs_2000", "pct_hs_2000", "pct_sc_2000", "pct_bc_2000", "lt_hs_2015_19", "hs_2015_19", "sc_2015_19", "bc_2015_19", "pct_lt_hs_2015_19", "pct_hs_2015_19", "pct_sc_2015_19", "pct_bc_2015_19"
]

nccs_crosswalk_demographic_schema = [
    "geoid_2010", "year", "total_population", 
    "white_perc", "black_perc", "asian_perc", "hawaiian_perc", 
    "american_alaskan_perc", "two_or_more_perc", "other_perc", "hispanic_perc"
]

nccs_crosswalk_economic_schema = [
    "geoid_2010", "year", "med_family_income_adj", 
    "med_gross_rent_adj", "med_household_income_adj", 
    "poverty_perc", "unemployment"
]

nccs_crosswalk_education_social_schema = [
    "geoid_2010", "year", "bachelors_perc", "turnover_perc"
]

nccs_crosswalk_housing_schema = [
    "geoid_2010", "year", "housing_units", "occupied", "vacant", 
    "renter_occ", "median_value_adj"
]

nccs_geographic_metadata_schema = [
    "geoid_2010", "State FIPS Code"
]

states_fips_schema = [
    "State", "Postal", "FIPS"
]

firearm_laws_schema = [
    "Law ID", "State", "State Postal Abbreviation", "FIPS Code", 
    "Law Class (num)", "Law Class", "Handguns or Long Guns"
]

exempt_orgs_schema = [
    "ein", "legal_name", "state", "country", "deductibility_status"
]

demographics_data_schema = [
    "demographic_subgroup", "national_2017_formal_volunteering_rate", "national_2019_formal_volunteering_rate", 
    "national_2021_formal_volunteering_rate", "national_2023_formal_volunteering_rate", 
    "national_2017_organizational_membership_rate", "national_2019_organizational_membership_rate", 
    "national_2021_organizational_membership_rate", "national_2023_organizational_membership_rate", 
    "national_2017_charitable_giving_rate", "national_2019_charitable_giving_rate",
    "national_2021_charitable_giving_rate", "national_2023_charitable_giving_rate",
    "national_2021_employer_promotes_volunteering_rate", "national_2023_employer_promotes_volunteering_rate",
]

national_data_schema = [
    "year", "national_formal_volunteering_rate", "national_organizational_membership_rate", 
    "national_charitable_giving_rate", "national_employer_promotes_volunteering_rate"
]


mental_health_hrsa_schema = [
    "Shortage Area Name", "Shortage Severity Score", "State Abbreviation", "Shortage Status", 
    "County", "State Name"
]

mental_health_irs_990_schema = [
    "EIN", "Organization Name", "City", "State", "ZIP Code", "Gross Receipts"
]

mental_health_cdc_schema = [
    "State", "Avg Mentally Unhealthy Days"
]

city_info_schema = [
    "F9_00_ORG_ADDR_CITY", "F9_00_ORG_ADDR_STATE", "Total_Rev_Prog_Desc",
    "Percent_Unhealthy_Days"
]

geocoded_aqi_dataset_schema = [
    "CITY", "CBSA", "Percent_Unhealthy_Days", "Latitude", "Longitude"
]

charities_2020_schema = [
    "Employee Identification Number", "Other Employee Benefit",
    "Other Salaries and Wages - Total", "Total Revenue",
    "Direct Expenses", "Total Contributions", "Number of Employees",
    "State"     
]

nonprofit_employment_2020_schema = ["Geographic Title", "NAICS", "Industry Title", "Average Establishments",
                                    "Annual Average Employment", "Total Annual Wages (in thousands)", "Annual Wages Per Employee",
                                    "Average Weekly Wage", "Percent Employment 501(c)(3)", "Wage Ratio"
]

ngo_categorization_schema = ['EIN','Name', 'FullAddr', 'City', 'State', 'Zip','County', 'NTEE_CODE', 'Category', 'is_category_LLM_generated']

NCCS_Address_Metadata_cbsa_schema = [
    'CENSUS_CBSA_FIPS', 'CENSUS_CBSA_NAME', 'CENSUS_CBSA_LEGAL_NAME', 'CENSUS_CBSA_LEGAL_CODE', 'CENSUS_CSA_FIPS'
]
NCCS_Address_Metadata_census_blocks_schema = [
    'CENSUS_BLOCK_FIPS', 'CENSUS_BLOCK_CODE', 'CENSUS_BLOCK_NAME',
        'CENSUS_BLOCK_HOUSING_UNIT_COUNT', 'CENSUS_BLOCK_POPULATION_COUNT',
        'CENSUS_FUNCTIONAL_STATUS', 'CENSUS_URBAN_AREA_CODE', 'CENSUS_TRACT_FIPS'
]
NCCS_Address_Metadata_counties_schema = [
    "CENSUS_COUNTY_FIPS", "CENSUS_COUNTY_NAME", "CENSUS_STATE_FIPS", "CENSUS_CBSA_FIPS"
]
NCCS_Address_Metadata_org_loc_schema = [
    'EIN2', 'ORG_FISCAL_YEAR', 'ORG_ADDR_FULL', 'ORG_ADDR_MATCH', 'LONGITUDE', 'LATITUDE',
    'ADDR_TYPE', 'GEOCODER_SCORE', 'GEOCODER_MATCH', 'CENSUS_BLOCK_FIPS'
]

NCCS_Address_Metadata_states_schema = [
    'CENSUS_STATE_FIPS', 'CENSUS_STATE_NAME', 'CENSUS_STATE_ABBR'
]
NCCS_Address_Metadata_tracts_schema = [
    'CENSUS_TRACT_FIPS', 'CENSUS_TRACT_CODE', 'CENSUS_TRACT_NAME', 'CENSUS_COUNTY_FIPS', 'CENSUS_STATE_FIPS'
]
NCCS_Address_Metadata_urban_areas_schema = [ 
    'CENSUS_URBAN_AREA_CODE', 'CENSUS_URBAN_AREA'
]

nccs_bmf_schema = [
    'ein_pk', 'ein_fk', 'org_name', 'org_name_secondary',
    'ntee_irs_code', 'ntee_nccs_code', 'ntee_modern_code',
    'nccs_level_1_category', 'nccs_level_2_category', 'nccs_level_3_category',
    'total_revenue', 'total_income', 'total_assets',
    'city', 'state', 'zipcode', 'street_address',
    'census_cbsa_fips_code', 'census_cbsa_name', 'census_block_fips_code',
    'census_urban_area', 'census_state_abbreviation', 'census_county_name',
    'address_full', 'address_matched', 'latitude', 'longitude',
    'geocoder_confidence_score', 'geocoder_match_type',
    'bmf_subsection_code', 'bmf_status_code', 'bmf_private_foundation_filing_required',
    'bmf_organization_code', 'bmf_income_code', 'bmf_group_exemption_number',
    'bmf_foundation_code', 'bmf_filing_requirement_code', 'bmf_deductibility_code',
    'bmf_classification_code', 'bmf_asset_code', 'bmf_affiliation_code',
    'organization_ruling_date', 'organization_fiscal_year', 'organization_ruling_year',
    'organization_first_year', 'organization_last_year', 'organization_year_count',
    'organization_personal_ico', 'organization_fiscal_period'
]

# with open('nccs/data/nonprofit-survey-spring-2021-puf.csv') as fp:

#     csv_data = csv.DictReader(fp, delimiter=',')
#     for row in csv_data:
#         survey_public_2021_schema = [key for key, val in row.items()]
#         break

def insert_into_table(csv_file, table_name, schema, mydb):

    insert_head = 'INSERT INTO {}('.format(table_name)
    for column in schema:
        insert_head += '`{}`'.format(column)
        insert_head += ','
    insert_head = insert_head[:-1] + ')'

    with open(csv_file, encoding='utf-8') as fp:

        csv_data = csv.DictReader(fp, delimiter=',')

        lens = []
        for i, row in enumerate(tqdm(csv_data)):
            insert_tail = 'VALUES('
            for column in schema:
                if column == 'REVENUE_AMT' and row[column] == 'NA':
                    insert_tail += '{},'.format('NULL')
                elif column == 'LOW_PR_TERR_BROADBAND' and row[column] == 'NULL':
                    insert_tail += '{},'.format('NULL')
                elif table_name == 'Broadband_Speeds_Per_County' and column == 'POSTAL_CODE':
                    insert_tail += '"{}",'.format(row["\ufeff" + column])
                elif row[column] == '':
                    insert_tail += '{},'.format('NULL')
                elif row[column] == ' ':
                    insert_tail += '{},'.format('NULL')
                else:
                    insert_tail += '"{}",'.format(row[column])

                if column == 'nteecc':
                    if row[column].startswith('B'):
                        lens.append(row[column])

            insert_tail = insert_tail[:-1] + ');'
            cursor = mydb.cursor()
            cursor.execute(insert_head + insert_tail)
            
                
        # import numpy as np
        # print(np.unique(lens, return_counts=True))
        # print(insert_tail)
        # if i > 100:
        #     exit(0)
    mydb.commit()


if __name__ == '__main__':

    tablename = sys.argv[1]
    password = sys.argv[2]
    mydb = mysql.connector.connect(host='localhost',
                           user='norpuser',
                           password=password,
                           db='norp',
                           auth_plugin='mysql_native_password')
    # mydb = 0
    base_path = "/home/norp-services/"
    
    if tablename == 'ngo':
        insert_into_table(f"{base_path}nccs/data/ngo_dataset.csv",
                        'ngo',
                        ngo_schema,
                        mydb)
        
    elif tablename == 'pub78':
        insert_into_table(f"{base_path}nccs/data/ExemptOrganizations.csv",
                        'pub78',
                        pub78_schema,
                        mydb)
        
    elif tablename == 'Georgia_Crime':
        insert_into_table(f"{base_path}nccs/data/Georgia_Crime.csv",
                          "Georgia_Crime",
                          georgia_crime_schema,
                          mydb)
        
    elif tablename == 'Uninsured_People':
        insert_into_table(f"{base_path}nccs/data/Uninsured_People.csv",
                          "Uninsured_People",
                          uninsured_people_schema,
                          mydb)
        
    elif tablename == 'Healthcare_Diversity':
        insert_into_table(f"{base_path}nccs/data/Health_Care_Diversity.csv",
                          "Health_Care_Diversity",
                          health_care_diversity_schema,
                          mydb)
        
    elif tablename == 'poverty_diversity':
        insert_into_table(f"{base_path}nccs/data/Poverty_Diversity.csv",
                          'Poverty_Diversity',
                          poverty_diversity_schema,
                          mydb)
        
    elif tablename == 'education':
        insert_into_table(f"{base_path}nccs/data/Education.csv",
                          'Education',
                          education_schema,
                          mydb)
        
    elif tablename == 'Household_Income':
        insert_into_table(f"{base_path}nccs/data/Household_Income.csv",
                          'Household_Income',
                          housing_income_schema,
                          mydb)
        
    elif tablename == 'ga_healthcare':
        insert_into_table(f"{base_path}nccs/data/access_ga_0.csv",
                          'ga_healthcare',
                          ga_healthcare_schema,
                          mydb)
        
    elif tablename == 'atlanta_crime':
        insert_into_table(f"{base_path}nccs/data/atlanta_crime.csv", 'atlanta_crime', atlanta_crime, mydb)

    elif tablename == 'bmf':
        insert_into_table(f"{base_path}nccs/data/BusinessMasterFile.csv",
                          'bmf',
                          bmf_schema,
                          mydb)

    elif tablename == 'merged_nccs':
        insert_into_table(f"{base_path}nccs/data/merged_2015_2017.csv",
                        'merged_nccs',
                        merged_nccs_schema,
                        mydb)

    elif tablename == 'census':
        insert_into_table(f"{base_path}nccs/data/census_fields_by_zip_code.csv",
                        'census_fields_by_zip_code',
                        census_fields_by_zip_code_schema,
                        mydb)

    # elif tablename == 'survey_public_2021':
    #     insert_into_table('nccs/data/nonprofit-survey-spring-2021-puf.csv',
    #                     'survey_public_2021',
    #                     survey_public_2021_schema,
    #                     mydb)

    elif tablename == 'Broadband_Speeds_Per_County':
        insert_into_table(f"{base_path}nccs/data/Broadband_Speeds_Per_County.csv",
                          'Broadband_Speeds_Per_County',
                          Broadband_Speeds_Per_County_schema,
                        mydb)

    elif tablename == "Broadband_Usage_Per_County":
        insert_into_table(f"{base_path}nccs/data/Broadband_Usage_Per_County.csv",
                          'Broadband_Usage_Per_County',
                          Broadband_Usage_Per_County_schema,
                        mydb)
    
    elif tablename == "F1023ez_Approvals":
        insert_into_table(f"{base_path}nccs/data/Combined_1023ez.csv",
                          'F1023ez_Approvals',
                          F1023ez_Approvals_schema,
                        mydb)

    elif tablename == "irs_990_rev_trends":
        insert_into_table(f"{base_path}nccs/data/irs_990_rev_trends.csv",
                          'irs_990_rev_trends',
                          irs_990_rev_trends_schema,
                          mydb)

    elif tablename == "economic_data":
        insert_into_table(f"{base_path}nccs/data/economic_data.csv",
                          'economic_data',
                          economic_data_schema,
                          mydb)
        
    elif tablename == "health_inequity":
        insert_into_table(f"{base_path}nccs/data/health_inequity.csv",
                          'health_inequity',
                          health_inequity_schema,
                          mydb)

        
    if tablename == "housing_prices":
        insert_into_table(f"{base_path}nccs/data/clean_house_prices_us.csv",
                          'housing_prices',
                          housing_prices_schema,
                          mydb)


    if tablename == "unemployment_rates":
        insert_into_table(f"{base_path}nccs/data/clean_unemployment_rates.csv",
                          'unemployment_rates',
                          unemployment_rates_schema,
                          mydb)
    if tablename == "homelessness_rates":
        insert_into_table(f"{base_path}nccs/data/homelessness_rates_cleaned.csv",
                          'homelessness_rates',
                          homelessness_rates_schema,
                          mydb)
    if tablename == "cost_of_living_us":
        insert_into_table(f"{base_path}nccs/data/cost_of_living_us_cleaned.csv",
                          'cost_of_living_us',
                          cost_of_living_us_schema,
                          mydb)

    elif tablename == "homelessness_age":
        insert_into_table(f"{base_path}nccs/data/experiencing_homelessness_age_demographics.csv",
                          'experiencing_homelessness_age_demographics',
                          homelessness_age_schema,
                          mydb)
    elif tablename == "homelessness_gender":
        insert_into_table(f"{base_path}nccs/data/experiencing_homelessness_gender_demographics.csv",
                          'experiencing_homelessness_gender_demographics',
                          homelessness_gender_schema,
                            mydb)
    elif tablename == "homelessness_ethnicity":
        insert_into_table(f"{base_path}nccs/data/experiencing_homelessness_ethnicity_demographics.csv",
                          'experiencing_homelessness_ethnicity_demographics',
                          homelessness_ethnicity_schema,
                            mydb)
    elif tablename == "homelessness_race":
        insert_into_table(f"{base_path}nccs/data/experiencing_homelessness_race_demographics.csv",
                          'experiencing_homelessness_race_demographics',
                          homelessness_race_schema,
                            mydb)
    elif tablename == "CrimeData":  
        insert_into_table(f"{base_path}nccs/data/Crime_Data_from_2020_to_Present.csv",
                          'CrimeData',
                          crimedata_schema,
                            mydb)    
    
    if tablename == "atlanta_crime_data":
        insert_into_table(f"{base_path}nccs/data/Atlanta_Crime_Data.csv",
                          'atlanta_crime_data',
                          atlanta_crime_detailed_schema,
                          mydb)
    
    if tablename == "nyc_crime_data":
        insert_into_table(f"{base_path}nccs/data/NYC_Crime_Data.csv",
                          'nyc_crime_data',
                          nyc_crime_schema,
                          mydb)
    
    if tablename == "la_crime_data":
        insert_into_table(f"{base_path}nccs/data/LA_Crime_Data.csv",
                          'la_crime_data',
                          la_crime_schema,
                          mydb)
    if tablename == "unemployment_rates_by_state":
        insert_into_table(f"{base_path}nccs/data/cleaned_unemployment_rates.csv",
                          'unemployment_rates_by_state',
                          unemployment_rates_by_state_schema,
                          mydb)
    
    if tablename == "philly_crime_data":
        insert_into_table(f"{base_path}nccs/data/Philly_Crime_Data.csv",
                          'philly_crime_data',
                          philly_crime_schema,
                          mydb)

    if tablename == "housing_value":
        insert_into_table(f"{base_path}nccs/data/housing_value.csv",
                          'housing_value',
                          housing_value_schema,
                          mydb)
        
    if tablename == "housing_rent":
        insert_into_table(f"{base_path}nccs/data/housing_rent.csv",
                          'housing_rent',
                          housing_rent_schema,
                          mydb)
        
    if tablename == "housing_year_built":
        insert_into_table(f"{base_path}nccs/data/housing_year_built.csv",
                          'housing_year_built',
                          housing_year_built_schema,
                          mydb)
        
    if tablename == "housing_heating_fuel":
        insert_into_table(f"{base_path}nccs/data/housing_heating_fuel.csv",
                          'housing_heating_fuel',
                          housing_heating_fuel_schema,
                          mydb)
        
    if tablename == "housing_mortgage":
        insert_into_table(f"{base_path}nccs/data/housing_mortgage.csv",
                          'housing_mortgage',
                          housing_mortgage_schema,
                          mydb)
        
    if tablename == "housing_gross_rent_percent":
        insert_into_table(f"{base_path}nccs/data/housing_gross_rent_percent.csv",
                          'housing_gross_rent_percent',
                          housing_gross_rent_percent_schema,
                          mydb)
        
    if tablename == "household_income_percent_without_mortgage":
        insert_into_table(f"{base_path}nccs/data/household_income_percent_without_mortgage.csv",
                          'household_income_percent_without_mortgage',
                          household_income_percent_without_mortgage_schema,
                          mydb)
        
    if tablename == "household_income_percent_with_mortgage":
        insert_into_table(f"{base_path}nccs/data/household_income_percent_with_mortgage.csv",
                          'household_income_percent_with_mortgage',
                          household_income_percent_with_mortgage_schema,
                          mydb)
        
    if tablename == "social_base":
        insert_into_table(f"{base_path}nccs/data/social_base.csv",
                          'social_base',
                          social_base_schema,
                          mydb)
        
    if tablename == "social_citizenship_status":
        insert_into_table(f"{base_path}nccs/data/social_citizenship_status.csv",
                          'social_citizenship_status',
                          social_citizenship_status_schema,
                          mydb)
        
    if tablename == "social_computer_internet_usage":
        insert_into_table(f"{base_path}nccs/data/social_computer_internet_usage.csv",
                          'social_computer_internet_usage',
                          social_computer_internet_usage_schema,
                          mydb)
        
    elif tablename == "social_education_attainment":
        insert_into_table(f"{base_path}nccs/data/social_education_attainment.csv",
                          'social_education_attainment',
                          social_education_attainment_schema,
                          mydb)
        
    elif tablename == "social_schooling":
        insert_into_table(f"{base_path}nccs/data/social_schooling.csv",
                          'social_schooling',
                          social_schooling_schema,
                          mydb)

    elif tablename == "cleaned_census_data":
        insert_into_table(f"{base_path}nccs/data/cleaned_census.csv",
                          'census',
                          census_schema,
                          mydb)
        
    elif tablename == "cleaned_charity_data":
        insert_into_table(f"{base_path}nccs/data/cleaned_charity.csv",
                          'charity',
                          charity_schema,
                          mydb)
    
    elif tablename == "cleaned_pub78_data":
        insert_into_table(f"{base_path}nccs/data/cleaned_pub78dataset.csv",
                          'pub78dataset',
                          pub78dataset_schema,
                          mydb)
        
    elif tablename == "economic_commute_to_work":
        insert_into_table(f"{base_path}nccs/data/economic_commute_to_work.csv",
                          'economic_commute_to_work',
                          economic_commute_to_work_schema,
                          mydb)

    elif tablename == "economic_health_insurance":
        insert_into_table(f"{base_path}nccs/data/economic_health_insurance.csv",
                          'economic_health_insurance',
                          economic_health_insurance_schema,
                          mydb)

    elif tablename == "economic_income_and_benefits":
        insert_into_table(f"{base_path}nccs/data/economic_income_and_benefits.csv",
                          'economic_income_and_benefits',
                          economic_income_and_benefits_schema,
                          mydb)

    elif tablename == "demographic_basics":
        insert_into_table(f"{base_path}nccs/data/demographic_basics.csv",
                          'demographic_basics',
                          demographic_basics_schema,
                          mydb)

    elif tablename == "demographic_race":
        insert_into_table(f"{base_path}nccs/data/demographic_race.csv",
                          'demographic_race',
                          demographic_race_schema,
                          mydb)

    elif tablename == 'food_access':
        insert_into_table(f'{base_path}nccs/data/food_access_data.csv',
                          'food_access',
                          food_access_schema,
                          mydb)
    elif tablename == 'us_shootings':
        insert_into_table(f'{base_path}nccs/data/us-shootings_cleaned.csv',
                        'us_shootings',
                        us_shootings_schema,
                        mydb)
        
    elif tablename == 'lead_ami':
        insert_into_table(f'{base_path}nccs/data/lead_ami_data.csv',
                          'lead_ami',
                          lead_ami_schema,
                          mydb)
        
    elif tablename == 'lead_smi':
        insert_into_table(f'{base_path}nccs/data/lead_smi_data.csv',
                          'lead_smi',
                          lead_smi_schema,
                          mydb)
        
    elif tablename == 'lead_fpl':
        insert_into_table(f'{base_path}nccs/data/lead_fpl_data.csv',
                          'lead_fpl',
                          lead_fpl_schema,
                          mydb)

    elif tablename == 'disadvantaged_communities':
        insert_into_table(f'{base_path}nccs/data/dac_data.csv',
                          'disadvantaged_communities',
                          disadvantaged_communities_schema,
                          mydb)
    elif tablename == 'F9_P09_T00_EXPENSES_2022_Spring2025':
        insert_into_table(f'{base_path}nccs/data/cleaned_expenses_2022.csv',
                          'F9_P09_T00_EXPENSES_2022_Spring2025', 
                          F9_P09_T00_EXPENSES_2022_schema,
                          mydb)
    if tablename == 'cleaned_2020_Expenses_file':
        insert_into_table(f'{base_path}nccs/data/cleaned_2020_Expenses_file.csv',
                          'Expenses_2020', 
                          cleaned_2020_Expenses_file_schema,
                          mydb)
    if tablename == 'cleaned_2021_Expenses_file':
        insert_into_table(f'{base_path}nccs/data/cleaned_2021_Expenses_file.csv',
                          'Expenses_2021', 
                          cleaned_2021_Expenses_file_schema,
                          mydb)
    if tablename == 'cleaned_2020_SCHOOLS_file':
        insert_into_table(f'{base_path}nccs/data/cleaned_2020_SCHOOLS_file.csv',
                          'SE_P01_T00_SCHOOLS_2020', 
                          SE_P01_T00_SCHOOLS_2020_schema,
                          mydb)
    if tablename == 'cleaned_2021_SCHOOLS_file':
        insert_into_table(f'{base_path}nccs/data/cleaned_2021_SCHOOLS_file.csv',
                          'SE_P01_T00_SCHOOLS_2021', 
                          SE_P01_T00_SCHOOLS_2021_schema,
                          mydb)
        
    if tablename == 'res_electricity_rates':
        insert_into_table(f'{base_path}nccs/data/res_electricity_rates.csv',
                          'res_electricity_rates',
                          res_electricity_rates_schema,
                          mydb)
    
    if tablename == 'ga_county_to_zcta':
        insert_into_table(f'{base_path}nccs/data/ga_county_to_zcta.csv',
                          'ga_county_to_zcta',
                          ga_county_to_zcta_schema,
                          mydb)

    elif tablename == 'life_expectancy':
        insert_into_table(f'{base_path}nccs/data/Life_Expectancy_final.csv',
                        'life_expectancy',
                        life_expectancy_schema,
                        mydb)
        
    elif tablename == "air_pollution":
        insert_into_table(
            f"{base_path}nccs/data/air_pollution_data.csv",
            "air_pollution",
            air_pollution_schema,
            mydb
        )

    elif tablename == 'Asian_Nonprofit':
        insert_into_table(f'{base_path}nccs/data/asian_nonprofit_data.csv',
                        'Asian_Nonprofit',
                        asian_nonprofit_schema,
                        mydb)


    elif tablename == "population_demographics":
        insert_into_table(
            f"{base_path}nccs/data/crimedata-2018-cleaned.csv",
            "population_demographics",
            population_demographics_schema,
            mydb
        )
     
    elif tablename == "Candid_NPO_Stats":
        insert_into_table(
            f"{base_path}nccs/data/candid_npo_stats.csv",
            "Candid_NPO_Stats",
            candid_npo_stats_schema,
            mydb
        )
        
                
    elif tablename == 'population_density':        
        insert_into_table(f"{base_path}nccs/data/combined_pop_den_data.csv", 
                          'population_density',
                            pop_den_schema,
                            mydb)
    elif tablename == 'zipcode_and_coordinates':        
            insert_into_table(f"{base_path}nccs/data/zipcode_coord_data.csv", 
                            'zipcode_and_coordinates',
                                zip_and_coord_schema,
                                mydb)
    elif tablename == 'revocations':
        insert_into_table(f"{base_path}nccs/data/2023-10-REVOCATIONS-ORGS.csv",
                          'revocations',
                          revocations_schema,
                          mydb)

    elif tablename == 'census_migrations':
        insert_into_table(f"{base_path}nccs/data/State_to_State_Migration_Table_2023_T13_1.csv",
                        'census_migrations',
                        census_migration_schema,
                        mydb)
    elif tablename == 'unemployment_usda':
        insert_into_table(f'{base_path}nccs/data/unemployment_usda.csv', 
                          'unemployment_usda',
                          unemployment_usda_schema,
                          mydb)
    elif tablename == 'education_usda':
        insert_into_table(f'{base_path}nccs/data/education_usda_mysql.csv',
                          'education_usda',
                          education_usda_schema,
                          mydb)

    elif tablename == 'nccs_crosswalk_demographic':
        insert_into_table(f'{base_path}nccs/data/demographic_data.csv', 
                          'nccs_crosswalk_demographic',
                          nccs_crosswalk_demographic_schema,
                          mydb)
    elif tablename == 'nccs_crosswalk_economic':
        insert_into_table(f'{base_path}nccs/data/economic_data.csv', 
                          'nccs_crosswalk_economic',
                          nccs_crosswalk_economic_schema,
                          mydb)
    elif tablename == 'nccs_crosswalk_education_social':
        insert_into_table(f'{base_path}nccs/data/education_social_data.csv', 
                          'nccs_crosswalk_education_social',
                          nccs_crosswalk_education_social_schema,
                          mydb)
    elif tablename == 'nccs_crosswalk_housing':
        insert_into_table(f'{base_path}nccs/data/housing_data.csv', 
                          'nccs_crosswalk_housing',
                          nccs_crosswalk_housing_schema,
                          mydb)
    elif tablename == 'nccs_geographic_metadata':
        insert_into_table(f'{base_path}nccs/data/geographic_metadata.csv', 
                          'nccs_geographic_metadata',
                          nccs_geographic_metadata_schema,
                          mydb)
        
    elif tablename == 'firearm_laws':
        insert_into_table(f'{base_path}nccs/data/firearm_law_data.csv', 
                          'firearm_laws',
                          firearm_laws_schema,
                          mydb)
    elif tablename == 'states_fips':
        insert_into_table(f'{base_path}nccs/data/states_fips.csv', 
                          'states_fips',
                          states_fips_schema,
                          mydb)

    elif tablename == 'city_info':
        insert_into_table(f'{base_path}nccs/data/city_info.csv',
                          'city_info',
                          city_info_schema,
                          mydb)
    elif tablename == 'geocoded_aqi_dataset':
        insert_into_table(f'{base_path}nccs/data/geocoded_aqi_dataset.csv',
                          'geocoded_aqi_dataset',
                          geocoded_aqi_dataset_schema,
                          mydb)

    elif tablename == 'exempt_orgs':
        insert_into_table(f'{base_path}nccs/data/2023-10-CURRENT-EXEMPT-ORGS-DATABASE.csv', 
                          'exempt_orgs',
                          exempt_orgs_schema,
                          mydb)
    elif tablename == 'demographics_data':
        insert_into_table(f'{base_path}nccs/data/2017-2023_CEV_Findings__National_Rates_of_All_Measures_by_Demographics_from_the_Current_Population_Survey_Civic_Engagement_and_Volunteering_Supplement_20250219.csv', 
                          'demographics_data',
                          demographics_data_schema,
                          mydb)
    elif tablename == 'national_data':
        insert_into_table(f'{base_path}nccs/data/2017-2023_CEV_Findings__National_Rates_of_All_Measures_from_the_Current_Population_Survey_Civic_Engagement_and_Volunteering_Supplement_20250305.csv', 
                          'national_data',
                          national_data_schema,
                          mydb)      
    elif tablename == 'mental_health_hrsa':
        insert_into_table(f'{base_path}nccs/data/cleaned_hrsa_data.csv', 
                          'mental_health_hrsa',
                          mental_health_hrsa_schema,
                          mydb)
    elif tablename == 'mental_health_irs_990':
        insert_into_table(f'{base_path}nccs/data/cleaned_irs_990_data.csv', 
                          'mental_health_irs_990',
                          mental_health_irs_990_schema,
                          mydb)
    elif tablename == 'mental_health_cdc':
        insert_into_table(f'{base_path}nccs/data/cleaned_cdc_data.csv', 
                          'mental_health_cdc',
                          mental_health_cdc_schema,
                          mydb)   
    elif tablename == 'charities_2020':
        insert_into_table(f'{base_path}nccs/data/cleaned_charities_2020.csv', 
                          'charities_2020',
                          charities_2020_schema,
                          mydb)
    elif tablename == 'nonprofit_employment_2020':
        insert_into_table(f'{base_path}nccs/data/cleaned_nonprofit_employment_2020.csv', 
                          'nonprofit_employment_2020',
                          nonprofit_employment_2020_schema,
                          mydb)
    elif tablename == 'eitc_county':
        insert_into_table(f"{base_path}nccs/data/eitc_county.csv", "eitc_county", eitc_county_schema, mydb)
    elif tablename == 'eitc_metro':
        insert_into_table(f"{base_path}nccs/data/eitc_metro.csv", "eitc_metro", eitc_metro_schema, mydb)
    elif tablename == 'eitc_nonmetro':
        insert_into_table(f"{base_path}nccs/data/eitc_nonmetro.csv", "eitc_nonmetro", eitc_nonmetro_schema, mydb)
    elif tablename == 'eitc_state':
        insert_into_table(f"{base_path}nccs/data/eitc_state.csv", "eitc_state", eitc_state_schema, mydb)
    elif tablename == 'nccs_efilers':
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P00-T00-HEADER-2022.csv", "F9_P00_T00_HEADER_2022", F9_P00_T00_HEADER_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P01-T00-SUMMARY-2022.csv", "F9_P01_T00_SUMMARY_2022", F9_P01_T00_SUMMARY_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P01-T00-SUMMARY-EZ-2022.csv", "F9_P01_T00_SUMMARY_EZ_2022", F9_P01_T00_SUMMARY_EZ_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P02-T00-SIGNATURE-2022.csv", "F9_P02_T00_SIGNATURE_2022", F9_P02_T00_SIGNATURE_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P03-T00-MISSION-2022.csv", "F9_P03_T00_MISSION_2022", F9_P03_T00_MISSION_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P03-T00-PROGRAMS-2022.csv", "F9_P03_T00_PROGRAMS_2022", F9_P03_T00_PROGRAMS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P04-T00-REQUIRED-SCHEDULES-2022.csv", "F9_P04_T00_REQUIRED_SCHEDULES_2022", F9_P04_T00_REQUIRED_SCHEDULES_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P04-T00-REQUIRED-SCHEDULES-EZ-2022.csv", "F9_P04_T00_REQUIRED_SCHEDULES_EZ_2022", F9_P04_T00_REQUIRED_SCHEDULES_EZ_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P05-T00-OTHER-IRS-FILING-2022.csv", "F9_P05_T00_OTHER_IRS_FILING_2022", F9_P05_T00_OTHER_IRS_FILING_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P06-T00-GOVERNANCE-2022.csv", "F9_P06_T00_GOVERNANCE_2022", F9_P06_T00_GOVERNANCE_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P06-T00-GOVERNANCE-EZ-2022.csv", "F9_P06_T00_GOVERNANCE_EZ_2022", F9_P06_T00_GOVERNANCE_EZ_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P07-T00-DIR-TRUST-KEY-2022.csv", "F9_P07_T00_DIR_TRUST_KEY_2022", F9_P07_T00_DIR_TRUST_KEY_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P08-T00-REVENUE-2022.csv", "F9_P08_T00_REVENUE_2022", F9_P08_T00_REVENUE_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P09-T00-EXPENSES-2022.csv", "F9_P09_T00_EXPENSES_2022", F9_P09_T00_EXPENSES_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P10-T00-BALANCE-SHEET-2022.csv", "F9_P10_T00_BALANCE_SHEET_2022", F9_P10_T00_BALANCE_SHEET_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P11-T00-ASSETS-2022.csv", "F9_P11_T00_ASSETS_2022", F9_P11_T00_ASSETS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/F9-P12-T00-FINANCIAL-REPORTING-2022.csv", "F9_P12_T00_FINANCIAL_REPORTING_2022", F9_P12_T00_FINANCIAL_REPORTING_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SA-P00-T00-HEADER-2022.csv", "SA_P00_T00_HEADER_2022", SA_P00_T00_HEADER_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SA-P01-T00-PUBLIC-CHARITY-STATUS-2022.csv", "SA_P01_T00_PUBLIC_CHARITY_STATUS_2022", SA_P01_T00_PUBLIC_CHARITY_STATUS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SA-P02-T00-SUPPORT-SCHEDULE-170-2022.csv", "SA_P02_T00_SUPPORT_SCHEDULE_170_2022", SA_P02_T00_SUPPORT_SCHEDULE_170_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SA-P03-T00-SUPPORT-SCHEDULE-509-2022.csv", "SA_P03_T00_SUPPORT_SCHEDULE_509_2022", SA_P03_T00_SUPPORT_SCHEDULE_509_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SA-P04-T00-SUPPORT-ORGS-2022.csv", "SA_P04_T00_SUPPORT_ORGS_2022", SA_P04_T00_SUPPORT_ORGS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SA-P05-T00-SUPPORT-ORGS-2022.csv", "SA_P05_T00_SUPPORT_ORGS_2022", SA_P05_T00_SUPPORT_ORGS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SA-P99-T00-PUBLIC-CHARITY-STATUS-2022.csv", "SA_P99_T00_PUBLIC_CHARITY_STATUS_2022", SA_P99_T00_PUBLIC_CHARITY_STATUS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SC-P01-T00-LOBBY-2022.csv", "SC_P01_T00_LOBBY_2022", SC_P01_T00_LOBBY_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SC-P02-T00-LOBBY-2022.csv", "SC_P02_T00_LOBBY_2022", SC_P02_T00_LOBBY_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SC-P03-T00-LOBBY-2022.csv", "SC_P03_T00_LOBBY_2022", SC_P03_T00_LOBBY_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SCHEDULE-TABLE-2022.csv", "SCHEDULE_TABLE_2022", SCHEDULE_TABLE_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SD-P01-T00-ORGS-DONOR-ADVISED-FUNDS-OTH-2022.csv", "SD_P01_T00_ORGS_DONOR_ADVISED_FUNDS_OTH_2022", SD_P01_T00_ORGS_DONOR_ADVISED_FUNDS_OTH_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SD-P02-T00-CONSERV-EASEMENTS-2022.csv", "SD_P02_T00_CONSERV_EASEMENTS_2022", SD_P02_T00_CONSERV_EASEMENTS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SD-P03-T00-ORGS-COLLECT-ART-HIST-TREASURE-OTH-2022.csv", "SD_P03_T00_ORGS_COLLECT_ART_HIST_TREASURE_OTH_2022", SD_P03_T00_ORGS_COLLECT_ART_HIST_TREASURE_OTH_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SD-P04-T00-ESCROW-CUSTODIAL-ARRANGEMENTS-2022.csv", "SD_P04_T00_ESCROW_CUSTODIAL_ARRANGEMENTS_2022", SD_P04_T00_ESCROW_CUSTODIAL_ARRANGEMENTS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SD-P05-T00-ENDOWMENT-2022.csv", "SD_P05_T00_ENDOWMENT_2022", SD_P05_T00_ENDOWMENT_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SD-P06-T00-LAND-BLDG-EQUIP-2022.csv", "SD_P06_T00_LAND_BLDG_EQUIP_2022", SD_P06_T00_LAND_BLDG_EQUIP_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SD-P07-T00-INVESTMENTS-OTH-SECURITIES-2022.csv", "SD_P07_T00_INVESTMENTS_OTH_SECURITIES_2022", SD_P07_T00_INVESTMENTS_OTH_SECURITIES_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SD-P09-T00-OTH-ASSETS-2022.csv", "SD_P09_T00_OTH_ASSETS_2022", SD_P09_T00_OTH_ASSETS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SD-P10-T00-OTH-LIABILITIES-2022.csv", "SD_P10_T00_OTH_LIABILITIES_2022", SD_P10_T00_OTH_LIABILITIES_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SD-P11-T00-RECONCILIATION-REVENUE-2022.csv", "SD_P11_T00_RECONCILIATION_REVENUE_2022", SD_P11_T00_RECONCILIATION_REVENUE_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SD-P12-T00-RECONCILIATION-EXPENSES-2022.csv", "SD_P12_T00_RECONCILIATION_EXPENSES_2022", SD_P12_T00_RECONCILIATION_EXPENSES_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SD-P99-T00-RECONCILIATION-NETASSETS-2022.csv", "SD_P99_T00_RECONCILIATION_NETASSETS_2022", SD_P99_T00_RECONCILIATION_NETASSETS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SE-P01-T00-SCHOOLS-2022.csv", "SE_P01_T00_SCHOOLS_2022", SE_P01_T00_SCHOOLS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SF-P01-T00-FRGN-ACTS-2022.csv", "SF_P01_T00_FRGN_ACTS_2022", SF_P01_T00_FRGN_ACTS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SF-P02-T00-FRGN-ORG-GRANTS-2022.csv", "SF_P02_T00_FRGN_ORG_GRANTS_2022", SF_P02_T00_FRGN_ORG_GRANTS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SF-P04-T00-FRGN-INTERESTS-2022.csv", "SF_P04_T00_FRGN_INTERESTS_2022", SF_P04_T00_FRGN_INTERESTS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SF-P99-T00-FRGN-ORG-GRANTS-2022.csv", "SF_P99_T00_FRGN_ORG_GRANTS_2022", SF_P99_T00_FRGN_ORG_GRANTS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SG-P01-T00-FUNDRAISING-ACTS-2022.csv", "SG_P01_T00_FUNDRAISING_ACTS_2022", SG_P01_T00_FUNDRAISING_ACTS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SG-P02-T00-FUNDRAISING-EVENTS-2022.csv", "SG_P02_T00_FUNDRAISING_EVENTS_2022", SG_P02_T00_FUNDRAISING_EVENTS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SG-P03-T00-GAMING-2022.csv", "SG_P03_T00_GAMING_2022", SG_P03_T00_GAMING_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SH-P01-T00-FAP-COMMUNITY-BENEFIT-POLICY-2022.csv", "SH_P01_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022", SH_P01_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SH-P02-T00-FAP-COMMUNITY-BENEFIT-POLICY-2022.csv", "SH_P02_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022", SH_P02_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SH-P03-T00-FAP-COMMUNITY-BENEFIT-POLICY-2022.csv", "SH_P03_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022", SH_P03_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SH-P05-T00-FAP-COMMUNITY-BENEFIT-POLICY-2022.csv", "SH_P05_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022", SH_P05_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SH-P99-T00-FAP-COMMUNITY-BENEFIT-POLICY-2022.csv", "SH_P99_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022", SH_P99_T00_FAP_COMMUNITY_BENEFIT_POLICY_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SI-P01-T00-GRANTS-INFO-2022.csv", "SI_P01_T00_GRANTS_INFO_2022", SI_P01_T00_GRANTS_INFO_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SI-P02-T00-GRANTS-US-ORGS-GOVTS-2022.csv", "SI_P02_T00_GRANTS_US_ORGS_GOVTS_2022", SI_P02_T00_GRANTS_US_ORGS_GOVTS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SI-P99-T00-GRANTS-US-ORGS-GOVTS-2022.csv", "SI_P99_T00_GRANTS_US_ORGS_GOVTS_2022", SI_P99_T00_GRANTS_US_ORGS_GOVTS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SJ-P01-T00-COMPENSATION-2022.csv", "SJ_P01_T00_COMPENSATION_2022", SJ_P01_T00_COMPENSATION_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SL-P01-T00-EXCESS-BENEFIT-TRANSAC-2022.csv", "SL_P01_T00_EXCESS_BENEFIT_TRANSAC_2022", SL_P01_T00_EXCESS_BENEFIT_TRANSAC_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SL-P02-T00-LOANS-INTERESTED-PERS-2022.csv", "SL_P02_T00_LOANS_INTERESTED_PERS_2022", SL_P02_T00_LOANS_INTERESTED_PERS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SM-P01-T00-NONCASH-CONTRIBUTIONS-2022.csv", "SM_P01_T00_NONCASH_CONTRIBUTIONS_2022", SM_P01_T00_NONCASH_CONTRIBUTIONS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SN-P01-T00-LIQUIDATION-TERMINATION-DISSOLUTION-2022.csv", "SN_P01_T00_LIQUIDATION_TERMINATION_DISSOLUTION_2022", SN_P01_T00_LIQUIDATION_TERMINATION_DISSOLUTION_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SN-P02-T00-DISPOSITION-OF-ASSETS-2022.csv", "SN_P02_T00_DISPOSITION_OF_ASSETS_2022", SN_P02_T00_DISPOSITION_OF_ASSETS_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SN-P99-T00-LIQUIDATION-TERMINATION-DISSOLUTION-2022.csv", "SN_P99_T00_LIQUIDATION_TERMINATION_DISSOLUTION_2022", SN_P99_T00_LIQUIDATION_TERMINATION_DISSOLUTION_2022_schema, mydb)
        insert_into_table(f"{base_path}nccs/data/updated_data/SR-P05-T00-TRANSACTIONS-RLTD-ORGS-2022.csv", "SR_P05_T00_TRANSACTIONS_RLTD_ORGS_2022", SR_P05_T00_TRANSACTIONS_RLTD_ORGS_2022_schema, mydb)
    
    elif tablename == 'ngos_categorization':
        insert_into_table(f'{base_path}nccs/data/FINAL_FULL_DATA_LLM_CLASSIFIED_MISSING_54K_CAT.csv', 
                          'NGOs_with_categories',
                          ngo_categorization_schema,
                          mydb)
    elif tablename == 'NCCS_Address_Metadata':
        insert_into_table(f"{base_path}nccs/data/NCCS_Address_Metadata_cbsa.csv", "NCCS_Address_Metadata_cbsa", NCCS_Address_Metadata_cbsa_schema, mydb) #=> error
        print("Inserted NCCS_Address_Metadata_cbsa")
        insert_into_table(f"{base_path}nccs/data/NCCS_Address_Metadata_census_blocks.csv", "NCCS_Address_Metadata_census_blocks", NCCS_Address_Metadata_census_blocks_schema, mydb)
        print("Inserted NCCS_Address_Metadata_census_blocks")
        insert_into_table(f"{base_path}nccs/data/NCCS_Address_Metadata_counties.csv", "NCCS_Address_Metadata_counties", NCCS_Address_Metadata_counties_schema, mydb)
        print("Inserted NCCS_Address_Metadata_counties")
        insert_into_table(f"{base_path}nccs/data/NCCS_Address_Metadata_org_loc.csv", "NCCS_Address_Metadata_org_loc", NCCS_Address_Metadata_org_loc_schema, mydb)
        print("Inserted NCCS_Address_Metadata_org_loc")
        insert_into_table(f"{base_path}nccs/data/NCCS_Address_Metadata_states.csv", "NCCS_Address_Metadata_states", NCCS_Address_Metadata_states_schema, mydb)  #=> error
        print("Inserted NCCS_Address_Metadata_states")
        insert_into_table(f"{base_path}nccs/data/NCCS_Address_Metadata_tracts.csv", "NCCS_Address_Metadata_tracts", NCCS_Address_Metadata_tracts_schema, mydb) #=> error
        print("Inserted NCCS_Address_Metadata_tracts")
        insert_into_table(f"{base_path}nccs/data/NCCS_Address_Metadata_urban_areas.csv", "NCCS_Address_Metadata_urban_areas", NCCS_Address_Metadata_urban_areas_schema, mydb) #=> error
        print("Inserted NCCS_Address_Metadata_urban_areas")        
    elif tablename == 'nccs_bmf':
        insert_into_table(f"{base_path}nccs/data/BMF_UNIFIED_V1.1_cleaned.csv",
                          "nccs_bmf",
                          nccs_bmf_schema,
                          mydb)
        
    mydb.close()
