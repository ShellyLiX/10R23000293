#install.packages("measurements")
library(DBI)
library(odbc)
library(dbplyr)
library(tidyverse)
library(readxl)
#library(measurements)
library(lubridate)
library(tidyr)

# Connect to Snowflake
con <- DBI::dbConnect(odbc::odbc(), "snowflake-moffitt")

## Identify Snowflake tables required for this request. 
CancerRegistry_General <- tbl(con,in_schema(sql("MCAP_CDSC_DEV.DATANORM"),"REGISTRY_GENERAL"))
Survey <-  tbl(con, in_schema (sql("MCAP_CDSC_DEV.DATANORM"),"SURVEY"))
PatientInfo <-  tbl(con, in_schema (sql("MCAP_CDSC_DEV.DATANORM"),"PATIENT_INFO"))
BillingEncount <-  tbl(con, in_schema (sql("MCAP_CDSC_DEV.DATANORM"),"BILLING_ENCOUNTERS"))
PortalSurvey <-  tbl(con, in_schema (sql("MCAP_CDSC_DEV.DATANORM"),"PORTAL_SURVEY"))
VitalHtWt <- tbl(con,in_schema ("DATACORE", "V_EMR_VITALS"))
CancerTreatments<- tbl(con, in_schema ("DATACORE", "V_REGISTRY_TREATMENT"))
TreatmentSummary_Info <- tbl(con, in_schema ("DATACORE", "V_REGISTRY_TREATMENT_SUMMARY"))
Biobanking <- tbl(con, in_schema (sql("MCAP_CDSC_DEV.DATANORM"),"BIOBANKING"))
BillingDiag <- tbl(con, in_schema ("DATACORE", "V_BILLING_DIAGNOSIS"))
MOSAIQTreatments <- tbl(con, in_schema ("DATACORE", "V_MOSAIQ_TREATMENT"))
Emr_med <-  tbl(con, in_schema (sql("MCAP_CDSC_DEV.DATANORM"),"EMR_MEDICATION"))

Survey <-  tbl(con, in_schema (sql("MCAP_CDSC_DEV.DATANORM"),"SURVEY"))
PhysicianNotes <- tbl(con, in_schema ("DATACORE", "V_EMR_PHYSICIAN_NOTE"))
SSF <- tbl(con,in_schema ("DATACORE", "V_REGISTRY_SSF"))
survey_response <- tbl(con, in_schema ("DATACORE", "V_SURVEY_RESPONSE_DETAIL"))
survey_metadata <- tbl(con,in_schema ("DATACORE", "V_D_SURVEY_METADATA"))

## Use the mrn_list to select patients and data points closer to cannabis survey date
MRN <- read.csv("mrn_list.csv") %>%
  mutate(MRN = as.character(mrn)) %>%
  select(MRN, date) %>%
  rename(cannabis_survey_date = date) 

MRNCohort <- PatientInfo %>%
  filter(MRN %in% !!MRN$MRN)%>%
  select(PATIENT_ID, MRN) %>% 
  collect() %>%
  merge(., MRN, by = "MRN")

# Cancer Registry Data #########################################################

# NOTE that multiple data lines are available for on patients 
# Select CR data when diagnosis dates are before and closest to cannabis survey date 
CR_data <- CancerRegistry_General %>% 
  filter(MRN %in% !!MRN$MRN)%>%
  select(MRN, PATIENT_ID, BIRTH_DT, GENDER_SRC_DESC, GENDER_CD, ETHNICITY_SRC_DESC, ETHNICITY_CD,
         RACE_CR_SRC_DESC_1, RACE_CR_CD_1, DX_DT, DX_DT_SRC, AGE_AT_DIAGNOSIS_NUM, 
         MARITAL_STATUS_AT_DX_DESC, PRIMARY_PAYOR_AT_DIAGNOSIS_DESC, PAYER_SOURCE_AT_DX_DESC,	
         CDSC_PRIMARY_SITE_GROUP, PRIMARY_SITE_CD, PRIMARY_SITE_DESC, PRIMARY_SITE_GROUP_DESC, 
         PRIMARY_SITE_REGION_DESC, CCSG_SITE_DESC, SUMMARY_OF_RX_1ST_COURSE_DESC,
         FIRST_TREATMENT_DT, FIRST_TREATMENT_DT_SRC, RX_SUMMARY_TREATMENT_STATUS_DESC,
         TOBACCO_USE_CIGARETTES_DESC, DERIVED_TOBACCO_SMOKING_STATUS_DESC,
         RX_SUMMARY_TREATMENT_STATUS_DESC, PATIENT_ADDRESS_STREET_AT_DX, PATIENT_ADDRESS_CITY_AT_DX,
         PATIENT_ADDRESS_POSTAL_CD_AT_DX, VITAL_STATUS_DESC, LAST_CONTACT_OR_DEATH_DT, 
         STAGE_TNM_CS_MIXED_GROUP_DESC, STAGE_TNM_CLIN_GROUP_2018_CD, STAGE_TNM_CLIN_GROUP_2018_DESC) %>%
  collect() %>%
  merge(., MRN, by = "MRN") %>% 
  mutate(ClosestDXDate = as.Date(cannabis_survey_date) - DX_DT) %>%
  filter(ClosestDXDate >= 0) %>%
  group_by(PATIENT_ID) %>% 
  slice_min(ClosestDXDate) %>%
  slice_min(FIRST_TREATMENT_DT) %>%
  distinct(ClosestDXDate, .keep_all = TRUE) 

# Comorbidity - Cancer Registry and Billing ###################################

# Select comorbidity data in cancer registry that diagnosed before cannabis survey 
comorb_CR <- CancerRegistry_General %>% 
  filter(MRN %in% !!MRN$MRN)%>%  
  select(MRN, PATIENT_ID, DX_DT,
         COMORBIDITY_01_CD, COMORBIDITY_01_DESC, COMORBIDITY_02_CD, COMORBIDITY_02_DESC, 
         COMORBIDITY_03_CD, COMORBIDITY_03_DESC, COMORBIDITY_04_CD, COMORBIDITY_04_DESC, 
         COMORBIDITY_05_CD, COMORBIDITY_05_DESC, COMORBIDITY_06_CD, COMORBIDITY_06_DESC, 
         COMORBIDITY_07_CD, COMORBIDITY_07_DESC, COMORBIDITY_08_CD, COMORBIDITY_08_DESC, 
         COMORBIDITY_09_CD, COMORBIDITY_09_DESC, COMORBIDITY_10_CD, COMORBIDITY_10_DESC) %>% 
  collect() %>%
  merge(., MRN, by = "MRN") %>% 
  mutate(DXBeforeSurvey = as.Date(cannabis_survey_date) - DX_DT) %>%
  filter(DXBeforeSurvey >= 0) 

# Select comorbidity data from billing info that diagnosed before cannabis survey
comorb_BILL <- BillingEncount  %>% filter(MRN %in% !!MRN$MRN)%>%
  select(MRN, PATIENT_ID, ADMISSION_DT, DISCHARGE_DT, 
         ICD_DX_CD, ICD_DX_DESC, ICD_DX_CD_TYPE, ICD_DX_TYPE,
         PATIENT_TYPE_CD) %>% 
  collect() %>%
  merge(., MRN, by = "MRN") %>% 
  mutate(BeforeSurvey = as.Date(cannabis_survey_date) - as.Date(ADMISSION_DT)) %>%
  filter(BeforeSurvey >= 0) 

# NOTE: We only used Billing data because it has a date for past 30 day diagnosis
comorb_data<- comorb_BILL %>%
  mutate(sleep_disorder = case_when(grepl("sleep", tolower(ICD_DX_DESC), fixed = TRUE) ~ 1, 
                                    grepl("insomnia", tolower(ICD_DX_DESC), fixed = TRUE) ~ 1, 
                                    TRUE ~ 0), 
         pain = case_when(grepl("pain", tolower(ICD_DX_DESC), fixed = TRUE) ~ 1,
                            TRUE ~ 0), 
         sexual_disorder = case_when(grepl("sexual dysfunction", tolower(ICD_DX_DESC), fixed = TRUE) ~ 1, 
                                    grepl("orgasmic disorder", tolower(ICD_DX_DESC), fixed = TRUE) ~ 1, 
                                    grepl("erectile dysfunction", tolower(ICD_DX_DESC), fixed = TRUE) ~ 1, 
                                    grepl("eating discorder", tolower(ICD_DX_DESC), fixed = TRUE) ~ 1, 
                                    TRUE ~ 0), 
         eating_disorder = case_when(grepl("eating discorder", tolower(ICD_DX_DESC), fixed = TRUE) ~ 1,
                                     TRUE ~ 0), 
         stress = case_when(grepl("acute stress reaction", tolower(ICD_DX_DESC), fixed = TRUE) ~ 1, 
                            grepl("post-traumatic stress", tolower(ICD_DX_DESC), fixed = TRUE) ~ 1, 
                            grepl("reaction to severe stress", tolower(ICD_DX_DESC), fixed = TRUE) ~ 1, 
                            grepl("stress, not elsewhere classified", tolower(ICD_DX_DESC), fixed = TRUE) ~ 1, 
                            TRUE ~ 0)) %>%
  mutate(sleep_disorder_curr = ifelse(BeforeSurvey <= 30 & sleep_disorder == 1, 1, 0), 
         pain_curr = ifelse(BeforeSurvey <=30 & pain == 1, 1, 0)) %>%
  group_by(MRN) %>%
  mutate(sleep_disorder_ever = max(sleep_disorder),
         sleep_disorder_curr = max(sleep_disorder_curr),
         sleep_disorder_3 = case_when(max(sleep_disorder_curr) == 1 ~ 1, 
                                      max(sleep_disorder) == 1 ~ 2, 
                                      max(sleep_disorder) == 0 ~ 3), 
         pain_ever = max(pain),
         pain_curr = max(pain_curr),
         pain_3 = case_when(max(pain_curr) == 1 ~ 1, 
                              max(pain) == 1 ~ 2, 
                              max(pain) == 0 ~ 3)) %>%
    select(MRN, PATIENT_ID, sleep_disorder_ever, sleep_disorder_curr, 
           sleep_disorder_3, pain_ever, pain_curr, pain_3) %>%
  unique()

# EPQ Survey Data ############################################################

EPQ_data <- Survey %>%
  filter(PATIENT_ID %in% !!MRNCohort$PATIENT_ID) %>%
#  filter(ADMINISTRATION_START_DT >="2009-01-01" & ADMINISTRATION_START_DT <="2018-12-31")%>%
  filter(SUBDOMAIN_DESC == "SF12" |
         SHORT_QUESTION_LABEL_CD %in% c("DM_GENDER", "DM_ETHNIC", "DM_RACE",
                                      "DM_EDUC", "DM_MARITAL", "DM_SEX_ORNT", 
                                      "DM_EMP_STAT", "CRA_SMOKE_5PK_EVER", 
                                      "CSM_ETOH_5DAY_MALE", "CSM_ETOH_4DAY_FEMALE",
                                      "PSY_LIVE_ALONE")) %>%
  collect() %>% 
  merge(., MRNCohort, by = "PATIENT_ID") 

#table(EPQ_data$QUESTION_TYPE, EPQ_data$SHORT_QUESTION_LABEL_CD)
EPQ_data_wide <- EPQ_data %>% 
  mutate(BetweenSurveyDate = as.Date(cannabis_survey_date) - as.Date(ADMINISTRATION_START_DT))%>%
  select(PATIENT_ID, MRN, ADMINISTRATION_START_DT, 
         BetweenSurveyDate, SURVEY_NM_CD, SURVEY_VERSION_CD, 
         QUESTION_TYPE, SHORT_QUESTION_LABEL_CD, SHORT_ANSWER_CD) %>%
  pivot_wider(names_from = "SHORT_QUESTION_LABEL_CD", 
              values_from = "SHORT_ANSWER_CD") %>%
  group_by(PATIENT_ID) %>% 
  slice_min(ADMINISTRATION_START_DT) 

EPQ_data_wide %>%
#  filter(SUBDOMAIN_DESC == "SF12") %>%
  group_by(SURVEY_VERSION_CD) %>%
  count()

EPQ_codebook <- EPQ_data %>%
  group_by(PATIENT_ID) %>% 
  slice_min(ADMINISTRATION_START_DT) %>%
  group_by(SHORT_QUESTION_LABEL_CD, SHORT_ANSWER_CD) %>%
  count()

# ESAS Survey Data ###########################################################
#Portal Survey only 503 patients have ESAS data
#1a- Identify questions of interest
ESAS_data <- PortalSurvey %>% 
  filter(PATIENT_ID %in% !!MRNCohort$PATIENT_ID) %>%
  filter(SURVEY_NM_CD == "ESAS" ) %>%
  distinct() %>% 
  collect() %>% 
  merge(., MRNCohort, by = "PATIENT_ID") 

ESAS_data_wide <- ESAS_data %>% 
  mutate(ESASSurveyDate = as.Date(cannabis_survey_date) - as.Date(ADMINISTRATION_START_DT))%>%
  select(PATIENT_ID, MRN, ADMINISTRATION_START_DT, 
         ESASSurveyDate, SURVEY_NM_CD, SURVEY_VERSION_CD, 
         QUESTION_TYPE, SHORT_QUESTION_LABEL_CD, SHORT_ANSWER_CD) %>%
  pivot_wider(names_from = "SHORT_QUESTION_LABEL_CD", 
              values_from = "SHORT_ANSWER_CD") %>%
  group_by(PATIENT_ID) %>% 
  slice_min(ADMINISTRATION_START_DT) 

select(PATIENT_ID, MRN, SURVEY_NM_DESC, PATIENT_FULL_NM, LANGUAGE_DESC,ADMINISTRATION_START_DT, ADMINISTRATION_FINISH_DT, 
       SURVEY_STATUS_DESC,QUESTION_ID,SURVEY_NM_CD,SURVEY_VERSION_CD, 
       CONTROL_ID, MODULE_NUM, MODULE_NM, DOMAIN_DESC,SUBDOMAIN_DESC,
       QUESTION_SEQ_NUM, QUESTION_LABEL_DESC,SHORT_QUESTION_LABEL_CD, QUESTION_TYPE,
       ANSWER_SEQUENCE_NUM, SURVEY_RESPONSE_TXT,ANSWER_LABEL_DESC,ANSWER_SUMMARY) %>% 
  mutate(SURVEY_YEAR=year(ADMINISTRATION_FINISH_DT)) %>% 

  CDSC_data <- CR_data %>%
  merge(., EPQ_data_wide, by = c("MRN", "PATIENT_ID"), all = T) %>%
  merge(., comorb_data, by  = c("MRN", "PATIENT_ID"))

write.csv(CDSC_data, "analysis_data.csv", row.names = F)
