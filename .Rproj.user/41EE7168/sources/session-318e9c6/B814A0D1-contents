if (!requireNamespace("DBI", quietly = TRUE)) install.packages("DBI")
if (!requireNamespace("RSQLite", quietly = TRUE)) install.packages("RSQLite")

#install.packages(c("DatabaseConnector", "SqlRender"))
library(DatabaseConnector)
library(SqlRender)
library(readxl)
library(glue)
library(RPostgres)
library(tidyr)
library(lubridate)

source("config.R")

# Database Connection
tryCatch({
  ParallelLogger::logInfo("Connecting to the database...")
  
  con <- dbConnect(RPostgres::Postgres(), dbname = db, host = host_db,
                   port = db_port, user = db_user, password = db_password)
  
  ParallelLogger::logInfo("Connected to the database successfully.")
}, error = function(e) {
  ParallelLogger::logError("Error connecting to the database:", conditionMessage(e))
  ParallelLogger::logError("Connection details:", db_info)
})

#---------------------------------- TTRUNCATE CASCADE if necessary

tryCatch({
  dbBegin(con)
  
  dbExecute(con, "TRUNCATE TABLE cds_cdm_01.person CASCADE;")
  
  dbCommit(con)
}, error = function(e) {
  dbRollback(con)
  ParallelLogger::logError("Error truncating tables:", conditionMessage(e))
})

#---------------------------------- Data and Mappings

input_data <- read.csv("synthetic_aml_data_nflow_corrected.csv", header = TRUE, sep = ";")
mapping <- read_excel("Mapping_table.xlsx", sheet = "Mappings")

#---------------------------------- PERSON table
male_concept_id <- 442985
female_concept_id <- 442986

# Iterate through each row in the mapping document
for (i in 1:nrow(input_data)) {
gender_value <- tolower(input_data$SEX[i])
gender_concept_id <- ifelse(input_data$SEX[i] == "m", male_concept_id, 
                    ifelse(input_data$SEX[i] == "f", female_concept_id, NA))

age <- input_data$AGE[i]
patient_id <- input_data$SUBJID[i]

#reference date; (Arbitrary) 01.01.1950
#current_year <- as.numeric(format(Sys.Date(), "%Y"))
specific_date <- dmy("01.01.1950")
year_of_birth <-  specific_date %m-% years(age)

insert_person_query <- glue::glue("
   INSERT INTO cds_cdm_01.person (gender_concept_id, gender_source_value, year_of_birth, person_source_value)
   VALUES ({as.numeric(gender_concept_id)}, '{gender_value}', {year_of_birth}, '{patient_id}');
 ")
 
dbExecute(con, as.character(insert_person_query))
}

