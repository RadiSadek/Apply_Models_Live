

################################################################################
#         Script for generating  daily offers for terminated credits           #
#      Apply Logistic Regression on all products (CityCash and Credirect)      #
#                          Version 1.0 (2021/07/11)                            #
################################################################################



########################
### Initial settings ###
########################

# Libraries
suppressMessages(suppressWarnings(library(RMySQL)))
suppressMessages(suppressWarnings(library(here)))
suppressMessages(suppressWarnings(library(dotenv)))
suppressMessages(suppressWarnings(require("reshape")))


# Defines the directory where custom .env file is located
load_dot_env(file = here('.env'))


#########################
### Command arguments ###
#########################

args <- commandArgs(trailingOnly = TRUE)
application_id <- args[1]


#######################
### Manual settings ###
#######################

# Defines the directory where the RScript is located
base_dir <- Sys.getenv("SCORING_PATH", unset = "", names = FALSE)


# Define product id
product_id <- NA



#####################
####### MySQL #######
#####################

db_host <- Sys.getenv("DB_HOST", 
                      unset = "localhost", 
                      names = FALSE)
db_port <- strtoi(Sys.getenv("DB_PORT", 
                             unset = "3306", 
                             names = FALSE))
db_name <- Sys.getenv("DB_DATABASE", 
                      unset = "citycash", 
                      names = FALSE)
db_username <- Sys.getenv("DB_USERNAME", 
                          unset = "root", 
                          names = FALSE)
db_password <- Sys.getenv("DB_PASSWORD", 
                          unset = "secret", 
                          names = FALSE)
con <- dbConnect(MySQL(), user=db_username, 
                 password=db_password, dbname=db_name, 
                 host=db_host, port = db_port)
sqlMode <- paste("SET sql_mode=''", sep ="")
suppressWarnings(fetch(dbSendQuery(con, sqlMode), 
                       n=-1))



#################################
####### Load source files #######
#################################

# Load other r files
source(file.path(base_dir,"Terminated.r"))
source(file.path(base_dir,"SQL_queries.r"))
source(file.path(base_dir,"Useful_Functions.r"))



###################################################
### Generate data of potential credits to offer ###
###################################################

# Read credit applications 
get_actives_sql <- paste("
SELECT id, status, date, signed_at, product_id, client_id, 
deactivated_at, sub_status, office_id, consultant_id
FROM ",db_name,".credits_applications 
WHERE id= ",application_id,sep="")
all_credit <- gen_query(con,get_actives_sql)


# Get company ID
company_id <- gen_query(con, 
    gen_get_company_id_query(db_name))
all_credit <- merge(all_credit,company_id,by.x = "product_id",
    by.y = "id",all.x = TRUE)


# Get last credit amount
credit_amount_sql <- paste("
SELECT application_id , amount as credit_amount
FROM ",db_name,".credits_plan_contract 
WHERE application_id=",application_id,sep ="")
credit_amount <- gen_query(con,credit_amount_sql)
all_credit$credit_amount <- credit_amount$credit_amount


# Get flagged GDPR marketing campaigns
flag_gdpr <- gen_query(con,
  gen_flag_gdpr(db_name,all_credit$client_id))$gdpr_marketing_messages


# Get max DPD on current credit
max_dpd <- gen_query(con,
  gen_plan_main_select_query(db_name,application_id))$max_delay



#####################################################
### Apply selection criteria for credits to offer ###
#####################################################

# Subset based on sub_status
if(!(all_credit$sub_status %in% c(123,128))){
  quit()
}


# Subset based on flagged GDPR
if(flag_gdpr==1 & !is.na(flag_gdpr)){
  quit()
}


# Select based on product_id
if(all_credit$product_id %in% c(12,13,59:65,53,54,51,22)){
  quit()
}


# Remove according to criteria of passed installments
get_main_sql <- paste(
"SELECT pay_day
FROM ",db_name,".credits_plan_main 
WHERE application_id = ",application_id, sep="")
plan_main <- gen_query(con,get_main_sql)
all_credit$tot_installments <- nrow(plan_main)
all_credit$passed_installments <- length(plan_main[
  plan_main$pay_day<=Sys.time(),])
get_period_sql <- paste(
"SELECT period 
FROM ",db_name,".products 
WHERE id= ",all_credit$product_id,sep="")
all_credit$period <- gen_query(con,get_period_sql)$period
flag_credit_next_salary <- ifelse(all_credit$product_id %in% 
    c(25:28,36,37,41:44,49,50,55:58,67,68), 1, 0)
if((flag_credit_next_salary==1 & all_credit$passed_installments==0) |
   (flag_credit_next_salary!=1 & all_credit$period==3 & 
    all_credit$passed_installments<2 & all_credit$tot_installments<4 & 
    all_credit$company_id!=5) |
   (flag_credit_next_salary!=1 & all_credit$passed_installments<4 & 
    all_credit$tot_installments>=4 & all_credit$company_id!=5) |
   (flag_credit_next_salary!=1 & all_credit$passed_installments==0 & 
    all_credit$company_id==5)) {
  quit()
}


# Flag is terminated before maturing
flag_limit_offer <- ifelse(all_credit$sub_status==123,1,0)


# Remove if more than 1 "varnat"
get_status_sql <- paste("
SELECT id, status, sub_status, product_id
FROM ",db_name,".credits_applications 
WHERE client_id= ",all_credit$client_id,sep="")
all_credit_status <- gen_query(con,get_status_sql)
all_credit_varnat <- subset(all_credit_status,all_credit_status$sub_status==122)
if(nrow(all_credit_varnat)>1){
  quit()
}


# Remove credits with already an offer of corresponding company
po_sql_query <- paste(
  "SELECT id, client_id, product_id, application_id, created_at, credit_amount,
  installment_amount,deleted_at,updated_at
  FROM ",db_name,".clients_prior_approval_applications",sep="")
po <- gen_query(con, po_sql_query)
po_raw <- po
po <- subset(po,po$client_id==all_credit$client_id)
po <- merge(po,company_id,by.x = "product_id",by.y = "id",all.x = TRUE)
po <- subset(po,po$company_id==all_credit$company_id & is.na(po$deleted_at))
if(nrow(po)>0){
  quit()
}


# Remove those who have active credit of corresponding company
all_credit_status <- merge(all_credit_status,company_id,
   by.x = "product_id",by.y = "id",all.x = TRUE)
all_credit_active <- subset(all_credit_status,
   all_credit_status$status==4 & 
   all_credit_status$company_id==all_credit$company_id)
if(nrow(all_credit_active)>0){
  quit()
}


# Join if VIP
is_vip_query <- paste(
  "SELECT id, is_vip
  FROM ",db_name,".clients 
  WHERE id=",all_credit$client_id,sep="")
is_vip <- gen_query(con, is_vip_query)
all_credit <- merge(all_credit,is_vip,by.x = "client_id",
  by.y = "id", all.x = TRUE)



#####################
### Compute score ###
#####################

if(nrow(all_credit)>0){

# Compute and append score
all_credit <- all_credit[!duplicated(all_credit$client_id),]
all_credit$max_amount <- NA
all_credit$max_installment_amount <- NA
all_credit$score_max_amount <- NA
all_credit$max_delay <- NA
for(i in 1:nrow(all_credit)){
  suppressWarnings(tryCatch({
    if(all_credit$product_id[i]==8 & all_credit$is_vip[i]==0){
      product_id <- 5
    } else {
      product_id <- NA
    }
    client_id <- all_credit$client_id[i]
    last_id <- all_credit$id[i]
    calc <- gen_terminated_fct(con,client_id,product_id,last_id,
                               flag_limit_offer)
    all_credit$max_amount[i] <- calc[[1]]
    all_credit$max_installment_amount[i] <- calc[[2]]
    all_credit$score_max_amount[i] <- calc[[3]]
    all_credit$max_delay[i] <- as.numeric(calc[[4]])
  }, error=function(e){}))
}



##################################
### Reapply selection criteria ###
##################################

# Select based on score and DPD
all_credit <- subset(all_credit,all_credit$max_amount>-Inf & 
                                 all_credit$max_amount<Inf)
all_credit <- subset(all_credit,all_credit$max_delay<=200)



#############################################
### Work on final dataset and write in DB ###
#############################################

# Read current database
id_max_query <- paste(
  "SELECT MAX(id) as max_id
  FROM ",db_name,".clients_prior_approval_applications",sep="")
id_max <- gen_query(con,id_max_query)$max_id+1

if(nrow(all_credit)>0){

# Create final dataframe for writing in DB
offers <- all_credit
offers$id <- seq(id_max,id_max+nrow(offers)-1,1)
offers$application_id <- NA
offers$group <- NA
offers$hide_until_date <- NA
offers$created_at <- Sys.time()
offers$updated_at <- NA
offers$deleted_at <- NA
offers$credit_amount_updated <- NA
offers$installment_amount_updated <- NA
offers <- offers[,c("id","office_id","client_id","group","product_id",
    "application_id","max_amount","max_installment_amount",
    "credit_amount_updated","installment_amount_updated","hide_until_date",
    "consultant_id","created_at","updated_at","deleted_at")]
names(offers)[names(offers)=="max_amount"] <- "credit_amount"
names(offers)[names(offers)=="max_installment_amount"] <- "installment_amount"
offers[is.na(offers)] <- "NULL"


# Adjust product ID
offers$product_id <- ifelse(offers$product_id %in% c(43,44,49,50,57,58),55,
                            offers$product_id)


# Make result ready for SQL query
string_sql <- gen_sql_string_po_terminated(offers,1)


# Output real offers
update_prior_query <- paste("INSERT INTO ",db_name,
".clients_prior_approval_applications VALUES ",
string_sql,";", sep="")


# Write in database
if(nrow(offers)>0){
  suppressMessages(suppressWarnings(dbSendQuery(con,update_prior_query)))
}}}

