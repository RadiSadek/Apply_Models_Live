

################################################################################
#               Joint script for Application and Behavioral scoring            #
#      Apply Logistic Regression on all products (CityCash and Credirect)      #
#                          Version 7.0 (2020/04/08)                            #
################################################################################



########################
### Initial settings ###
########################

# Libraries
suppressMessages(suppressWarnings(library(RMySQL)))
suppressMessages(suppressWarnings(library(here)))
suppressMessages(suppressWarnings(library(dotenv)))
suppressMessages(suppressWarnings(require("reshape")))
suppressMessages(suppressWarnings(require(jsonlite)))


# Defines the directory where custom .env file is located
load_dot_env(file = here('.env'))


#########################
### Command arguments ###
#########################

args <- commandArgs(trailingOnly = TRUE)
application_id <- args[1]
product_id <- args[2]


#######################
### Manual settings ###
#######################

# Defines the directory where the RScript is located
base_dir <- Sys.getenv("SCORING_PATH", unset = "", names = FALSE)



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
source(file.path(base_dir,"Additional_Restrictions.r"))
source(file.path(base_dir,"Addresses.r"))
source(file.path(base_dir,"Adjust_Scoring_Prior_Approval.r"))
source(file.path(base_dir,"Logistic_App_CityCash.r"))
source(file.path(base_dir,"Logistic_App_Credirect_installments.r"))
source(file.path(base_dir,"Logistic_App_Credirect_payday.r"))
source(file.path(base_dir,"Logistic_App_Credirect_Fraud.r"))
source(file.path(base_dir,"Logistic_Beh_CityCash.r"))
source(file.path(base_dir,"Logistic_Beh_Credirect.r"))
source(file.path(base_dir,"Useful_Functions.r"))
source(file.path(base_dir,"Empty_Fields.r"))
source(file.path(base_dir,"Cutoffs.r"))
source(file.path(base_dir,"SQL_queries.r"))
source(file.path(base_dir,"Disposable_Income.r"))
source(file.path(base_dir,"Behavioral_Variables.r"))
source(file.path(base_dir,"Normal_Variables.r"))
source(file.path(base_dir,"CKR_variables.r"))
source(file.path(base_dir,"Generate_Adjust_Score.r"))



########################
####### Settings #######
########################

# Load predefined libraries
rdata <- file.path(base_dir, "rdata", 
                   "citycash_repeat.rdata")
load(rdata)
rdata2 <- file.path(base_dir, "rdata", 
                    "citycash_app.rdata")
load(rdata2)
rdata3 <- file.path(base_dir, "rdata", 
                    "credirect_installments.rdata")
load(rdata3)
rdata4 <- file.path(base_dir, "rdata", 
                    "credirect_payday.rdata")
load(rdata4)
rdata5 <- file.path(base_dir, "rdata", 
                    "credirect_repeat.rdata")
load(rdata5)
rdata6 <- file.path(base_dir, "rdata", 
                    "credirect_app_fraud.rdata")
load(rdata6)


# Load Risky Coordinates
risky_address <- read.csv(file.path(base_dir, "risky_coordinates", 
                    "risky_coordinates.csv"),sep=";")


####################################
### Read database and build data ###
####################################

# Read credits applications
all_df <- suppressWarnings(fetch(dbSendQuery(con, 
              gen_big_sql_query(db_name,application_id)), n=-1))
all_df$date <- ifelse(all_df$status %in% c(4,5), all_df$signed_at, 
                      all_df$created_at)


# Apply some checks to main credit dataframe
if(!is.na(product_id)){
  all_df$product_id <- product_id
}
if(nrow(all_df)>1){
  all_df <- all_df[!duplicated(all_df$application_id),]
}


# Read product's periods and amounts
products  <- suppressWarnings(fetch(dbSendQuery(con, 
               gen_products_query(db_name,all_df)), n=-1))
products_desc <- suppressWarnings(fetch(dbSendQuery(con, 
               gen_products_query_desc(db_name,all_df)), n=-1))


# Read all previous credits or applications of client
all_credits <- suppressWarnings(fetch(dbSendQuery(con, 
      gen_all_credits_query(db_name,all_df)), n=-1))
all_credits$date <- ifelse(all_credits$status %in% c(4,5), 
      all_credits$signed_at, all_credits$created_at)


# Check if client has a risk profile
risk <- suppressWarnings(fetch(dbSendQuery(con, 
    gen_risky_query(db_name,all_df)), n=-1))


# Check number of varnat 
flag_varnat <- gen_nb_varnat(all_credits)


# Read total amount of current credit
total_amount_curr <- suppressWarnings(fetch(dbSendQuery(con, 
    gen_total_amount_curr_query(db_name,application_id)), n=-1))


# Read CKR 
data_ckr_bank <- gen_query_ckr(all_df,all_credits,1)
data_ckr_financial <- gen_query_ckr(all_df,all_credits,2)


# Read all previous active or terminated credits of client
all_id <- subset(all_credits, all_credits$id==application_id | 
    (all_credits$status %in% c(4,5) &
    (!(all_credits$sub_status %in% c(129,122,133)) | 
       is.na(all_credits$sub_status)) & 
     all_credits$client_id==all_df$client_id))
all_id <- subset(all_id, all_id$created_at<=
                 all_id$created_at[all_id$id==application_id])


# Select for max delay if past AND active: must have at least 30 days of passed
all_id_max_delay <- all_id[all_id$id != application_id,]
all_actives_past <- subset(all_id, 
    all_id$id!=application_id & all_id$status==4)
if(nrow(all_actives_past)>0){
  all_id_max_delay <- gen_select_relevant_ids_max_delay(db_name,
      all_actives_past,all_id_max_delay)
}


# Generate variables for payments of previous credits
nrow_all_id <- nrow(all_id)
all_id <- all_id[order(all_id$signed_at),]
if (nrow_all_id>1){
  cash_flow <- gen_last_paid(all_id)
  total_amount <- gen_last_total_amount(all_id)
  prev_amount <- gen_last_prev_amount(all_id)
  prev_paid_days <- gen_prev_paid_days(all_id)
}


# Get correct max days of delay (of relevant previous credits)
nrow_all_id_max_delay <- nrow(all_id_max_delay)
if (nrow_all_id_max_delay>=1){
  list_ids_max_delay <- gen_select_relevant_ids(all_id_max_delay,
     nrow_all_id_max_delay)
  data_plan_main_select <- suppressWarnings(fetch(dbSendQuery(con, 
     gen_plan_main_select_query(db_name,list_ids_max_delay)), n=-1))
} 


# Get average expenses according to client's address 
addresses <- suppressWarnings(fetch(dbSendQuery(con, 
  gen_address_query(all_df$client_id,"App\\\\Models\\\\Clients\\\\Client")), 
  n=-1))
if(nrow(addresses)==0){
  addresses <- suppressWarnings(fetch(dbSendQuery(con, 
  gen_address_query(all_df$client_id,
  "App\\\\Models\\\\Credits\\\\Applications\\\\Application")), n=-1))
}


# Get if office is self approval
all_df$self_approval <- suppressWarnings(fetch(dbSendQuery(con, 
  gen_self_approval_office_query(db_name,all_df$office_id)), n=-1))$self_approve


# Get dataframe of API data 
tryCatch(
  api_df <- gen_dataframe_json(suppressWarnings(fetch(dbSendQuery(
    con,gen_api_data(db_name,application_id)), n=-1))), 
  error=function(e) 
  {api_df <- NA})
if(!exists('api_df')){
  api_df <- NA
}
api_df <- gen_treat_api_df(api_df)



############################################
### Compute and rework additional fields ###
############################################

# Compute flag if credit is up to next salary
flag_credit_next_salary <- ifelse(all_df$product_id %in% 
      c(25:28,36,37,41:44,49,50,55:58,67:68), 1, 0)


# Compute flag if product is credirect
flag_credirect <- ifelse(products_desc$company_id==2, 1, 0)


# Compute flag if product is cashpoint
flag_cashpoint <- ifelse(products_desc$company_id==5, 1, 0)


# Compute flag if client has previous otpisan or tsediran
flag_exclusion <- ifelse(length(which(names(
  table(all_id$sub_status)) %in% c(124,133)))>0, 1,
  ifelse(nrow(risk)>0, 1, 0))


# Get and rename columns for CKR variables
all_df <- cbind(all_df, data_ckr_financial)
names(all_df)[(ncol(all_df)-8):ncol(all_df)] <- c("ckr_cur_fin","ckr_act_fin",
   "ckr_fin_fin",	"src_ent_fin","amount_fin","cred_count_fin",
   "outs_principal_fin","outs_overdue_fin","cession_fin")
all_df <- cbind(all_df, data_ckr_bank)	
names(all_df)[(ncol(all_df)-8):ncol(all_df)] <- c("ckr_cur_bank",
   "ckr_act_bank","ckr_fin_bank","src_ent_bank","amount_bank","cred_count_bank",
   "outs_principal_bank","outs_overdue_bank","cession_bank")


# Set period variable (monthly, twice weekly, weekly)
period <- products_desc$period


# Compute and generate general variables
all_df <- gen_norm_var(period,all_df,products,1)


# Compute and generate variables specific for behavioral model
data_plan_main_select_def <- ifelse(exists("data_plan_main_select"),
          data_plan_main_select,NA)
all_df <- gen_other_rep(nrow_all_id,all_id,all_df,flag_credirect,
                        data_plan_main_select_def,application_id)


# Get flag if credit is behavioral or not
flag_beh <- ifelse(all_df$credits_cum==0, 0, 1)
flag_rep <- ifelse(nrow(subset(all_id,all_id$status==5))>0,1,0)


# Correct days since last default if necessary
if(flag_beh==1){
  flag_app_quickly <- gen_all_days_since_credit(db_name,all_credits,all_df)
  all_df$days_diff_last_credit <- 
    ifelse(is.na(all_df$days_diff_last_credit),all_df$days_diff_last_credit,
    ifelse(flag_app_quickly==1 & flag_credirect==1,0,
    all_df$days_diff_last_credit))
} else {
  all_df$days_diff_last_credit <- NA
  flag_app_quickly <- NA
}
if(flag_cashpoint==1){
  all_df$days_diff_last_credit <- 30
}


# Compute ratio of number of payments
all_df$ratio_nb_payments_prev <- ifelse(flag_beh==1,prev_paid_days/	
       total_amount$installments,NA)


# Compute ratio of refinanced
all_df$refinance_ratio <- ifelse(flag_beh==1,
       gen_ratio_refinance_previous(db_name,all_id),NA)


# Compute and rework CKR variables, suitable for model application
all_df <- gen_ckr_variables(all_df,flag_beh,flag_credirect)


# Compute flag of bad CKR for city cash
flag_bad_ckr_citycash <- ifelse(is.na(all_df$amount_fin),0,
    ifelse(all_df$amount_fin==0, 0,
    ifelse(all_df$outs_overdue_fin/all_df$amount_fin>=0.1 & 
           all_df$status_active_total %in% c(74,75), 1, 0)))


# Compute if previous is online 
all_id <- get_company_id_prev(db_name,all_id)
all_df <- gen_prev_online(db_name,all_id,all_df,application_id)


# Get flag if credit is behavioral but with same company
flag_beh_company <- ifelse(
  nrow(all_id[all_id$company_id==
       all_id$company_id[all_id$id==application_id],])>1,1,0)


# Compute flag if last paid credit is maybe hidden refinance
all_df <- gen_ratio_last_amount_paid(db_name,all_id,all_df,application_id,
  products_desc,nrow_all_id,cash_flow,total_amount)


# Compute amount differential 
all_df$amount_diff <- ifelse(nrow_all_id<=1, NA, all_df$amount - 
                               prev_amount$amount)


# Compute income variables
t_income <- gen_t_income(db_name,application_id,period)
disposable_income_adj <- gen_disposable_income_adj(db_name,application_id,
        all_df,addresses,period)
all_df$total_income <- gen_income(db_name,application_id)


# Read relevant product amounts (not superior to amount of application)
products <- subset(products, products$amount<=all_df$amount)


# Prepare final dataframe
scoring_df <- gen_final_df(products,application_id)


# Make back-up dataframe
df <- all_df


# Correct empty and missing value fields (to standard format)
df <- gen_null_to_na(df)


# Get if empty field threshold is passed
empty_fields <- gen_empty_fields(flag_beh,flag_credirect,df)
threshold_empty <- ifelse(flag_credirect==0 & flag_beh==0, 7,
   ifelse(flag_credirect==1 & flag_beh==0, 4,
   ifelse(flag_credirect==0 & flag_beh==1, 8, 5)))


# Adjust count of empty fields accordingly
empty_fields <- ifelse(flag_credirect==1, empty_fields, 
   ifelse(is.na(df$total_income) | df$total_income==0, 
   threshold_empty, empty_fields))


# Readjust fields
df <- gen_norm_var2(df)


# Compute flag exclusion for cession in CKR
flag_cession <- ifelse(flag_credirect==1 & df$amount_cession_total>0, 1, 0)


# Compute flag if new credirect but old citycash
flag_new_credirect_old_city <- ifelse(flag_credirect==1 & flag_beh==1 &
 nrow(all_id[all_id$company_id==2 & all_id$id!=application_id
      & all_id$status %in% c(4,5),])==0, 1, 0)


# Compute flag if has current active
if(flag_beh_company==1){
  flag_active <- gen_flag_if_curr_active(all_id,application_id)
} else {
  flag_active <- cbind(NA,NA)
}


# Get flag if client is dead
flag_is_dead <- ifelse(is.na(suppressWarnings(fetch(dbSendQuery(con,
 gen_flag_is_dead(db_name,all_df$client_id)), n=-1))$dead_at),0,1)


# Get flag if client is in a risky address
flag_risky_address <- gen_flag_risky_address(db_name,application_id,
                                             risky_address,all_df)
df$risky_address <- flag_risky_address$flag_risky_address



############################################################
### Apply model coefficients according to type of credit ###
############################################################

scoring_df <- gen_apply_score(
  empty_fields,threshold_empty,flag_exclusion,
  flag_varnat,flag_is_dead,flag_credit_next_salary,flag_credirect,
  flag_beh,all_df,scoring_df,df,products,df_Log_beh_CityCash,
  df_Log_CityCash_App,df_Log_beh_Credirect,df_Log_Credirect_App_installments,
  df_Log_Credirect_App_payday,period,all_id,prev_amount,amount_tab,
  t_income,disposable_income_adj,flag_new_credirect_old_city,api_df,0)


# Set initial scoring reason
scoring_decision <- gen_decline_reason(scoring_df,all_df,10,NA)


# Build column PD
if(!("pd" %in% names(scoring_df))){
  scoring_df$pd <- NA
}


######################################
### Generate final output settings ###
######################################

# Generate scoring dataframe
scoring_df$created_at <- Sys.time()
scoring_df <- scoring_df[,c("application_id","amount","period","score","color",
                            "pd","created_at")]


# Readjust score when applicable
scoring_df <- gen_apply_policy(scoring_df,flag_credirect,flag_cession,
   flag_bad_ckr_citycash,all_df,all_id,flag_beh,prev_amount,products,
   application_id,flag_new_credirect_old_city,flag_credit_next_salary,
   flag_beh_company,flag_cashpoint,0)
scoring_decision <- gen_decline_reason(scoring_df,all_df,15,scoring_decision)


# Apply repeat restrictions to refinances and with potential refinance
if(flag_beh_company==1){
  if((max(flag_active)==1 & flag_credirect==0) | 
     (flag_active[1]==1 & flag_credirect==1)){
    scoring_df <- gen_restrict_beh_refinance(db_name,all_df,all_id,
      scoring_df,flag_active,application_id,flag_credirect,flag_cashpoint)
  }
}
scoring_decision <- gen_decline_reason(scoring_df,all_df,22,scoring_decision)


# Get fraud flag
fraud_flag <- ifelse(flag_credirect==1 & flag_beh==0 & 
   empty_fields<threshold_empty, gen_app_credirect_fraud(
   df,scoring_df,products,df_Log_Credirect_Fraud,period,all_df,
   prev_amount,amount_tab,t_income,disposable_income_adj,db_name), "NULL")


# Reselect columns 
scoring_df <- scoring_df[,c("application_id","amount","period","score","color",
                            "pd","created_at")]


# Recorrect for prior approvals - terminated
scoring_df <- gen_correction_po(con,db_name,all_df,all_id,
                                scoring_df,products,period,application_id)
scoring_decision <- gen_decline_reason(scoring_df,all_df,49,scoring_decision)


# Recorrect for prior approvals - refinances
scoring_df <- gen_correction_po_ref(con,db_name,all_df,all_id,
                                    scoring_df,products,period)
scoring_decision <- gen_decline_reason(scoring_df,all_df,72,scoring_decision)


# Check if early paid previous credit : no offer for City Cash
if(flag_beh_company==1 & flag_credirect==0 & flag_cashpoint==0){
   scoring_df <- gen_corection_early_repaid(con,db_name,scoring_df,
    all_df,all_id,flag_credit_next_salary)
}
scoring_decision <- gen_decline_reason(scoring_df,all_df,83,scoring_decision)


# Reselect columns 
scoring_df <- scoring_df[,c("application_id","amount","period","score","color",
                            "pd","created_at")]


# Create column for table display
scoring_df <- gen_final_table_display(scoring_df)


# Update table credits applications
update_table_extras_query <- paste("UPDATE ",db_name,
  ".credits_applications SET scoring_warning = ",fraud_flag,
  " WHERE id=",application_id, sep="")
suppressMessages(suppressWarnings(dbSendQuery(con, 
  update_table_extras_query)))


# Update table credits applications decisions
update_table_extras_query2 <- paste("UPDATE ",db_name,
  ".credits_applications_decisions SET scoring_decision = ",scoring_decision,
  " WHERE application_id=",application_id, sep="")
suppressMessages(suppressWarnings(dbSendQuery(con, 
   update_table_extras_query2)))


# Update table credits applications scoring
write_sql_query <- paste("
  DELETE FROM ",db_name,".credits_applications_scoring WHERE application_id=",
  application_id, sep="")
suppressMessages(dbSendQuery(con,write_sql_query))
suppressMessages(dbWriteTable(con, name = "credits_applications_scoring", 
  value = scoring_df,
  field.types = c(application_id="numeric", amount="integer", 
  period="integer", score="character(20)",color="integer", 
  display_score="character(20)",pd="numeric",created_at="datetime"),
  row.names = F, append = T))



#######
# END #
#######

