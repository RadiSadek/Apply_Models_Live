

################################################################################
#             New script for generating new daily refinance offers             #
#      Apply Logistic Regression on all products (CityCash and Credirect)      #
#                          Version 1.0 (2020/06/04)                            #
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
source(file.path(base_dir,"Refinance.r"))
source(file.path(base_dir,"SQL_queries.r"))
source(file.path(base_dir,"Useful_Functions.r"))



#############################################################
### Generate data of potential credits to offer refinance ###
#############################################################

# Read credit applications 
get_actives_sql <- suppressWarnings(dbSendQuery(con, paste("
SELECT id, status, date, product_id, client_id
FROM ",db_name,".credits_applications 
WHERE id=",application_id,sep="")))
all_credits <- fetch(get_actives_sql,n=-1)


# Join company ID
company_id <- suppressWarnings(fetch(dbSendQuery(con, 
    gen_get_company_id_query(db_name)), n=-1))
select <- merge(all_credits,company_id,by.x = "product_id",
    by.y = "id",all.x = TRUE)


# Apply time window criteria
select$time_since <- round(difftime(as.Date(substring(Sys.time(),1,10)),
    select$date,units=c("days")),0)


# Get flagged GDPR marketing campaigns
flag_gdpr <- suppressWarnings(fetch(dbSendQuery(con,
  gen_flag_gdpr(db_name,select$client_id)), n=-1))$gdpr_marketing_messages


# Subset based on flagged GDPR
if(flag_gdpr==1 & !is.na(flag_gdpr)){
  quit()
}


# Check if credit is terminated
if(select$status[1] %in% c(1,2,3,5)){
  quit()
}


# Remove if client already an offer 
po_sql_query <- paste(
  "SELECT application_id, created_at, deleted_at, product_id, min_amount
   FROM ",db_name,".prior_approval_refinances WHERE application_id=",
  application_id,sep="")
po <- suppressWarnings(fetch(dbSendQuery(con, po_sql_query), n=-1))
po_raw <- po
select <- select[!(select$id %in% po$application_id),]
if(nrow(select)==0){
  quit()
}


# Read daily installments and payments
data_sql_daily <- suppressWarnings(dbSendQuery(con, paste("
SELECT application_id, installment_num, discount_amount
FROM ",db_name,".credits_plan_main WHERE application_id=",application_id,
sep="")))
daily <- fetch(data_sql_daily,n=-1)
daily <- daily[daily$application_id %in% select$id,]
daily_raw <- daily


# Get installment number
nb_installments <- aggregate(daily$installment_num,
  by=list(daily$application_id),FUN=max)
daily <- merge(daily,nb_installments,by.x = "application_id",by.y = "Group.1",
  all.x = TRUE)
names(daily)[ncol(daily)] <- "nb_installments"
daily <- daily[!duplicated(daily$application_id),]


# Compute paids installment ratio
paid_install_sql <- suppressWarnings(dbSendQuery(con, paste("
SELECT application_id, COUNT(application_id) as installments_paid
FROM ",db_name,".credits_plan_main
WHERE application_id = ",application_id,
" AND payed_at IS NOT NULL AND pay_day<= '",substring(Sys.time(),1,10),
"' GROUP BY application_id;
", sep ="")))
paid_install <- fetch(paid_install_sql,n=-1)
daily <- merge(daily,paid_install,by.x = "application_id",
   by.y = "application_id",all.x = TRUE)
daily$installments_paid <- ifelse(is.na(daily$installments_paid),0,
   daily$installments_paid)
daily$installment_ratio <- round(
  daily$installments_paid / daily$nb_installments,2)
select <- merge(select,
  daily[,c("application_id","installment_ratio")],by.x = "id",
  by.y = "application_id")


# Filter those whose passed installments are lower than 30% and not yet 100%
select <- subset(select,select$installment_ratio>=0.3 & 
 select$installment_ratio<=1)
if(nrow(select)==0){
  quit()
}


# Get final credit amount
credit_amount_sql <- suppressWarnings(dbSendQuery(con, paste("
SELECT application_id,final_credit_amount, amount as credit_amount
FROM ",db_name,".credits_plan_contract", sep ="")))
credit_amount <- fetch(credit_amount_sql,n=-1)
select <- merge(select,credit_amount,by.x = "id",
   by.y = "application_id",all.x = TRUE)


# Get eventual taxes
taxes_sql <- suppressWarnings(dbSendQuery(con, paste("
SELECT application_id, amount, paid_amount 
FROM ",db_name,".credits_plan_taxes WHERE application_id= ", application_id,
sep ="")))
taxes <- fetch(taxes_sql,n=-1)
taxes_raw <- taxes
taxes <- taxes[taxes$application_id %in% daily$application_id,]
if(nrow(taxes)==0){
  taxes_agg <- 0
} else {
  taxes_agg <- sum(taxes$amount)
}


# Get discounts
discount_agg <- sum(daily_raw$discount_amount)


# Get all payments for each credit
paid <- fetch(suppressWarnings(dbSendQuery(con, paste("
SELECT object_id, amount, pay_date 
FROM ",db_name,".cash_flow
WHERE nomenclature_id in (90,100,101,102) AND object_id =",application_id,
" AND deleted_at IS NULL AND object_type=4",sep=""))), n=-1)
paid_raw <- paid
paid <- paid[paid$object_id %in% daily$application_id,]


# Get products periods and amounts of products
products <- fetch(suppressWarnings(dbSendQuery(con, paste("
SELECT product_id, amount 
FROM ",db_name,".products_periods_and_amounts",sep=""))), n=-1)


# Get hitherto payments and ratios
sum_paid_agg  <- sum(paid$amount)
sum_taxes_agg  <- taxes_agg
sum_discount_agg  <- discount_agg
select$paid_hitherto <- sum_paid_agg
select$tax_amount <- sum_taxes_agg
select$discount_amount <- sum_discount_agg
select$left_to_pay <- select$final_credit_amount + 
  select$tax_amount - select$paid_hitherto - select$discount_amount
if(!is.na(select$left_to_pay) & select$left_to_pay==0){
  quit()
}


# Check if client has still VIP status 
is_vip_query <- paste(
  "SELECT id, is_vip
  FROM ",db_name,".clients",sep="")
is_vip <- suppressWarnings(fetch(dbSendQuery(con, is_vip_query), n=-1))
select <- merge(select,is_vip,by.x = "client_id",by.y = "id", all.x = TRUE)


# Remove Flex credits and other Ipoteki
select <- subset(select,!(select$product_id %in%
  c(25,36,41,43,50,28,26,37,42,44,49,27,55,58,57,56,22,3,53,54,51,65,12,13,
    62,63,61,64,59,60)))



#####################
### Score credits ###
#####################

# Append score
if(nrow(select)>0){
result_df <- select[,2, drop=FALSE]
result_df$max_amount <- NA
result_df$score_max_amount <- NA
result_df$product_id <- NA
result_df$max_installment <- NA
result_df$days_delay <- NA
result_df$office_id <- NA
result_df$third_side <- NA
for(i in 1:nrow(result_df)){
  suppressWarnings(tryCatch({
    application_id <- result_df$id[i]
    if(select$product_id[i]==8 & select$is_vip[i]==0){
      product_id <- 5
    } else {
      product_id <- NA
    }
    calc <- gen_refinance_fct(con,application_id,product_id)
    result_df$max_amount[i] <- calc[[1]]
    result_df$score_max_amount[i] <- calc[[2]]
    result_df$max_delay[i] <- as.numeric(calc[[3]])
    result_df$product_id[i] <- as.numeric(calc[[4]])
    result_df$max_installment[i] <- as.numeric(calc[[5]])
    result_df$days_delay[i] <- as.numeric(calc[[6]])
    result_df$office_id[i] <- as.numeric(calc[[7]])
    result_df$third_side[i] <- as.numeric(calc[[8]])
  }, error=function(e){}))
}


# Make final data frame
select <- select[,-which(names(select) %in% c("product_id"))]
select <- merge(select,result_df,by.x = "id",by.y = "id",all.x = TRUE)



#############################
### Apply filter criteria ###
#############################

# Select successful offers
select <- subset(select,!(is.na(select$score_max_amount)))
select <- select[!duplicated(select$id),]


# Subset based on current DPD
if(nrow(select[!is.na(select$days_delay) & select$days_delay>300,])>0){
  quit()
}


# Subset based on not real offices
select$ok_office <- ifelse(flag_real_office(select$office_id)==1,1,0)
if(nrow(select[!is.na(select$ok_office) & select$ok_office==0,])>0){
  quit()
}


# Subset based on if on third side
if(nrow(select[!is.na(select$third_side) & select$third_side==1,])>0){
  quit()
}


# Get number of terminated credits for the client
if(nrow(select)==0){
  quit()
}
get_actives_sql <- suppressWarnings(dbSendQuery(con, paste("
SELECT id, status, date, product_id, client_id
FROM ",db_name,".credits_applications 
WHERE client_id=",select$client_id,sep="")))
all_credits_id <- fetch(get_actives_sql,n=-1)


# Join company ID
company_id <- suppressWarnings(fetch(dbSendQuery(con, 
  gen_get_company_id_query(db_name)), n=-1))
all_credits_id <- merge(all_credits_id,company_id,by.x = "product_id",
                by.y = "id",all.x = TRUE)
all_credits_id <- subset(all_credits_id,all_credits_id$id!=application_id & 
  all_credits_id$status==5 & all_credits_id$company_id==select$company_id)


# Refilter according to aformentioned criteria
if(nrow(all_credits_id)>0){
  select$nb_criteria <- 1
} else {
  select$nb_criteria <- 0
}
select$filter_criteria <- ifelse(select$nb_criteria==0,0.5,
   ifelse(select$score_max_amount %in% c("Good 4"), 0.3,
   ifelse(select$score_max_amount %in% c("Good 3"), 0.4,
   ifelse(select$score_max_amount %in% c("Good 2"), 0.45,0.5))))
select <- subset(select,select$installment_ratio>=select$filter_criteria)


# Rework additional fields
select$amount_differential <- select$max_amount - select$credit_amount
select$max_amount <- ifelse(select$max_amount==-Inf,NA,select$max_amount)
select$next_amount_diff <- select$max_amount - select$left_to_pay


# Subset based on if next amount is higher than hitherto due amount
select <- subset(select,select$next_amount_diff>100)



#########################################################
### Work on final credit offer amount and write in DB ###
#########################################################

# Get minimum amount to offer
if(nrow(select)>0){
for (i in 1:nrow(select)){
  local <- products[products$product_id==select$product_id[i] & 
                    products$amount>=select$left_to_pay[i],]$amount
  select$min_amount[i] <- local[which.min(abs(local - select$left_to_pay[i]))]
}


# Set all credirect offers to one and only product
select$product_id <- ifelse(is.na(select$product_id),select$product_id,
                     ifelse(select$product_id==9,48,select$product_id))


# Write in Database
select$ref_application_id <- NA
select$status <- 1
select$processed_by <- NA
select$created_at <- Sys.time()
select$updated_at <- NA
select$deleted_at <- NA
select$max_amount_updated <- NA
select$max_installment_updated <- NA
select <- select[,c("id","product_id","min_amount","max_amount",
    "max_installment","max_amount_updated","max_installment_updated",
    "ref_application_id","status","processed_by",
    "created_at","updated_at","deleted_at")]
names(select)[1] <- "application_id"


# Replace NAs by NULLs
select[is.na(select)] <- "NULL"


# Make result ready for SQL query
string_sql <- gen_sql_string_po_refinance(select,1)


# Output real offers
update_prior_query <- paste("INSERT INTO ",db_name,
".prior_approval_refinances VALUES ",
string_sql,";", sep="")


# Write in database
  suppressMessages(suppressWarnings(dbSendQuery(con,update_prior_query)))
}}


