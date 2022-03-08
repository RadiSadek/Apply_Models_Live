

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
get_actives_sql <- paste("
SELECT id, status, date, product_id, client_id
FROM ",db_name,".credits_applications 
WHERE status in (4,5)",sep="")
all_credits <- gen_query(con,get_actives_sql)


# Join company ID
company_id <- gen_query(con, 
    gen_get_company_id_query(db_name))
all_credits <- merge(all_credits,company_id,by.x = "product_id",
    by.y = "id",all.x = TRUE)


# Select actives
select <- subset(all_credits,all_credits$status %in% c(4))


# Apply time window criteria
select$time_since <- round(difftime(as.Date(substring(Sys.time(),1,10)),
    select$date,units=c("days")),0)
select <- subset(select,select$time_since<=200)


# Remove credits with already an offer 
po_sql_query <- paste(
  "SELECT application_id, created_at, deleted_at, product_id, min_amount
   FROM ",db_name,".prior_approval_refinances",sep="")
po <- gen_query(con, po_sql_query)
po_raw <- po
select <- select[!(select$id %in% po$application_id),]


# Read daily installments and payments
data_sql_daily <- paste("
SELECT application_id, installment_num, discount_amount
FROM ",db_name,".credits_plan_main")
daily <- gen_query(con,data_sql_daily)
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
paid_install_sql <- paste("
SELECT application_id, COUNT(application_id) as installments_paid
FROM ",db_name,".credits_plan_main
WHERE payed_at IS NOT NULL AND pay_day<= '",substring(Sys.time(),1,10),
"' GROUP BY application_id;
", sep ="")
paid_install <- gen_query(con,paid_install_sql)
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


# Get final credit amount
credit_amount_sql <- paste("
SELECT application_id,final_credit_amount, amount as credit_amount
FROM ",db_name,".credits_plan_contract", sep ="")
credit_amount <- gen_query(con,credit_amount_sql)
select <- merge(select,credit_amount,by.x = "id",
   by.y = "application_id",all.x = TRUE)


# Get eventual taxes
taxes_sql <- paste("
SELECT application_id, amount, paid_amount 
FROM ",db_name,".credits_plan_taxes", sep ="")
taxes <- gen_query(con,taxes_sql)
taxes_raw <- taxes
taxes <- taxes[taxes$application_id %in% daily$application_id,]
taxes_agg <- aggregate(taxes$amount,
                       by=list(taxes$application_id),FUN=sum)
names(taxes_agg) <- c("application_id","tax_amount")


# Get discounts
discount_agg <- aggregate(daily_raw$discount_amount,
      list(daily_raw$application_id),FUN=sum)
names(discount_agg) <- c("application_id","discount_amount")


# Get all payments for each credit
paid <- gen_query(con, paste("
SELECT object_id, amount, pay_date 
FROM ",db_name,".cash_flow
WHERE nomenclature_id in (90,100,101) 
AND deleted_at IS NULL AND object_type=4",sep=""))
paid_raw <- paid
paid <- paid[paid$object_id %in% daily$application_id,]


# Get products periods and amounts of products
products <- gen_query(con, paste("
SELECT product_id, amount 
FROM ",db_name,".products_periods_and_amounts",sep=""))


# Get hitherto payments and ratios
paid_agg <- aggregate(paid$amount,by=list(paid$object_id),FUN=sum)
names(paid_agg) <- c("application_id","paid_hitherto")
select <- merge(select ,paid_agg,by.x = "id",
  by.y = "application_id",all.x = TRUE)
select <- merge(select,taxes_agg,by.x = "id",
  by.y = "application_id", all.x = TRUE)
select <- merge(select,discount_agg,by.x = "id",
  by.y = "application_id", all.x = TRUE)
select$paid_hitherto <- ifelse(is.na(select$paid_hitherto),0,
  select$paid_hitherto)
select$tax_amount <- ifelse(is.na(select$tax_amount),0,
 select$tax_amount)
select$discount_amount <- ifelse(is.na(select$discount_amount),0,
 select$discount_amount)
select$left_to_pay <- select$final_credit_amount + 
  select$tax_amount - select$paid_hitherto - select$discount_amount


# Check if client has still VIP status 
is_vip_query <- paste(
  "SELECT id, is_vip
  FROM ",db_name,".clients",sep="")
is_vip <- gen_query(con, is_vip_query)
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
select <- subset(select,select$days_delay<=300)


# Subset based on not real offices
select$ok_office <- ifelse(flag_real_office(select$office_id)==1,1,0)
select <- subset(select,select$ok_office==1)


# Subset based on if on third side 
select <- subset(select,select$third_side==0)


# Get number of terminated credits per client
all_credits$nb <- 1
agg_term_citycash <- aggregate(all_credits$nb[
      all_credits$status==5 & all_credits$company_id==1],
    by=list(all_credits$client_id[
      all_credits$status==5 & all_credits$company_id==1]),FUN=sum)
agg_term_credirect <- aggregate(all_credits$nb[
  all_credits$status==5 & all_credits$company_id==2],
  by=list(all_credits$client_id[
    all_credits$status==5 & all_credits$company_id==2]),FUN=sum)
select <- merge(select,agg_term_citycash,
               by.x = "client_id",by.y = "Group.1",all.x = TRUE)
select <- merge(select,agg_term_credirect,
               by.x = "client_id",by.y = "Group.1",all.x = TRUE)
names(select)[ncol(select)-1] <- "nb_term_citycash"
names(select)[ncol(select)] <- "nb_term_credirect"
select$nb_term_citycash <- ifelse(is.na(select$nb_term_citycash),0,
                                 select$nb_term_citycash)
select$nb_term_credirect <- ifelse(is.na(select$nb_term_credirect),0,
                                  select$nb_term_credirect)


# Refilter according to aformentioned 2 criteria
select$nb_criteria <- ifelse(select$company_id==1,select$nb_term_citycash,
   select$nb_term_credirect)
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
if(nrow(select)>1){
for(i in 2:nrow(select)){
  string_sql <- paste(string_sql,gen_sql_string_po_refinance(select,i),sep=",")
}}


# Output real offers
update_prior_query <- paste("INSERT INTO ",db_name,
".prior_approval_refinances VALUES ",
string_sql,";", sep="")


# Write in database
  suppressMessages(suppressWarnings(dbSendQuery(con,update_prior_query)))
}}



###################################
### Updating certain old offers ###
###################################

# Choose credits for updating
po_old <- po_raw
po_old <- subset(po_old,is.na(po_old$deleted_at))
po_old$time_past <- as.numeric(
  round(difftime(as.Date(substring(Sys.time(),1,10)),
  as.Date(substring(po_old$created_at,1,10)),units=c("days")),2))
po_old <- subset(po_old,po_old$time_past>0 & po_old$time_past<=360 &
  po_old$time_past%%30==0 & is.na(po_old$deleted_at))


# Remove credits which have izpadejirali
po_old$id <- po_old$application_id
for (i in 1:nrow(po_old)){
  po_old$last_padej[i] <- max(gen_query(con, 
      gen_plan_main_actives_past_query(db_name,po_old[i,]))$pay_day)
}
po_to_remove <- subset(po_old,
      po_old$last_padej<=as.Date(substring(Sys.time(),1,10)))
po_old <- po_old[!(po_old$application_id %in% po_to_remove$application_id),]


# Append score
po_old$max_amount <- NA
po_old$score_max_amount <- NA
po_old$max_installment <- NA
po_old$days_delay <- NA
po_old$office_id <- NA
po_old$third_side <- NA
for(i in 1:nrow(po_old)){
  suppressWarnings(tryCatch({
    application_id <- po_old$application_id[i]
    calc <- gen_refinance_fct(con,application_id,product_id)
    po_old$max_amount[i] <- calc[[1]]
    po_old$score_max_amount[i] <- calc[[2]]
    po_old$max_delay[i] <- as.numeric(calc[[3]])
    po_old$max_installment[i] <- as.numeric(calc[[5]])
    po_old$days_delay[i] <- as.numeric(calc[[6]])
    po_old$office_id[i] <- as.numeric(calc[[7]])
    po_old$third_side[i] <- as.numeric(calc[[8]])
  }, error=function(e){}))
}
po_old$ok_office <- ifelse(flag_real_office(po_old$office_id)==1,1,0)


# Change database
po_not_ok <- subset(po_old,is.infinite(po_old$max_amount) | 
  po_old$max_amount<po_old$min_amount | po_old$days_delay>300 |
  po_old$ok_office==0 | po_old$third_side==1)
po_ok <- po_old[!(po_old$application_id %in% po_not_ok$application_id),]

if(nrow(po_to_remove)>0){
  po_to_remove$max_amount <- -999
  po_not_ok_query <- paste("UPDATE ",db_name,
      ".prior_approval_refinances SET updated_at = '",
      substring(Sys.time(),1,19),"' WHERE application_id IN",
      gen_string_po_terminated(po_to_remove), sep="")
  suppressMessages(suppressWarnings(dbSendQuery(con,po_not_ok_query)))
  suppressMessages(suppressWarnings(dbSendQuery(con,
      gen_string_delete_po_refinance(po_to_remove,po_to_remove$max_amount,
      "max_amount_updated",db_name))))
}

if(nrow(po_not_ok)>0){
  po_not_ok$max_amount <- -999
  po_not_ok_query <- paste("UPDATE ",db_name,
      ".prior_approval_refinances SET updated_at = '",
      substring(Sys.time(),1,19),"' WHERE application_id IN",
      gen_string_po_terminated(po_not_ok), sep="")
  suppressMessages(suppressWarnings(dbSendQuery(con,po_not_ok_query)))
  suppressMessages(suppressWarnings(dbSendQuery(con,
      gen_string_delete_po_refinance (po_not_ok,po_not_ok$max_amount,
      "max_amount_updated",db_name))))
}

if(nrow(po_ok)>0){
  po_ok_query <- paste("UPDATE ",db_name,
    ".prior_approval_refinances SET updated_at = '",
    substring(Sys.time(),1,19),"' WHERE application_id IN",
    gen_string_po_terminated(po_ok), sep="")
  suppressMessages(suppressWarnings(dbSendQuery(con,po_ok_query)))
  suppressMessages(suppressWarnings(dbSendQuery(con,
    gen_string_delete_po_refinance (po_ok,po_ok$max_amount,"max_amount_updated",
    db_name))))
  suppressMessages(suppressWarnings(dbSendQuery(con,
    gen_string_delete_po_refinance (po_ok,po_ok$max_installment,
    "max_installment_updated",db_name))))
}


if(substring(Sys.time(),9,10) %in% c("01")){
  
  po_sql_query <- paste(
    "SELECT application_id, created_at, deleted_at, product_id, min_amount,
    max_amount, max_amount_updated, max_installment_updated
    FROM ",db_name,".prior_approval_refinances",sep="")
  po_all <- gen_query(con, po_sql_query)
  po_all <- subset(po_all,is.na(po_all$deleted_at))
  
  po_all_not_ok <- subset(po_all,po_all$max_amount_updated==-999)
  if(nrow(po_all_not_ok)>0){
    po_all_not_ok_query <- paste("UPDATE ",db_name,
      ".prior_approval_refinances SET status = 4, updated_at = '",
      substring(Sys.time(),1,19),"', deleted_at = '",
      paste(substring(Sys.time(),1,10),"04:00:00",sep=),"'
      WHERE application_id IN ",gen_string_po_refinance(po_all_not_ok), sep="")
    suppressMessages(suppressWarnings(dbSendQuery(con,po_all_not_ok_query)))
  }

  po_all <- subset(po_all,po_all$max_amount_updated!=-999)
  if(nrow(po_all)>0){
    po_all[is.na(po_all)] <- "NULL"
    po_change_query <- paste("UPDATE ",db_name,
      ".prior_approval_refinances SET updated_at = '",
      substring(Sys.time(),1,19),"' WHERE application_id IN",
      gen_string_po_refinance(po_all), sep="")
    suppressMessages(suppressWarnings(dbSendQuery(con,po_change_query)))
    suppressMessages(suppressWarnings(dbSendQuery(con,
      gen_string_delete_po_refinance(po_all,po_all$max_amount_updated,
      "max_amount",db_name))))
    suppressMessages(suppressWarnings(dbSendQuery(con,
      gen_string_delete_po_refinance(po_all,po_all$max_installment_updated,
      "max_installment",db_name))))
  }
}



#######################################################
### Check for special cases and deleted immediately ###
#######################################################

# Read special cases (deceased and gdrk marketing clients) 
get_special_sql <- paste("
SELECT id
FROM ",db_name,".clients
WHERE gdpr_marketing_messages=1 OR dead_at IS NOT NULL",sep="")
special <- gen_query(con,get_special_sql)

# Remove special cases if has offer
po_special_sql_query <- paste(
  "SELECT application_id, created_at
  FROM ",db_name,".prior_approval_refinances
  WHERE deleted_at IS NULL",sep="")
po_special <- gen_query(con, po_special_sql_query)
po_special <- merge(po_special,all_credits[,c("id","client_id")],
                    by.x = "application_id",by.y = "id",all.x = TRUE)
po_special <- po_special[po_special$client_id %in% special$id,]

if(nrow(po_special)>0){
  po_special_query <- paste("UPDATE ",db_name,
     ".prior_approval_refinances SET status = 4, updated_at = '",
     substring(Sys.time(),1,19),"', deleted_at = '",
     paste(substring(Sys.time(),1,10),"04:00:00",sep=),"'
     WHERE application_id IN ",gen_string_po_refinance(po_special), sep="")
  suppressMessages(suppressWarnings(dbSendQuery(con,po_special_query)))
}



#########
## END ##
#########

