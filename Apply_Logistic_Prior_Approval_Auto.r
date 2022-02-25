

################################################################################
#         Script for generating  daily offers for terminated credits           #
#      Apply Logistic Regression on all products (CityCash and Credirect)      #
#                          Version 1.0 (2020/06/23)                            #
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
source(file.path(base_dir,"Useful_Functions.r"))
source(file.path(base_dir,"SQL_queries.r"))
source(file.path(base_dir,"Terminated.r"))



###################################################
### Generate data of potential credits to offer ###
###################################################

# Read credit applications 
get_actives_sql <- suppressWarnings(dbSendQuery(con, paste("
SELECT id, status, date, signed_at, product_id, client_id, 
deactivated_at, sub_status, office_id, consultant_id
FROM ",db_name,".credits_applications 
WHERE status IN (4,5)",sep="")))
all_credits <- fetch(get_actives_sql,n=-1)


# Get company ID
company_id <- suppressWarnings(fetch(dbSendQuery(con, 
    gen_get_company_id_query(db_name)), n=-1))
all_credits <- merge(all_credits,company_id,by.x = "product_id",
    by.y = "id",all.x = TRUE)


# Subset terminated credits
all_credits_raw <- all_credits
all_credits <- subset(all_credits,all_credits$status %in% c(5))
all_credits <- subset(all_credits, is.na(all_credits$sub_status) | 
  all_credits$sub_status %in% c(123,128))


# Subset based on time difference since deactivation
all_credits <- rbind(
  subset(all_credits,all_credits$company_id==2 & 
    substring(all_credits$deactivated_at,1,10)==(as.Date(Sys.time())-2)),
  subset(all_credits,all_credits$company_id==1 & all_credits$sub_status==128 & 
    substring(all_credits$deactivated_at,1,10)==(as.Date(Sys.time())-3)),
  subset(all_credits,all_credits$company_id==1 & all_credits$sub_status==123 & 
    substring(all_credits$deactivated_at,1,10)==(as.Date(Sys.time())-4)))


# Get last credit amount
credit_amount_sql <- suppressWarnings(dbSendQuery(con, paste("
SELECT application_id , amount as credit_amount
FROM ",db_name,".credits_plan_contract", sep ="")))
credit_amount <- fetch(credit_amount_sql,n=-1)
all_credits <- merge(all_credits,credit_amount,by.x = "id",
                     by.y = "application_id",all.x = TRUE)
all_credits <- all_credits[rev(order(all_credits$date)),]
all_credits <- all_credits[order(all_credits$client_id), ]
all_credits <- all_credits[!duplicated(all_credits$client_id),]



#####################################################
### Apply selection criteria for credits to offer ###
#####################################################

# Remove credits with more than 1 "varnat"
all_credits_varnat <- subset(all_credits_raw,all_credits_raw$sub_status==122)
all_credits_varnat$count <- 1
count_varnat <- as.data.frame(aggregate(all_credits_varnat$count,
    by=list(all_credits_varnat$client_id),FUN=sum))
names(count_varnat) <- c("client_id","tot_varnat")
all_credits <- merge(all_credits,count_varnat,by.x = "client_id",
                     by.y = "client_id",all.x = TRUE)
select_credits  <- subset(all_credits,is.na(all_credits$tot_varnat) | 
                          all_credits$tot_varnat<2)


# Remove credits with already an offer of corresponding company
po_sql_query <- paste(
  "SELECT id, client_id, product_id, application_id, created_at, credit_amount,
  installment_amount,deleted_at,updated_at
  FROM ",db_name,".clients_prior_approval_applications",sep="")
po <- suppressWarnings(fetch(dbSendQuery(con, po_sql_query), n=-1))
po <- merge(po,company_id,by.x = "product_id",by.y = "id",all.x = TRUE)
po_raw <- po
po_select_citycash <- po[is.na(po$deleted_at) & po$company_id==1,]
po_select_credirect <- po[is.na(po$deleted_at) & po$company_id==2,]
select_credits_citycash <- select_credits[!(select_credits$client_id %in% 
    po_select_citycash$client_id) & select_credits$company_id==1,]
select_credits_credirect <- select_credits[!(select_credits$client_id %in% 
    po_select_credirect$client_id) & select_credits$company_id==2,]
select_credits <- rbind(select_credits_citycash,select_credits_credirect)


# Remove those who have active credit of corresponding company
actives_cur_citycash <- subset(all_credits_raw,all_credits_raw$status %in% c(4)
   & all_credits_raw$company_id==1)
actives_cur_credirect <- subset(all_credits_raw,all_credits_raw$status %in% c(4)
   & all_credits_raw$company_id==2)
select_credits_citycash <- subset(select_credits,
   !(select_credits$client_id %in% actives_cur_citycash$client_id) & 
     select_credits$company_id==1)
select_credits_credirect <- subset(select_credits,
   !(select_credits$client_id %in% actives_cur_credirect$client_id) & 
     select_credits$company_id==2)
select_credits <- rbind(select_credits_citycash,select_credits_credirect)


# Join if VIP
is_vip_query <- paste(
  "SELECT id, is_vip
  FROM ",db_name,".clients",sep="")
is_vip <- suppressWarnings(fetch(dbSendQuery(con, is_vip_query), n=-1))
select_credits <- merge(select_credits,is_vip,by.x = "client_id",
  by.y = "id", all.x = TRUE)


# Remove Big Fin and other Ipoteki
select_credits <- subset(select_credits,!(select_credits$product_id %in%
   c(12,13,59:65,53,54,51,22)))


#####################
### Compute score ###
#####################

if(nrow(select_credits)>0){

# Compute and append score
select_credits <- select_credits[!duplicated(select_credits$client_id),]
select_credits$max_amount <- NA
select_credits$max_installment_amount <- NA
select_credits$score_max_amount <- NA
select_credits$max_delay <- NA
for(i in 1:nrow(select_credits)){
  suppressWarnings(tryCatch({
    if(select_credits$product_id[i]==8 & select_credits$is_vip[i]==0){
      product_id <- 5
    } else {
      product_id <- NA
    }
    client_id <- select_credits$client_id[i]
    last_id <- select_credits$id[i]
    calc <- gen_terminated_fct(con,client_id,product_id,last_id,0)
    select_credits$max_amount[i] <- calc[[1]]
    select_credits$max_installment_amount[i] <- calc[[2]]
    select_credits$score_max_amount[i] <- calc[[3]]
    select_credits$max_delay[i] <- as.numeric(calc[[4]])
  }, error=function(e){}))
}



##################################
### Reapply selection criteria ###
##################################

# Select based on score and DPD
select_credits <- subset(select_credits,select_credits$max_amount>-Inf & 
                           select_credits$max_amount<Inf)
select_credits <- subset(select_credits,select_credits$max_delay<=200)



#############################################
### Work on final dataset and write in DB ###
#############################################

# Read current database
id_max <- max(po$id)+1

if(nrow(select_credits)>0){

# Create final dataframe for writing in DB
offers <- select_credits
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
if(nrow(offers)>1){
  for(i in 2:nrow(offers)){
    string_sql <- paste(string_sql,gen_sql_string_po_terminated(offers,i),
                        sep=",")
  }
}


# Output real offers
update_prior_query <- paste("INSERT INTO ",db_name,
".clients_prior_approval_applications VALUES ",
string_sql,";", sep="")


# Write in database
if(nrow(offers)>0){
  suppressMessages(suppressWarnings(dbSendQuery(con,update_prior_query)))
}}}


###################################
### Updating certain old offers ###
###################################

# Choose credits for updating
po_old <- po_raw
po_old$time_past <- as.numeric(
  round(difftime(as.Date(substring(Sys.time(),1,10)),
  as.Date(substring(po_old$created_at,1,10)),units=c("days")),2))
po_old <- subset(po_old,po_old$time_past>0 & po_old$time_past<=360 &
  po_old$time_past%%30==0 & is.na(po_old$deleted_at))


# See if any new credit created after offer
po_old <- merge(po_old,gen_if_credit_after_po_terminated(
  all_credits_raw,po_old,"last_appl_id_citycash",1),by.x = "client_id",
  by.y = "client_id",all.x = TRUE) 
po_old <- merge(po_old,gen_if_credit_after_po_terminated(
  all_credits_raw,po_old,"last_appl_id_credirect",2),by.x = "client_id",
  by.y = "client_id",all.x = TRUE) 
po_old$last_id <- ifelse(po_old$company_id==1,po_old$last_appl_id_citycash,
    po_old$last_appl_id_credirect)
po_old <- po_old[,-which(names(po_old) %in% c("last_appl_id_credirect",
                "last_appl_id_citycash"))]
po_old$criteria <- ifelse(po_old$company_id==1,
                   ifelse(po_old$signed_at.x>=po_old$created_at,0,1),
                   ifelse(po_old$signed_at.y>=po_old$created_at,0,1))
po_old <- subset(po_old,po_old$criteria==1)


# Join if VIP
is_vip <- suppressWarnings(fetch(dbSendQuery(con, is_vip_query), n=-1))
po_old <- merge(po_old,is_vip,by.x = "client_id",
  by.y = "id", all.x = TRUE)


# Update scoring to selected credits
for(i in 1:nrow(po_old)){
  suppressWarnings(tryCatch({
  if(po_old$product_id[i]==8 & po_old$is_vip[i]==0){
    product_id <- 5
  } else {
    product_id <- NA
  }
  client_id <- po_old$client_id[i]
  last_id <- po_old$last_id[i]
  calc <- gen_terminated_fct(con,client_id,product_id,last_id,0)
  po_old$credit_amount[i] <- calc[[1]]
  po_old$installment_amount[i] <- calc[[2]]
  po_old$max_delay[i] <- as.numeric(calc[[4]])
  }, error=function(e){}))
}


# Change database
po_not_ok <- subset(po_old,is.infinite(po_old$credit_amount))
po_ok <- subset(po_old,!(is.infinite(po_old$credit_amount)))

if(nrow(po_not_ok)>0){
  po_not_ok$credit_amount <- -999
  po_not_ok$installment_amount <- -999
  po_ok_not_query <- paste("UPDATE ",db_name,
       ".clients_prior_approval_applications SET updated_at = '",
       substring(Sys.time(),1,19),"' WHERE id IN",
       gen_string_po_terminated(po_not_ok), sep="")
  suppressMessages(suppressWarnings(dbSendQuery(con,po_ok_not_query)))
  suppressMessages(suppressWarnings(dbSendQuery(con,
     gen_string_delete_po_terminated(po_not_ok,po_not_ok$credit_amount,
     "credit_amount_updated",db_name))))
  suppressMessages(suppressWarnings(dbSendQuery(con,
     gen_string_delete_po_terminated(po_not_ok,po_not_ok$installment_amount,
     "installment_amount_updated",db_name))))
  po_not_ok_credirect <- subset(po_not_ok,po_not_ok$company_id==2)
  if(nrow(po_not_ok_credirect)>0){
    suppressMessages(suppressWarnings(dbSendQuery(con,
       gen_string_delete_po_terminated(po_not_ok_credirect,
        po_not_ok_credirect$credit_amount,"credit_amount",db_name))))
    suppressMessages(suppressWarnings(dbSendQuery(con,
       gen_string_delete_po_terminated(po_not_ok_credirect,
       po_not_ok_credirect$installment_amount,"installment_amount",db_name))))
  }
}

if(nrow(po_ok)>0){
  po_ok_query <- paste("UPDATE ",db_name,
       ".clients_prior_approval_applications SET updated_at = '",
       substring(Sys.time(),1,19),"' WHERE id IN",
       gen_string_po_terminated(po_ok), sep="")
  suppressMessages(suppressWarnings(dbSendQuery(con,po_ok_query)))
  suppressMessages(suppressWarnings(dbSendQuery(con,
    gen_string_delete_po_terminated(po_ok,po_ok$credit_amount,
    "credit_amount_updated",db_name))))
  suppressMessages(suppressWarnings(dbSendQuery(con,
    gen_string_delete_po_terminated(po_ok,po_ok$installment_amount,
    "installment_amount_updated",db_name))))
  po_ok_credirect <- subset(po_ok,po_ok$company_id==2)
  if(nrow(po_ok_credirect)>0){
    suppressMessages(suppressWarnings(dbSendQuery(con,
       gen_string_delete_po_terminated(po_ok_credirect,
       po_ok_credirect$credit_amount,"credit_amount",db_name))))
    suppressMessages(suppressWarnings(dbSendQuery(con,
       gen_string_delete_po_terminated(po_ok_credirect,
       po_ok_credirect$installment_amount,"installment_amount",db_name))))
  }
}


# Update at beginning of month for City Cash
if(substring(Sys.time(),9,10)=="01"){
  
  po_sql_query <- paste(
    "SELECT id, credit_amount, updated_at, installment_amount, product_id, 
  created_at, credit_amount_updated,installment_amount_updated, deleted_at
  FROM ",db_name,".clients_prior_approval_applications
  WHERE deleted_at IS NULL",sep="")
  po_all <- suppressWarnings(fetch(dbSendQuery(con,po_sql_query),n=-1))
  po_all <- merge(po_all,company_id,by.x = "product_id",
    by.y = "id",all.x = TRUE)
  po_all <- subset(po_all,po_all$company_id==1)

  po_all_not_ok <- subset(po_all,po_all$credit_amount_updated==-999)
  if(nrow(po_all_not_ok)>0){
    po_all_not_ok_query <- paste("UPDATE ",db_name,
      ".clients_prior_approval_applications SET updated_at = '",
      substring(Sys.time(),1,19),"', deleted_at = '",
      paste(substring(Sys.time(),1,10),"04:00:00",sep=),"'
      WHERE id IN",gen_string_po_terminated(po_all_not_ok), sep="")
    suppressMessages(suppressWarnings(dbSendQuery(con,po_all_not_ok_query)))
  }
  
  po_all <- subset(po_all,po_all$credit_amount_updated!=-999)
  if(nrow(po_all)>0){
    po_change_query <- paste("UPDATE ",db_name,
      ".clients_prior_approval_applications SET updated_at = '",
      substring(Sys.time(),1,19),"' WHERE id IN",
      gen_string_po_terminated(po_all), sep="")
    suppressMessages(suppressWarnings(dbSendQuery(con,po_change_query)))
    suppressMessages(suppressWarnings(dbSendQuery(con,
      gen_string_delete_po_terminated(po_all,po_all$credit_amount_updated,
      "credit_amount",db_name))))
    suppressMessages(suppressWarnings(dbSendQuery(con,
      gen_string_delete_po_terminated(po_all,po_all$installment_amount_updated,
      "installment_amount",db_name))))
  }
}


#######################################################
### Check for special cases and deleted immediately ###
#######################################################

# Read special cases (deceased and gdrk marketing clients) 
get_special_sql <- suppressWarnings(dbSendQuery(con, paste("
SELECT id
FROM ",db_name,".clients
WHERE gdpr_marketing_messages=1 OR dead_at IS NOT NULL",sep="")))
special <- fetch(get_special_sql,n=-1)

# Remove special cases if has offer
po_get_special_query <- paste(
  "SELECT id, client_id
  FROM ",db_name,".clients_prior_approval_applications
  WHERE deleted_at IS NULL",sep="")
po_special <- suppressWarnings(fetch(dbSendQuery(con, po_get_special_query), 
                                     n=-1))
po_special <- po_special[po_special$client_id %in% special$id,]

if(nrow(po_special)>0){
  po_special_query <- paste("UPDATE ",db_name,
   ".clients_prior_approval_applications SET updated_at = '",
   substring(Sys.time(),1,19),"', deleted_at = '",
   paste(substring(Sys.time(),1,10),"04:00:00",sep=),"'
   WHERE id IN",gen_string_po_terminated(po_special), sep="")
  suppressMessages(suppressWarnings(dbSendQuery(con,po_special_query)))
}



###########
### End ###
###########

