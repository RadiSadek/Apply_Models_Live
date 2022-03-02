
##################################
######## Define SQL queries ######
##################################

# Define big query which reads from credits_applications
gen_big_sql_query <- function(db_name,application_id){
big_sql_query <- paste("SELECT 
",db_name,".credits_applications_clients.application_id, 
",db_name,".credits_applications_clients.ownership,
",db_name,".credits_applications_clients.education,
",db_name,".credits_applications_clients.egn,
",db_name,".credits_applications_clients.household_children,
",db_name,".credits_applications_clients.household_total,
",db_name,".credits_applications_clients.on_address,
",db_name,".credits_applications_clients.marital_status,
",db_name,".credits_applications_clients.has_viber,
",db_name,".credits_applications_data_other.purpose,
",db_name,".credits_applications_clients_work.experience_employer,
",db_name,".credits_applications_clients_work.status AS status_work,
",db_name,".credits_plan_contract.amount,
",db_name,".credits_plan_contract.installments,
",db_name,".credits_applications.signed_at,
",db_name,".credits_applications.created_at,
",db_name,".credits_applications.client_id,
",db_name,".credits_applications.office_id,
",db_name,".credits_applications.status,
",db_name,".credits_applications.sub_status,
",db_name,".credits_applications.product_id,
",db_name,".credits_applications.deactivated_at,
",db_name,".credits_applications.third_side_date
FROM ",db_name,".credits_applications_clients
LEFT JOIN ",db_name,".credits_applications_data_other
ON ",db_name,".credits_applications_clients.application_id = ",db_name,
".credits_applications_data_other.application_id
LEFT JOIN ",db_name,".credits_applications_clients_work
ON ",db_name,".credits_applications_clients.application_id = ",db_name,
".credits_applications_clients_work.application_id
LEFT JOIN ",db_name,".credits_plan_contract
ON ",db_name,".credits_applications_clients.application_id = ",db_name,
".credits_plan_contract.application_id
LEFT JOIN ",db_name,".credits_applications
ON ",db_name,".credits_applications_clients.application_id = ",db_name,
".credits_applications.id
WHERE credits_applications_clients.application_id=", application_id, sep="")
return(big_sql_query)
}

# Define query for products periods and amounts
gen_products_query <- function(db_name,all_df){
  return(paste("SELECT * FROM ", db_name, ".products_periods_and_amounts
               WHERE product_id IN (",
               all_df$product_id, ")", sep=""))
}

# Define query for products 
gen_products_query_desc <- function(db_name,all_df){
  return(paste("SELECT id, period, brand_id AS company_id FROM ", db_name, 
  ".products WHERE id=", all_df$product_id, sep=""))
}

# Define query for income
gen_income_sql_query <- function(db_name,application_id){
  return(paste("SELECT application_id, amount, sub_type 
  FROM ",db_name,".credits_applications_clients_money_income 
  WHERE deleted_at IS NULL AND application_id=",all_df$application_id, sep=""))
}

# Define query for normal expenses 
gen_expenses_sql_query <- function(db_name,all_df){
  return(paste("SELECT application_id, amount 
  FROM ",db_name,".credits_applications_clients_money_expense
  WHERE deleted_at IS NULL AND application_id=",all_df$application_id, sep=""))
}

# Define query for expenses for loans 
gen_loans_sql_query <- function(db_name,all_df){
  return(paste("SELECT application_id, installment
  FROM ",db_name,".credits_applications_clients_money_loans
  WHERE deleted_at IS NULL AND application_id=",all_df$application_id, sep=""))
}

# Define query for getting all credits for client 
gen_all_credits_query <- function(db_name,all_df){
  return(paste("SELECT id, client_id, signed_at, created_at, 
  deactivated_at, status, sub_status, product_id
  FROM ",db_name,".credits_applications 
  WHERE client_id=",all_df$client_id, sep =""))
}

# Define query to get if client is defined as risky 
gen_risky_query <- function(db_name,all_df){
  return(paste("SELECT egn
  FROM ",db_name,".clients_risk
  WHERE egn=",all_df$egn, " AND forbidden_credit_approval = 1", sep =""))
}

# Define query to get total amount of current application amount
gen_total_amount_curr_query <- function(db_name,application_id){
  return(paste("SELECT final_credit_amount
  FROM ",db_name,".credits_plan_contract 
  WHERE application_id=", application_id, sep =""))
}

# Define query to get the pay days of previous actives credits
gen_plan_main_actives_past_query <- function(db_name,string_id){
  return(paste("SELECT application_id, pay_day
  FROM ",db_name,".credits_plan_main WHERE application_id in(", 
  string_id," )", sep=""))
}

# Define query to get total amount of current application amount
gen_prev_amount_query <- function(db_name,all_id){
  return(paste("SELECT amount FROM ",db_name,
  ".credits_plan_contract WHERE application_id=", 
  all_id$id[nrow(all_id)-1], sep=""))
}

# Define query to get the maximum of delay days from previous credits
gen_plan_main_select_query <- function(db_name,list_ids_max_delay){
  return(paste("SELECT MAX(days_delay) AS max_delay FROM ",db_name, 
  ".credits_plan_main WHERE application_id in(",list_ids_max_delay," 
  )", sep=""))
}

# Define query to get paid amount on last day of previous credit
gen_last_paid_amount_query <- function(var,db){
  return(paste("SELECT a.object_id, a.pay_date as last_pay_date, a.amount 
  FROM ",db,".cash_flow a INNER JOIN
  (SELECT object_id, MAX(pay_date) AS pay_date
  FROM ",db,".cash_flow
  WHERE nomenclature_id IN (90,100,101) AND deleted_at IS NULL 
  AND object_type = 4 AND object_id=",var,") AS b
  ON a.object_id=b.object_id AND a.pay_date=b.pay_date", sep =""))
}

# Define query to get last paid amount of previous credit 
gen_paid_amount_query <- function(var,db){		
  return(paste("SELECT object_id, amount 		
  FROM ",db,".cash_flow		
  WHERE nomenclature_id IN (90,100,101) AND deleted_at 
  IS NULL AND object_type = 4	AND object_id=",var, sep =""))		
}

# Define query to get total paid amount of previous credit 
gen_total_paid_amount_query <- function(var,db){		
  return(paste("SELECT object_id, SUM(amount) 		
  FROM ",db,".cash_flow		
  WHERE nomenclature_id IN (90,100,101) AND deleted_at 
  IS NULL AND object_type = 4	AND object_id=",var,
  " GROUP BY object_id",sep =""))		
}

# Define query to get all payments of previous credit 
gen_all_payments_query <- function(var,db){		
  return(paste("SELECT object_id, amount, pay_date	
  FROM ",db,".cash_flow		
  WHERE nomenclature_id IN (90,100,101) AND deleted_at 
  IS NULL AND object_type = 4	AND object_id=",var,sep =""))		
}

# Define query to get the last amount of previous credit
gen_last_cred_amount_query <- function(var,db){
  return(paste("SELECT final_credit_amount, installments, amount
  FROM ",db,".credits_plan_contract 
  WHERE application_id=", var, sep =""))
}

# Define query to get expenses according to city
gen_address_query <- function(var,arg){
  return(paste("SELECT 
    c.household, c.child, c.pensioner
    FROM ",db_name,".addresses AS a
    RIGHT JOIN ",db_name,".cities AS c ON c.id=a.city_id
    WHERE a.addressable_type = '",arg,"'
    AND a.addressable_id = ",var," AND a.type = 2", sep =""))
}

# Define query to get coordinates of address (per application)
gen_address_coordinates_query <- function(db_name,application_id){
  return(paste("SELECT lat, lon, type, location_precision
  FROM ",db_name,".addresses 
  WHERE addressable_type=
  'App\\\\Models\\\\Credits\\\\Applications\\\\Application'
  AND addressable_id=",application_id," AND type IN (1,2,3)",sep =""))
}

# Define query to get coordinates of address (per client)
gen_address_client_coordinates_query <- function(db_name,all_df){
  return(paste("SELECT lat, lon, type, location_precision
  FROM ",db_name,".addresses 
  WHERE addressable_type=
  'App\\\\Models\\\\Clients\\\\Client'
  AND addressable_id=",all_df$client_id," AND type IN (1,2,3)",sep =""))
}


# Define query to get company id (credirect or city cash) 
gen_get_company_id_query <- function(db_name){
  return(get_company_id_query <- paste("SELECT id, brand_id AS 
     company_id FROM ", db_name,".products", sep=""))
}

# Define query to get the CKR status 
gen_query_ckr <- function(all_df,all_credits,type_of){
  names_col <- c("current_status_active","status_active","status_finished",
                 "source_entity_count","amount_drawn","cred_count", 
                 "outstanding_performing_principal",
                 "outstanding_overdue_principal","amount_cession")
  query_ckr <- paste("SELECT 
  ",db_name,".clients_ckr_files.client_id,
  ",db_name,".clients_ckr_files.created_at,
  ",db_name,".clients_ckr_files.application_id,
  ",db_name,".clients_ckr_files_data.current_status_active, 
  ",db_name,".clients_ckr_files_data.status_active, 
  ",db_name,".clients_ckr_files_data.status_finished,
  ",db_name,".clients_ckr_files_data.source_entity_count,
  ",db_name,".clients_ckr_files_data.amount_drawn,
  ",db_name,".clients_ckr_files_data.cred_count,
  ",db_name,".clients_ckr_files_data.outstanding_performing_principal,
  ",db_name,".clients_ckr_files_data.outstanding_overdue_principal,	
  ",db_name,".clients_ckr_files_data.amount_cession
  FROM ",db_name,".clients_ckr_files_data
  INNER JOIN ",db_name,".clients_ckr_files
  ON ",db_name,".clients_ckr_files.id=",db_name,".clients_ckr_files_data.file_id
  WHERE ",db_name,".clients_ckr_files_data.type=",type_of," AND ",db_name,
                     ".clients_ckr_files.client_id=",all_df$client_id, sep ="")
  result_df <- suppressWarnings(dbFetch(dbSendQuery(con, query_ckr)))
  # result_df <- merge(result_df, all_credits[,c("id","date")], 
  #                    by.x = "application_id",
  #                    by.y = "id", all.x = TRUE)
  if(nrow(result_df)==0){
    empty_df <- as.data.frame(cbind(NA,NA,NA,NA,NA,NA,NA,NA,NA))
    names(empty_df) <- names_col
    return(empty_df)
  } else {
    result_df$date_curr <- all_df$date
    result_df$date_diff <- difftime(result_df$date_curr, result_df$created_at, 
                                    units=c("days"))
    result_df <- result_df[order(result_df$date_diff),]
    result_final <- as.data.frame(matrix(nrow = 1, 
                                         ncol = ncol(result_df)))
    names(result_final) <- names(result_df)
    for(i in 1:ncol(result_final)){
      result_final[1,i] <- result_df[which(!is.na(result_df[,i]))[1],i]
    }
    return(result_final[,names_col])}
}

# Define query for SEON phone variables
gen_seon_phones_query <- function(db_name,criteria,var){
  return(paste(
"SELECT b.registered 
FROM ",db_name,".seon_requests a
JOIN ",db_name,".seon_requests_accounts b
ON a.id=b.requests_id
WHERE a.type=1 AND b.type=",criteria," AND application_id=",var,
sep=""))
}

# Define query to get max installment amount per application id
gen_max_pmt_main <- function(db_name,id){
  return(paste(
"SELECT max(pmt_final) AS max_pmt
FROM ",db_name,".credits_plan_main
WHERE application_id=",id,sep=""))
}

# Define query to get passed installments at time of choice
gen_passed_install_before_query <- function(db_name,id,time_choice){
  return(paste(
"SELECT COUNT(application_id) as passed_installments
FROM ",db_name,".credits_plan_main 
WHERE application_id=",id," AND pay_day<='",time_choice,"'",sep=""))
}

# Define query to get passed and paid installments at time of choice
gen_passed_paid_install_before_query <- function(db_name,id,time_choice){
  return(paste(
"SELECT COUNT(application_id) as passed_installments
FROM ",db_name,".credits_plan_main 
WHERE payed_at IS NOT NULL AND application_id=",id," AND pay_day<='",
time_choice,"'",sep=""))
}

# Read PO terminated data per client_id
gen_po_terminated_query <- function(db_name,input){
  return(paste(
    "SELECT id, client_id, application_id, credit_amount, installment_amount,
     deleted_at, created_at, product_id 
     FROM ",db_name,".clients_prior_approval_applications 
     WHERE client_id=",input,
    sep=""))
}

# Read PO refinance data per client_id
gen_po_refinance_query <- function(db_name,input){
  return(paste(
    "SELECT application_id, max_amount, max_installment,
     deleted_at, product_id
     FROM ",db_name,".prior_approval_refinances 
     WHERE deleted_at IS NOT NULL AND application_id IN (", 
     input,")",sep=""))
}

# Read PO refinance data per client_id
gen_flag_is_dead <- function(db_name,input){
  return(paste(
    "SELECT dead_at
     FROM ",db_name,".clients
     WHERE id=",input,sep=""))
}


# Read PO refinance data per client_id
gen_flag_gdpr <- function(db_name,input){
  return(paste(
    "SELECT gdpr_marketing_messages
     FROM ",db_name,".clients
     WHERE id=",input,sep=""))
}

# Get discount amount per application_id
gen_discount_amount <- function(db_name,input){
  return(paste(
    "SELECT SUM(discount_amount) AS discount_amount
     FROM ",db_name,".credits_plan_main
     WHERE application_id=",input,sep=""))
}

# Get taxes per credit
gen_taxes_amount <- function(db_name,input){
  return(paste(
    "SELECT SUM(amount) AS tax_amount
     FROM ",db_name,".credits_plan_taxes
     WHERE tax_id NOT IN (4,22) AND 
     application_id=",input,sep=""))
}

# Read ACTIVE PO refinance data per client_id
gen_po_active_refinance_query <- function(db_name,input){
  return(paste(
    "SELECT application_id, max_amount, deleted_at, product_id
     FROM ",db_name,".prior_approval_refinances 
     WHERE deleted_at IS NULL AND application_id IN (", 
    input,")",sep=""))
}

# Read all phone numbers
gen_get_phone_numbers <- function(db_name){
  return(paste(
    "SELECT client_id 
    FROM ",db_name,".clients_phones 
    WHERE number IN (SELECT number
    FROM ",db_name,".clients_phones
    WHERE client_id=",all_df$client_id,")",sep=""))
}

# Read all emails
gen_get_email <- function(db_name){
  return(paste(
    "SELECT id 
    FROM ",db_name,".clients 
    WHERE email IN (SELECT email
    FROM ",db_name,".clients
    WHERE id=",all_df$client_id,")",sep=""))
}

# Get if office is self approval 
gen_self_approval_office_query <- function(db_name,input){
  return(paste(
    "SELECT self_approve
     FROM ",db_name,".structure_offices 
     WHERE id IN (", 
    input,")",sep=""))
}

# Get API data
gen_api_data <- function(db_name,application_id){
  return(paste(
    "SELECT CAST(payload AS CHAR)
     FROM ",db_name,".api_credits_applications
     WHERE application_id IN (", 
    application_id,")",sep=""))
}

