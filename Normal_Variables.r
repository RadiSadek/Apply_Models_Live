
########################################
######## Define normal functions  ######
########################################

# Function to generate certain fields
gen_norm_var <- function(period,all_df,products,criteria_age){
  
  all_df$maturity <- ifelse(period==1, all_df$installments*7/30,
     ifelse(period==2, all_df$installments*14/30, all_df$installments))
  all_df$gender <- ifelse(substring(all_df$egn,9,9) %in% c(0,2,4,6,8), 1, 2)
  all_df$installment_amount <- products[
     products$period == unique(products$period)
     [which.min(abs(all_df$installments - unique(products$period)))] & 		
     products$amount == unique(products$amount)
     [which.min(abs(all_df$amount - unique(products$amount)))] & 		
     products$product_id == all_df$product_id, ]$installment_amount
  if(!(substring(all_df$egn,3,3) %in% c("5","4")) & 
     (substring(all_df$egn,1,2) %in% c("00","01","02","03","04","05","06"))){
    all_df$dob <- NA
    all_df$age <- 18
  } else{
    all_df$dob <- as.Date(ifelse(as.character(substring(all_df$egn,1,2)) %in% 
      c("00","01","02","03","04","05","06","07","08","09","10","11","12",
        "13","14","15"),
      as.Date(paste("20",substring(all_df$egn,1,2),"-",
      (as.numeric(substring(all_df$egn,3,3))-4),
      substring(all_df$egn,4,4),"-",substring(all_df$egn,5,6),sep="")),
      ifelse(as.character(substring(all_df$egn,1,2)) %in% c("10","11"), NA,
      as.Date(paste("19",substring(all_df$egn,1,2),"-",
      substring(all_df$egn,3,4),"-",substring(all_df$egn,5,6), sep="")))), 
        origin="1970-01-01")
    if(criteria_age==1){
      date_ref <- all_df$date
    } else {
      date_ref <- Sys.time()
    }
    all_df$age <- ifelse(is.na(all_df$dob), 18, 
        floor(as.numeric(difftime(date_ref, all_df$dob, 
        units=c("days"))/365.242)))}
  
  return(all_df)
}

# Function to regenerate certain functions
gen_norm_var2 <- function(df){
  df$gender <- ifelse(df$gender==2,0,1)
  df$maturity <- ifelse(df$maturity>=20,20,df$maturity)
  df$purpose <- ifelse(df$purpose==0, NA, df$purpose)
  df$age <- ifelse(df$age>90,89,
       ifelse(df$age<18,18, df$age))
  df$days_diff_last_credit <- ifelse(df$days_diff_last_credit<0, 0, 
       df$days_diff_last_credit)
  return(df)
}

# Function to generate SEON variables
gen_seon_phones <- function(db_name,criteria,var){
  return(suppressWarnings(dbFetch(dbSendQuery(con, 
  gen_seon_phones_query(db_name,criteria,var)))))
}


