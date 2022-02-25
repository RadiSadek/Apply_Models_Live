
##############################################################################
######## Functions to apply logisit regression on application Credirect ######
##############################################################################

gen_app_credirect_payday <- function(df,scoring_df,products,
  df_Log_Credirect_App_payday,period,all_df,prev_amount,amount_tab,
  t_income,disposable_income_adj,flag_credit_next_salary,api_df){
  
  # Cut and bin
  df$age <- ifelse(df$age<=20,"20_less",
                       ifelse(df$age<=23,"21_23","more_24"))
  df$age <- as.factor(df$age)
  
  df$gender <- as.factor(df$gender)
  
  df$ownership <- ifelse(df$ownership %in% c(1), "1", 
      ifelse(is.na(df$ownership), "not_1", "not_1"))
  df$ownership <- as.factor(df$ownership)
  
  df$education <- ifelse(is.na(df$education),"2",
      ifelse(df$education==1,"1",
      ifelse(df$education %in% c(3,4), "3_4", "2")))
  df$education <- as.factor(df$education)
  
  df$status_work <- ifelse(is.na(df$status_work), "other",
      ifelse(df$status_work %in% c(1,4,5,9,10,12), "1_4_5_9_10_12","other"))
  df$status_work <- as.factor(df$status_work)
  
  df$experience_employer <- ifelse(is.na(df$experience_employer), "0_12",
      ifelse(df$experience_employer<=12,"0_12",
      ifelse(df$experience_employer<=120,"13_120","more_120")))
  df$experience_employer <- as.factor(df$experience_employer)
  
  df$purpose <- ifelse(is.na(df$purpose), "other",
      ifelse(df$purpose %in% c(2,5),"2_5","other"))
  df$purpose <- as.factor(df$purpose)
  
  df$cred_count_total <- ifelse(is.na(df$cred_count_total), "other",
      ifelse(df$cred_count_total==0,"0_and_more_8",
      ifelse(df$cred_count_total>=8,"0_and_more_8","other")))
  df$cred_count_total <- as.factor(df$cred_count_total)
  
  df$status_finished_total <- ifelse(is.na(df$status_finished_total),
      "other",ifelse(df$status_finished_total %in% c(73:75),"73_74_75",
      "other"))
  df$status_finished_total <- as.factor(df$status_finished_total)
  
  df$outs_overdue_ratio_total <- ifelse(
    is.na(df$outs_overdue_ratio_total), "0_0.01",
    ifelse(df$outs_overdue_ratio_total==-999,"0_0.01",
    ifelse(df$outs_overdue_ratio_total<=0.01,"0_0.01",
    ifelse(df$outs_overdue_ratio_total<=0.04,"0.01_0.04","more_0.04"))))
  df$outs_overdue_ratio_total <- as.factor(df$outs_overdue_ratio_total)
  
  df$viber_phone <- 
    ifelse(is.na(df$has_viber), "other",
    ifelse(df$has_viber=="False", "False","other"))
  df$viber_phone <- as.factor(df$viber_phone)
  
  if(is.na(api_df$email)){
    df$email_char_ratio <- NA
  } else {
    df$email_name <- strsplit(api_df$email,"@")[[1]][1]
    df$email_char_ratio <- nchar(rawToChar(
      unique(charToRaw(df$email_name)))) / nchar(df$email_name)
  }
  df$API_email_char_ratio <- ifelse(is.na(df$email_char_ratio), "other",
    ifelse(df$email_char_ratio<=0.6,"0_0.6","other"))
  df$API_email_ratio_char <- as.factor(df$API_email_char_ratio)
  
  df$API_payment_method <- ifelse(is.na(api_df$payment_method), "other",
    ifelse(api_df$payment_method==2, "2",
    ifelse(api_df$payment_method==3, "other", "other")))
  df$API_payment_method <- as.factor(df$API_payment_method)
  
  df$API_amount <- 
    ifelse(is.na(api_df$amount),"250_650",
    ifelse(api_df$amount<=150,"less_150",
    ifelse(api_df$amount<=200,"200",
    ifelse(api_df$amount<=650,"250_650","more_700"))))    
  df$API_amount <- as.factor(df$API_amount)
  
  df$API_referral_source <- ifelse(
    is.na(api_df$referral_source) | is.null(api_df$referral_source), "other",
     ifelse(api_df$referral_source=="facebook","facebook","other"))
  df$API_referral_source <- as.factor(df$API_referral_source)
  
  device_type <- ifelse(grepl("Android",api_df$user_agent),"Android",
     ifelse(grepl("iPhone",api_df$user_agent),"iPhone",
     ifelse(grepl("Win64",api_df$user_agent),"Windows",
      ifelse(grepl("Windows",api_df$user_agent),"Windows",
      ifelse(grepl("Linux",api_df$user_agent),"Linux","other")))))
  df$API_device <- ifelse(is.na(device_type), "other",
     ifelse(device_type=="Windows", "Windows", "other"))
  df$API_device <- as.factor(df$API_device)
  
  # Apply logisic regression
  for(i in 1:nrow(scoring_df)){
    
    period_tab <- as.numeric(scoring_df$period[i])
    amount_tab <- as.numeric(scoring_df$amount[i])
    
    apply_logit <- predict(df_Log_Credirect_App_payday, 
                           newdata=df, type="response")
    scoring_df$score[i] <- apply_logit
    scoring_df$score[i] <- gen_group_scores(scoring_df$score[i],
           all_df$office_id,0,1,1)
    scoring_df$pd[i] <- round(apply_logit,3)
  
    # Compute flag of disposable income
    product_tab <- subset(products, products$product_id==all_df$product_id & 
         products$period==as.numeric(period_tab) &
         products$amount==as.numeric(amount_tab))
    scoring_df$color[i] <- ifelse(scoring_df$score[i]=="Bad", 1, 
         ifelse(scoring_df$score[i]=="Indeterminate", 2,
         ifelse(scoring_df$score[i]=="Good 1", 3,
         ifelse(scoring_df$score[i]=="Good 2", 4,
         ifelse(scoring_df$score[i]=="Good 3", 5,
         ifelse(scoring_df$score[i]=="Good 4",6,NA))))))
    
  }
  return(scoring_df)
}

