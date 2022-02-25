
##############################################################################
######## Functions to apply logistic regression on application Credirect  ####
########                      for FRAUDS                                  ####
##############################################################################

gen_app_credirect_fraud <- function(df,scoring_df,products,
                             df_Log_Credirect_Fraud,period,
                             all_df,prev_amount,amount_tab,
                             t_income,disposable_income_adj,db_name){
  
  # Apply credirect fraud model 
  df$age_cut <- ifelse(df$age<=28,"28_less","29_more")
  df$age <- as.factor(df$age_cut)
  
  df$education_cut <- ifelse(is.na(df$education), "2_3", 
     ifelse(df$education %in% c(2,3), "2_3", df$education))
  df$education <- as.factor(df$education_cut)
  
  df$marital_status_cut <- ifelse(is.na(df$marital_status), "2_5",
     ifelse(df$marital_status %in% c(1,3,4), "1_3_4", "2_5"))
  df$marital_status <- as.factor(df$marital_status_cut)
  
  df$ownership_cut <- ifelse(df$ownership %in% c(1), "1", 
    ifelse(is.na(df$ownership), "not_1", "not_1"))
  df$ownership <- as.factor(df$ownership_cut)
  
  df$purpose_cut <- ifelse(is.na(df$purpose), "1_6",
     ifelse(df$purpose %in% c(1,6),"1_6",
     ifelse(df$purpose==0, "1_6", "not_1_6")))
  df$purpose <- as.factor(df$purpose_cut)
  
  df$status_finished_total_cut <- ifelse(is.na(df$status_finished_total),
                                         "0_71_72",
    ifelse(df$status_finished_total %in% c(0,71,72),"0_71_72",
    ifelse(df$status_finished_total %in% c(73,74),"73_74", "75")))
  df$status_finished_total <- as.factor(df$status_finished_total_cut)
  
  df$outs_overdue_ratio_total_cut <- ifelse(
    is.na(df$outs_overdue_ratio_total), "more_0.08_missing",
    ifelse(df$outs_overdue_ratio_total==-999,"more_0.08_missing",
    ifelse(df$outs_overdue_ratio_total<=0.03,"0_0.03",
    ifelse(df$outs_overdue_ratio_total<=0.08,"0.03_0.08","more_0.08_missing"))))
  df$outs_overdue_ratio_total <- as.factor(df$outs_overdue_ratio_total_cut)
  
  df$viber_registered_cut <- ifelse(is.na(df$has_viber), "other",
     ifelse(df$has_viber==0, "False",
     ifelse(df$has_viber==1, "other", "other")))
  df$viber_registered <- as.factor(df$viber_registered_cut)
  
  df$whatsapp_registered_cut <- "other"
  df$whatsapp_registered <- as.factor(df$whatsapp_registered_cut)
  
  apply_logit <- predict(df_Log_Credirect_Fraud, newdata=df, type="response")
  fraud_flag  <- gen_group_scores_fraud(apply_logit)
  
  # Apply check for phone number
  check_phone <- suppressWarnings(fetch(dbSendQuery(con, 
    gen_get_phone_numbers(db_name)), n=-1))
  check_phone <- as.data.frame(check_phone[!duplicated(check_phone$client_id),])
  if(nrow(check_phone)>1){
      fraud_flag <- 1
  }
  
  # Apply email criteria
  check_email <- suppressWarnings(fetch(dbSendQuery(con, 
    gen_get_email (db_name)), n=-1))
  check_email <- as.data.frame(check_email[!duplicated(check_email$id),])
  if(nrow(check_email)>1){
    fraud_flag <- 1
  }
  
  # Apply age criteria
  if(all_df$age>=65){
    fraud_flag <- 1
  }

  return(fraud_flag)
}
