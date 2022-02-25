
##############################################################################
######## Functions to apply logisit regression on application Credirect ######
##############################################################################

gen_app_credirect_installments <- function(df,scoring_df,products,
  df_Log_Credirect_App_installments,period,all_df,prev_amount,amount_tab,
  t_income,disposable_income_adj,flag_credit_next_salary,api_df){
  
  df$age <- ifelse(df$age<=20,"20_less","more_20")
  df$age <- as.factor(df$age)
  
  df$education <- ifelse(is.na(df$education),"2",
     ifelse(df$education==1,"1",
     ifelse(df$education %in% c(3,4), "3_4", "2")))
  df$education <- as.factor(df$education)
  
  df$marital_status <- ifelse(is.na(df$marital_status), "2_4_5",
     ifelse(df$marital_status %in% c(1,3), "1_3", "2_4_5"))
  df$marital_status <- as.factor(df$marital_status)
  
  df$status_work <- ifelse(is.na(df$status_work), "other",
     ifelse(df$status_work %in% c(4,5,9,10,12), "4_5_9_10_12","other"))
  df$status_work <- as.factor(df$status_work)
  
  df$status_finished_total <- ifelse(is.na(df$status_finished_total),
    "0_71_72_73",ifelse(df$status_finished_total %in% c(0,71,72),"0_71_72_73",
    "74_75"))
  df$status_finished_total <- as.factor(df$status_finished_total)
  
  df$experience_employer <- 
    ifelse(is.na(df$experience_employer), "less_24",
           ifelse(df$experience_employer<=24,"less_24","more_24"))
  df$experience_employer <- as.factor(df$experience_employer)
  
  df$outs_overdue_ratio_total <- ifelse(
    is.na(df$outs_overdue_ratio_total), "0_0.01",
    ifelse(df$outs_overdue_ratio_total==-999,"0_0.01",
    ifelse(df$outs_overdue_ratio_total<=0.01,"0_0.01",
    ifelse(df$outs_overdue_ratio_total<=0.21,"0.01_0.21","more_0.21"))))
  df$outs_overdue_ratio_total <- as.factor(df$outs_overdue_ratio_total)
  
  df$viber_phone <- 
    ifelse(is.na(df$has_viber), "other",
    ifelse(df$has_viber=="False", "False","other"))
  df$viber_phone <- as.factor(df$viber_phone)
  
  df$API_payment_method <- ifelse(is.na(api_df$payment_method), "other",
    ifelse(api_df$payment_method==2, "2",
    ifelse(api_df$payment_method==3, "other", "other")))
  df$API_payment_method <- as.factor(df$API_payment_method)
  
  df$API_period <- 
    ifelse(is.na(api_df$period),"5_6",
    ifelse(api_df$period<=4,"less_4",
    ifelse(api_df$period<=6,"5_6","more_7")))
  df$API_period <- as.factor(df$API_period)
  
  # Apply logisic regression
  for(i in 1:nrow(scoring_df)){
    
    period_tab <- as.numeric(scoring_df$period[i])
    amount_tab <- as.numeric(scoring_df$amount[i])
  
    apply_logit <- predict(df_Log_Credirect_App_installments, newdata=df, 
                           type="response")
    
    scoring_df$score[i] <- apply_logit
    scoring_df$score[i] <- gen_group_scores(scoring_df$score[i],
          all_df$office_id,0,1,0)
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

