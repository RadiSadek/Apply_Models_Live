
#########################################################################
######## Functions to apply logisit regression on repeat Credirect ######
#########################################################################

gen_beh_credirect <- function(df,scoring_df,products,df_Log_beh_Credirect,
                             period,all_df,prev_amount,amount_tab,
                             t_income,disposable_income_adj,crit_po,
                             flag_new_credirect_old_city,api_df){
  #  Cut and bin
  df$ownership <- ifelse(is.na(df$ownership),"1_2_3",
    ifelse(df$ownership==4, "4", "1_2_3"))
  df$ownership <- as.factor(df$ownership)
  
  df$education <- ifelse(is.na(df$education), "2_3",
    ifelse(df$education==1,"1",
    ifelse(df$education==4,"4","2_3")))
  df$education <- as.factor(df$education)
  
  df$status_work <- ifelse(is.na(df$status_work), "other",
    ifelse(df$status_work %in% c(1,4,5),"1_4_5","other"))
  df$status_work <- as.factor(df$status_work)
  
  df$age <- ifelse(df$age<=22,"22_less","23_more")
  df$age <- as.factor(df$age)
  
  df$purpose <- ifelse(is.na(df$purpose), "other",
    ifelse(df$purpose %in% c(2,4,5),"2_4_5","other"))
  df$purpose <- as.factor(df$purpose)
  
  df$experience_employer <- ifelse(is.na(df$experience_employer),"more_3",
    ifelse(df$experience_employer<=2,"0_2","more_3"))
  df$experience_employer <- as.factor(df$experience_employer)
  
  df$ratio_nb_payments_prev <- 
    ifelse(is.na(df$ratio_nb_payments_prev),"0_0.99", 
           ifelse(df$ratio_nb_payments_prev<1,"0_0.99","1_more"))
  df$ratio_nb_payments_prev <- as.factor(df$ratio_nb_payments_prev)
  
  df$refinance_ratio <- ifelse(df$refinance_ratio<=0.49,"0_0.49","0.5_1")
  df$refinance_ratio <- as.factor(df$refinance_ratio)
  
  df$credits_cum <- 
    ifelse(df$credits_cum<=1,"1",
    ifelse(df$credits_cum==2,"2",
    ifelse(df$credits_cum==3,"3",
    ifelse(df$credits_cum<=6,"4_6","more_7"))))
  df$credits_cum <- as.factor(df$credits_cum)
  
  df$max_delay <- ifelse(is.na(df$max_delay),"31_55",
    ifelse(df$max_delay %in% c(0,1),"less_1",
    ifelse(df$max_delay<=30,"2_30",
    ifelse(df$max_delay<=55,"31_55",
    ifelse(df$max_delay<=90,"56_90",
    ifelse(df$max_delay<=180,"90_180",
    ifelse(df$max_delay<=360,"180_360","360_more")))))))
  df$max_delay <- as.factor(df$max_delay)
  
  df$status_finished_total <- 
    ifelse(is.na(df$status_finished_total),"other",
    ifelse(df$status_finished_total %in% c(74,75),"74_75","other"))
  df$status_finished_total <- as.factor(df$status_finished_total)
  
  df$source_entity_count_total <- 
    ifelse(is.na(df$source_entity_count_total), "9_less",
    ifelse(df$source_entity_count_total<=9,"9_less","10_more"))
  df$source_entity_count_total <- as.factor(df$source_entity_count_total)
  
  df$has_viber <- ifelse(is.na(df$has_viber),"other",
     ifelse(df$has_viber==1,"other","no"))
  df$has_viber <- as.factor(df$has_viber)
  
  df$API_payment_method <- ifelse(is.na(api_df$payment_method), "other",
     ifelse(api_df$payment_method==2, "2",
     ifelse(api_df$payment_method==3, "other", "other")))
  df$API_payment_method <- as.factor(df$API_payment_method)
  
  device_type <- ifelse(grepl("Android",api_df$user_agent),"Android",
     ifelse(grepl("iPhone",api_df$user_agent),"iPhone",
     ifelse(grepl("Win64",api_df$user_agent),"Windows",
     ifelse(grepl("Windows",api_df$user_agent),"Windows",
     ifelse(grepl("Linux",api_df$user_agent),"Linux","other")))))
  df$API_device <- ifelse(is.na(device_type), "other",
     ifelse(device_type=="Windows", "Windows", "other"))
  df$API_device <- as.factor(df$API_device)
  
  df$API_hour_app <- as.numeric(substring(api_df$partial_app_at,12,13))
  df$API_hour_app <- ifelse(is.na(df$API_hour_app), "other",
     ifelse(df$API_hour_app %in% c(0:7),"0_7","other"))
  df$API_hour_app <- as.factor(df$API_hour_app)
  
  df$outs_overdue_ratio_total <- ifelse(
    is.na(df$outs_overdue_ratio_total), "0_0.01",
    ifelse(df$outs_overdue_ratio_total==-999,"0.01_0.17",
    ifelse(df$outs_overdue_ratio_total==0,"0",
    ifelse(df$outs_overdue_ratio_total<=0.01,"0_0.01",
    ifelse(df$outs_overdue_ratio_total<=0.17,"0.01_0.17","more_0.17")))))
  df$outs_overdue_ratio_total <- as.factor(df$outs_overdue_ratio_total)
  
  amount_diff_loc <- ifelse(!is.na(api_df$amount),
     as.numeric(api_df$amount) - prev_amount$amount,NA)
  df$API_amount_diff <- ifelse(is.na(amount_diff_loc),"100_250",
     ifelse(amount_diff_loc<=50,"less_50",
     ifelse(amount_diff_loc<=250,"100_250",
     ifelse(amount_diff_loc<=450,"300_450",
     ifelse(amount_diff_loc<=950,"500_950","more_1000")))))
  df$API_amount_diff <- as.factor(df$API_amount_diff)

  
  # Apply model
  for(i in 1:nrow(scoring_df)){
    
    period_tab <- as.numeric(scoring_df$period[i])
    amount_tab <- as.numeric(scoring_df$amount[i])
    
    apply_logit <- predict(df_Log_beh_Credirect, newdata=df, type="response")
    scoring_df$score[i] <- apply_logit
    scoring_df$score[i] <- gen_group_scores(scoring_df$score[i],
         all_df$office_id,1,1,0)
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
    
    if(crit_po!=0){
      scoring_df$installment_amount[i] <- product_tab$installment_amount
    }
    
    
  }
  return(scoring_df)
}


  
  