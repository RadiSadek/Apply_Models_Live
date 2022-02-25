
#########################################################################
######## Functions to apply logisit regression on repeat City Cash ######
#########################################################################

gen_beh_citycash <- function(df,scoring_df,products,df_Log_beh_CityCash,period,
                             all_id,all_df,prev_amount,amount_tab,
                             t_income,disposable_income_adj,crit_po){
  # Cut and bin
  df$age_cut <- ifelse(df$age<=29,"less_29",
     ifelse(df$age<=37,"30_37",
     ifelse(df$age<=53,"38_53","more_54")))
  df$age <- as.factor(df$age_cut)
  
  df$marital_status <- ifelse(is.na(df$marital_status),
     "1_2_3_4", df$marital_status)
  df$marital_status <- ifelse(df$marital_status==5, "5", "1_2_3_4")
  df$marital_status <- as.factor(df$marital_status)
  
  df$education_cut <- ifelse(is.na(df$education),"2_3",
    ifelse(df$education==1,"1",
    ifelse(df$education==4,"4","2_3")))
  df$education <- as.factor(df$education_cut)
  
  df$status_work_cut <- ifelse(is.na(df$status_work), "other",
     ifelse(df$status_work %in% c(5,10,9,12) ,"5_10","other"))
  df$status_work <- as.factor(df$status_work_cut)
  
  df$experience_employer_cut <- 
    ifelse(is.na(df$experience_employer),"13_156", 
    ifelse(df$experience_employer<=12,"0_12",
    ifelse(df$experience_employer<=156,"13_156","more_156")))
  df$experience_employer <- as.factor(df$experience_employer_cut)
  
  df$on_address_cut <- 
    ifelse(is.na(df$on_address),"more_24", 
    ifelse(df$on_address<=24,"0_24", "more_24"))
  df$on_address <- as.factor(df$on_address_cut)
  
  df$household_children_cut <- ifelse(is.na(df$household_children), "0_1",
    ifelse(df$household_children<=1,"0_1", "more_1"))
  df$household_children <- as.factor(df$household_children_cut)
  
  df$flag_high_last_paid_cut <- ifelse(is.na(df$flag_high_last_paid),1,
                                       df$flag_high_last_paid)
  df$flag_high_last_paid <- as.factor(df$flag_high_last_paid_cut)
  
  df$purpose_cut <- ifelse(is.na(df$purpose), "other",
    ifelse(df$purpose %in% c(6), "6",
    ifelse(df$purpose %in% c(4), "4", 
    ifelse(df$purpose %in% c(2,5),"2_5","other"))))
  df$purpose <- as.factor(df$purpose_cut)
  
  df$days_diff_last_credit_cut <- ifelse(is.na(df$days_diff_last_credit), 
     "less_2",ifelse(df$days_diff_last_credit<=2,"less_2","3_more"))
  df$days_diff_last_credit <- as.factor(df$days_diff_last_credit_cut)
  
  df$max_delay_cut <- ifelse(df$max_delay<=2,"less_2",
     ifelse(df$max_delay<=6,"3_6",
     ifelse(df$max_delay<=60,"7_60","60_more")))
  df$max_delay <- as.factor(df$max_delay_cut)
  
  df$credits_cum_cut <- ifelse(df$credits_cum<=1,"1",
     ifelse(df$credits_cum<=5,"2_5",
     ifelse(df$credits_cum<=8,"6_8","more_9")))
  df$credits_cum <- as.factor(df$credits_cum_cut)
  
  df$ratio_nb_payments_prev_cut <- ifelse(is.na(df$ratio_nb_payments_prev),
     "less_0.49",
     ifelse(df$ratio_nb_payments_prev<=0.49,"less_0.49",
     ifelse(df$ratio_nb_payments_prev<=0.59,"0.49_0.59",
     ifelse(df$ratio_nb_payments_prev<=0.94,"0.59_0.94","more_0.94"))))
  df$ratio_nb_payments_prev <- as.factor(df$ratio_nb_payments_prev_cut)
  
  df$status_finished_total_cut <- ifelse(is.na(df$status_finished_total),
      "other",ifelse(df$status_finished_total %in% c(71,72),"71_72","other"))
  df$status_finished_total <- as.factor(df$status_finished_total_cut)
  
  df$outs_overdue_ratio_total_cut <- 
    ifelse(is.na(df$outs_overdue_ratio_total),"0.04_0.5",
    ifelse(df$outs_overdue_ratio_total==-999,"0.04_0.5",
    ifelse(df$outs_overdue_ratio_total<=0.04,"0.04_less",
    ifelse(df$outs_overdue_ratio_total<=0.5,"0.04_0.5",
    ifelse(df$outs_overdue_ratio_total<0.9,"0.5_0.9","more_0.9"))))) 
  df$outs_overdue_ratio_total <- as.factor(df$outs_overdue_ratio_total_cut)
  
  for(i in 1:nrow(scoring_df)){
    
    period_tab <- as.numeric(scoring_df$period[i])
    amount_tab <- as.numeric(scoring_df$amount[i])
    
    if(df$total_income<100 | is.na(df$total_income)){
      ratio_tab <- 0.08}
    else {
      ratio_tab <- products[products$period == period_tab & 
            products$amount == amount_tab & 
            products$product_id == all_df$product_id, ]$installment_amount/
            t_income}
    
    if (ratio_tab>=3) {ratio_tab <- 3}
    
    acceptable_installment_amount <- products$installment_amount[
      products$period==period_tab & products$amount==amount_tab]
    
    amount_diff_loc <- amount_tab - prev_amount$amount
    df$amount_diff <- ifelse(is.na(amount_diff_loc),"0_250",
           ifelse(amount_diff_loc<0,"less_0",
           ifelse(amount_diff_loc>=300,"more_300","0_250")))
    
    
    # Compute correct maturity for each amount and product_id
    current_maturity <- ifelse(period==1, period_tab*7/30, 
         ifelse(period==2, period_tab*14/30, period_tab))
    df$maturity <- ifelse(current_maturity<=3.03, "less_3.03",
         ifelse(current_maturity<=6.3, "3.03_6.3","more_6.3"))
    df$maturity <- as.factor(df$maturity)
    
    # Apply logistic model to each amount and installment
    apply_logit <- predict(df_Log_beh_CityCash, newdata=df, type="response")
    scoring_df$score[i] <- apply_logit
    scoring_df$score[i] <- gen_group_scores(scoring_df$score[i],
                                            all_df$office_id,1,0,0)
    scoring_df$pd[i] <- round(apply_logit,3)
    
    # Compute flag of disposable income
    product_tab <- subset(products, products$product_id==all_df$product_id & 
        products$period==as.numeric(period_tab) &
        products$amount==as.numeric(amount_tab))
    
    scoring_df$color[i] <- 0
    
    scoring_df$color[i] <- 
          ifelse(scoring_df$score[i]=="Bad", 1, 
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

