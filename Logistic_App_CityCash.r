
##############################################################################
######## Functions to apply logisit regression on application City Cash ######
##############################################################################

gen_app_citycash <- function(df,scoring_df,products,df_Log_CityCash_App,period,
                             all_df,prev_amount,amount_tab,
                             t_income,disposable_income_adj){
  # Cut and bin
  df$marital_status <- ifelse(is.na(df$marital_status),"2_3", 
    ifelse(df$marital_status %in% c(2,3), "2_3",
    ifelse(df$marital_status==4,"1_4",
    ifelse(df$marital_status==1,"1_4",
    ifelse(df$marital_status==5,"5","2_3")))))
  df$marital_status <- as.factor(df$marital_status)
  df$age <- ifelse(df$age<=26,"less_26",
    ifelse(df$age<=33,"27_33",
    ifelse(df$age<=45,"34_45",
    ifelse(df$age<=57,"46_57","more_58"))))
  df$age <- as.factor(df$age)
  df$gender <- as.factor(df$gender)
  df$education <- ifelse(is.na(df$education), "2",
    ifelse(df$education==1,"1",
    ifelse(df$education==2,"2","3_4")))   
  df$education <- as.factor(df$education)
  df$status_work <- ifelse(is.na(df$status_work), "other",
    ifelse(df$status_work %in% c(5,9),"5_9","other"))
  df$status_work <- as.factor(df$status_work)
  df$experience_employer <- ifelse(is.na(df$experience_employer),"9_72",
    ifelse(df$experience_employer<=9,"9_less",
    ifelse(df$experience_employer<=72,"9_72","72_more")))
  df$experience_employer <- as.factor(df$experience_employer)
  df$on_address <- ifelse(is.na(df$on_address),"36_335",
    ifelse(df$on_address<=35,"1_35",
    ifelse(df$on_address<=335,"36_335","more_336")))
  df$on_address <- as.factor(df$on_address)
  df$status_active_total <- ifelse(is.na(df$status_active_total),"other",
    ifelse(df$status_active_total==0,"0",
    ifelse(df$status_active_total %in% c(74,75), "74_75","other")))
  df$status_active_total <- as.factor(df$status_active_total)
  df$flag_location_curr <- ifelse(is.na(df$risky_address), "other",
    ifelse(df$risky_address==1,"1","other"))
  df$flag_location_curr <- as.factor(df$flag_location_curr)
  df$self_approval_cut <- 
    ifelse(is.na(df$self_approval), "other",
    ifelse(df$self_approval==1,"1","other"))
  df$self_approval <- as.factor(df$self_approval_cut)
  
  
  # Apply logisic regression
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
    
    acceptable_installment_amount <- products$installment_amount[
      products$period==period_tab & products$amount==amount_tab]
    
    if (ratio_tab>=3) {ratio_tab <- 3}
    
    ratio_tab <- as.numeric(ratio_tab)
    df$ratio_installment_income <- ifelse(is.na(ratio_tab), 999, 
           ratio_tab)
    df$ratio_installment_income <- ifelse(
      is.na(df$ratio_installment_income),"other",
      ifelse(df$ratio_installment_income<=0.05,"less_0.05",
      ifelse(df$ratio_installment_income<=0.11,"0.05_0.11","more_0.11")))
    df$ratio_installment_income <- as.factor(df$ratio_installment_income)
    
    # Compute correct maturity for each amount and product_id
    current_maturity <- ifelse(period==1, period_tab*7/30, 
                               ifelse(period==2, period_tab*14/30, period_tab))
    df$maturity <- ifelse(current_maturity<=3, "less_3",
                          ifelse(current_maturity<=6,"4_5","more_6"))
    df$maturity <- as.factor(df$maturity)
    
    # Apply logistic model to each amount and installment
    apply_logit <- predict(df_Log_CityCash_App, newdata=df, type="response")
    scoring_df$score[i] <- apply_logit
    scoring_df$score[i] <- gen_group_scores(scoring_df$score[i],
                                            all_df$office_id,0,0,0)
    scoring_df$pd[i] <- round(apply_logit,3)
    
    # Compute flag of disposable income
    product_tab <- subset(products, products$product_id==all_df$product_id & 
                            products$period==as.numeric(period_tab) &
                            products$amount==as.numeric(amount_tab))
    
    scoring_df$color[i] <- 0
    
    scoring_df$color[i] <- ifelse(scoring_df$color[i]==1 | 
              scoring_df$score[i]=="Bad", 1, 
              ifelse(scoring_df$score[i]=="Indeterminate", 2,
              ifelse(scoring_df$score[i]=="Good 1", 3,
              ifelse(scoring_df$score[i]=="Good 2", 4,
              ifelse(scoring_df$score[i]=="Good 3", 5,
              ifelse(scoring_df$score[i]=="Good 4", 6, NA))))))
  }
  return(scoring_df)
}


