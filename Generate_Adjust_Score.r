
#######################################################
######## Apply logistic models and policy rule ########
#######################################################

# Function to apply scoring model 
gen_apply_score <- function(empty_fields,threshold_empty,flag_exclusion,
  flag_varnat,flag_is_dead,flag_credit_next_salary,flag_credirect,
  flag_beh,all_df,scoring_df,df,products,df_Log_beh_CityCash,
  df_Log_CityCash_App,df_Log_beh_Credirect,df_Log_Credirect_App_installments,
  df_Log_Credirect_App_payday,period,all_id,prev_amount,amount_tab,
  t_income,disposable_income_adj,flag_new_credirect_old_city,api_df,
  criteria_po){
  
  # Apply model coefficients according to type of credit 
  if (empty_fields>=threshold_empty){
    
    scoring_df$score <- "NULL"
    scoring_df$color <- 2
    
  } else if (flag_exclusion==1 | flag_varnat==1 | flag_is_dead==1){
    
    scoring_df$score <- "Bad"
    scoring_df$color <- 1
    
  } else if (flag_beh==1 & flag_credirect==0){
    scoring_df <- gen_beh_citycash(df,scoring_df,products,df_Log_beh_CityCash,
      period,all_id,all_df,prev_amount,amount_tab,
      t_income,disposable_income_adj,criteria_po)
  } else if (flag_beh==1 & flag_credirect==1){
    scoring_df <- gen_beh_credirect(df,scoring_df,products,df_Log_beh_Credirect,
      period,all_df,prev_amount,amount_tab,t_income,
      disposable_income_adj,criteria_po,flag_new_credirect_old_city,api_df)
  } else if (flag_beh==0 & flag_credirect==0){
    scoring_df <- gen_app_citycash(df,scoring_df,products,df_Log_CityCash_App,
      period,all_df,prev_amount,amount_tab,
      t_income,disposable_income_adj)
  } else if (flag_beh==0 & flag_credirect==1 & flag_credit_next_salary==1){
    scoring_df <- gen_app_credirect_payday(df,scoring_df,products,
      df_Log_Credirect_App_payday,period,all_df,prev_amount,
      amount_tab,t_income,disposable_income_adj,
      flag_credit_next_salary,api_df)
  } else {
    scoring_df <- gen_app_credirect_installments(df,scoring_df,products,
      df_Log_Credirect_App_installments,period,all_df,
      prev_amount,amount_tab,t_income,disposable_income_adj,
      flag_credit_next_salary,api_df)
  }
  
  return(scoring_df)
  
}

# Function to apply policy rules
gen_apply_policy <- function(scoring_df,flag_credirect,flag_cession,
     flag_bad_ckr_citycash,all_df,all_id,flag_beh,prev_amount,products,
     application_id,flag_new_credirect_old_city,flag_credit_next_salary,
     flag_beh_company,flag_cashpoint,crit){
  
  if(flag_cession==1 & flag_credirect==1){
    scoring_df <- gen_adjust_score(scoring_df, c("Bad","Indeterminate"))
  }
  
  # Apply if City Cash application
  if(flag_bad_ckr_citycash==1 & flag_credirect==0){
    scoring_df <- gen_adjust_score(scoring_df, c("Bad","Indeterminate"))
  }
  if(flag_beh_company==0 & flag_credirect==0 & flag_cashpoint==0 & 
     all_df$product_id!=22){
    scoring_df <- gen_restrict_citycash_app(scoring_df)
  }
  if(flag_beh_company==0 & flag_credirect==0 & all_df$product_id==22){
    scoring_df <- gen_restrict_big_fin_app(scoring_df)
  }
  if(flag_beh_company==0 & flag_credirect==0 & flag_cashpoint==1){
    scoring_df <- gen_restrict_cashpoint_app(scoring_df,all_df,flag_beh)
  }
  if(flag_beh_company==1 & flag_credirect==0 & all_df$product_id!=22){
    scoring_df <- gen_restrict_citycash_beh(scoring_df,prev_amount,products,
     all_id,all_df,db_name,application_id,crit)
  }
  if(flag_beh_company==1 & flag_credirect==0 & all_df$product_id==22){
    scoring_df <- gen_restrict_big_fin_rep(scoring_df,prev_amount)
  }

  
  # Apply if Credirect application
  if(flag_beh==0 & flag_credirect==1){
    scoring_df <- gen_restrict_credirect_app(scoring_df,all_df,
        flag_credit_next_salary,flag_new_credirect_old_city)
  }
  if(flag_beh==1 & flag_credirect==1 & flag_new_credirect_old_city==1){
    scoring_df <- gen_restrict_credirect_app(scoring_df,all_df,
        flag_credit_next_salary,flag_new_credirect_old_city)
  }
  if(flag_beh==1 & flag_credirect==1 & flag_new_credirect_old_city==0){
    scoring_df <- gen_restrict_credirect_beh(scoring_df,all_df,all_id,
        application_id,flag_credit_next_salary)
  }

  
  return(scoring_df)
  
}

