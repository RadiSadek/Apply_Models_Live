
#####################################################
######## Functions to compute CKR variables  ########
#####################################################

# Function to set correctly CKR variables
gen_ckr_variables <- function(all_df,flag_beh,flag_credirect){
  
  all_df$ckr_act_fin <- ifelse(all_df$cred_count_fin>0 & all_df$ckr_act_fin==0, 
          0, all_df$ckr_act_fin)
  all_df$ckr_act_bank <- ifelse(all_df$cred_count_bank>0 & 
          all_df$ckr_act_bank==0, 0, all_df$ckr_act_bank)
  all_df$ckr_status <- ifelse(flag_beh==1, NA, ifelse(flag_credirect==1, NA,
          max(all_df$ckr_act_fin, all_df$ckr_cur_fin, all_df$ckr_act_bank,
          all_df$ckr_cur_bank)))
  all_df$outs_overdue_ratio_bank <- ifelse(all_df$outs_overdue_bank==0 & 
          all_df$outs_principal_bank==0, -999,
          all_df$outs_overdue_bank/all_df$outs_principal_bank)
  all_df$outs_overdue_ratio_fin <- ifelse(all_df$outs_overdue_fin==0 & 
          all_df$outs_principal_fin==0, -999,
          all_df$outs_overdue_fin/all_df$outs_principal_fin)
  all_df$outs_overdue_ratio_bank <- ifelse(all_df$outs_overdue_ratio_bank>1, 1, 
          all_df$outs_overdue_ratio_bank)
  all_df$outs_overdue_ratio_fin <- ifelse(all_df$outs_overdue_ratio_fin>1, 1, 
          all_df$outs_overdue_ratio_fin)
  all_df$outs_overdue_ratio_total <- ifelse(
    all_df$outs_overdue_ratio_bank>all_df$outs_overdue_ratio_fin, 
    all_df$outs_overdue_ratio_bank, all_df$outs_overdue_ratio_fin)
  all_df$source_entity_count_total <- all_df$src_ent_fin + all_df$src_ent_bank
  all_df$status_finished_total <- ifelse(all_df$ckr_fin_fin>all_df$ckr_fin_bank, 
         all_df$ckr_fin_fin, all_df$ckr_fin_bank)
  all_df$status_active_total <- ifelse(all_df$ckr_act_fin>all_df$ckr_act_bank, 
         all_df$ckr_act_fin, all_df$ckr_act_bank)
  all_df$cred_count_total <- all_df$cred_count_bank + all_df$cred_count_fin
  all_df$cession_bank <- ifelse(is.na(all_df$cession_bank), 0, 
         all_df$cession_bank) 	
  all_df$cession_fin <- ifelse(is.na(all_df$cession_fin), 0, all_df$cession_fin)	
  all_df$amount_cession_total <- ifelse(all_df$cession_bank>all_df$cession_fin, 	
         all_df$cession_bank, all_df$cession_fin)
  
  return(all_df)
}


# Function to compute old CKR variables , with old format
gen_old_ckr <- function(var){
  return(ifelse(is.na(var),NA,
  ifelse(var==0, 400,
  ifelse(var==70 | var==71, 401,
  ifelse(var==72, 402,
  ifelse(var==73, 403,
  ifelse(var==74 | var==75, 404, var)))))))
}




