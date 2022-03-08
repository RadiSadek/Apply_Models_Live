
#############################################################
######## Apply restrictions on final scoring dataframe ######
#############################################################

# Function to apply restrictions for City Cash applications
gen_restrict_citycash_app <- function(scoring_df){
  
  score_df_800 <- subset(scoring_df,scoring_df$amount>800)
  criteria_800 <- length(names(table(score_df_800$score))
     [names(table(score_df_800$score)) %in% c("Good 4")])
  score_df_600 <- subset(scoring_df,scoring_df$amount>600)
  criteria_600 <- length(names(table(score_df_600$score))
    [names(table(score_df_600$score)) %in% c("Good 2","Good 3","Good 4")])
  score_df_400 <- subset(scoring_df,scoring_df$amount>400)
  criteria_400 <- length(names(table(score_df_400$score))
    [names(table(score_df_400$score)) %in% c("Good 1","Good 2",
    "Good 3","Good 4")])
  
  scoring_df$color <- ifelse(scoring_df$score %in% c("NULL"),scoring_df$color,
    ifelse(scoring_df$amount>1000,1,
    ifelse(criteria_800==0 & scoring_df$amount>800,1,
    ifelse(criteria_600==0 & scoring_df$amount>600,1,
    ifelse(criteria_400==0 & scoring_df$amount>400,1,scoring_df$color)))))

  return(scoring_df)
}

# Function to apply restrictions for City Cash applications
gen_restrict_cashpoint_app <- function(scoring_df,all_df,flag_beh){
  
  # Apply policy accroding to score 
  score_df_800 <- subset(scoring_df,scoring_df$amount>800)
  criteria_800 <- length(names(table(score_df_800$score))
     [names(table(score_df_800$score)) %in% c("Good 4")])
  
  score_df_600 <- subset(scoring_df,scoring_df$amount>600)
  criteria_600 <- length(names(table(score_df_600$score))
     [names(table(score_df_600$score)) %in% c("Good 3","Good 4")])
  
  score_df_400 <- subset(scoring_df,scoring_df$amount>400)
  criteria_400 <- length(names(table(score_df_400$score))
     [names(table(score_df_400$score)) %in% c("Good 2",
     "Good 3","Good 4")])
  
  score_df_0 <- subset(scoring_df,scoring_df$amount>0)
  criteria_0 <- length(names(table(score_df_0$score))
     [names(table(score_df_0$score)) %in% c("Good 1","Good 2",
     "Good 3","Good 4")])
  
  scoring_df$color <- ifelse(scoring_df$score %in% c("NULL"),scoring_df$color,
    ifelse(scoring_df$amount>1000,1,
    ifelse(criteria_800==0 & scoring_df$amount>800,1,
    ifelse(criteria_600==0 & scoring_df$amount>600,1,
    ifelse(criteria_400==0 & scoring_df$amount>400,1,
    ifelse(criteria_0==0 & scoring_df$amount>0,1,scoring_df$color))))))
  
  # No Indeterminates
  scoring_df$color <- ifelse(scoring_df$score %in% c("Bad","Indeterminate"),
    1, scoring_df$color)  

  # Take only one third of Good 1 for certain offices
  if(all_df$office_id %in% c("150") & !is.na(scoring_df$pd[1])){
    scoring_df$color <- ifelse(scoring_df$pd>0.275,1,scoring_df$color)
  }
  if(flag_beh==1 & !is.na(scoring_df$pd[1])){
      scoring_df$color <- ifelse(scoring_df$pd>0.275,1,scoring_df$color)  
  }
  return(scoring_df)
}

# Function to apply restrictions for City Cash repeats
gen_restrict_citycash_beh <- function(scoring_df,prev_amount,products,all_id,
                                      all_df,db_name,application_id,crit){
  
  # Check if has Good 1 at least somewhere in table
  criteria <- length(names(table(scoring_df$score))
    [names(table(scoring_df$score)) %in% 
        c("Good 1","Good 2","Good 3","Good 4")])
  scoring_df$color <- ifelse(scoring_df$score %in% c("NULL"),scoring_df$color,
    ifelse(criteria==0 & scoring_df$amount>prev_amount$amount,1,
           scoring_df$color))
  
  # Check if installment ratio is OK
  if(!("installment_amount" %in% names(scoring_df))){
    scoring_df <- merge(scoring_df,products[,c("amount","period",
       "installment_amount")],
       by.x = c("amount","period"),by.y = c("amount","period"),all.x = TRUE)
  }
  allowed_installment <- gen_installment_ratio(db_name,all_id,all_df,
      application_id,crit)
  for(i in 1:nrow(scoring_df)){
    if(scoring_df$installment_amount[i]>allowed_installment){
      scoring_df$color[i] <- 1
    }
  }
  
  # Limit based on maximum amount differential
  for(i in 1:nrow(all_id)){
     all_id$amount[i] <- gen_query(con,
      gen_big_sql_query(db_name,all_id$id[i]))$amount
  }
  max_prev_amount <- max(all_id$amount[
     all_id$company_id==all_id$company_id[all_id$id==application_id] & 
     all_id$status %in% c(4,5)])
  scoring_df$color <- ifelse(scoring_df$score %in% c("Good 4") & 
     scoring_df$amount>(max_prev_amount+600),1,
     ifelse(scoring_df$score %in% c("Good 1","Good 2","Good 3",
     "Indeterminate") & scoring_df$amount>(max_prev_amount+400),1,
     scoring_df$color))

  return(scoring_df)
}

# Function to apply restrictions for Credirect applications
gen_restrict_credirect_app <- function(scoring_df,all_df,
     flag_credit_next_salary,flag_new_credirect_old_city){

  if(flag_credit_next_salary==1){
    scoring_df$color <- 
      ifelse(scoring_df$score %in% c("Good 4") & scoring_df$amount>800,1,
      ifelse(scoring_df$score %in% c("Good 3","Good 2","Good 1",
                                     "Indeterminate") &
             scoring_df$amount>800,1,
             scoring_df$color))
  } else {
    scoring_df$color <- 
      ifelse(scoring_df$score %in% c("Good 4") & scoring_df$amount>1400,1,
      ifelse(scoring_df$score %in% c("Good 3","Good 2","Good 1",
                                     "Indeterminate") &
             scoring_df$amount>800,1,
             scoring_df$color))
  }
  if(all_df$age<21){
    scoring_df$color <- ifelse(scoring_df$amount>500 & scoring_df$color>=2,1,
        scoring_df$color)
  }
  
  # Apply filter for new Credirects but old City Cash
  if(flag_new_credirect_old_city==1){
    scoring_df$color <- ifelse(scoring_df$score %in% 
     c("Indeterminate","Good 1"), 1, scoring_df$color)
  } 
  return(scoring_df)
}

# Function to apply restrictions for Credirect behavioral
gen_restrict_credirect_beh <- function(scoring_df,all_df,all_id,application_id,
       flag_credit_next_salary){

  # Get company ID to filter past credits only for Credirect and credit amounts
  all_df_local <- get_company_id_prev(db_name,all_df)
  all_id_local <- all_id[all_id$status %in% c(5) & 
                         all_id$company_id==all_df_local$company_id,]
  all_id_local_active <- all_id[all_id$status %in% c(4) & 
                           all_id$company_id==all_df_local$company_id,]
  all_id_local <- subset(all_id_local, all_id_local$sub_status %in% 
                         c(123,126,128))
  
  # Get amounts of previous credits
  if(nrow(all_id_local)>0){
    for(i in 1:nrow(all_id_local)){
      all_id_local$amount[i] <- gen_query(con,
       gen_last_cred_amount_query(all_id_local$id[i],db_name))$amount}
  }
  if(nrow(all_id_local_active)>0){
    for(i in 1:nrow(all_id_local_active)){
      all_id_local_active$amount[i] <- gen_query(con,
       gen_last_cred_amount_query(all_id_local_active$id[i],db_name))$amount}
  }
  
  # Get nb passed installments at deactivation & if prev is until next salary
  prev_vars <- gen_prev_deactiv_date(db_name,all_df,all_id,application_id)
  passed_install_at_pay <- prev_vars[1]
  prev_next_salary <- prev_vars[2]
  
  # Define maximum step with previous
  max_step_prev_low <- ifelse(passed_install_at_pay!=-999,
   (ifelse(prev_next_salary==0,
   (ifelse(passed_install_at_pay==0,200,
    ifelse(passed_install_at_pay==1,400,
    ifelse(passed_install_at_pay==2,500,
    ifelse(passed_install_at_pay==3,500,Inf))))),
   (ifelse(passed_install_at_pay==0,300,Inf)))),300)
  
  max_step_prev_high <- ifelse(passed_install_at_pay!=-999,
   (ifelse(prev_next_salary==0,
   (ifelse(passed_install_at_pay==0,400,
    ifelse(passed_install_at_pay==1,600,
    ifelse(passed_install_at_pay==2,800,
    ifelse(passed_install_at_pay==3,1000,Inf))))),
   (ifelse(passed_install_at_pay==0,500,Inf)))),500)
  
  # Apply policy rules for Credirect Installments
  if(flag_credit_next_salary==0){
    
    scoring_df$allowed_amount_app <- 
      ifelse(scoring_df$score %in% c("NULL","Bad","Indeterminate"),0,
             ifelse(scoring_df$score %in% c("Good 4"),1400,800))
    
    # If has at least 1 terminated 
    if(nrow(all_id_local)>0){

      scoring_df$allowed_amount_rep <- 
        ifelse(scoring_df$score %in% c("Bad","NULL"),
               max(all_id_local$amount) + min(0,max_step_prev_low),
        ifelse(scoring_df$score %in% c("Indeterminate") & scoring_df$pd>0.735,
               max(all_id_local$amount) + min(-200,max_step_prev_low),
        ifelse(scoring_df$score %in% c("Indeterminate") & scoring_df$pd>0.72,
               max(all_id_local$amount) + min(0,max_step_prev_low),
        ifelse(scoring_df$score %in% c("Indeterminate"),
               max(all_id_local$amount) + min(400,max_step_prev_low), 
        ifelse(scoring_df$score %in% c("Good 1"),
               max(all_id_local$amount) + min(400,max_step_prev_low),
        ifelse(scoring_df$score %in% c("Good 2"),
               max(all_id_local$amount) + min(600,max_step_prev_high),
        ifelse(scoring_df$score %in% c("Good 3"),
               max(all_id_local$amount) + min(800,max_step_prev_high), 
               max(all_id_local$amount) + min(1200,max_step_prev_high))))))))
      
      for (i in 1:nrow(scoring_df)){
        scoring_df$allowed_amount[i] <- max(scoring_df$allowed_amount_rep[i],
                scoring_df$allowed_amount_app[i])
      }
      scoring_df$color <- ifelse(scoring_df$amount>scoring_df$allowed_amount,
                1,scoring_df$color)
    } 
    
    # If only has at least 1 active (no terminated)
    else if(nrow(all_id_local_active)>0){
      for (i in 1:nrow(scoring_df)){
        scoring_df$allowed_amount[i] <- max(scoring_df$allowed_amount_app[i],
                  max(all_id_local_active$amount))}
    
    # Precaution condition
    } else {
      scoring_df$allowed_amount <- scoring_df$allowed_amount_app
    }
    
    scoring_df$color <- ifelse(scoring_df$amount>scoring_df$allowed_amount,
                               1,scoring_df$color)
    
  # Apply policy rules for Pay Day
  } else {
      scoring_df$allowed_amount_app <- 
        ifelse(scoring_df$score %in% c("NULL","Bad","Indeterminate"),0,
        ifelse(scoring_df$score %in% c("Good 4"),800,800))
      
      if(nrow(all_id_local)>0){
        scoring_df$allowed_amount_rep <- 
          ifelse(scoring_df$score %in% c("Indeterminate") & scoring_df$pd>0.735,
             max(all_id_local$amount) + min(-200,max_step_prev_low),
          ifelse(scoring_df$score %in% c("Indeterminate") & scoring_df$pd>0.72,
             max(all_id_local$amount) + min(0,max_step_prev_low),
         ifelse(scoring_df$score %in% c("Indeterminate"),
             max(all_id_local$amount) + min(400,max_step_prev_low), 
          ifelse(scoring_df$score %in% c("Good 1"),
             max(all_id_local$amount) + min(400,max_step_prev_low),
          ifelse(scoring_df$score %in% c("Good 2"),
             max(all_id_local$amount) + min(600,max_step_prev_high),
          ifelse(scoring_df$score %in% c("Good 3"),
             max(all_id_local$amount) + min(800,max_step_prev_high), 
             max(all_id_local$amount) + min(1200,max_step_prev_high)))))))
        
        for (i in 1:nrow(scoring_df)){
          scoring_df$allowed_amount[i] <- max(scoring_df$allowed_amount_rep[i],
                                              scoring_df$allowed_amount_app[i])}
      } else {
        
        scoring_df$allowed_amount <- scoring_df$allowed_amount_app
        
      }
      scoring_df$allowed_amount <- ifelse(scoring_df$allowed_amount>800,800,
                                          scoring_df$allowed_amount)
      scoring_df$color <- ifelse(scoring_df$amount>scoring_df$allowed_amount,
                                 1,scoring_df$color)
  }

  return(scoring_df)
}

# Function to apply restrictions for Big Fin applications
gen_restrict_big_fin_app <- function(scoring_df){
  scoring_df$color <- 
    ifelse(scoring_df$score %in% c("NULL"),scoring_df$color,
    ifelse(scoring_df$score %in% c("Good 3","Good 4"),scoring_df$color,1))
  scoring_df <- subset(scoring_df,scoring_df$amount<=3000)
  return(scoring_df)
}


# Function to apply restrictions for Big Fin repeats
gen_restrict_big_fin_rep <- function(scoring_df,prev_amount){
  scoring_df$color <- 
    ifelse(scoring_df$score %in% c("NULL"),scoring_df$color,
    ifelse(scoring_df$score %in% c("Good 2","Good 3","Good 4"),
           scoring_df$color,1))
    
  criteria <- length(names(table(scoring_df$score))
    [names(table(scoring_df$score)) %in% 
    c("Good 3","Good 4")])
  scoring_df$color <- ifelse(scoring_df$score %in% c("NULL"),scoring_df$color,
    ifelse(criteria==0 & scoring_df$amount>prev_amount$amount,1,
    scoring_df$color))
  
  return(scoring_df)
}


# Readjust score if necessary for certain cases
gen_adjust_score <- function(scoring_df,crit){
  for(i in 1:nrow(scoring_df)){
    if(scoring_df$score[i] %in% crit){
      scoring_df$color[i] <- 1} 
  }
  return(scoring_df)
}

# Function to apply restrictions to refinances
gen_restrict_beh_refinance <- function(db_name,all_df,all_id,
    scoring_df,flag_active,application_id,flag_credirect,flag_cashpoint){
  
  # Apply restrictions
  if(flag_credirect==1 & all_df$max_delay>180){
    scoring_df$color <- ifelse(scoring_df$color>1 & scoring_df$score!=
     "NULL",1,scoring_df$color)
  } else {
    scoring_df$color <- scoring_df$color
  }
    
  filter_company <- ifelse(flag_credirect==1,2,
                    ifelse(flag_cashpoint==1,5,1))
  all_id_local <- all_id[all_id$company_id==filter_company,]
  all_id_local_active <- subset(all_id_local,all_id_local$status==4)
  all_id_local <- all_id_local[all_id_local$id!=application_id,]
  
  # Apply restrictions if applicable 
  if(nrow(all_id_local)>0){
    
    # Check if client has current active refinance offer
    string_sql <- all_id_local$id[1]
    if(nrow(all_id_local)>1){
      for(i in 2:nrow(all_id_local)){
        string_sql <- paste(string_sql,all_id_local$id[i],sep=",")}
    }
    
    # Active PO refinance offers
    check_active_refs_office <- gen_query(con,
        gen_po_active_refinance_query(db_name,string_sql))
    
    # Terminated PO refinance offers
    check_term_refs_office <- gen_query(con,
        gen_po_refinance_query(db_name,string_sql))
    check_term_refs_office <- subset(check_term_refs_office,
        substring(check_term_refs_office$deleted_at,12,20)!="04:00:00")
    
    # Apply criteria if no relevant offer
    if(nrow(check_term_refs_office)>0){
      check_term_refs_office$difftime <- difftime(Sys.time(),
        check_term_refs_office$deleted_at,units = c("days"))
      check_term_refs_office <- subset(check_term_refs_office,
        check_term_refs_office$difftime<=3)
      check_term_refs_office <- check_term_refs_office[rev(
        order(check_term_refs_office$deleted_at)),]
      if(nrow(check_term_refs_office)>0){
        check_term_refs_office <- check_term_refs_office[1,]
        all_id_local_left <- subset(all_id_local,
          all_id_local$signed_at>=check_term_refs_office$deleted_at)
      } else {
        all_id_local_left <- as.data.frame(NA)
      }
    } else {
      all_id_local_left <- as.data.frame(NA)
    }
    
    # Application for refinance is rejected if no offer for refinance
    if(nrow(check_active_refs_office)==0 & nrow(check_term_refs_office)==0){
      scoring_df$color <- ifelse(scoring_df$color>1 & scoring_df$score!=
        "NULL",1,scoring_df$color)
    }
    
    # Application for refinance is rejected if offer but credit after offer
    if(nrow(check_active_refs_office)==0 & 
       nrow(check_term_refs_office)>0 & nrow(all_id_local_left)>0){
       scoring_df$color <- ifelse(scoring_df$color>1 & scoring_df$score!=
             "NULL",1,scoring_df$color)
    }
    
     # If Credirect : Reject if product is not CreDirect Потребителски - Рефинанс
     if(all_df$product_id!=48 & flag_credirect==1){
       scoring_df$color <- ifelse(scoring_df$color>1 & scoring_df$score!=
                                    "NULL",1,scoring_df$color)
    }
    
  }
  
  # Application for refinance is rejected if dpd >300 days
  if(nrow(all_id_local_active)>0){
    string_sql <- all_id_local_active$id[1]
    if(nrow(all_id_local_active)>1){
      for(i in 2:nrow(all_id_local_active)){
        string_sql <- paste(string_sql,all_id_local_active$id[i],sep=",")}
    }
    max_dpd <- max(gen_query(con,
        gen_plan_main_select_query(db_name,string_sql))$max_delay)
    if(max_dpd>=300){
      scoring_df$color <- ifelse(scoring_df$color>1 & scoring_df$score!=
                                   "NULL",1,scoring_df$color)
    }
  }

  return(scoring_df)
}

