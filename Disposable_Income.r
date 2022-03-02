

############################################################################
######## Functions to compute income, expenses and disposable income  ######
############################################################################

# Function to generate aggregated total income
gen_income <- function(db_name,application_id) {
  data_income_all <- suppressWarnings(dbFetch(dbSendQuery(con,
      gen_income_sql_query(db_name,application_id))))
  data_income <- gen_aggregate_income_exp(data_income_all)
  return(data_income)
}

# Function to generate disposable income (income - expenses)
gen_disposable_income_adj <- function(db_name,application_id,all_df,
                                      addresses,period,criteria){
  data_income_all  <- suppressWarnings(dbFetch(dbSendQuery(con, 
        gen_income_sql_query(db_name,all_df))))
  data_income <- gen_aggregate_income_exp(data_income_all)
  
  data_expenses_all  <- suppressWarnings(dbFetch(dbSendQuery(con, 
        gen_expenses_sql_query(db_name,all_df))))
  data_expenses <- gen_aggregate_income_exp(data_expenses_all)

  data_loans_all  <- suppressWarnings(dbFetch(dbSendQuery(con, 
       gen_loans_sql_query(db_name,all_df))))
  data_loans <- gen_aggregate_income_exp(data_loans_all)
  
  children <- ifelse(is.na(all_df$household_children), 0, 
       all_df$household_children)
  household <- ifelse(is.na(all_df$household_total), 0, 
       all_df$household_total)
  household <- ifelse(3 %in% names(table(data_income_all$sub_type)),
       household - table(data_income_all$sub_type)["3"], household)

  disposable_income <- data_income - 
    ifelse(3 %in% names(table(data_income_all$sub_type)),
           table(data_income_all$sub_type)["3"]*addresses$pensioner,0) -
    ifelse(children>=1, addresses$child*children,0) -
    ifelse((household-children)>0,
       (household-children)*addresses$household,0) - data_expenses - data_loans
  
  disposable_income_adj <- ifelse(period==1,disposable_income*7/30,
       ifelse(period==2,disposable_income*14/30, disposable_income))
  return(disposable_income_adj)
  
}

# Function to generate adjusted total income to time period (weekly,...)
gen_t_income <- function(db_name,application_id,period){
  data_income_all  <- suppressWarnings(dbFetch(dbSendQuery(con, 
        gen_income_sql_query(db_name,application_id))))
  data_income <- gen_aggregate_income_exp(data_income_all)
  
  t_income <- ifelse(period==1,data_income*7/30,
      ifelse(period==2,data_income*14/30,data_income))
  return(t_income)
}


