
###################################################
######## Functions to deal with missing data ######
###################################################

# Transform all NULLs to NAs
gen_null_to_na <- function(df){
  if (df$age=="NULL" | is.na(df$age)) {
    df$age <- as.numeric(NA)}
  if (df$maturity=="NULL" | is.na(df$maturity)) {
    df$maturity <- as.numeric(NA)}
  if (df$ckr_status=="NULL" | is.na(df$ckr_status) ) {
    df$ckr_status <- as.numeric(NA)}
  if (df$gender=="NULL" | is.na(df$gender)) {
    df$gender <- as.numeric(NA)}
  if (df$ownership=="NULL" | is.na(df$ownership)) {
    df$ownership <- as.numeric(NA)}
  if (df$education=="NULL" | is.na(df$education)) {
    df$education <- as.numeric(NA)}
  if (df$marital_status=="NULL" | is.na(df$marital_status)) {
    df$marital_status <- as.numeric(NA)}
  if (df$household_children=="NULL" | is.na(df$household_children)) {
    df$household_children <- as.numeric(NA)}
  if (df$on_address=="NULL" | is.na(df$on_address)) {
    df$on_address <- as.numeric(NA)}
  if (df$total_income=="NULL" | is.na(df$total_income)) {
    df$total_income <- as.numeric(NA)}
  if (df$purpose=="NULL" | is.na(df$purpose)) {
    df$purpose <- as.numeric(NA)}
  if (df$experience_employer=="NULL" | is.na(df$experience_employer)) {
    df$experience_employer <- as.numeric(NA)}
  if (df$max_delay=="NULL" | is.na(df$max_delay)) {
    df$max_delay <- as.numeric(NA)}
  if (df$status_work=="NULL" | is.na(df$status_work)) {
    df$status_work <- as.numeric(NA)}
  if (df$credits_cum=="NULL" | is.na(df$credits_cum)) {
    df$credits_cum <- 0}
  if (df$days_diff_last_credit=="NULL" | is.na(df$days_diff_last_credit)) {
    df$days_diff_last_credit <- as.numeric(NA)}
  return(df)
}

# Function to count number of empty fields for each model
gen_empty_fields <- function(flag_beh,flag_credirect,df){
  empty_fields <- ifelse(flag_beh==1 & flag_credirect==0, sum(is.na(df[,
        c("ownership","education","household_children",
          "on_address","experience_employer","purpose",
          "status_work","total_income")])),
     ifelse(flag_beh==1 & flag_credirect==1, sum(is.na(df[,c(
       "experience_employer","ownership","education","status_work",
       "status_active_total")])),
     ifelse(flag_credirect==0, sum(is.na(df[,c("ownership","education",
       "purpose","experience_employer","marital_status",
       "status_active_total","cred_count_total","status_work",
       "total_income")])),
     sum(is.na(df[,c("education","purpose","experience_employer",
        "marital_status","status_work")])))))
  return(empty_fields)
}
