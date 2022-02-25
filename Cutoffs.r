

################################################################
######## Define cutoffs for logistical regression models  ######
################################################################


############# City Cash 

# Application City Cash - worst Offices
cu_app_city_bad_offices <- c(0.39,0.35,0.3,0.22,0.17)

# Application City Cash - normal Offices
cu_app_city_norm_offices <- c(0.46,0.35,0.3,0.22,0.17)

# Repeat City Cash
cu_beh_city <- c(0.4,0.3,0.225,0.15,0.075)



############# Credirect 

# Application Credirect PayDay
cu_app_cred_flex <- c(0.26,0.125,0.1,0.07,0.045)

# Application Credirect Installments
cu_app_cred_user <- c(0.7,0.58,0.5,0.425,0.35)

# Repeat all Credirect
cu_beh_cred <- c(0.75,0.4,0.275,0.175,0.1)

# Application Credirect Frauds
cu_app_cred_frauds <- 0.2

