

################################################################
######## Functions to generate addresses per application  ######
################################################################

# Function to generate and arrange coordinates
gen_coordinates <- function(db_name,application_id,all_df) {
  
  # Read addresses (per application_id and per client)
  address <- suppressWarnings(fetch(dbSendQuery(con,
    gen_address_coordinates_query(db_name,application_id)), n=-1))
  address_client <- suppressWarnings(fetch(dbSendQuery(con,
    gen_address_client_coordinates_query(db_name,all_df)), n=-1))
  if(nrow(address)>0){
    address$per_app <- 1
  }
  if(nrow(address_client)>0){
    address_client$per_app <- 0
  }
  
  # Choose correctly
  address <- rbind(address,address_client)
  address <- subset(address,!is.na(address$lat) & !is.na(address$lon))
  address$hierarchy <- 
     ifelse(address$type==2 & address$per_app==1,1,
     ifelse(address$type==1 & address$per_app==1,2,
     ifelse(address$type==2 & address$per_app==0,3,
     ifelse(address$type==1 & address$per_app==0,4,
     ifelse(address$type==3 & address$per_app==1,5,
     ifelse(address$type==3 & address$per_app==0,6,999))))))
  
  # Rearrange some erroeneous coordinates due to OpenStreetMap 
  address$lat <- round(as.numeric(address$lat),5)
  address$lon <- round(as.numeric(address$lon),5)
  
  # Take preferabbly current, but will take official if neeeded
  address$type <- ifelse(address$type==3,0,address$type)
  address <- address[order(address$hierarchy),]
  address <- address[1,]
  
  # Create final dataframe
  return(address)

}

# Determine if address is risky or not 
gen_flag_risky_address <- function(db_name,application_id,risky_address,all_df){
  
  # Get current address
  address <- gen_coordinates(db_name,application_id,all_df)
  
  if(nrow(address)!=0){
    
    if((!is.na(address$lat) & !is.na(address$lon))){
      
    # Get list of risky addresses 
    risky_address$cur_lon <- address$lon
    risky_address$cur_lat <- address$lat
    
    # Compute distance between current address and risky addresses
    risky_address$cst_a <- 
      (sin((risky_address$cur_lat-risky_address$lat)*pi/180/2))^2 + 
      cos(risky_address$lat*pi/180)*cos(risky_address$cur_lat*pi/180)*
      (sin((risky_address$cur_lon-risky_address$lon)*pi/180/2))^2
    risky_address$cst_c <- 2*atan2(sqrt(risky_address$cst_a),
       sqrt(1-risky_address$cst_a))
    risky_address$distance <- 6371e3 * risky_address$cst_c
    
    # Compute flag of risky address
    flag_risky_address <- ifelse(min(risky_address$distance)<=500 & 
                                 address$location_precision==1,1,0)
    }
    else flag_risky_address <- NA
    } else {
    flag_risky_address <- NA
  }

  
  return(cbind(flag_risky_address,address))
  
}


