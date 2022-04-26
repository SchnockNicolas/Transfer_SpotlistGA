
googleAuthR::gar_set_client(json = "client.json", scopes = c("https://www.googleapis.com/auth/analytics.readonly"))

Sys.setenv(GA_AUTH_FILE = "clientservice.json")

library(magrittr)
library(googleAnalyticsR)
library(data.table)
library(dplyr)

#gs4_auth(path = "clientservice.json")

# Setup --------
 ga_auth()

ga_id <- "235058392" # Twistiti

dates <- seq.Date(from =  as.Date("2022-02-01"), to =  Sys.Date() - 1, by = "days") #


for(i in c(1:length(dates)))
{ # i <- 1
  file <- paste0("data/", dates[i], ".rds")
  if(!file.exists(file))
  {
    
    sessions <- google_analytics_3(id = ga_id, 
                                   start=dates[i], end=dates[i], 
                                   metrics = c("sessions", "users", "pageviews", "transactionRevenue", "productAddsToCart", "goal1Completions","goal2Completions", "goal3Completions", "transactions"), 
                                   dimensions = c("date", "hour", "minute", "sourceMedium"), #, )
                                   max_results = 50000)
    
    if(nrow(sessions) > 0)
    {
      if("sourceMedium" %in% colnames(sessions)) sessions$sourceMedium <- tolower(sessions$sourceMedium )
      #   sessions$medium <- tolower(sessions$medium )
      
      
      # # S'assure de ne pos avoir de duplicates
      # sessions <- as.data.table(sessions)[, .(sessions = sum(sessions, na.rm = T),
      #                                         users = sum(users, na.rm = T),
      #                                         pageviews = sum(pageviews, na.rm = T),
      #                                         transactionRevenue = sum(transactionRevenue, na.rm = T),
      #                                         transactions = sum(transactions, na.rm = T),
      #                                         productAddsToCart = sum(productAddsToCart, na.rm = T),
      #                                         goal1Completions = sum(goal1Completions, na.rm = T),
      #                                         goal2Completions = sum(goal2Completions, na.rm = T),
      #                                         goal3Completions = sum(goal3Completions, na.rm = T)
      # ), by = c("date", "sourceMedium", "campaign", "countryIsoCode")] %>%
      #sessions <- sessions %>% mutate(accountID = accountID_var, connectionID = connectionID ) %>% as.data.frame()
      
      # if("AddData" %in% names(params))  {
      #   for(n in names(params$AddData)) sessions[, n] <- params$AddData[n]
      # }
      # 
      # sessions <- fromBiztoDB(mapping, sessions) ; # ssessions %>% View()
      
      # sessions$date <- as.Date(sessions$date, format = "%Y%m%d")
      
      #   sql_insert_direct(connection[1, "destinationTable"] , sessions,  conn, constraints = c(mapping[which(mapping$DimensionMetric %in% c("date", "Dimension")  & mapping$constraint == 1), "colDB"] ))
      
      saveRDS(sessions, file)
      
    } #"GA_Analytics"
    
  }
  
  
}
