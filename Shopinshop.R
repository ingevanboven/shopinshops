######################################
# Automatisering SHOP IN SHOP Update #
######################################
{
  library(stringr)
  library(lubridate)
  library(plyr)
  library(dplyr)
  library(ISOweek)
  library(googlesheets)
  library(httr)
  
  ######################################
  #Defining date range for all queries: 
  charDate <- ymd(Sys.Date())- days(0:8)-days(1)
  charDate <- str_replace_all(charDate, "-", ".")
  
  #Number of days for timestamp
  #Script always takes the x(timestamp) most recent days as output
  timestamp <- 7
  ######################################
  #Preparation 
  {
    #Create upload file
    dfTussen <- data.frame(nrow = 1)
  }}

#############################################
#After Dinner Shop
#############################################
{ 
  # Name shop | jaartal | weeknummer
  SHOP <- "After dinner shop"
  year <- 2019
  weekNR <- 11
}

#After Dinner Shop - S&N MESSAGE
{
  # Defining ES query
  query <- "info.type:SearchAndNavigationMessage AND user.testklant:false AND NOT _exists_:user.agentId AND user.anoniem:false AND (searchUrl:(\\/webshop\\/assortiment\\/\\_\\/N\\-1z103mv* OR *after\\-dinner*))"
  
  #Assign relevant list(s) to outputfields
  outputFields <- c("timestamp_event", "user.klantNummer", "browser.ipAddress")
  
  #Source Elastic import script
  source("/home/c.stienen/General R Scripts/Elastic_import_empty_indices.R")
  
  "Importing data from Elastic: All Done!"
  
  #Remove all internal IP's 
  source("/home/c.stienen/Info/IP Bidfood.R")
  dfES <- subset(dfES, !(dfES$browser.ipAddress %in% ipBidfood))
  dfES$browser.ipAddress <- NULL
  rm(ipBidfood)
  
  #Filtering correct timestamps
  {
    dfES$t2 <- as.Date(str_extract(dfES$timestamp_event, "^.{10}"))
    t1 <- Sys.Date()
    t1 <- ymd(t1) - 1
    t2 <- ymd(t1) - (timestamp - 1)
    date <- seq(from = t2, to = t1, by = "day")
    dfES <- subset(dfES, dfES$t2 %in% as.list(date))
  }
  
  #Adding number of pageviews&uniqueklantnrs
  dfES$count <- 1 
  PageView <- sum(dfES$count)
  PageViewCust <- length(unique(dfES$user.klantNummer))
}

#After Dinner Shop - A2C MESSAGE
{
  # Defining ES query
  query <- "(info.type:AddToCartMessage AND context.user.testklant:false AND NOT _exists_:context.user.agentId AND context.user.anoniem:false AND context.searchUrl:(\\/webshop\\/assortiment\\/\\_\\/N\\-1z103mv* OR *after\\-dinner*))"
  
  #Assign relevant list(s) to outputfields
  outputFields <- c("timestamp_event", "context.browser.ipAddress", "totaalBedrag")
  
  #Source Elastic import script
  source("/home/c.stienen/General R Scripts/Elastic_import_empty_indices.R")
  
  "Importing data from Elastic: All Done!"
  
  #Remove all internal IP's 
  source("/home/c.stienen/Info/IP Bidfood.R")
  dfES <- subset(dfES, !(dfES$context.browser.ipAddress %in% ipBidfood))
  dfES$context.browser.ipAddress <- NULL
  rm(ipBidfood)
  
  #Filtering correct timestamps
  {
    dfES$t2 <- as.Date(str_extract(dfES$timestamp_event, "^.{10}"))
    t1 <- Sys.Date()
    t1 <- ymd(t1) - 1
    t2 <- ymd(t1) - (timestamp - 1)
    date <- seq(from = t2, to = t1, by = "day")
    dfES <- subset(dfES, dfES$t2 %in% as.list(date))
  }
  
  #Adding number of A2C
  A2C <- sum(dfES$totaalBedrag)
  
}

#After Dinner Shop - SubmitOrder MESSAGE
{
  # Defining ES query
  query <- "(info.type:SubmitOrderMessage AND context.user.testklant:false AND NOT _exists_:context.user.agentId AND context.user.anoniem:false AND context.searchUrl:(\\/webshop\\/assortiment\\/\\_\\/N\\-1z103mv* OR *after\\-dinner*))"
  #Assign relevant list(s) to outputfields
  outputFields <- c("timestamp_event", "context.user.klantNummer", "totaalBedrag", "context.browser.ipAddress", "orderDatum_date")
  
  #Source Elastic import script
  source("/home/c.stienen/General R Scripts/Elastic_import_empty_indices.R")
  
  "Importing data from Elastic: All Done!"
  
  #Filtering correct timestamps
  {
    dfES$t2 <- as.Date(str_extract(dfES$orderDatum_date, "^.{10}"))
    t1 <- Sys.Date()
    t1 <- ymd(t1) - 1
    t2 <- ymd(t1) - (timestamp - 1)
    date <- seq(from = t2, to = t1, by = "day")
    dfES <- subset(dfES, dfES$t2 %in% as.list(date))
  }
  
  #Adding number of submitorder&uniekklantnr
  Totaalbedrag <- sum(dfES$totaalBedrag)
  TotaalbedragCust <- length(unique(dfES$context.user.klantNummer))
}

#After Dinner Shop - DF ORDERNEN VOOR UPLOAD
{ 
  #dfTussen vullen met data
  
  dfTussen$SHOP <- SHOP
  dfTussen$year <- year
  dfTussen$weekNR <- weekNR
  dfTussen$PageView <- PageView
  dfTussen$PageViewCust <- PageViewCust
  dfTussen$A2C <- A2C
  dfTussen$Totaalbedrag <- Totaalbedrag
  dfTussen$TotaalbedagCust <- TotaalbedragCust
  
  dfTussen$nrow <- NULL
  
  #dfshop krijgt data van dftussen
  
  dfSHOPS <- dfTussen
}

#############################################
#BURGERBAR
#############################################
{ 
  # Name shop | jaartal | weeknummer
  SHOP <- "Burgerbar"
  year <- 2020
  weekNR <- 10
}
#BURGERBAR - S&N MESSAGE
{
  # Defining ES query
  query <- "info.type:SearchAndNavigationMessage AND user.testklant:false AND NOT _exists_:user.agentId AND user.anoniem:false AND (searchUrl:(\\/webshop\\/assortiment\\/\\_\\/N\\-1z103mv* OR *after\\-dinner*))"
  
  #Assign relevant list(s) to outputfields
  outputFields <- c("timestamp_event", "user.klantNummer", "browser.ipAddress")
  
  #Source Elastic import script
  source("N:/Merchandising/27 Team Optimalisatie/00 IBN-CSN/SHOPS update/Elastic_import_empty_indices.R")
  
  "Importing data from Elastic: All Done!"
  
  #Remove all internal IP's 
  source("N:/Merchandising/27 Team Optimalisatie/00 IBN-CSN/SHOPS update/IP Bidfood.R")
  dfES <- subset(dfES, !(dfES$browser.ipAddress %in% ipBidfood))
  dfES$browser.ipAddress <- NULL
  rm(ipBidfood)
  
  #Filtering correct timestamps
  {
    dfES$t2 <- as.Date(str_extract(dfES$timestamp_event, "^.{10}"))
    t1 <- Sys.Date()
    t1 <- ymd(t1) - 1
    t2 <- ymd(t1) - (timestamp - 1)
    date <- seq(from = t2, to = t1, by = "day")
    dfES <- subset(dfES, dfES$t2 %in% as.list(date))
  }
  
  #Adding number of pageviews&uniqueklantnrs
  dfES$count <- 1 
  PageView <- sum(dfES$count)
  PageViewCust <- length(unique(dfES$user.klantNummer))
}

#BURGERBAR - A2C MESSAGE
{
  # Defining ES query
  query <- "(info.type:AddToCartMessage AND context.user.testklant:false AND NOT _exists_:context.user.agentId AND context.user.anoniem:false AND context.searchUrl:(\\/webshop\\/assortiment\\/\\_\\/N\\-1z103mv* OR *after\\-dinner*))"
  
  #Assign relevant list(s) to outputfields
  outputFields <- c("timestamp_event", "context.browser.ipAddress")
  
  #Source Elastic import script
  source("/home/c.stienen/General R Scripts/Elastic_import_empty_indices.R")
  
  "Importing data from Elastic: All Done!"
  
  #Remove all internal IP's 
  source("/home/c.stienen/Info/IP Bidfood.R")
  dfES <- subset(dfES, !(dfES$context.browser.ipAddress %in% ipBidfood))
  dfES$context.browser.ipAddress <- NULL
  rm(ipBidfood)
  
  #Filtering correct timestamps
  {
    dfES$t2 <- as.Date(str_extract(dfES$timestamp_event, "^.{10}"))
    t1 <- Sys.Date()
    t1 <- ymd(t1) - 1
    t2 <- ymd(t1) - (timestamp - 1)
    date <- seq(from = t2, to = t1, by = "day")
    dfES <- subset(dfES, dfES$t2 %in% as.list(date))
  }
  
  #Adding number of A2C
  dfES$count <- 1 
  A2C <- sum(dfES$totaalBedrag)
  
}

#BURGERBAR - SubmitOrder MESSAGE
{
  # Defining ES query
  query <- "(info.type:SubmitOrderMessage AND context.user.testklant:false AND NOT _exists_:context.user.agentId AND context.user.anoniem:false AND context.searchUrl:(\\/webshop\\/assortiment\\/\\_\\/N\\-1z103mv* OR *after\\-dinner*))"
  #Assign relevant list(s) to outputfields
  outputFields <- c("timestamp_event", "context.user.klantNummer", "totaalBedrag", "context.browser.ipAddress", "orderDatum_date")
  
  #Source Elastic import script
  source("/home/c.stienen/General R Scripts/Elastic_import_empty_indices.R")
  
  "Importing data from Elastic: All Done!"
  
  #Filtering correct timestamps
  {
    dfES$t2 <- as.Date(str_extract(dfES$orderDatum_date, "^.{10}"))
    t1 <- Sys.Date()
    t1 <- ymd(t1) - 1
    t2 <- ymd(t1) - (timestamp - 1)
    date <- seq(from = t2, to = t1, by = "day")
    dfES <- subset(dfES, dfES$t2 %in% as.list(date))
  }
  
  #Adding number of submitorder&uniekklantnr
  dfES$count <- 1
  Totaalbedrag <- sum(dfES$totaalBedrag)
  TotaalbedragCust <- length(unique(dfES$context.user.klantNummer))
}

#BURHER BAR - DF ORDERNEN VOOR UPLOAD
{ 
  #dfTussen vullen met data
  
  dfTussen$SHOP <- SHOP
  dfTussen$year <- year
  dfTussen$weekNR <- weekNR
  dfTussen$PageView <- PageView
  dfTussen$PageViewCust <- PageViewCust
  dfTussen$A2C <- A2C
  dfTussen$Totaalbedrag <- Totaalbedrag
  dfTussen$TotaalbedagCust <- TotaalbedragCust
  
  #dfshop krijgt data van dftussen
  
  dfSHOPS <- rbind(dfSHOPS, dfTussen)
}

#############################################
#UPLOAD naar GOOGLE SHEETS
#############################################
{
  rm(dfES, charDate, date, t1, t2, timestamp)
  write.csv(dfConversie, paste(paste("/home/c.stienen/conversieDashboard/Datasets/dfConversie", dfConversie$YearWeek, sep = ""),".csv", sep=""),
            row.names = FALSE, quote = TRUE)
  
  #CHECK GS_upload_cronjob.R script in home folder for upload! 
  #options(httr_oob_default=TRUE) 
  #set_config(config(ssl_verifypeer = 0L))
  #gs_auth()
  #conversieSheets <- gs_title("Conversie en target dashboard - input")
  #gs_add_row(ss = conversieSheets,
  #           ws = "Rstudio_Elastic",
  #           input = dfConversie,
  #           verbose = FALSE)
}
