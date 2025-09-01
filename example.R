library(curl)
library(dplyr)
library(tidyverse)
library(jsonlite)
library(tidyjson)
library(httr)
library(readxl)
library(devtools)
options(scipen=999)


install_github("iso-code/aquaZisConnect")
library(aquazisConnect)
hub<-"http://w-db10-pegel51:7979"
shared_data<-"../shared_data"
logs<-"../shared_data"

check_hub_connection(hub)

#aquire all station data from aquaZis
as_meta<-get_aquazis_meta(hub, shared_data, logs)
f_station<-as_meta %>% filter(NAME=="Ahmsen")

############################################
#get specific zrList for all water levels

zrlist<-get_aquazis_zrlist(hub,st_id=f_station[1], parameter="Wasserstand")
zrlist

#get timeseries values from zrid in period
###############################################
begin <- Sys.time()-(60*60*24)*14
end <- Sys.time()
zrid=zrlist$zrid
intervall<-"l"

#if timeseries produces only NA no data is available for the period.
#Find the end of validated data here:
verified_to<-get_az_valid_to(hub, zrid, begin, end, intervall = "l", stepsize = 30, max_retries = 5)

#Hier einmal freigegebenen Bereich durchsuchen für alle die Wasserstand haben
zrlist<-get_aquazis_zrlist(hub,parameter="Wasserstand")
zrids<-zrlist$zrid

verified_to<-get_az_valid_to(hub,zrid, begin, end, intervall = "l", stepsize = 30, max_retries = 5)

final_table<-get_verified_periods(hub, zrids, begin, end, intervall = "l", stepsize = 365, max_retries = 5)

######################################################
zr_data<-get_aquazis_zr(hub, zrid, begin, end)
zr<-extract_az_ts(zr_data,"l")

########################################################

ORT<-as_meta %>% filter(NAME=="Ahmsen") %>% distinct(ORT)

parameter <- list(
  f_ort= as.character(ORT),
  f_parameter = "Wasserstand",
 #  f_defart = "K",
  #  f_herkunft = "F",
   # f_reihenart = "Z",
   f_quelle = "P"
)

zrlist<-get_aquazis_zrlist(hub,parameter)
zrlist


bz# Rating Curves
###################################
ORT<-as_meta %>% filter(NAME=="Ahmsen") %>% distinct(ORT)

parameter <- list(
  f_ort= as.character(ORT),
  f_parameter = "Abflusskurve",
  # f_defart = "K",
  #  f_herkunft = "F",
  #  f_reihenart = "Z",
    f_quelle = "P"
)

zrlist<-get_aquazis_zrlist(hub,parameter)
zrlist

#mylist<-strsplit(response$results$filename,"=")
#result <- lapply(mylist, function(x) {
#  x[2] <- sub("\\..*", "", x[2])
#  x
#})

#vec <- sapply(result, function(x) sub("\\..*", "", x[2]))
#zrid<-response$results$zrid[get_recent_eta(vec)]


#Abflussmessungen = Abfluss
#Wasserstand
parameter <- list(
  f_ort= as.character(ORT),
  f_parameter = "Wasserstand",
  f_defart = "M",
  f_aussage = "Mes"

)

zrlist<-get_aquazis_zrlist(hub,parameter)
zrid<-zrlist$zrid

data_query = list(zrid = as.character(zrid),
#                  von = beginn,
 #                 bis = ende,
                  zpform = "#Y#m#d#H#M#S",
                  yform = "7.3f",
                  compact = "true")

zr_data<-get_aquazis_zr(hub, data_query)


#zr_data$data$Info$`MaxFokus-Von` #zeitreihen beginn



#dim(zr_data$data$Daten)
#zr_data$data$Daten[41650:41702,]


# Etawert,
