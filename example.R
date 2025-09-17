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
hub<-""
shared_data<-"../data_latest"
logs<-"../logs"

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
begin <- Sys.time()-(60*60*24)*30
end <- Sys.time()
zrid=zrlist$zrid
intervall<-"l"
zrid<-"47767"
#if timeseries produces only NA no data is available for the period.
#Find the end of validated data here:
verified_to<-get_az_valid_to(hub, zrid, begin, end, intervall = "l", stepsize = 365, max_retries = 5)

#Hier einmal freigegebenen Bereich durchsuchen für alle die Wasserstand haben
zrlist<-get_aquazis_zrlist(hub,parameter="Wasserstand")
zrids<-zrlist$zrid

final_table<-get_verified_periods(hub, zrids, begin, end, intervall = "l", stepsize = 365, max_retries = 5)

as_meta[which(as_meta$ORT==zrlist$ort[which(zrlist$zrid == zrid)]),]


tt<-merge(zrlist,final_table) %>%  distinct(ort,zrid,parameter,start,valid,end)
colnames(tt)[1]<-"ORT"
final_meta<-merge(tt,as_meta) %>% arrange(NAME)
write_rds(final_meta,"verified_levels_nrw.rds")

######################################################
zr_data<-get_aquazis_zr(hub, zrid, begin, end)
zr<-extract_az_ts(zr_data,"l")

########################################################
# Rating Curves
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

zrlist<-get_aquazis_zrlist(hub,parameter="Wasserstand",art="M", aussage ="Mes")
zrid<-zrlist$zrid

# Etawert,
