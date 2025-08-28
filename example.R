source("Skripte/utils.R")


hub<-""

#aquire all station data from aquaZis
as_meta<-get_aquazis_meta(hub)
as_meta

#get all available timeseries as list from aquazis
zrlist<-get_aquazis_zrlist(hub)
zrlist

############################################
#get specific zrList for all water levels
parameter_W <- list(
  f_parameter = "Wasserstand",
  f_defart = "K", 
  f_herkunft = "F",
  f_reihenart = "Z",
  f_quelle = "P"
)

zrlist<-get_aquazis_zrlist(hub,parameter_W)
zrlist

###############################################
beginn <- "20240101000000" # Format "TT/MM/JJJJ HH:mm:ss"
ende <- "2025501000000" # Format "TT.MM.JJJJ HH:mm:ss"
zrid="48056"

data_query = list(zrid = zrid, 
             von = beginn, 
           #  bis = ende, 
             zpform = "#Y#m#d#H#M#S", 
             yform = "7.3f", 
             compact = "true")

zr_data<-get_aquazis_zr(hub, data_query)
zr_data
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



# Etawert, 