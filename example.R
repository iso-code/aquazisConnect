library(curl)
library(dplyr)
library(tidyverse)
library(jsonlite)
library(tidyjson)
library(httr)
library(readxl)
library(devtools)
library(furrr)
options(scipen=999)


install_github("iso-code/aquaZisConnect")
library(aquazisConnect)
hub<-""
shared_data<-"../data_latest"
logs<-"../logs"
setwd("aquazisConnect")
check_hub_connection(hub)

#aquire all station data from aquaZis
as_meta<-get_aquazis_meta(hub, shared_data, logs)
f_station<-as_meta %>% filter(NAME=="Ahmsen")
ort<-as.character(f_station$ORT)
############################################
#get specific zrList for all water levels

zrlist<-get_aquazis_zrlist(hub, parameter="Wasserstand", type="P")
zrlist

#get timeseries values from zrid in period
###############################################
begin <- Sys.time()-(60*60*24)*365
end <- Sys.time()
zrids=zrlist$zrid
intervall<-"l"

#zrid<-"48415"
#if timeseries produces only NA no data is available for the period.
#Find the end of validated data here:
verified_to<-get_az_valid_to(hub, zrid, begin, end, intervall = "l", stepsize = 365, max_retries = 10)
verified_to

verified_to<-get_verified_periods(hub, zrids, begin, end, intervall = "l", stepsize = 365, max_retries = 10) 
verified_to

final_table <- get_verified_periods_parallel(hub, zrids, begin, end, intervall = "l", stepsize = 365, max_retries = 10)


######################################################
begin <- Sys.time()-(60*60*24)*1665
end <- Sys.time()
zrid<-"48266"
zr_data<-get_aquazis_zr(hub, zrid, begin, end)
zr<-extract_az_ts(zr_data,"l")

########################################################
# Rating Curves
########################################################

begin <- Sys.time()-(60*60*24)*3650
end <- Sys.time()

as_meta$NAME
ORT<-as_meta %>% filter(NAME=="Ahmsen") %>% distinct(ORT) %>% as.character()

zrlist<-get_aquazis_zrlist(hub,ORT,parameter="Wasserstand",type="mes")
zrid<-zrlist$zrid
zr <-get_aquazis_zr(hub, zrid, begin = begin, end = end)
w<-zr$data$Daten

zrlist<-get_aquazis_zrlist(hub,ORT,parameter="Abfluss",type="mes")
zrid<-zrlist$zrid
zr <-get_aquazis_zr(hub, zrid, begin = begin, end = end)
q<-zr$data$Daten

wq<-merge(w,q,by="V1") %>% tibble()
wq$V2.x<-as.numeric(wq$V2.x)
wq$V2.y<-as.numeric(wq$V2.y)
plot(wq$V2.x,wq$V2.y)


###Alle Messungnen

ort<-as_meta %>% filter(NAME=="Ahmsen")

zrlist<-get_aquazis_zrlist(hub,ort$ORT,type="mes")
pars<-zrlist$parameter

results_list <- lapply(zrlist$zrid, function(zrid) {
  Sys.sleep(1)  # Pause wegen API
extract_az_ts(get_aquazis_zr(hub, zrid, begin = begin, end = end))
})
names(results_list) <-  zrlist$einheit
results_list
merged_df <- reduce(results_list, full_join, by = "V1")
colnames(merged_df)<- c("datetime",zrlist$einheit)
merged_df


##################### erellt Abfluskuven
zrlist<-get_aquazis_zrlist(hub,st_id=ort$ORT, parameter="Wasserstand")
zrlist

#vec <- sapply(result, function(x) sub("\\..*", "", x[2]))
#zrid<-response$results$zrid[get_recent_eta(vec)]

#Available Parameters
#[1] ""                     "C * Wurzel(I)"        "Batteriespannung"     "Q=C*Wurzel(I)*P"      "Potenzfunktion_12"    "Q = vm * A"
#[7] "Abfluss"              "Abflussdifferenz"     "Abflussfülle"         "Abflusskurven"        "Abflusskurve"         "Abflussmessergebnis"
#[13] "Abflussspende"        "Abfluss_Beurteilung"  "AEO"                  "Akimaspline_11"       "Akimaspline_8"        "Akkuspannung"
#[19] "Bedeckungsgrad"       "Begleitrechnung"      "Behälterinhalt"       "Bemess.-Niederschlag" "benetzter Umfang"     "Beta-Wert"
#[25] "Betriebsspannung"     "Breite"               "Dampfdruck"           "dbz"                  "Delta W"              "Durchfluss"
#[31] "Erdbodentemperatur"   "Etawert"              "Fließgeschwindigkeit" "Fließquerschnitt"     "Fließquerschnitt(St)" "Förderstrom"
#[37] "Fuelle"               "Gebietsniederschlag"  "Geschwindigkeit"      "Globalstrahlung"      "GOK"                  "GSM-Signalqualität"
#[43] "Himmelsstrahlung"     "HQFülle(1)"           "HQFülle(10)"          "HQFülle(100)"         "HQFülle(2)"           "HQFülle(20)"
#[49] "HQFülle(25)"          "HQFülle(5)"           "HQFülle(50)"          "Hydraulischer Radius" "Klappenstellung"      "Krautwuchs"
#[55] "kvonW"                "KvonW"                "Leistungsfähigkeit"   "Leitfähigkeit"        "Luftdruck"            "Luftfeuchte"
#[61] "Luftfeuchte_rel"      "Lufttemperatur"       "Manning-Strickler"    "Nasstemperatur"       "Neuschneehöhe"        "Niederschlag"
#[67] "Niederschlag N0"      "Niederschlag_Inhalt"  "Oberflächengeschw."   "Parameter_US_Anlage"  "Pegel"                "Pegelnullpunkt"
#[73] "Pegelwasserstan"      "Profilbeiwert"        "Querprofil"           "Schalldruckpegel_dB_" "Schneehöhe"           "Sickerwasser"
#[79] "Signalstärke"         "Sonnenscheindauer"    "Speicherinhalt"       "Status"               "Stauwert"             "Teilfüllungskurve"
#[85] "Teilfüllung_6"        "Teilfüllung_11"       "Teilfüllung_7"        "Teilfüllung_8"        "Temperatur"           "Temperatur_Luft"
#[91] "Verdunstung"          "Verdunstung_Haude"    "Wasseräquivalent"     "Wasserspiegelgefälle" "Wasserstand"          "WasserstandNHN"
#[97] "WasserstandNN"        "Wasserstandkurve"     "Wasserstand_O"        "Wasserstand_U"        "Wassertemperatur"     "Wassertiefe"
#[103] "Windgeschwindigkeit"  "Windrichtung"         "Windsektor"           "Windstärke"
