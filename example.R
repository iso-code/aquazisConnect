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
zrlist<-get_aquazis_zrlist(hub,type="P", parameter="Wasserstand")
orte<-as.numeric(zrlist$ort)
orte<-orte[!is.na(orte)]

plan(multisession, workers = parallel::detectCores() - 1)

meta_query_parallel <- function(hub, ort) {
#  Sys.sleep(1)
  get_aquazis_meta(hub, ort)
}

if(!check_hub_connection(hub)) {
  meta_list <- readr::read_rds(file.path(shared_data, "aquazis_stations.rds")) %>%
    split(.$ORT)
} else {
  meta_list <- future_map_dfr(orte, ~meta_query_parallel(hub, .x))
  fallback_meta(meta_list, shared_data)
}

meta_tibble <- dplyr::bind_rows(meta_list)

################################################
f_station<-as_meta %>% filter(NAME=="Rheda")
ort<-as.character(f_station$ORT)
ort
############################################
#get specific zrList for all water levels

zrlist<-get_aquazis_zrlist(hub,ort,type="P", parameter="Wasserstand")
head(zrlist)

zrlist<-get_aquazis_zrlist(hub,ort, parameter="Abfluss")
#get timeseries values from zrid in period
###############################################
begin <- Sys.time()-(60*60*24)*365
end <- Sys.time()
zrids=zrlist$zrid
intervall<-"l"

#zrid<-"48415"
#if timeseries produces only NA no data is available for the period.
#Find the end of validated data here:
verified_to<-get_az_valid_to(hub, zrids[1], begin, end, intervall = "l", stepsize = 365, max_retries = 10)
verified_to

verified_to<-get_verified_periods(hub, zrids, begin, end, intervall = "l", stepsize = 365, max_retries = 10) 
verified_to

final_table <- get_verified_periods_parallel(hub, zrids, begin, end, intervall = "l", stepsize = 365, max_retries = 10)
final_table

######################################################
begin <- final_table$start[1]
end <- final_table$valid[1]
zr_data<-get_aquazis_zr(hub, zrids[1], begin, end)
zr<-extract_az_ts(zr_data,"l")



###########################################
ort<-as_meta %>% filter(NAME=="Ahmsen")
ort
zrlist<-get_aquazis_zrlist(hub,ort$ORT,type="mes")
pars<-zrlist$parameter
zrlist
results_list <- lapply(zrlist$zrid, function(zrid) {
  #Sys.sleep(1)  # Pause wegen API
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
