library(curl)
library(dplyr)
library(tidyverse)
library(jsonlite)
library(tidyjson)
library(httr)
library(readxl)
options(scipen=999)

#' Get metadata from an AQUAZIS hub
#'
#' This function retrieves metadata from an AQUAZIS hub and returns it as a tibble.
#'
#' @param hub Character. Base URL of the AQUAZIS hub.
#'
#' @return A tibble containing metadata.
#' @export
#'
#' @examples
#' \dontrun{
#' get_aquazis_meta("https://aquazis.example.com")
#' }
get_aquazis_meta <- function(hub){
  getMeta<-curl(paste0(hub,"/get_sd?f_ort=%2A"))
  MetaDaten<-fromJSON(readLines(getMeta,warn=FALSE))
  return(tibble(MetaDaten$data))
}

#' Get time series list (ZR) from an AQUAZIS hub
#'
#' This function retrieves a list of time series from the AQUAZIS hub.
#' Optionally, a parameter can be provided to filter the request.
#'
#' @param hub Character. Base URL of the AQUAZIS hub.
#' @param parameter List or NULL. Request parameters (e.g., \code{list(f_parameter="Abflusskurve")}).
#'
#' @return A tibble with results or, for certain parameters, the raw JSON list.
#' @export
#'
#' @examples
#' \dontrun{
#' get_aquazis_zrlist("https://aquazis.example.com")
#' get_aquazis_zrlist("https://aquazis.example.com", list(f_parameter="Abflusskurve"))
#' }
get_aquazis_zrlist<-function(hub, parameter=NULL){
  
  zr_list_url<- paste0(hub,"/zrlist_from_db")
  
  if(!is.null(parameter)){
    ts_list<-do_aquazis_query(zr_list_url,parameter)
    ts_list<-fromJSON(readLines(ts_list,warn=FALSE))
    if(parameter$f_parameter == "Abflusskurve")
      {return(ts_list)}
    else
      {return(ts_list$results)}
  }
  
  else{
   
    ts_list<-curl(zr_list_url)
    ts_list<-fromJSON(readLines(ts_list, warn=FALSE))
    return(tibble(ts_list$results))
  }
  

}


#' Get time series data (ZR) from an AQUAZIS hub
#'
#' This function retrieves time series data for a given parameter from the AQUAZIS hub.
#'
#' @param hub Character. Base URL of the AQUAZIS hub.
#' @param parameter List. Request parameters (e.g., \code{list(f_parameter="Abfluss")}).
#'
#' @return A JSON object (as a list) containing time series data.
#' @export
#'
#' @examples
#' \dontrun{
#' get_aquazis_zr("https://aquazis.example.com", list(f_parameter="Abfluss"))
#' }
get_aquazis_zr<-function(hub, parameter){
  zr_list_url<- paste0(hub,"/get_zr")
  ts_data<-do_aquazis_query(zr_list_url,parameter)
  ts_data<-fromJSON(readLines(ts_data,warn=FALSE))
  return(ts_data)
}
  
#' Execute an AQUAZIS query
#'
#' Helper function to perform a request against an AQUAZIS hub and return the connection.
#'
#' @param hub Character. Base URL of the endpoint (including path).
#' @param parameter List. Request parameters as a named list.
#'
#' @return A curl connection to the query URL.
#' @export
#'
#' @examples
#' \dontrun{
#' do_aquazis_query("https://aquazis.example.com/get_zr", list(f_parameter="Abfluss"))
#' }
do_aquazis_query<-function(hub,parameter){
  query_string <- paste0(
    names(parameter), "=", vapply(parameter, URLencode, character(1), reserved = TRUE),
    collapse = "&"
  )
  
  full_url <- paste0(hub, "?", query_string)
  return(curl(full_url))
}

#' Get most recent ETA curves
#'
#' This function identifies two-digit values in a vector, finds those that occur
#' at least twice, and returns the indices of the highest such value.
#'
#' @param vec Character vector. Input vector with numeric values as strings.
#'
#' @return An integer vector with indices of the highest repeated two-digit value.
#' @export
#'
#' @examples
#' vec <- c("01", "02", "12", "12", "07", "12", "09")
#' get_recent_eta_curves(vec)
get_recent_eta_curves<-function(vec){
  # Filtere nur zweistellige Zahlen
  zweistellig <- vec[nchar(vec) == 2]
  # Finde welche mindestens zweimal vorkommen
  counts <- table(zweistellig)
  mehrfach <- names(counts[counts >= 2])
  # Finde die höchste davon
  hoechste <- max(as.numeric(mehrfach))
  # Finde die Indizes dieser Zahl im Originalvektor
  indizes <- which(vec == as.character(hoechste))
  return(indizes)
}


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

