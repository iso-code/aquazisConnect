#' Check if an AQUAZIS hub is reachable
#'
#' Sends a simple GET request to check whether the given hub URL is reachable.
#' Returns TRUE if reachable, FALSE otherwise. Does not throw an error.
#'
#' @param hub Character. Base URL of the AQUAZIS hub (e.g., "https://aquazis.example.com").
#' @param timeout Integer. Request timeout in seconds (default: 5).
#' @param logpath Character (optional). Path to a log directory for failed attempts.
#'
#' @return Logical. TRUE if the hub is reachable, FALSE otherwise.
#'
#' @examples
#' \dontrun{
#' check_hub_connection("https://aquazis.example.com")
#' }
#'
#' @export
check_hub_connection <- function(hub, timeout = 5, logpath = NULL) {
  if (is.null(hub) || !nzchar(hub)) {
    warning("Hub-URL ist leer oder NULL.")
    return(FALSE)
  }

  # Minimalen Ping versuchen
  test_url <- paste0(hub, "/")  # Root sollte immer erreichbar sein

  result <- tryCatch({
    response <- httr::GET(test_url, httr::timeout(timeout))
    if (httr::status_code(response) < 400) {
      TRUE
    } else {
      warning_msg <- paste("Hub nicht erreichbar (Status:", httr::status_code(response), ")")
      if (!is.null(logpath)) {
        readr::write_lines(
          paste("WARNUNG:", Sys.time(), warning_msg),
          file.path(logpath, "hub_connection.log"),
          append = TRUE
        )
      }
      warning(warning_msg)
      FALSE
    }
  }, error = function(e) {
    error_msg <- paste("Verbindungsfehler:", conditionMessage(e))
    if (!is.null(logpath)) {
      readr::write_lines(
        paste("FEHLER:", Sys.time(), error_msg),
        file.path(logpath, "hub_connection.log"),
        append = TRUE
      )
    }
    warning(error_msg)
    FALSE
  })

  return(result)
}


#' Get metadata (station information) from an AQUAZIS hub
#'
#' This function retrieves metadata (e.g., station list) from a given AQUAZIS hub.
#' If the request fails, a fallback `.rds` file can be used (if `shared_data` is specified).
#' Warnings and errors can optionally be logged.
#'
#' @param hub Character. Base URL of the AQUAZIS hub (e.g., `"https://aquazis.example.com"`). Must not be `NULL` or empty.
#' @param shared_data Character (optional). Path to a directory where fallback metadata is stored or should be saved as `"aquazis_stations.rds"`.
#' @param logpath Character (optional). Path to a directory where warnings and errors will be logged in `"try.outFile"`.
#'
#' @return A tibble containing the metadata (`data`) from the AQUAZIS hub.
#' If the request fails and fallback data exists, the fallback is returned instead.
#'
#' @examples
#' \dontrun{
#' # Basic usage
#' get_aquazis_meta("https://aquazis.example.com")
#'
#' # With fallback and logging
#' get_aquazis_meta("https://aquazis.example.com", shared_data = "data", logpath = "logs")
#' }
#'
#' @export
get_aquazis_meta <- function(hub, shared_data = NULL, logpath = NULL) {
  if (is.null(hub) || !nzchar(hub)) {
    stop("Parameter 'hub' darf nicht NULL oder leer sein.")
  }

  # Ziel-URL vorbereiten
  url <- paste0(hub, "/get_sd?f_ort=%2A")
  fallback_file <- if (!is.null(shared_data)) file.path(shared_data, "aquazis_stations.rds") else NULL
  log_file <- if (!is.null(logpath)) file.path(logpath, "try.outFile") else NULL

  tryCatch(
    {
      # Abruf der Daten
      raw_data <- readLines(url, warn = FALSE)
      json_data <- jsonlite::fromJSON(raw_data)

      # Optional speichern
      if (!is.null(fallback_file)) {
        readr::write_rds(json_data, fallback_file)
      }

      return(tibble::as_tibble(json_data$data))
    },
    warning = function(w) {
      if (!is.null(log_file)) {
        readr::write_lines(
          paste("WARNUNG:", Sys.time(), conditionMessage(w)),
          log_file,
          append = TRUE
        )
      }
      invokeRestart("muffleWarning")
    },
    error = function(e) {
      if (!is.null(log_file)) {
        readr::write_lines(
          paste("FEHLER:", Sys.time(), conditionMessage(e)),
          log_file,
          append = TRUE
        )
      }

      # Fallback nur wenn Datei existiert
      if (!is.null(fallback_file) && file.exists(fallback_file)) {
        fallback_data <- readr::read_rds(fallback_file)
        return(tibble::as_tibble(fallback_data))
      } else {
        stop("Abruf fehlgeschlagen und keine Fallback-Datei verfügbar.")
      }
    }
  )
}


#' Get time series list (ZR) from an AQUAZIS hub
#'
#' This function retrieves a list of time series from the AQUAZIS hub.
#' Optionally, a parameter can be provided to filter the request.
#'
#' @param hub Character. Base URL of the AQUAZIS hub.
#' @param parameter character. Request parameters (e.g., \code{c("Wasserstand)}).
#'
#' @return A tibble with results or, for certain parameters, the raw JSON list.
#' @export
#'
#' @examples
#' \dontrun{
#' get_aquazis_zrlist("https://aquazis.example.com")
#' get_aquazis_zrlist("https://aquazis.example.com", list(f_parameter="Abflusskurve"))
#' }
get_aquazis_zrlist<-function(hub, st_id="", parameter=""){


  parameter <- list(
    f_ort = st_id,
      f_parameter = parameter,
    f_defart = "K",
    f_herkunft = "F",
    f_reihenart = "Z",
    f_quelle = "P"
  )


  zr_list_url<- paste0(hub,"/zrlist_from_db")

  if(!is.null(parameter)){
    ts_list<-create_aquazis_query(zr_list_url,parameter)
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
get_aquazis_zr<-function(hub=NULL, zrid=NULL, begin="", end=""){

  if (is.null(zrid) || is.na(zrid)) {
    stop("Error: zrid NULL or NA")
  }

  if (is.null(hub) || is.na(hub)) {
    stop("Error: Zrid NULL or NA")
  }


  parameter = list(zrid = zrid,
                    von = begin,
                    bis = end,
                    zpform = "#Y#m#d#H#M#S",
                    yform = "7.3f",
                    compact = "true")

  zr_list_url<- paste0(hub,"/get_zr")

  ts_data<-create_aquazis_query(zr_list_url,parameter)
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
#' create_aquazis_query("https://aquazis.example.com/get_zr", list(f_parameter="Abfluss"))
#' }
create_aquazis_query<-function(hub,parameter){
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

