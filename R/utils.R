#' Check if an AQUAZIS hub is reachable
#'
#' Sends a simple GET request to check whether the given hub URL is reachable.
#' Returns TRUE if reachable, FALSE otherwise. Does not throw an error.
#'
#' @param hub Character. Base URL of the AQUAZIS hub (e.g. "https://aquazis.example.com").
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
#' Retrieves metadata (e.g., station list) from a given AQUAZIS hub.
#' If the request fails, a fallback `.rds` file can be used (if `shared_data` is specified).
#'
#' @param hub Character. Base URL of the AQUAZIS hub (must not be `NULL` or empty).
#' @param ort Filter ORT by pattern, ? is playceholder for one character, * is for multiple characters
#' @param flatten Character. Whether to flatten the JSON response (default: "true").
#' @param logpath Character (optional). Path to a directory where warnings and errors
#'   will be logged in "try.outFile".
#'
#' @return A tibble containing the metadata (`data`) from the AQUAZIS hub.
#' If the request fails and fallback data exists, the fallback is returned instead.
#'
#' @examples
#' \dontrun{
#' get_aquazis_meta("https://aquazis.example.com")
#' get_aquazis_meta("https://aquazis.example.com", shared_data = "data", logpath = "logs")
#' }
#'
#' @export
get_aquazis_meta <- function(hub, ort, logpath = NULL, flatten = "true") {
  if (is.null(hub) || !nzchar(hub)) {
    stop("Parameter 'hub' darf nicht NULL oder leer sein.")
  }

  hub <- sub("/+$", "", hub)

  if (is.logical(flatten)) {
    flatten_q <- if (isTRUE(flatten)) "true" else "false"
  } else {
    flatten_q <- tolower(as.character(flatten))
    if (!flatten_q %in% c("true", "false")) {
      stop("Parameter 'flatten' muss TRUE/FALSE oder 'true'/'false' sein.")
    }
  }

  log_file <- if (!is.null(logpath)) file.path(logpath, "try.outFile") else NULL
  log_it <- function(level, msg) {
    if (!is.null(log_file)) {
      readr::write_lines(
        paste(level, format(Sys.time(), "%Y-%m-%d %H:%M:%S%z"), msg),
        log_file,
        append = TRUE
      )
    }
  }

  if (!is.null(ort) && nzchar(ort)) {
    url <- paste0(hub, "/get_sd?f_ort=", curl::curl_escape(ort), "&flatten=", flatten_q)
  } else {
    url <- paste0(hub, "/get_sd?flatten=", flatten_q)
  }

  json_data <- tryCatch(
    {
      raw_data <- readLines(url, warn = FALSE, encoding = "UTF-8")
      jsonlite::fromJSON(raw_data, simplifyVector = TRUE)
    },
    warning = function(w) {
      log_it("WARNUNG:", conditionMessage(w))
      invokeRestart("muffleWarning")
    },
    error = function(e) {
      log_it("FEHLER:", conditionMessage(e))
      NULL
    }
  )

  if (is.null(json_data)) {
    return(tibble::tibble())
  }

  out <- if (!is.null(json_data$data)) json_data$data else json_data
  if (is.data.frame(out)) {
    return(tibble::as_tibble(out))
  }
  tibble::as_tibble(out, .name_repair = "unique")
}


#' Get time series list (ZR) from an AQUAZIS hub
#'
#' Retrieves a list of time series from the AQUAZIS hub.
#' Optionally, a parameter can be provided to filter the request.
#'
#' @param hub Character. Base URL of the AQUAZIS hub.
#' @param parameter Character. Request parameter (e.g. "Wasserstand").
#' @param st_id Character. Optional station ID(s) to filter the request.
#' @param type Character. Optional type to filter the request. (p=validated, mes=measurements)
#' @return A tibble with results or, for certain parameters, the raw JSON list.
#'
#' @examples
#' \dontrun{
#' get_aquazis_zrlist("https://aquazis.example.com")
#' get_aquazis_zrlist("https://aquazis.example.com", parameter = "Abflusskurve")
#' }
#'
#' @export
get_aquazis_zrlist<-function(hub, st_id=NULL, parameter=NULL, type=NULL){

  zr_list_url<- paste0(hub,"/zrlist_from_db")
  
  if(is.null(type)) type<-""
  if(is.null(parameter)) parameter<-""

if (!is.null(st_id) && length(st_id) >= 1 && !is.null(parameter) && length(parameter) > 1) {
  
  if (type == "mes") {
    parameter <- list(
      f_ort = as.character(st_id),
      f_parameter = parameter,
      f_defart = "M",
      f_aussage = type
    )
  } else {
    parameter <- list(
      f_ort = st_id,
      f_parameter = parameter,
      f_defart = "K",
      f_herkunft = "F",
      f_reihenart = "Z",
      f_quelle = type
    )
  }

} else if (!is.null(st_id) && length(st_id) >= 1) {
  
  if (type == "mes") {
    parameter <- list(
      f_ort = as.character(st_id),
      f_defart = "M",
      f_aussage = type
    )
  } else {
    parameter <- list(
      f_ort = st_id,
      f_defart = "K",
      f_herkunft = "F",
      f_reihenart = "Z",
      f_quelle = type
    )
  }

} else if (!is.null(parameter) && length(parameter) >= 1) {
  
  if (type == "mes") {
    parameter <- list(
      f_parameter = parameter,
      f_defart = "M",
      f_aussage = type
    )
  } else {
    parameter <- list(
      f_parameter = parameter,
      f_defart = "K",
      f_herkunft = "F",
      f_reihenart = "Z",
      f_quelle = type
    )
  }

}

    
 else {stop("Error: Provide zrid and/or Parameter")}

  if(!is.null(parameter)){
    ts_list<-create_aquazis_query(zr_list_url,parameter)
    return(ts_list$results)

}
}

#Abflussmessungen = Abfluss
#Wasserstand
#parameter <- list(
#  f_ort= as.character(ORT),
#  f_parameter = "Wasserstand",
#  f_defart = "M",
#  f_aussage = "Mes"
#)


#' Get time series data (ZR) from an AQUAZIS hub
#'
#' Retrieves time series data for a given ZR-ID and optional time range.
#'
#' @param hub Character. Base URL of the AQUAZIS hub.
#' @param zrid Character. ZR identifier (time series id) to request.
#' @param begin Character. Optional start date/time as string (default: "").
#' @param end Character. Optional end date/time as string (default: "").
#'
#' @return A JSON object (as a list) containing time series data.
#'
#' @examples
#' \dontrun{
#' get_aquazis_zr("https://aquazis.example.com",
#'                zrid = "12345",
#'                begin = "2020-01-01",
#'                end = "2020-12-31")
#' }
#'
#' @export
get_aquazis_zr<-function(hub=NULL, zrid=NULL, begin="", end=""){

zr_list_url<- paste0(hub,"/get_zr")

  if (is.null(zrid) || is.na(zrid)) {
    stop("Error: zrid NULL or NA")
  }

  if (is.null(hub) || is.na(hub)) {
    stop("Error: hub NULL or NA")
  }

    all_data <- list()
    last_date_prev <- NA

repeat{

  if(is.POSIXct(begin) && is.POSIXct(end) ) { 
  begin<-format(begin,"%Y%m%d%H%M%S")
  end<-format(end,"%Y%m%d%H%M%S")
  }

  parameter = list(zrid = as.character(zrid),
                    von = begin,
                    bis = end,
                    zpform = "#Y#m#d#H#M#S",
                    yform = "7.3f",
                    compact = "true")

  

  ts_data<-create_aquazis_query(zr_list_url,parameter)
  all_data[[length(all_data) + 1]] <- ts_data$data$Daten

  last_date <- max(ts_data$data$Daten[,1], na.rm = TRUE)

  if (!is.finite(last_date)) {
  warning("Kein gültiges Datum gefunden (alle Werte NA oder ungültig).")
  last_date <- NA
  }

  message(paste0("Retrieved data up to: ", last_date))

  if(last_date == end || (!is.na(last_date_prev) && last_date == last_date_prev)){ 
    break
  }
  
  begin <- as.POSIXct(last_date, format="%Y%m%d%H%M%S")+1
  end <- as.POSIXct(end, format="%Y%m%d%H%M%S")
  
  last_date_prev <- last_date

  }

  if (length(all_data) > 0) {
    ts_data$data$Daten <- do.call(rbind, all_data)
  }

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
#'
#' @examples
#' \dontrun{
#' create_aquazis_query("https://aquazis.example.com/get_zr",
#'                      list(f_parameter = "Abfluss"))
#' }
#'
#' @export
create_aquazis_query<-function(hub,parameter){
  query_string <- paste0(
    names(parameter), "=", vapply(parameter, URLencode, character(1), reserved = TRUE),
    collapse = "&"
  )

  full_url <- paste0(hub, "?", query_string)
  result <- data.frame()
  
  result <- tryCatch({
    # HTTP-Anfrage
    resp <- curl::curl_fetch_memory(full_url)
    
    if(resp$status_code == 500){
      message("create_aquazis_query: Server error 500: data could not be loaded.")
      return(resp)
    }

    # Statuscode prüfen
    if (resp$status_code != 200) {
      message("create_aquazis_query: HTTP error ", resp$status_code, ": data could not be loaded.")
      return(resp)
    }
    
        
    # Antwortinhalt in String umwandeln
    json_text <- rawToChar(resp$content)
   #write(message(nchar(json_text, type = "bytes")),file="log.tmp",append=TRUE)
    # JSON parsen
    return(jsonlite::fromJSON(json_text))
    

  })

}

#' Extract time series from AQUAZIS data
#'
#' This function extracts a subset of columns from an AQUAZIS time series dataset
#' and converts them to proper types for further processing.
#'
#' @param zr_data A list or object returned by AQUAZIS API containing the `$data$Daten` field.
#' @param intervall Character, either `"l"` for low-resolution or `"r"` for high-resolution time series.
#'                 Defaults to `"l"`.
#'
#' @return A tibble with two or three columns depending on `intervall`:
#' \itemize{
#'   \item `V1` - POSIXct datetime
#'   \item `V2` - numeric value
#'   \item (optional) `V3` - numeric value for `"r"` interval
#' }
#'
#' @importFrom dplyr select
#' @importFrom lubridate as_datetime
#' @export
extract_az_ts<-function(zr_data, intervall="l"){

empty_result <- data.frame(
  V1 = NA,
  V2 = NA,
    stringsAsFactors = FALSE
  )

  zr <- as.data.frame(zr_data$data$Daten) 

  if (nrow(zr) == 0) {
   message("extract_az_ts: No data available.")
   zr<-empty_result
  } else {
     zr <- zr  %>%   {
    if (intervall == "l") {
      select(., 1:2)
    } else if (intervall == "r") {
      select(., 2:4)
    } else {
      .
    }
  }
  zr$V2<-as.numeric(zr$V2)
  zr$V1<-lubridate::as_datetime(zr$V1)
}
  return(zr)
}


#' Get Valid-To Timestamp for AQUAZIS Time Series
#'
#' This function retrieves and processes AQUAZIS time series data for a given hub and ZR-ID,
#' and determines the latest timestamp before a continuous block of missing data (`NA`s).
#' If the data is insufficient, it performs exponential backoff retries to fetch more historical data.
#'
#' @param hub Character. The hub identifier to query data from.
#' @param zrid Character or numeric. The ZR-ID used to identify the data series.
#' @param begin POSIXct. The start date/time of the data request.
#' @param end POSIXct. The end date/time of the data request.
#' @param intervall Character. The interval string for time series aggregation. Default is `"l"`.
#' @param stepsize Integer. Number of days to go further back in time when retrying. Default is `30`.
#' @param max_retries Integer. Maximum number of exponential backoff retries. Default is `5`.
#'
#' @return POSIXct. The timestamp indicating the "valid to" point, which is the last valid time before a missing data block.
#'
#' @examples
#' \dontrun{
#'   hub <- "example_hub"
#'   zrid <- "123456"
#'   begin <- as.POSIXct("2025-01-01")
#'   end <- as.POSIXct("2025-08-01")
#'   valid_to <- get_az_valid_to(hub, zrid, begin, end)
#'   print(valid_to)
#' }
#'
#' @export
get_az_valid_to <- function(hub, zrid, begin, end, intervall = "l", stepsize = 30, max_retries = 5) {
  i <- 1
  retry_count <- 0
  wait_base <- 1
  wait_time <- wait_base
  zr <- data.frame()
  zr_data <- NULL
  
  repeat {
    Sys.sleep(wait_time)
  
    resp <- safe_get_aquazis_zr(hub, zrid, begin, end)
   # print(resp)
    if (resp$status_code == 429) {
      message(sprintf("get_az_valid_to: HTTP 429: Rate Limit. Waiting %.1f s...", wait_time))
   
      wait_time <- min(wait_time + 2, 10)
      retry_count <- retry_count + 1
      if (retry_count > max_retries) {
        message("get_az_valid_to: Max an retries reached without after rate limiting.")
         return(tibble::tibble(zrid = zrid, start = NA, valid = NA, ts_valid=NA, end = NA))
      }
      next
    }
    
    if (resp$status_code != 200) {
      if (resp$status_code == 500 && retry_count < max_retries) {
        message(sprintf("get_az_valid_to: error (500): %s. Retry in %s...", resp$error,wait_time))
        Sys.sleep(wait_time)
        #wait_time <- min(wait_time + 2, 10)
        retry_count <- retry_count + 1
        next
      }
      message(sprintf("get_az_valid_to: error (%d): %s", resp$status_code, resp$error))
      return(tibble::tibble(zrid = zrid, start = NA, valid = NA, ts_valid=NA, end = NA))
    }
    
    zr_data <- resp$data
    
    # Zeitreihe extrahieren
    zr <- tryCatch(extract_az_ts(zr_data, intervall),
                   error = function(e) data.frame(),silent=TRUE)
    
    if (!is.data.frame(zr) || nrow(zr) == 0) {
      message("get_az_valid_to: extract_az_ts delivered no data frame or empty data frame.")
      return(tibble::tibble(zrid = zrid, start = NA, valid = NA, ts_valid=NA, end = NA))
    }
    
    # Genügend Daten vorhanden
    if (nrow(zr) > 3) break
    
    # Sonst Zeitfenster erweitern
    message("get_az_valid_to: No Data found. Increasing time range...")
    begin <- begin - (60 * 60 * 24) + (13 + i)
    i <- i + stepsize
    retry_count <- retry_count + 1
    if (retry_count > max_retries) {
      message("get_az_valid_to: Max an retries reached without sufficient data.")
      return(tibble::tibble(zrid = zrid, start = NA, valid = NA, ts_valid=NA, end = NA))
    }
  }
  
  # --- Daten aufbereiten ---
  zr$V2 <- suppressWarnings(as.numeric(zr$V2))
  zr$V1 <- suppressWarnings(lubridate::as_datetime(zr$V1))
  
  ts_start <- tryCatch(lubridate::as_datetime(zr_data$data$Info$`MaxFokus-Von`), error = function(e) NA)
  ts_end   <- tryCatch(lubridate::as_datetime(zr_data$data$Info$`Fokus-Bis`), error = function(e) NA)
  ts_valid <- tryCatch(lubridate::as_datetime(zr_data$data$Info$`MaxFokus-Bis`), error = function(e) NA)
  xcoord<-zr_data$data$Info$'ZR-X'
  ycoord<-zr_data$data$Info$'ZR-Y'
  
  valid_to <- tryCatch({
    if (any(is.na(zr$V2))) {
      lubridate::parse_date_time(
        zr$V1[max(which(is.na(zr$V2))) - 2],
        tz = "UTC", orders = "ymdHMS"
      )
    } else NA
  }, error = function(e) NA)
  
#  message(paste0("Processing Nr.",zrid))
  tibble::tibble(
    zrid = zrid,
    start = ts_start,
    valid = valid_to,
    ts_valid = ts_valid,
    end   = ts_end,
    xcoord = xcoord,
    ycoord = ycoord
  )
}




#' Retrieve verified periods for multiple AQUAZIS time series
#'
#' This function iterates over a list of ZRIDs and determines the latest verified
#' time for each series by calling [get_az_valid_to()]. The results are combined
#' into a single tibble.
#'
#' @param hub Character string. Base URL of the AQUAZIS hub.
#' @param zrids Character or numeric vector. One or more ZRID identifiers of time series.
#' @param begin POSIXct or character. Start date/time of the query period.
#' @param end POSIXct or character. End date/time of the query period.
#' @param intervall Character string. Interval type:
#' `"l"` for left time intervall of measurement or `"r"` for right intervall of measurement.
#' @param stepsize Integer. Step size in days used for retrying data retrieval
#' if insufficient values are returned. Default is 30.
#' @param max_retries Integer. Maximum number of retry attempts per ZRID. Default is 5.
#'
#' @return A tibble containing the verified periods for all requested ZRIDs.
#' Each row corresponds to one ZRID with its associated metadata and verified date range.
#'
#' @details
#' The function calls [get_az_valid_to()] for each element in `zrids`. If the data
#' cannot be retrieved immediately, retries are performed with increasing back-off
#' using `stepsize` until either valid data is found or the maximum number of retries
#' (`max_retries`) is reached.
#'
#' @examples
#' \dontrun{
#' hub_url <- "http://example-hub"
#' zrids <- c("12345", "67890")
#' begin <- Sys.time() - 60*60*24*30  # 30 days ago
#' end <- Sys.time()
#'
#' verified <- get_verified_periods(
#'   hub = hub_url,
#'   zrids = zrids,
#'   begin = begin,
#'   end = end
#' )
#' print(verified)
#' }
#'
#' @importFrom tibble tibble
#' @importFrom dplyr bind_rows
#' @export
get_verified_periods <- function(hub, zrids, begin, end, intervall = "l", stepsize = 30, max_retries = 5) {

  result <- tibble()
  i=1

  for (zrid in zrids) {
   print(paste0("Processing Nr.",i," of ",length(zrids), "  Zrid: ", zrid))
    # verified_to als tibble mit start und end
    verified_to <- get_az_valid_to(
      hub = hub,
      zrid = zrid,
      begin = begin,
      end = end,
      intervall = intervall,
      stepsize = stepsize,
      max_retries = max_retries
    )
    i=i+1
    result <- bind_rows(result, verified_to)
  }

  return(result)
}

#' Safely fetch AQUAZIS time series data
#'
#' This function wraps `get_aquazis_zr()` with `tryCatch()` to handle potential errors
#' when querying the AQUAZIS API. It returns a structured list with status codes and error messages.
#'
#' @param hub Character. Base URL of the AQUAZIS hub.
#' @param zrid Character or numeric. ZR ID of the time series to fetch.
#' @param begin POSIXct, Date, or character. Start datetime for the query period.
#' @param end POSIXct, Date, or character. End datetime for the query period.
#'
#' @return A list with three elements:
#' \describe{
#'   \item{status_code}{Integer. HTTP-like status code: 200 = success, 429 = rate limited, 500 = other error.}
#'   \item{data}{The result from `get_aquazis_zr()` if successful, otherwise NULL.}
#'   \item{error}{Character string with the error message if the request failed, otherwise NULL.}
#' }
#'
#' @examples
#' \dontrun{
#' safe_result <- safe_get_aquazis_zr(
#'   hub = "http://example-hub",
#'   zrid = "12345",
#'   begin = "2025-01-01 00:00:00",
#'   end = "2025-01-31 23:59:59"
#' )
#' if (safe_result$status_code == 200) {
#'   print(safe_result$data)
#' } else {
#'   message("Error: ", safe_result$error)
#' }
#' }
#'
#' @export
safe_get_aquazis_zr <- function(hub, zrid, begin, end) {
  tryCatch({
    # get_aquazis_zr liefert entweder DataFrame oder Liste (API-Antwort)
    zr_data <- get_aquazis_zr(hub, zrid, begin, end)
    
    # kein Fehler → einfach zurückgeben
    list(status_code = 200, data = zr_data, error = NULL)
    
  }, error = function(e) {
    msg <- conditionMessage(e)
    if (grepl("429", msg)) {
      list(status_code = 429, data = NULL, error = msg)
    } else {
      list(status_code = 500, data = NULL, error = msg)
    }
  })
}

#' Retrieve verified periods in parallel
#'
#' This function calls \code{get_az_valid_to()} for multiple ZRIDs in parallel
#' to efficiently retrieve verified time periods. Parallel execution is handled
#' via the \pkg{furrr} package.
#'
#' @param hub A connection or identifier for the data hub used by \code{get_az_valid_to()}.
#' @param zrids A vector of ZRID values for which to retrieve data.
#' @param begin Start date of the query period (e.g. \code{"2024-01-01"}).
#' @param end End date of the query period (e.g. \code{"2024-12-31"}).
#' @param intervall Character string specifying the query interval 
#'   (default: \code{"l"}; depends on the definition in \code{get_az_valid_to()}).
#' @param stepsize Step size for the query (default: \code{30}).
#' @param max_retries Maximum number of retry attempts in case of errors 
#'   (default: \code{5}).
#' @param workers Number of parallel worker processes (default: \code{4}).
#'
#' @return A \pkg{tibble} containing the combined results of all ZRIDs.
#'   The structure depends on the output of \code{get_az_valid_to()}.
#'
#' @details
#' The function uses \code{future_map()} from the \pkg{furrr} package to perform
#' parallel execution. Each ZRID is processed using the helper function 
#' \code{process_zrid()}, which wraps a call to \code{get_az_valid_to()} inside 
#' a \code{tryCatch()} block for robust error handling.
#'
#' If a ZRID fails, a warning message is displayed and a row with \code{NA} values 
#' is returned for that ZRID instead of stopping the entire process.
#'
#' @examples
#' \dontrun{
#' result <- get_verified_periods_parallel(
#'   hub = my_hub_connection,
#'   zrids = c("ZRID001", "ZRID002"),
#'   begin = "2024-01-01",
#'   end = "2024-12-31",
#'   workers = 8
#' )
#' }
#'
#' @seealso [furrr::future_map()], [future::plan()], [get_az_valid_to()]
#' @export
#'
get_verified_periods_parallel <- function(hub, zrids, begin, end, intervall = "l", stepsize = 30, max_retries = 5, workers = 4) {
  
  plan(multisession, workers = workers)
  
  # Helper function for processing a single ZRID
process_zrid <- function(zrid) {
  library(lubridate)
  log_file <- file.path(tempdir(), paste0("log_zrid_", zrid, ".txt"))
  tryCatch({
    sink(log_file)  # Nachrichten in die Datei umleiten
    message(paste0("Processing ZRID: ", zrid))
    result <- get_az_valid_to(
      hub = hub,
      zrid = zrid,
      begin = begin,
      end = end,
      intervall = intervall,
      stepsize = stepsize,
      max_retries = max_retries
    )
    sink()  
    return(result)
  }, error = function(e) {
    sink()  
    message(sprintf("Error processing ZRID %s: %s", zrid, conditionMessage(e)))
    return(tibble::tibble(zrid = zrid, start = NA, valid = NA, ts_valid = NA, end = NA))
  })
}
  
  # Parallel processing with future_map
  results <- future_map(zrids, process_zrid, .progress = TRUE)
  
  # Combine all results
  result <- bind_rows(results)
  return(result)
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

#' Write consolidated AQUAZIS metadata as fallback RDS file
#'
#' This function combines a list of metadata tibbles into a single tibble,
#' removes duplicates, and saves the result as an RDS file in the specified directory.
#' The purpose is to provide a consolidated fallback after parallel retrieval of metadata.
#'
#' @param meta_list List of tibbles. The metadata, typically the result of parallel retrieval.
#' @param shared_data Character. Path to the directory where \code{aquazis_stations.rds} will be saved.
#'
#' @return No return value. The function is called for its side effect (writing the file).
#' @export
#'
#' @examples
#' \dontrun{
#' fallback_meta(meta_list, "data_latest")
#' }
fallback_meta <- function (meta_list,shared_data)
{
meta_tibble <- dplyr::bind_rows(meta_list) %>% dplyr::distinct()
fallback_file <- file.path(shared_data, "aquazis_stations.rds")
if(!dir.exists(shared_data)) dir.create(shared_data, recursive = TRUE)
readr::write_rds(meta_tibble, fallback_file)
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

