fit_rating_curve <- function(wq, low_limit=NULL, multiplier=1) {
  
  fit_data_weighted<-data.frame()

  wq <- wq %>% 
    filter(!is.na(Wasserstand), !is.na(Abfluss), Abfluss > 0)

  # Startwerte
  start_w0 <- min(wq$Wasserstand, na.rm = TRUE) - 0.01

  # Daten nur dort, wo (Wasserstand - start_w0) eindeutig positiv ist
  fit_data <- wq %>% filter(Wasserstand > start_w0 + 0.5)

  if (!is.null(low_limit)) {
    low_data <- fit_data %>% filter(Abfluss <= low_limit)
    # Dupliziere die Niedrigwasserdaten 'multiplier' mal
    fit_data_weighted <- bind_rows(
      fit_data,
      low_data[rep(seq_len(nrow(low_data)), multiplier), ]
    )
  }

  if (nrow(fit_data) < 3) {
    stop("Zu wenige Datenpunkte nach Filter für eine stabile Anpassung.")
  }

  # Obergrenze für w0: immer unterhalb des minimalen Wasserstands der Fit-Daten
  wmin <- min(fit_data$Wasserstand, na.rm = TRUE)
  upper_w0 <- wmin - 1e-6
  lower_w0 <- start_w0 - 1 # etwas Spielraum nach unten

  # Startwerte aus Linearisation (log-Linear)
  x <- fit_data$Wasserstand - start_w0
  if (any(x <= 0)) {
    stop("Start_w0 führt zu nicht positiven (Wasserstand - w0). Bitte anpassen.")
  }

  lm_fit <- lm(log(fit_data$Abfluss) ~ log(x))
  start_a <- exp(coef(lm_fit)[1])
  start_b <- coef(lm_fit)[2]

  # Grenzen für a und b (positiv, sinnvoller Bereich)
  lower <- c(a = 1e-10, b = 0,    w0 = lower_w0)
  upper <- c(a = 1e10,  b = 10,   w0 = upper_w0)

  fit_data_nls <- if (!is.null(low_limit)) fit_data_weighted else fit_data

  fit <- nls(
    Abfluss ~ a * (Wasserstand - w0)^b,
    data = fit_data_nls,
    start = list(a = start_a, b = start_b, w0 = start_w0),
    algorithm = "port",
    lower = lower,
    upper = upper,
    control = nls.control(maxiter = 200)
  )

  coefs <- coef(fit)
  list(
    a = unname(coefs["a"]),
    b = unname(coefs["b"]),
    w0 = unname(coefs["w0"]),
    model = fit
  )
}


calc_hydroyear<-function(dataset, date_col="V1", region="EU"){
  
  hyd_beginn<-ifelse(region=="EU",11,10)

  data <- dataset %>%
    mutate(
      date = as.Date(.data[[date_col]]),
      hydro_year = ifelse(month(date) >= hyd_beginn, 
                                year(date) + 1, year(date)),
      hydro_month = month(date, label = TRUE, abbr = TRUE, locale="C")
    )

      return(data)

}

calc_hydro_year_stats <- function(zr, value_col = "V2") {

  max_year <- max(zr$hydro_year, na.rm = TRUE)
  zr <- zr %>% filter(hydro_year != max_year)

  stats <- zr %>%
    group_by(hydro_year) %>%
    filter(!is.na(.data[[value_col]])) %>% 
    slice_head(n = -1) %>%
    summarise(
      min_value = min(.data[[value_col]], na.rm = TRUE),
      max_value = max(.data[[value_col]], na.rm = TRUE),
      mean_value = mean(.data[[value_col]], na.rm = TRUE),
      q25 = quantile(.data[[value_col]], 0.25, na.rm = TRUE),
      q75 = quantile(.data[[value_col]], 0.75, na.rm = TRUE),
      n = n()
    )  %>%
  mutate(
    min_value = na_if(min_value, Inf),
    max_value = na_if(max_value, -Inf)
  )%>%
    arrange(hydro_year)
  
  return(stats)
}

#####################################################

fit_rating_curve_power <- function(wq) {
  start_w0 <- min(wq$Wasserstand, na.rm = TRUE) - 0.01
  fit_data <- wq %>% filter(Wasserstand > start_w0 + 0.5)
  lm_fit <- lm(log(fit_data$Abfluss) ~ log(fit_data$Wasserstand - start_w0))
  start_a <- exp(coef(lm_fit)[1])
  start_b <- coef(lm_fit)[2]
  fit <- nls(
    Abfluss ~ a * (Wasserstand - w0)^b,
    data = fit_data,
    start = list(a = start_a, b = start_b, w0 = start_w0),
    algorithm = "port",
    lower = c(a = 1e-10, b = 0, w0 = start_w0 - 1),
    upper = c(a = 1e10, b = 10, w0 = min(fit_data$Wasserstand, na.rm = TRUE) - 1e-6)
  )
  coefs <- coef(fit)
  list(type = "power", a = unname(coefs["a"]), b = unname(coefs["b"]), w0 = unname(coefs["w0"]), model = fit)
}

fit_rating_curve_linear <- function(wq) {
  start_w0 <- min(wq$Wasserstand, na.rm = TRUE) - 0.01
  fit_data <- wq %>% filter(Wasserstand > start_w0 + 0.5)
  fit <- nls(
    Abfluss ~ m * (Wasserstand - w0),
    data = fit_data,
    start = list(m = 1, w0 = start_w0),
    algorithm = "port",
    lower = c(m = 1e-10, w0 = start_w0 - 1),
    upper = c(m = 1e10, w0 = min(fit_data$Wasserstand, na.rm = TRUE) - 1e-6)
  )
  coefs <- coef(fit)
  list(type = "linear", m = unname(coefs["m"]), w0 = unname(coefs["w0"]), model = fit)
}

fit_rating_curve_exp <- function(wq) {
  start_w0 <- min(wq$Wasserstand, na.rm = TRUE) - 1
  fit_data <- wq %>% filter(Wasserstand > start_w0 + 0.5)
  fit <- nls(
    Abfluss ~ a * exp(b * (Wasserstand - w0)),
    data = fit_data,
    start = list(a = 1, b = 0.01, w0 = start_w0),
    algorithm = "port",
    lower = c(a = 1e-10, b = 0, w0 = start_w0 - 1),
    upper = c(a = 1e10, b = 10, w0 = min(fit_data$Wasserstand, na.rm = TRUE) - 1e-6)
  )
  coefs <- coef(fit)
  list(type = "exp", a = unname(coefs["a"]), b = unname(coefs["b"]), w0 = unname(coefs["w0"]), model = fit)
}

fit_rating_curve_spline <- function(wq) {
  fit_data <- wq %>% filter(!is.na(Abfluss), !is.na(Wasserstand))
  spline_fit <- smooth.spline(x = fit_data$Abfluss, y = fit_data$Wasserstand)
  list(type = "spline", model = spline_fit)
}

fit_rating_curve_polyN <- function(wq, degree = 3) {
  fit_data <- wq %>% filter(!is.na(Abfluss), !is.na(Wasserstand))
  fit <- lm(Wasserstand ~ poly(Abfluss, degree, raw = TRUE), data = fit_data)
  coefs <- coef(fit)
  list(
    type = paste0("poly", degree),
    degree = degree,
    coefs = coefs,
    model = fit
  )
}

