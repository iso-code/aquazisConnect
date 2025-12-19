
install_github("iso-code/aquaZisConnect")
library(aquazisConnect)
library(tidyverse)
hub<-"http://w-db10-pegel51:7979"
shared_data<-"../data_latest"
logs<-"../logs"
setwd("aquazisConnect")
check_hub_connection(hub)
########################################################
# Rating Curves
########################################################
begin <- Sys.time()-(60*60*24)*3650
end <- Sys.time()

zrlist<-get_aquazis_zrlist(hub, parameter="Abfluss",type="P")
names<-c("Rheda","Ahmsen","Glesch","Niedermehnen","Eitorf","Stah","Eschweiler")

as_meta<-get_aquazis_meta(hub, shared_data, logs)
nn<-names[7]
ORT<-as_meta %>% filter(NAME==nn) %>% distinct(ORT) %>% as.character()

NAME<-as_meta %>% filter(NAME==nn) %>% distinct(NAME)

zrlist<-get_aquazis_zrlist(hub,ORT,parameter="Wasserstand",type="mes")
zrid<-zrlist$zrid
zr <-get_aquazis_zr(hub, zrid, begin = begin, end = end)
w<-zr$data$Daten

zrlist<-get_aquazis_zrlist(hub,ORT,parameter="Abfluss",type="mes")
zrid<-zrlist$zrid
zr <-get_aquazis_zr(hub, zrid, begin = begin, end = end)
q<-zr$data$Daten 

wq<-merge(w,q,by="V1") %>% tibble()
wq<-tibble(Wasserstand=as.numeric(wq$V2.x), Abfluss=as.numeric(wq$V2.y))
wq<-wq %>% filter(!is.na(Abfluss), !is.na(Wasserstand))
# Statistische Kennzahlen der Hydrojahre
zrlist<-get_aquazis_zrlist(hub,ORT, parameter="Abfluss",type="P")
zr_data<-get_aquazis_zr(hub, zrlist$zrid, begin, end)
zr<-extract_az_ts(zr_data,"l")

hydro_zr<-calc_hydroyear(zr, date_col="V1", region="EU")
result <- calc_hydro_year_stats(hydro_zr, value_col = "V2")
#result

##############################################################################
# Wähle die gewünschte Kurvenart: "power", "linear", "exp", "spline", "polyN"
curve_type <- "power"

# Funktion auswählen
fit_fun <- switch(
  curve_type,
  power = fit_rating_curve_power,
  linear = fit_rating_curve_linear,
  exp = fit_rating_curve_exp,
  spline = fit_rating_curve_spline,
  polyN = fit_rating_curve_polyN
)

pa_curve <- fit_fun(wq)
log_curve <- seq(0.1, max(c(result$max_value,wq$Abfluss), na.rm = TRUE)*1.05, length.out = 100)


if (curve_type == "power") {
  W_fit <- pa_curve$w0 + (log_curve / pa_curve$a)^(1 / pa_curve$b)
  wq <- wq %>%
    mutate(
      W_curve = pa_curve$w0 + (Abfluss / pa_curve$a)^(1 / pa_curve$b)
    )
} else if (curve_type == "linear") {
  W_fit <- pa_curve$w0 + (log_curve / pa_curve$m)
  wq <- wq %>%
    mutate(
      W_curve = pa_curve$w0 + (Abfluss / pa_curve$m)
    )
} else if (curve_type == "exp") {
  W_fit <- pa_curve$w0 + (log(log_curve / pa_curve$a) / pa_curve$b)
  wq <- wq %>%
    mutate(
      W_curve = pa_curve$w0 + (log(Abfluss / pa_curve$a) / pa_curve$b)
    )
} else if (curve_type == "spline") {
  W_fit <- predict(pa_curve$model, log_curve)$y
  wq <- wq %>%
    mutate(
      W_curve = predict(pa_curve$model, Abfluss)$y
    )
}  else if (curve_type == "polyN") {
  W_fit <- predict(pa_curve$model, newdata = data.frame(Abfluss = log_curve))
  wq <- wq %>%
    mutate(
      W_curve = predict(pa_curve$model, newdata = data.frame(Abfluss = Abfluss))
    )
}


breaks<-c(0,
          max(min(wq$Abfluss),mean(result$mean_value)*0.5), 
          mean(result$mean_value), 
          mean(result$max_value)*1.05)

wq <- wq %>%
  mutate(
    abweichung = Wasserstand - W_curve,
    q_group = cut(Abfluss, breaks = breaks, include.lowest = TRUE, right = TRUE)
  )


W_fit_df <- tibble(Abfluss = log_curve, Wasserstand = W_fit)  %>%
mutate(q_group = cut(Abfluss, breaks = breaks, include.lowest = TRUE, right = TRUE))


# Mittelwert der Kurve je Gruppe berechnen
mean_curve_df <- wq %>%
  group_by(q_group) %>%
  summarise(mean_curve = mean(W_curve, na.rm = TRUE),
            abw_curve = mean(abs(abweichung), na.rm = TRUE),
           .groups = "drop") 


# Schwankung relativ zur Kurve je Gruppe
schwankung <- wq %>%
  filter(!is.na(q_group)) %>%   # NAs vor summarise entfernen!
  group_by(q_group) %>%
  summarise(
    max_abweichung = abs(max(abweichung, na.rm = TRUE)),
    min_abweichung = -abs(min(abweichung, na.rm = TRUE)),
    schwankungsbereich = max_abweichung - min_abweichung,
    n = n()
  )

# Mit schwankung joinen
schwankung <- schwankung %>%
  left_join(mean_curve_df, by = "q_group") %>%
  filter(!is.na(mean_curve)) %>%
  mutate(schwankung_rel = 100 * (abs(abw_curve) / mean_curve))

# Mittelwert des Abflusses je Gruppe berechnen
mean_q_df <- wq %>%
  filter(!is.na(q_group)) %>%
  group_by(q_group) %>%
  summarise(mean_q = mean(Abfluss, na.rm = TRUE), .groups = "drop")

# Mit schwankung joinen
schwankung <- schwankung %>%
  left_join(mean_q_df, by = "q_group")

print(schwankung)
##########################
#streuung <- wq %>%
#  group_by(q_group) %>%
#  summarise(
#    n = n(),
#    mean_q = mean(Abfluss, na.rm = TRUE),
#    sd_q = sd(Abfluss, na.rm = TRUE),
#    min_q = min(Abfluss, na.rm = TRUE),
#    max_q = max(Abfluss, na.rm = TRUE),
#    mean_w = mean(Wasserstand, na.rm = TRUE),
#    sd_w = sd(Wasserstand, na.rm = TRUE),
#    min_w = min(Wasserstand, na.rm = TRUE),
#    max_w = max(Wasserstand, na.rm = TRUE)
#  )
##########################
streuung <- wq %>%
  group_by(q_group) %>%
  summarise(
    n = n(),
    mean_q = mean(Abfluss, na.rm = TRUE),
    sd_abweichung = sd(abweichung, na.rm = TRUE)
  ) %>%
  filter(!is.na(q_group))

print(streuung)
streuung[is.na(streuung)]<-0.1

curve_df <- W_fit_df %>%
  left_join(streuung %>% select(q_group, sd_abweichung), by = "q_group") %>%
  mutate(
    ymin = Wasserstand - sd_abweichung,
    ymax = Wasserstand + sd_abweichung
  )

curve_df$sd_abweichung <- approx(
  x = which(!is.na(curve_df$sd_abweichung)),
  y = curve_df$sd_abweichung[!is.na(curve_df$sd_abweichung)],
  xout = seq_along(curve_df$sd_abweichung),
  rule = 2
)$y

huell <- wq  %>%
  filter(!is.na(q_group)) %>%
  group_by(q_group) %>%
  summarise(
    max_abweichung = max(abweichung, na.rm = TRUE),
    min_abweichung = min(abweichung, na.rm = TRUE)
  )

curve_df <- curve_df %>%
  left_join(huell, by = "q_group") %>%
  mutate(
    obere_huell = Wasserstand + abs(max_abweichung),
    untere_huell = Wasserstand + min_abweichung
  )

curve_df$obere_huell <- approx(
  x = which(!is.na(curve_df$obere_huell)),
  y = curve_df$obere_huell[!is.na(curve_df$obere_huell)],
  xout = seq_along(curve_df$obere_huell),
  rule = 2
)$y

curve_df$untere_huell <- approx(
  x = which(!is.na(curve_df$untere_huell)),
  y = curve_df$untere_huell[!is.na(curve_df$untere_huell)],
  xout = seq_along(curve_df$untere_huell),
  rule = 2
)$y

last_max <- tail(curve_df$max_abweichung[!is.na(curve_df$max_abweichung)], 1)
last_min <- tail(curve_df$min_abweichung[!is.na(curve_df$min_abweichung)], 1)

curve_df$max_abweichung[is.na(curve_df$max_abweichung)] <- last_max
curve_df$min_abweichung[is.na(curve_df$min_abweichung)] <- last_min

curve_df <- curve_df %>%
  mutate(
    obere_huell = Wasserstand + max_abweichung,
    untere_huell = Wasserstand + min_abweichung
  )
##############

# PDF-Ausgabe mit ggplot
pdf(paste("./pdf/",ORT,"_",NAME,"_",curve_type,"_Abflusskurve_Unsicherheit.pdf"), width = 11.69, height = 8.27)

ggplot(wq, aes(x = Abfluss, y = Wasserstand)) +
  geom_point(alpha = 0.5, color = "black", aes(shape = "Messwerte")) +
  geom_line(data = curve_df, aes(x = Abfluss, y = Wasserstand, color = "Angepasste Kurve"), size = 1.2) +
  geom_ribbon(
    data = curve_df,
    aes(x = Abfluss, ymin = ymin, ymax = ymax, fill = "Standardabweichung \n der Gruppe"),
    alpha = 0.2, inherit.aes = FALSE
  ) +
  geom_line(data = curve_df, aes(x = Abfluss, y = obere_huell, color = "Obere Hüllkurve"), linetype = "dashed", size = 1) +
  geom_line(data = curve_df, aes(x = Abfluss, y = untere_huell, color = "Untere Hüllkurve"), linetype = "dashed", size = 1) +
  geom_vline(xintercept = breaks, linetype = "dotted", color = "grey40") +
  geom_text(
    data = tibble(x = breaks, y = max(wq$Wasserstand, na.rm = TRUE) * 1.05, label = round(breaks, 2)),
    aes(x = x, y = y, label = paste0(c("Q=0","Q=0.5 MQ: ","MQ= ","Qmax="),as.character(round(breaks,digits=1)))),
    angle = 90, vjust = 0, hjust = 0, size = 4, color = "grey20"
  ) +
  # Schwankungsbereich als Text im Plot (in cm und %)
  geom_text(
    data = schwankung,
    aes(
      x = mean_q*1.5,
      y = mean_curve * 2,
      label = paste0("ΔW= ", round(schwankungsbereich, 1), 
                    " cm\n[n:(",as.character(n),"), " , round(schwankung_rel, 1), "%]")
    ),
    color = "blue",
    size = 4,
    vjust = 0
  ) +
  labs(
    title = paste("Station:", NAME, "| Ort:", ORT),
    subtitle = paste0("Abflusskurve mit Unsicherheitsbereich und Schwankungsbereich, Typ: ", curve_type),
    x = "Abfluss [m³/s]",
    y = "Wasserstand [cm]",
    color = "Kurven",
    fill = "Unsicherheit",
    shape = "Messwerte"
  ) 
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "bold"),
    legend.position = "right"
  )

dev.off()












