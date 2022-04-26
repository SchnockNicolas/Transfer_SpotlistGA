library(dplyr)
library(prophet)

files <- list.files("data")
df <- do.call('rbind', lapply(files, function(file) readRDS(paste0("data/", file))))
df$ds <- strptime(paste0(df$date, " ", df$hour, ":", df$minute, ":00"), format="%Y-%m-%d  %H:%M:%S")
  
df <- df %>% group_by(ds) %>% summarise("y" = sum(sessions,na.rm = T))


 

m <- prophet(df, changepoint.prior.scale=0.01)
future <- make_future_dataframe(m, periods = 300, freq = 60 * 60)
fcst <- predict(m, future)
plot(m, fcst)


prophet_plot_components(m, forecast)