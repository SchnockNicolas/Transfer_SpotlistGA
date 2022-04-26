library(dplyr)
library(prophet)

source("secrets.R")
source("functions_SQL.R")

conn <- SQL_Connection(Server, Database, uid, pwd, driver)

files <- list.files("data")
df <- do.call('rbind', lapply(files, function(file) readRDS(paste0("data/", file))))


df$ds <- strptime(paste0(df$date, " ", df$hour, ":", df$minute, ":00"), format="%Y-%m-%d  %H:%M:%S")
  
df <- df %>% group_by(ds) %>% summarise("y" = sum(sessions,na.rm = T))


m <- prophet(df, changepoint.prior.scale=0)
future <- make_future_dataframe(m, periods = 300, freq = 60 * 60)
fcst <- predict(m, future)
plot(m, fcst)

plot(m, fcst) + add_changepoints_to_plot(m)

saveRDS(fcst, "fcst.rds")

#prophet_plot_components(m, fcst)



z <- read.csv("Zuny.csv", sep = ";") %>% mutate(Date = as.Date(Date, format = "%d/%m/%Y")) %>% mutate(dateTime = 
                                                                                                        strptime(paste(Date, Start.Time), format="%Y-%m-%d  %H:%M:%S")
                                                                                                      , val = 1)%>% filter(!is.na(dateTime)) 


z$dateTime <- as.POSIXct(z$dateTime , origin="1970-01-01", tz="GMT")



ggplot() + 
  geom_line(data = df %>% filter(ds >= "2022-04-02" & ds < "2022-04-03") ,mapping= aes(x=as.POSIXct(ds , origin="1970-01-01", tz="GMT"), y=y)) +
  geom_point(data = z %>% filter(dateTime >= "2022-04-02" & dateTime < "2022-04-03") , mapping = aes(dateTime, val), color = "red", size = 10) +
  geom_line(data = fcst %>% filter(ds >= "2022-04-02" & ds < "2022-04-03") , mapping = aes(ds, yhat), color = "blue") +
  theme_minimal()





SQLSTR <- "Select * FROM TVSpotlist where connectionID = 'Con_CPTZD7172Z' and date > '2022-03-01'"
x <- dbSendQuery(conn, SQLSTR) %>% dbFetch()

x <- x %>% mutate(dateTime = 
               strptime(paste(date, StartTime), format="%Y-%m-%d  %H:%M:%S")
             , daterounded = lubridate::ceiling_date(dateTime,unit="1 minutes"))

x$daterounded <- as.POSIXct(x$daterounded , origin="1970-01-01", tz="GMT")

r <- merge(x, fcst %>% select(ds, yhat, yhat_upper), by.x = c("daterounded"), by.y = c("ds")) %>% merge(df %>% mutate(ds = as.POSIXct(ds, origin="1970-01-01", tz="GMT") ), by.x = c("daterounded"), by.y = c("ds"))  %>% mutate(Metric13 = y - yhat, visits_lower = y - yhat_upper)

r <- r %>% select(date, Channel, StartTime, BreakCode, Metric13) %>% mutate(accountID = 1011513809, connectionID = 'Con_CPTZD7172Z')
sum(r$visits_lower) 

sql_insert_direct("TVSpotlist", r,  conn, constraints = c("date", "Channel", "StartTime", "BreakCode"))




SQLSTR <- "Select date, Channel, sum(Metric13) as visits, sum(Metric17) as GRP FROM TVSpotlist where connectionID = 'Con_CPTZD7172Z' and date > '2022-03-01' GROUP BY date, Channel"
x <- dbSendQuery(conn, SQLSTR) %>% dbFetch()


plot(x$GRP, x$visits)




SQLSTR <- "Select date, sum(Metric13) as visits, sum(Metric17) as GRP FROM TVSpotlist where connectionID = 'Con_CPTZD7172Z' and date > '2022-03-01' GROUP BY date"
x <- dbSendQuery(conn, SQLSTR) %>% dbFetch() %>% mutate(date = as.Date(date))


ggplot(x) +
  geom_line(mapping =  aes(x = date, y = GRP), size = 0.5, colour = "#FF0000") +
  geom_line(mapping =  aes(x = date, y = visits), size = 0.5, colour = "#112446") +
  labs(y = "Visits", title = "GRP - Visits") +
  theme_minimal()








