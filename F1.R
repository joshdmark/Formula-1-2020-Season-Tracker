## load packages
library(tidyverse)
library(data.table)
library(sqldf)
library(RCurl)

## CSV downloads
# http://ergast.com/mrd/db/#csv

## load all required data
circuits <- fread('https://raw.githubusercontent.com/joshdmark/Formula-1-2020-Season-Tracker/master/data/circuits.csv')
races <- fread('https://raw.githubusercontent.com/joshdmark/Formula-1-2020-Season-Tracker/master/data/races.csv')
results <- fread('https://raw.githubusercontent.com/joshdmark/Formula-1-2020-Season-Tracker/master/data/results.csv')
status <- fread('https://raw.githubusercontent.com/joshdmark/Formula-1-2020-Season-Tracker/master/data/status.csv')
drivers <- fread('https://raw.githubusercontent.com/joshdmark/Formula-1-2020-Season-Tracker/master/data/drivers.csv')
constructors <- fread('https://raw.githubusercontent.com/joshdmark/Formula-1-2020-Season-Tracker/master/data/constructors.csv')


## combine races and circuits
races <- sqldf("select r.*, c.circuitRef, c.name as circuit_name, c.location as circuit_location, 
              c.country as circuit_country, c.lat as circuit_lat, c.lng as circuit_lon, c.alt as circuit_altitude 
             from races r 
             left join circuits c on r.circuitID = c.circuitID")

## remove circuits for space
rm(circuits)

## clean up and add data to results 
results <- sqldf("select r.*
                ,s.status
                ,d.driverRef, d.number as driver_nbr, d.code as driver_code, d.forename as driver_forename, d.surname as driver_surname
                ,d.dob as driver_dob, d.nationality as driver_nationality 
                ,c.constructorRef, c.name as constructor_name, c.nationality as constructor_nationality 
             from results r 
             left join status s on r.statusId = s.statusId
             left join drivers d on r.driverId = d.driverId
             left join constructors c on r.constructorId = c.constructorId") %>% 
  data.frame() %>% 
  distinct() %>% 
  mutate(driver_name = paste(driver_forename, driver_surname, sep = ' '), 
         driver_name = str_to_upper(driver_name))

## remove for space
rm(status, drivers, constructors)

## combine results & races 
f1 <- results %>% merge(races, by = 'raceId', all.x = TRUE) %>% data.frame()

## 2020 season data for dashboard 
f1_2020 <- f1 %>% filter(year == 2020)
f1_2020 <- f1_2020 %>% 
  arrange(date) %>% 
  group_by(driver_name) %>% 
  mutate(ytd_pts_total = sum(points), 
         ytd_pts = cumsum(points)) %>% 
  ungroup() %>% data.frame() %>% 
  arrange(date, desc(ytd_pts)) %>% 
  group_by(date) %>% 
  # mutate(ytd_rank = rank(ytd_pts*-1, ties.method = 'min')) %>% 
  mutate(ytd_rank = frankv(ytd_pts*-1, ties.method = 'random')) %>% 
  ungroup() %>% data.frame()

## write file
fwrite(f1_2020, "C:/Users/joshua.mark/Downloads/F1/f1_2020.csv")

