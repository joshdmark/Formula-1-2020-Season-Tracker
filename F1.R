## load packages
library(tidyverse)
library(data.table)
library(sqldf)
library(RCurl)
library(stringr)
library(lubridate)

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

## qualifying results by driver per race 
team_grid_starts <- f1_2020 %>% 
  select(race_dt = date, constructor_name, driver_name, grid) %>% 
  arrange(race_dt)

## lineups per team, drivers in each team 
team_lineups <- team_grid_starts %>% select(constructor_name, driver_name) %>% distinct()

## wide format team lineups (each driver & their teammates)
team_pairings <- suppressWarnings(sqldf("select t1.*, t2.driver_name as d2
           from team_lineups t1 
           left join team_lineups t2 
           on t1.constructor_name = t2.constructor_name
           where t1.driver_name <> t2.driver_name") %>% 
  data.frame() %>% 
  arrange(constructor_name) %>% 
  group_by(constructor_name, driver_name) %>% 
  summarise(teammates = toString(d2)) %>% 
  data.frame() %>% 
  separate(teammates, into = c('teammate1', 'teammate2'), sep = ', '))

## add teammates to data 
qual_grid <- sqldf("select tgs.*, tp.teammate1, tp.teammate2, tgs2.grid as teammate1_grid, tgs3.grid as teammate2_grid
                   from 
                   team_grid_starts tgs 
                   left join team_pairings tp 
                      on tgs.constructor_name = tp.constructor_name 
                      and tgs.driver_name = tp.driver_name
                   left join team_grid_starts tgs2 
                      on tp.teammate1 = tgs2.driver_name
                      and tgs.race_dt = tgs2.race_dt
                   left join team_grid_starts tgs3 
                      on tp.teammate2 = tgs3.driver_name
                      and tgs.race_dt = tgs3.race_dt") %>% 
  distinct() %>% 
  mutate(teammate_grid = coalesce(teammate1_grid, teammate2_grid))

## add teammate qualifying to f1_2020
f1_master_data <- sqldf("select f.*, qg.teammate1, qg.teammate1_grid, qg2.teammate2, qg2.teammate2_grid
             from f1_2020 f 
             left join qual_grid qg
                on f.date = qg.race_dt 
                and f.constructor_name = qg.constructor_name
                and f.driver_name = qg.driver_name
            left join qual_grid qg2
                on f.date = qg2.race_dt 
                and f.constructor_name = qg2.constructor_name
                and f.driver_name = qg2.driver_name") %>% 
  data.frame() %>% 
  mutate(teammate = ifelse(!is.na(teammate1_grid), teammate1, teammate2),
         teammate_grid = coalesce(teammate1_grid, teammate2_grid)) %>% 
  select(-teammate1, -teammate1_grid, -teammate2, -teammate2_grid) %>% 
  mutate(qual_win = ifelse(grid < teammate_grid, 1, 0))

## make fastest lap ind 
f1_master_data <- f1_master_data %>% 
  mutate(fastest_lap_seconds = as.numeric(as.POSIXct(strptime(fastestLapTime, format = "%M:%OS"))) - 
           as.numeric(as.POSIXct(strptime("0", format = "%S")))) %>% 
  group_by(raceId) %>% 
  mutate(fastest_lap_race = min(fastest_lap_seconds, na.rm = TRUE)) %>% 
  ungroup() %>% 
  data.frame() %>% 
  mutate(fastest_lap_point = ifelse(fastest_lap_seconds == fastest_lap_race, 1, 0))


## write file
# fwrite(f1_2020, "C:/Users/joshua.mark/Downloads/F1/f1_2020.csv")
fwrite(f1_master_data, "C:/Users/joshua.mark/Downloads/F1/f1_2020.csv")


