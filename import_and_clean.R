library(tidyverse)
library(magrittr)
library(rjson)
library(glue)
library(curl)
library(lubridate)

### needed for clean_matches to work
raw <- fromJSON(file = "https://aoe2.net/api/strings")
civ.strings <- raw$civ %>% map_df(~.x) #use after 1/26
mtype.strings <- raw$map_type %>% map_df(~.x) %>% 
  rbind(tibble(string = NA, id = NA)) 
civ.strings_pre_lotw <- civ.strings %>% slice(-5, - 30) %>% 
  mutate(id = row_number() - 1) ## use for any before 1/26 when LOTW was released

mtype.strings %<>% 
  rbind(tibble(string = NA, id = NA))

####
#start = POSIXct
#end = posixct
#by = character
#destination = character

get_matches <- function(start, end, by, destination){
  dest <- destination
  timestamps <- seq.POSIXt(start, end, by = by) %>% as.integer()
  calls_list <- paste0("https://aoe2.net/api/matches?game=aoe2de&count=1000&since=", timestamps)
  for(i in 1:length(timestamps)){
    curl_download(calls_list[i], destfile = glue("{dest}/{timestamps[i]}.json"))
    print(timestamps[i] %>% as.POSIXct(origin = "1970-01-01"))
  }
}

#################################

###destination character

import_matches <-  function(dest){
  n <- list.files(dest) %>% length()
  files_list <- list.files(dest)
  match_list<- list()
  
  for(i in 1:n){
    match_list[[i]] <- fromJSON(file = glue("{dest}/{files_list[i]}")) 
  }### end loop
  match_list[1:n] #index prunes away extra entries for some reason possibly not needed
  
}

##################


clean_matches <- function(matches, civ.strings, mtype.strings){
  
  filtered_games <- matches %>% flatten() %>%
    keep(~.x$rating_type == 2) %>% 
    discard(~.x$players[[1]]$won %>% is.null())  %>% 
    discard(~.x$players %>% length() == 1)# filters for 1v1 RM with both players recorded
  
  meta_data <- do.call(rbind, filtered_games) %>% as.data.frame()
  rm(filtered_games) #remove filtered_games object for memory
  meta_data[meta_data == 'NULL'] <- NA # prevents NULL columns from being dropped ,sometimes Version and server are null
  uuid <- meta_data$match_uuid %>% unlist()
  
  ## meta data should be indexed correctly, if not can use inner_join on uuid
  map_type <- meta_data$map_type %>% unlist() %>% as_tibble() %>% 
    inner_join(mtype.strings, by = c("value" = "id")) %>% 
    select(-value) %>% rename(map_type = string)
  start_time  <- meta_data$started %>% unlist() %>% as.POSIXct(tz = "UTC", origin = "1970-01-01")
  end_time  <- meta_data$finished %>% unlist() %>% as.POSIXct(tz = "UTC", origin = "1970-01-01")
  patch <- meta_data$version %>% unlist()
  server <- meta_data$server %>% unlist()
  #
  
  players1 <- meta_data %>% select(players) %>% pull() %>% 
    map(~.x[[1]]) %>% (function(.x) do.call(rbind, .x) ) %>% as_tibble() %>%
    unnest(cols = c(profile_id, steam_id, name, country, slot, slot_type, 
                    rating, rating_change, color, 
                    team, civ, won)) %>% 
    cbind(uuid) %>% select(-clan, -games, -wins, -streak, -drops) %>% 
    cbind(map_type, server, patch, start_time, end_time) %>% 
    mutate(duration = end_time - start_time) %>% 
    inner_join(civ.strings, by = c("civ" = "id")) %>% 
    select(-civ) %>% rename(civ= string) %>% 
    distinct(uuid, .keep_all = TRUE)
  
  players2 <- meta_data %>% select(players) %>% pull() %>% 
    map(~.x[[2]]) %>% (function(.x) do.call(rbind, .x) ) %>% as_tibble() %>%
    unnest(cols = c(profile_id, steam_id, name, country, slot, slot_type, 
                    rating, rating_change, color, 
                    team, civ, won)) %>% 
    cbind(uuid) %>% select(-clan, -games, -wins, -streak, -drops) %>% 
    cbind(map_type, server, patch, start_time, end_time) %>%
    mutate(duration = end_time - start_time) %>% 
    inner_join(civ.strings, by = c("civ" = "id")) %>% 
    select(-civ) %>% rename(civ = string) %>% 
    distinct(uuid, .keep_all = TRUE)
  
  rm(meta_data)#remove meta_data object for memory
  
 players1 %<>% inner_join(players2 %>% select(uuid, civ), by = "uuid",  
                         suffix = c("", "_opp"))%>% 
   mutate(matchup = str_c(civ, civ_opp, sep = " ") %>%
            str_split( pattern = " ") %>%
            map_chr(~str_sort(.x) %>% str_c( collapse = "-")))
 
 players2 %<>% inner_join(players1 %>% select(uuid, civ), by = "uuid",  
                         suffix = c("", "_opp")) %>% 
   mutate(matchup = str_c(civ, civ_opp, sep = " ") %>%
            str_split( pattern = " ") %>%
            map_chr(~str_sort(.x) %>% str_c( collapse = "-")))

 list(players1, players2)
 ### join on uuid to make the data wide
 ### else bind rows to make the data long
}

######
##color mappings appear to be incorrect so not used yet
color_names <- function(vector){
  
  case_when(vector == 1 ~ "Blue", 
            vector == 2. ~ "Red",
            vector == 3 ~ "Green",
            vector == 4 ~ "Yellow",
            vector == 5 ~ "Teal",
            vector == 6 ~ "Purple",
            vector == 7 ~ 'Orange',
            vector == 8 ~"Grey",
            TRUE ~ "Not Recorded")
}


#####
# workflow get_matches() -> import_matches() -> clean_matches() -> write_csv()
#####     call api & save as JSON -> import JSONs to R -> transform to table -> save as CSV

#############
## run code 
start <- mdy_hms("3-15-21 0:00:00") %>% as.POSIXct() # arbitary timestamps
end <- mdy_hms("3-31-21 23:00:00") %>% as.POSIXct()
by <- "hour"
destination <- "~/aoe2analysis/matches/March15-31"

get_matches(start = start, end = end, by = "hour", destination)
matches <- import_matches(dest = destination)
cleaned_matches <- clean_matches(matches, civ.strings, mtype.strings) # pre_lotw required for games before 1/26/21

matches_long <- cleaned_matches %>% map_df(rbind) #make data long
write.csv(matches_long, "LateMarch.csv", row.names = FALSE)


