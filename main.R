library(readr)
library(dplyr)

data_folder <- "datasets"

timezone_change <- function() {
  list(
    "UTC" = 0,
    "Eastern time" = -5,
    "Pacific time" = -8
  )
}

get_date <- function(date) {
  date_cut <- unlist(strsplit(date, "/"))
  return(list(
    day = as.integer(date_cut[1]),
    month = as.integer(date_cut[2]),
    year = as.integer(date_cut[3])
  ))
}

get_time <- function(time) {
  new <- as.character(time)
  new_cut <- unlist(strsplit(new, ":"))
  return(list(
    hour = as.integer(new_cut[1]),
    minute = as.integer(new_cut[2]),
    second = as.integer(new_cut[3])
  ))
}

month_to_days <- function(year, month) {
  leap_year <- (year %% 4 == 0 & (year %% 100 != 0 | year %% 400 == 0))
  feb <- 28 + ifelse(leap_year, 1, 0)
  new_month <- c(31, feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
  return(new_month[month])
}

datetime_change <- function(date, time, timezone, offsets) {
  new_time <- get_time(time)
  new_date <- get_date(date)

  offset <- offsets[[timezone]]
  new_time$hour <- new_time$hour + offset
  if (new_time$hour >= 24) {
    new_time$hour <- new_time$hour - 24
    new_date$day <- new_date$day + 1
  } else if (new_time$hour < 0) {
    new_time$hour <- new_time$hour + 24
    new_date$day <- new_date$day - 1
  }
  dim <- month_to_days(new_date$year, new_date$month)
  if (new_date$day > dim) {
    new_date$day <- 1
    new_date$month <- new_date$month + 1
    if (new_date$month > 12) {
      new_date$month <- 1
      new_date$year <- new_date$year + 1
    }
  } else if (new_date$day < 1) {
    new_date$month <- new_date$month - 1
    if (new_date$month < 1) {
      new_date$month <- 12
      new_date$year <- new_date$year - 1
    }
    new_date$day <- month_to_days(new_date$year, new_date$month)
  }
  new_date <- sprintf("%04d-%02d-%02d",
                      new_date$year, new_date$month, new_date$day)
  new_time <- sprintf("%02d:%02d:%02d",
                      new_time$hour, new_time$minute, new_time$second)
  return(list(date = new_date, time = new_time))
}

transform_to_utc <- function(data, new_timezone) {
  data %>%
    rowwise() %>%
    mutate(
      UTC_transformed = list(
        datetime_change(date, time, timezone, new_timezone)
      ),
      date = UTC_transformed$date,
      time = UTC_transformed$time
    ) %>%
    ungroup() %>%
    select(-UTC_transformed)
}

process_datasets <- function() {
  advertiser <- read_csv(file.path(data_folder, "advertiser.csv"))
  campaigns <- read_csv(file.path(data_folder, "campaigns.csv"))
  clicks <- read_tsv(file.path(data_folder, "clicks.tsv"))
  impressions <- read_tsv(file.path(data_folder, "impressions.tsv"))

  new_timezone <- timezone_change()
  clicks <- transform_to_utc(clicks, new_timezone)
  impressions <- transform_to_utc(impressions, new_timezone)
  list(
    advertiser = advertiser,
    campaigns = campaigns,
    clicks = clicks,
    impressions = impressions
  )
}

datasets <- process_datasets()

new_impressions <- inner_join(datasets$impressions, datasets$campaigns,
                              by = c("campaign_id" = "id"))
final_impressions <- inner_join(new_impressions, datasets$advertiser,
                                by = c("advertiser_id" = "ID"))

new_clicks <- inner_join(datasets$clicks, datasets$campaigns,
                         by = c("campaign_id" = "id"))
final_clicks <- inner_join(new_clicks, datasets$advertiser,
                           by = c("advertiser_id" = "ID"))

write.csv(final_impressions, "impressions_processed.csv")
write.csv(final_clicks, "clicks_processed.csv")