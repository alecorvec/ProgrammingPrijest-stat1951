library(readr)
library(dplyr)

timezone_change <- function() {
  # function used to transform a timezone into it's number equivalent
  list(
    "UTC" = 0,
    "Eastern time" = -5,
    "Pacific time" = -8
  )
}

get_date <- function(date) {
  # function parsing a date string
  # transforming it into a list of the 3 date values as integer
  # "01/11/2018" -> 1, 11, 2018
  date_cut <- unlist(strsplit(date, "/"))
  return(list(
    day = as.integer(date_cut[1]),
    month = as.integer(date_cut[2]),
    year = as.integer(date_cut[3])
  ))
}

get_time <- function(time) {
  # function parsing a time double, as a string
  # transforming it into a list of the 3 date values as integer
  # 05:40:00 -> "05:40:00" -> 5, 40, 0
  new <- as.character(time)
  new_cut <- unlist(strsplit(new, ":"))
  return(list(
    hour = as.integer(new_cut[1]),
    minute = as.integer(new_cut[2]),
    second = as.integer(new_cut[3])
  ))
}

month_to_days <- function(year, month) {
  # function used to get the number of days in a month
  leap_year <- (year %% 4 == 0 & (year %% 100 != 0 | year %% 400 == 0))
  # conditions used to update the value of february, if it's a leap year

  feb <- 28 + ifelse(leap_year, 1, 0)
  new_month <- c(31, feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
  return(new_month[month])
}

datetime_change <- function(date, time, timezone, offsets) {
  # main function to update the date and time base on timezones

  new_time <- get_time(time)
  new_date <- get_date(date)
  # get both time and date as list of numeric values

  offset <- offsets[[timezone]]
  new_time$hour <- new_time$hour - offset
  # get the offset, update the value of the hour with it :
  # 18:00 Eastern time will become 23:00 UTC (23 - (-5))

  if (new_time$hour >= 24) {
    new_time$hour <- new_time$hour - 24
    new_date$day <- new_date$day + 1
  } else if (new_time$hour < 0) {
    new_time$hour <- new_time$hour + 24
    new_date$day <- new_date$day - 1
  }
  # Conditions to update the hour value and day value :
  # if hour is superior than 24, will reset and become next day
  # if hour is inferior than 0, will reset (+ 24) and become previous day

  mtd <- month_to_days(new_date$year, new_date$month)
  if (new_date$day > mtd) {
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
  # Conditions to update the day value :
  # if day is superior than mtd (number of days in the month), resets and adds a month
  # if day is inferior than 1, resets to previous month
  # both conditions also handles month changes :
  # if month is either superior to 12 or inferior to 1, resets both month and year

  new_date <- sprintf("%04d-%02d-%02d",
                      new_date$year, new_date$month, new_date$day)
  new_time <- sprintf("%02d:%02d:%02d",
                      new_time$hour, new_time$minute, new_time$second)
  # sprintf is then called to make sure both time and date string are in the correct format

  return(list(date = new_date, time = new_time))
}

transform_to_utc <- function(data, new_timezone) {
  # main function to transform the data
  data %>%
    rowwise() %>%
    # first the data is passed to rowwise, a function used to apply transformations on a dataframe row by row

    mutate(
      UTC_transformed = list(
        datetime_change(date, time, timezone, new_timezone)
      ),
      date = UTC_transformed$date,
      time = UTC_transformed$time,
      timezone = "UTC"
    ) %>%
    # the mutate adds the UTC_transformed columns, with the list created by datetime_change() as values.
    # those values are then used to change both the date and time columns.
    # the timezone column is also updated to only UTC

    ungroup() %>%
    # returns the data back to a dataframe

    select(-UTC_transformed)
    # removes the UTC_transformed column from the dataframe
}

process_datasets <- function() {
  data_folder <- "datasets"
  advertiser <- read_csv(file.path(data_folder, "advertiser.csv"))
  campaigns <- read_csv(file.path(data_folder, "campaigns.csv"))
  clicks <- read_tsv(file.path(data_folder, "clicks.tsv"))
  impressions <- read_tsv(file.path(data_folder, "impressions.tsv"))
  # read all csv/tsv files into their own variable

  new_timezone <- timezone_change()
  # create timezone list as numbers

  clicks <- transform_to_utc(clicks, new_timezone)
  impressions <- transform_to_utc(impressions, new_timezone)
  # transforms the clicks and impressions dataframes to update each row to UTC

  list(
    advertiser = advertiser,
    campaigns = campaigns,
    clicks = clicks,
    impressions = impressions
  )
  # returns all datasets in a list

}

datasets <- process_datasets()
# get all datasets, transformed to UTC

new_impressions <- inner_join(datasets$impressions, datasets$campaigns,
                              by = c("campaign_id" = "id"))
final_impressions <- inner_join(new_impressions, datasets$advertiser,
                                by = c("advertiser_id" = "ID"))

new_clicks <- inner_join(datasets$clicks, datasets$campaigns,
                         by = c("campaign_id" = "id"))
final_clicks <- inner_join(new_clicks, datasets$advertiser,
                           by = c("advertiser_id" = "ID"))

# both inner join are very similar :
# joins the wanted table (impression or click)to the campaigns first based on the campaign_id
# then joins the newly created table to advertisers based on the advertiser_id

write.csv(final_impressions, "impressions_processed.csv")
write.csv(final_clicks, "clicks_processed.csv")
# write all the processed data into new csv files
