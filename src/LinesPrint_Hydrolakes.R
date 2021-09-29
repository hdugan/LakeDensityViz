
###########################################################################################################
############ inspired by https://www.whackdata.com/2014/08/04/line-graphs-parallel-processing-r/
############ based on code from 
############ https://github.com/Brideau/GeospatialLineGraphs/blob/master/01GenerateData.R 
###########################################################################################################

library(foreach)
library(doParallel)
library(data.table)
library(raster)
library(tidyverse)

# Time the code
start <- proc.time()

# If you have your data in a CSV file, use this instead
hydro = read_csv('data/Hydrolake_tabulate.txt') %>%
  group_by(Id) %>%
  summarise(AREA = sum(AREA), PERCENT = sum(PERCENTAGE))

grid = read_csv('data/GridCentroids.csv') %>%
  left_join(hydro,by = 'Id') %>%
  mutate(AREA = ifelse(is.na(AREA),0,AREA), PERCENT = ifelse(is.na(PERCENT),0,PERCENT))
all.data <- as.data.table(grid)
names(all.data) = c('Longitude','Latitude','ID','GridArea','Area','Percent')
head(all.data)


startEnd <- function(lats, lngs) {
  # Find the "upper left" (NW) and "bottom right" (SE) coordinates 
  # of a set of data.
  #
  # Args:
  #  lats: A list of latitude coordinates
  #  lngs: A list of longitude coordinates
  #
  # Returns: 
  #   A list of values corresponding to the northwest-most and 
  # southeast-most coordinates
  
  # Convert to real number and remove NA values
  lats <- na.omit(as.numeric(lats))
  lngs <- na.omit(as.numeric(lngs))
  
  topLat <- max(lats)
  topLng <- min(lngs)
  botLat <- min(lats)
  botLng <- max(lngs)
  
  return(c(topLat, topLng, botLat, botLng))
}


startEndVals <- startEnd(all.data$Latitude, all.data$Longitude)
remove(startEnd)

startLat <- startEndVals[1]
endLat <- startEndVals[3]
startLng <- startEndVals[2]
endLng <- startEndVals[4]
remove(startEndVals)

interval.v.num = 350.0
interval.h.num = 700.0
interval.v <- (startLat - endLat) / interval.v.num
interval.h <- (endLng - startLng) / interval.h.num
# remove(num_intervals)

lat.list <- seq(startLat, endLat + interval.v, -1*interval.v)

# Prepare the data to be sent in
# If you have a value you want to sum, use this
data <- all.data[,list(Longitude, Latitude, Area)]

# If you want to perform a count, use this
# data <- all.data[,list(Longitude, Latitude)]
# data[,Value:=1]

sumInsideSquare <- function(pointLat, pointLng, data) {
  # Sum all the values that fall within a square on a map given a point,
  # an interval of the map, and data that contains lat, lng and the values
  # of interest
  
  setnames(data, c("lng", "lat", "value"))
  
  # Get data inside lat/lon boundaries
  lng.interval <- c(pointLng, pointLng + interval.h)
  lat.interval <- c(pointLat - interval.v, pointLat)
  data <- data[lng %between% lng.interval][lat %between% lat.interval]
  
  return(sum(data$value))
}

# Debugging
# squareSumTemp <- sumInsideSquare(testLat, testLng, interval, data)

# Given a start longitude and an end longitude, calculate an array of values
# corresponding to the sums for that latitude

calcSumLat <- function(startLng, endLng, lat, data) {
  row <- c()
  lng <- startLng
  while (lng < endLng) {
    row <- c(row, sumInsideSquare(lat, lng, data))
    lng <- lng + interval.h
  }
  
  return(row)
}

# Debugging
# rowTemp <- calcSumLat(startLng, endLng, testLat, interval, data)
# write.csv(rowTemp, file = "Temp.csv", row.names = FALSE)

# Set up parallel computing with the number of cores you have
cl <- makeCluster(detectCores()-1, outfile = "./Progress.txt")
registerDoParallel(cl)

all.sums <- foreach(lat=lat.list, .packages=c("data.table")) %dopar% {
  
  lat.data <- calcSumLat(startLng, endLng, lat, data)
  
  # Progress indicator that works on Mac/Windows
  print((startLat - lat)/(startLat - endLat)*100) # Prints to Progress.txt
  
  lat.data
  
}

stopCluster(cl = cl)

# Convert to data frame
all.sums.table <- as.data.table(all.sums)

# Save to disk so I don't have to run it again
if (!file.exists("./data/GeneratedData")) {
  dir.create("./data/GeneratedData")
}
output.file <- "./data/GeneratedData/HydroLakes0.5_Area.csv"
write_csv(all.sums.table, file = output.file)

# End timer
totalTime <- proc.time() - start
print(totalTime)

# remove(cl, endLat, endLng, startLat, startLng, lat.list, start, calcSumLat, sumInsideSquare, interval)