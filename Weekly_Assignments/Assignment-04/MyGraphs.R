require(knitr)
require(markdown)
require(sqldf)
require(ggplot2)
AirLineData <- read.table("output/part-r-00000", sep = "\t")
attach(AirLineData)
uniqueAirlines <- array(unique(AirLineData[,1]))
countUniqueAirlines <- length(uniqueAirlines)
slopeArray <- vector()
constant <- vector()
for (indx in 1:countUniqueAirlines)
{
  singleAirLinedf <- sqldf(paste("select * FROM AirLineData where V1='",uniqueAirlines[indx],"'",sep=""))
  model_dist <- lm (singleAirLinedf$V4 ~ singleAirLinedf$V3)
  model_airTime <- lm (singleAirLinedf$V4 ~ singleAirLinedf$V2)
  coefvar <- coefficients(model_airTime)
  finalvar <- paste(coefvar[2], "is the slope for time graph for the Airline ",uniqueAirlines[indx])
  constvar <- paste(coefvar[1], "is the y-intercept for time graph for the Airline ",uniqueAirlines[indx])
  slopeArray <- append(slopeArray,finalvar)
  constant <- append(constant,constvar)
  mse_distance <- mean(residuals(model_dist)^2)
  rmse <- mse_distance^(0.5)
  png(paste (uniqueAirlines[indx],"Distance.png",sep="_"), 500, 400)
  plot ( singleAirLinedf$V3, singleAirLinedf$V4 , main= paste (uniqueAirlines[indx],"Distance with Mean Square Error as:",rmse),xlab="Distance",ylab="Avg Price")
  abline(model_dist , col=2 , lwd = 3)
  mse_airTime <- mean(residuals(model_airTime)^2)
  rtmse <- mse_airTime^(0.5)
  png(paste (uniqueAirlines[indx],"AirTime.png",sep="_"), 500, 400)
  plot ( singleAirLinedf$V2, singleAirLinedf$V4 , main= paste (uniqueAirlines[indx],"AirTime with Mean Square Error as:",rtmse),xlab="AirTime",ylab="Avg Price")
  abline(model_airTime , col=2 , lwd = 3)
  dev.off()
}
print(slopeArray)
print(constant)
