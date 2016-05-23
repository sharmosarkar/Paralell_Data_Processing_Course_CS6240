## Imports
from pyspark import SparkConf, SparkContext
import sys
import os
from datetime import *
from string import *
# Author : Sharmo & Sarita
## Constants
APP_NAME = "Missed_python"
##OTHER FUNCTIONS/CLASSES

def main(sc):
   Missed()

def Missed():
      YEAR = 0
      DATE = 5
      AL_CARRIER = 6
      ORIGIN = 11#14
      DESTINATION = 20#23
      AC_ARR_TIME = 41
      AC_DEP_TIME = 30
      SC_ARR_TIME = 40
      SC_DEP_TIME = 29
      CANCEL_FLG = 47
      FLT_NO = 10
      TAIL_NO = 9
    
    # consts used only in sanity checks
      CRS_ELAPSED_TIME = 50
    # time is in mins
      CONNECTION_TIME_UPPER_lIMIT = 6*60
      CONNECTION_TIME_LOWER_LIMIT = 30
      master = sc.textFile(sys.argv[1])
      header = master.first()
    #  Below Lines help in filtering header, sanity check and  separate required data for arrival and departure
      master = master.filter(lambda x:x!= header or x[YEAR]!="" or x[DATE]!="" or x[AL_CARRIER]!="" or x[AC_ARR_TIME]!="" or x[AC_DEP_TIME]!="" or x[SC_ARR_TIME]!="" or x[SC_DEP_TIME]!="")\
	    		.map(lambda x:x.replace(", ", "").replace("\"", "").split(",")).map(lambda x:(x[AL_CARRIER],x))
      arrivingFLT = master.map(lambda x:(x[1][AL_CARRIER]+ "-" + x[1][DESTINATION],x[1][TAIL_NO]+x[1][FLT_NO]+":" +x[1][SC_ARR_TIME] + ":" +x[1][AC_ARR_TIME]+":"+x[1][CANCEL_FLG]+":"+x[1][DATE]\
			+":"+x[1][YEAR]))
      departingFLT = master.map(lambda x:(x[1][AL_CARRIER]+ "-" + x[1][ORIGIN],x[1][TAIL_NO]+x[1][FLT_NO]+":" +x[1][SC_DEP_TIME] + ":" +x[1][AC_DEP_TIME]+":"+x[1][CANCEL_FLG]+":"+x[1][DATE]\
 			+":"+x[1][YEAR]))
    # Below is the data join for two segregated datasets
      data = arrivingFLT.join(departingFLT)
    # For each entry, it calls the processData function which does the actual business logic
      joinResults = data.map(lambda j:processData(j))
    # Below is the reducebykey operation for each unique entry in the dataset, based on our key
      reduceText = joinResults.reduceByKey(lambda a,b: ((a[0]+b[0]),(a[1]+b[1])))
    # Below prints the results as Airline, year with Missed Connections,Total Connections and the Percentages of two-way hopped Missed connections
      percentage = reduceText.map (lambda x : (x[0],(x[1][0] , x[1][1] , join([str(float(x[1][1])/float(x[1][0])*100),'%'])))).collect()
      finalResult = sc.parallelize(percentage)
      finalResult.saveAsTextFile(sys.argv[2])

def processData(field):
   dteFrmt = "%Y-%m-%d"
   key = field[0]
   values = field[1]
   connections = 0
   missedConnections = 0
   arr = values[0]
   dep = values[1]
   arrival = arr.split(":")
   departure = dep.split(":")
   keyFinal = (field[0].split("-")[0],arrival[5])
   arrDate = datetime.strptime(arrival[4],dteFrmt)
   depDate = datetime.strptime(departure[4],dteFrmt)
   schtimeDiff = 0
   acTimeDiff  = 0
   diffDays = (depDate-arrDate).days
   t= []
   # For flights on the same day
   if(depDate == arrDate):
      schtimeDiff = toMins(departure[1]) - toMins(arrival[1])
      if (isConnected(schtimeDiff)):
         connections = connections +1 
         print departure[2],arrival[2]
         acTimeDiff = toMins(departure[2]) - toMins(arrival[2])
         if (isMissed(acTimeDiff , arrival[3] , departure[3])):
            missedConnections = missedConnections + 1
  # For flights on different days (Different year)
   else:
      diffDays = (depDate- arrDate)/(1000*60*60*24)
      if (diffDays == 1):
         schtimeDiff =  toMins(departure[1])+(24*60) - toMins(arrival[1])
         if (isConnected(schtimeDiff)):
            connections = connections +1 
            acTimeDiff = toMins(departure[2])+(24*60) - toMins(arrival[2])
            if (isMissed(acTimeDiff , arrival[3] , departure[3])):
               missedConnections = missedConnections + 1
  # Below tuple keeps track for missed and total connection

   t = (keyFinal , (int(connections),int(missedConnections)))
   return t
 # To check if a connection
def isConnected(timeDiff):
   try:
      CONNECTION_TIME_UPPER_lIMIT = 6*60
      CONNECTION_TIME_LOWER_LIMIT = 30
      return (timeDiff <= CONNECTION_TIME_UPPER_lIMIT and timeDiff >= CONNECTION_TIME_LOWER_LIMIT )
   except:
      return False

# To check if a missed connection
def isMissed(timeDiff,arrCancelledBit,depCancelledBit):
   try:
      CONNECTION_TIME_UPPER_lIMIT = 6*60
      CONNECTION_TIME_LOWER_LIMIT = 30
      return (timeDiff < CONNECTION_TIME_LOWER_LIMIT or int(arrCancelledBit) == 1 or int(depCancelledBit) == 1)
   except:
      return False
 # Function that act as a helper to do minutes conversion
def toMins(time):
   try:
      if (len(time) == 2):
         return  int(time)
      if (len(time) == 3):
         return int(time[0:1])*60 + int(time[1:])
      else:
         return int(time[0:2])*60 + int(time[2:4])
   except:
      return 0


if __name__ == "__main__":

   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   conf = conf.setMaster("local[*]")
   sc   = SparkContext(conf=conf)
   # Execute Main functionality
   main(sc)
