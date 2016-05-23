Assignment 5 : Missed Connections
JAVA File - Missed_Connection_2.java
Team: Sarita and Sharmo
Date: 02/18/2016
 



Prerequisites:
 Install the required softwares
 - Oracle JDK version 1.7
 - Hadoop version 2.7.1
 - AWS CLI + Configuration

Make sure the following PATH variables are set before running:
- JAVA_HOME
- HADOOP_HOME
- HADOOP_CLASSPATH


Directions to Execute:


1. Extract Joshi_Sarkar_A5.tar.gz to folder Joshi_Sarkar_A5 and navigate inside.

2. Create a folder named 'all' and copy all the .gz files inside it.

3. Make sure no hadoop jobs are running. 
Execute stop scripts if unsure
.
Go through the Makefile to see the sequence of rules to be executed.
############Common steps to be executed in order to generate the JAR file
• make jar
• make permission
###########For the program to run on HDFS filesystem
• make hstart
• make pseudo 
• make hstop
• make clean
###########For the program to run on AWS
• make emr
• make output

ANALYSIS and OUTPUT:

This program "Missed_Connection_2.java" is implemented using map-reduce framework.It has below mentioned classes
The Mapper Class -> MapperDestination.class
This takes the key value as Carrier,Year, Origin or Destination Id and the Date into consideration.
Date in the key is an effective factor which reduces the time of execution of this program for the given dataset by a considerable amount.
The values mapped to this key are based on whether the key is for the ORIGIN or DESTINATION
For ORIGIN, values are "Origin" tag to distinguish as origin, Scheduled arrival time, Arrival time, Cancellation Bit Status and the Origin Id
For DESTINATION, values are "Destination" tag to distinguish as Destination, Scheduled Departure Time, Departure time, Cancellation Bit and the Destination Id.

The Reducer Class -> FlightTicketReducer.class
The Reducer class then on receiving these values, distinguish the received values as either the Origin data values or the Destination data values based on the tag set in the map phase.
Accordingly, we create two separate arraylist holding the data entries based on these tags.
Then we iterate over the collected ArrayList entry to check for the conditions for a
1) Connection - A connection is any pair of flight F and G of the same carrier such as F.Destination = G.Origin and the scheduled departure of G is <= 6 hours and >= 30 minutes after the scheduled arrival of F
2) Missed Connection- A connection is missed when the actual arrival of F < 30 minutes before the actual departure of G.

The reducer emits the key and value pair as : Key -> Carrier and Year and Value as Missed Connections and TotalNumber of Connections

The Data_Splitter Class-> This takes care of pre-processing the csv files to split accordingly

The Flight_Sanity_Data_Check Class -> This is the usual sanity checker based on previous assignments and iterative updates made to handle condition for this assignment.

EXECUTION and ASSUMPTIONS made:
Apart from the definition of the Missed Connection given in the assignment statement, we believe that if the cancellation bit for a particular flight is set to 1 it implies that it is clearly a missed connection.
Also, for optimizing the code performance from the initial time of execution from 45 mins for 25 CSV files, by selecting proper Key and Values in the map phase (Addition of the date and origin or destination id in the key) adds up to the optimization of the code.

CURRENT TIME OF EXECUTION -> HDFS environment -> 17mins
			-> AWS via command line -> 15 mins (With Actual execution of the JAR file timestamp as 12 mins)
As the data comes based on the date in the Key value, we do a context write of the Missed connections only when the value of the Missed Connection is not zero.

Piazza states that we do have to consider the missed connections for flights that happen to have the definition for a connection with G.departure time moving to the next day.
We tried implementing the same but due to infrastructure issues, hadoop hangs on running this long.
So, it is restricted by the given hardware. Also it adds up to a complexity of n^2 which is too high for this chunk of data.

OUTPUT:

The output is generated with the help of an automated bash script, script.sh
This bash script calculates the Missed connection year-wise for each unique Airline along with the percentage of Missed Connections for those Airlines.
The output file name is result.txt and it is auto generated in the current filepath <Filepath>/output1/out/result.txt
This file has the output in the format of filename (Carrier+Year details), TotalConnections,Missed Connections and Percentage of Missed Connections

Output File -> result.txt


REFERENCES :

www.stackoverflow.com
Class demo programs for the concept of join and the code snippet for Demo.scala and DemoReducer.java
http://tldp.org/HOWTO/Bash-Prog-Intro-HOWTO.html
http://www.tldp.org/LDP/abs/html/wrapper.html
