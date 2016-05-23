Author : Sharmo and Sarita

Assignment 6 : Missed Connections
Scala File - Missed.scala
Java File - Flight_Data_Sanity_Check.java
Python File - Missed_python.py
Team: Sarita and Sharmo
Date: 03/06/2016
 
Prerequisites:
 Install the required softwares
 - Oracle JDK version 1.7
 - Hadoop version 2.7.1
 - AWS CLI + Configuration
 - Spark Framework
 - PySpark and Python 2.7+

Make sure the following PATH variables are set before running:
- JAVA_HOME
- HADOOP_HOME
- HADOOP_CLASSPATH

Source Code Files :::
Missed.scala (Business logic)
Flight_Data_Sanity_Check.java (Data Cleansing)
Missedpython.py (Python implementation)

Directions to Execute:

1. Extract Joshi_Sarkar_A6.tar.gz to folder Joshi_Sarkar_A6 and navigate inside.

2. Create a folder named 'all' and copy all the .gz files inside it.

3. Make sure no hadoop jobs are running. 
Execute stop scripts if unsure
.
Go through the Makefile to see the sequence of rules to be executed.
############Common steps to be executed in order to generate the JAR file
• make jar
############To run the scala implementation of the program locally
• make runscala
############To run the python implementation of the program locally
• make runpython
###########to clean i.e removing jar and classes
• make clean
###########For the program to run on AWS
• make aws

ANALYSIS and OUTPUT:
The input is all CSV files from the Flight data.
These are then split every row based on "," delimiter.
The data is cleansed based on the general rules for the Flight Dataset (as from Assignmnet 01)
Then we then create 2 RDDs one for arrival flight details and other for departure flight details. 

Departure Data :-
 
Key: Carrier Code, Origin Airport Code
Value: Scheduled Departure time, Actual Departure Time, Cancelled, Flight Date

Arrival Data :-

Key: Carrier Code, Destination Airport Code, 
Value: Scheduled Arrival time, Actual Arrival Time, Cancelled, Flight Date
 

Then we join both the RDDs which groups the data from both the RDDs sharing the same key.

Now all the resulting output of the join are potential connections.
We use the following logic for checking whether the flights are truely connected or not, and if connected then whether there was a missed connection or not.
  A connection is any pair of flight F and G of the same carrier such as F.Destination = G.Origin and the scheduled departure of G is <= 6 hours and >= 30   minutes after the scheduled arrival of F.
  A connection is missed when the actual arrival of F < 30 minutes before the actual departure of G

The output from the program is in the form of :::
AirLineCarrier Year Connections Missed_Connections Percentage_Missed_connections  (part-00000)

EXECUTION TIME COMPARISION ::

Map reduce on local: 1.5 hrs approx
Map reduce on EMR: 1 hrs aprox

Spark(Scala) on local: 45 mins approx
Spark(Scala) on EMR: 40 mins approx

Spark(Python) on local : 1.25 hrs approx


----------Conclusion-----------

We can conclude that Spark implementation runs faster than Map Reduce implementation. This is due to the fact that Spark uses RDDs and the data thats loaded into the Spark program is in these RDDs. As opposed to using ArrayLists in the MapReduce Implementation (to separate values/entries corresponding to Origin or destination flight details) we simply used RDDs. The transformtion operations on these RDDs go a long way to speed up the process. The direct join operation amongst the RDDs made this Spark implementation both readable and more faster in execution. Amongst the two Spark implementations, the Scala implementation runs faster than the Python implementation. This is due to the fact that Scala is a complied language while Python is an interpreted language. Besides this, the Spark RDDs are most affected when it comes to Python executors. Every data that comes to and goes from the Python executor has to pass through the socket and the JVM worker. This creates some overheads. Python is a process based executor while Scala is a thread based executor. Each Python executor runs its own process which results in potentially higher memory usage than the thread based Scala. These are a few reasons why the Python implementation using Spark is shown to be so slow in comparison to Scala.

REFERENCES :

www.stackoverflow.com
Class demo programs for the concept of join and the code snippet for Demo.scala and DemoReducer.java
http://tldp.org/HOWTO/Bash-Prog-Intro-HOWTO.html
http://www.tldp.org/LDP/abs/html/wrapper.html
