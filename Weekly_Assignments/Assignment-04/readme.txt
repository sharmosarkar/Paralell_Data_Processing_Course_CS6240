Assignment 4 : Linear Regression.... JAVA File - Linear_Regression.java
Team: Sarita and Sharmo
Date: 02/12/2016
 


Prerequisites:

1. Install the required softwares
 - Oracle JDK version 1.7
 - Hadoop version 2.7.1
 - AWS CLI + Configuration
 - R
 - markdown-pandoc
 Make sure the following PATH variables are set before running:
- JAVA_HOME
- HADOOP_HOME
- HADOOP_CLASSPATH


Directions to Execute:


1. Extract Joshi_Sarkar_A4.tar.gz to folder Joshi_Sarkar_A4 and navigate inside.

2. Create a folder named 'all' and copy all the .gz files inside it.

3. Make sure no hadoop jobs are running. 
Execute stop scripts if unsure
.
Go through the Makefile to see the sequence of rules to be executed

• make jar
• make permissiona
• make hstart
• make pseudo
• make hstop
• make clean
• make emr
• make report

Print is enabled that displays the Slope and the Y-intercept using R programming

After the script is executed successfully, A report MyReport.pdf is created along with a Report.html
This report contains all conclusion to our analysis and findings
This code works smoothly on emr in 6 mins inluding the cluster start time.
Analysis clearly explained in the report MyReport.pdf
