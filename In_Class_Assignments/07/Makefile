#!/bin/bash
# Author: Nat Tuck
# Modified : Sharmodeep Sarkar

jar:
	sbt package
	#cp target/scala-*/demo_*.jar Demo.jar

run: jar
	sbt run

prof: jar
	rm -rf out
	SBT_OPTS="-Xprof" sbt run

hadoop: jar
	rm -rf out
	HADOOP_CLASSPATH=`perl cp.pl` hadoop jar Demo.jar

clean:
	rm -rf index-output
	rm -rf final-output
	rm -rf project target *.jar out
	
