#Author : sarita and Sharmo
format: 
	hdfs namenode -format

hstart:
	/usr/local/hadoop/sbin/start-dfs.sh
	/usr/local/hadoop/sbin/start-yarn.sh
	/usr/local/hadoop/sbin/mr-jobhistory-daemon.sh start historyserver


hstop:
	/usr/local/hadoop/sbin/mr-jobhistory-daemon.sh stop historyserver 
	/usr/local/hadoop/sbin/stop-yarn.sh
	/usr/local/hadoop/sbin/stop-dfs.sh

jan:
	hadoop fs -mkdir -p /user/jan
	hadoop fs -mkdir -p /user/jan/input
clean:
	rm -rf output

permission:
	sudo chmod 777 Missed_Connection_2.jar
jar:
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.6.3.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.6.3.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.3.jar Missed_Connection_2.java
	jar cvf Missed_Connection_2.jar *.class

pseudo: clean
	/usr/local/hadoop/sbin/stop-all.sh
	hadoop namenode -format
	/usr/local/hadoop/sbin/start-all.sh
	hadoop fs -mkdir -p /joshi_sarkar/assign05/input
	hadoop fs -put all/* /joshi_sarkar/assign05/input
	hadoop jar Missed_Connection_2.jar Missed_Connection_2 /joshi_sarkar/assign05/input /joshi_sarkar/assign05/output
	hadoop fs -get /joshi_sarkar/assign05/output/part-r-00000 output
	./script.sh
emr:	clean
	aws s3 mb s3://assign05
	aws s3 cp Missed_Connection_2.jar s3://assign05/
	aws s3 cp ./all/ s3://assign05/input --recursive
	aws emr create-cluster --name "Assignment4" --release-label emr-4.3.0 --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge --steps Type=CUSTOM_JAR,Name="Missed_Connection_2 JAR",ActionOnFailure=CONTINUE,Jar=s3://assign05/Missed_Connection_2.jar,MainClass=Missed_Connection_2,Args=[s3://assign05/input,s3://assign05/output] --auto-terminate --log-uri s3://assign05/log --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a --enable-debugging > mycluster.txt
output:
	aws s3 cp s3://assign05/output/part-r-00000 output
	./script.sh
