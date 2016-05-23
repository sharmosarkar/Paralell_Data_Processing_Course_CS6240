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
	sudo chmod 777 Linear_Regression.jar
jar:
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.6.3.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.6.3.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.3.jar Linear_Regression.java
	jar cvf Linear_Regression.jar *.class

pseudo: clean
	/usr/local/hadoop/sbin/stop-all.sh
	hadoop namenode -format
	/usr/local/hadoop/sbin/start-all.sh
	hadoop fs -mkdir -p /joshi_sarkar/assign04/input
	hadoop fs -put all/* /joshi_sarkar/assign04/input
	hadoop jar Linear_Regression.jar Linear_Regression /joshi_sarkar/assign04/input /joshi_sarkar/assign04/output
	hadoop fs -get /joshi_sarkar/assign04/output
emr:	clean
	aws s3 mb s3://assign04
	aws s3 cp Linear_Regression.jar s3://assign04/
	aws s3 cp ./all/ s3://assign04/input --recursive
	aws emr create-cluster --name "Assignment4" --release-label emr-4.3.0 --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge --steps Type=CUSTOM_JAR,Name="Linear_Regression JAR",ActionOnFailure=CONTINUE,Jar=s3://assign04/Linear_Regression.jar,MainClass=Linear_Regression,Args=[s3://assign04/input,s3://assign04/output] --auto-terminate --log-uri s3://assign04/log --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a --enable-debugging > mycluster.txt
report:
	aws s3 cp s3://assign04/output/part-r-00000 output
	Rscript MyGraphs.R
	Rscript MyReport.R
