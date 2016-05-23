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

permission:
	sudo chmod 777 ClusterAnalysis.jar

pseudo:
	/usr/local/hadoop/sbin/stop-all.sh
	hadoop namenode -format
	/usr/local/hadoop/sbin/start-all.sh
	hadoop fs -mkdir -p /joshi_sarkar/assignment3/input
	hadoop fs -put ./all/* /joshi_sarkar/assignment3/input
	hadoop jar Cluster_Analysis_Average.jar /joshi_sarkar/assignment3/input /joshi_sarkar/assignment3/output
	hadoop fs -get /joshi_sarkar/assignment3/output
	Rscript ./TimeAnalysis.R
emr:
	aws s3 mb s3://sjss03
	aws s3 cp Cluster_Analysis_Average.jar s3://sjss03/
	aws s3 cp ./all/ s3://sjss03/input --recursive
	aws emr create-cluster --name "Assignment3_Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge --steps Type=CUSTOM_JAR,Name="Cluster_Analysis_Average JAR",ActionOnFailure=CONTINUE,Jar=s3://sjss03/Cluster_Analysis_Average.jar,Args=[s3://sjss03/input,s3://sjss03/output] --auto-terminate --log-uri s3://sjss03/log --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a --enable-debugging
