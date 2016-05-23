format:
	hdfs namenode -format
hstart:
	sbin/start-dfs.sh
	sbin/start-yarn.sh
	sbin/mr-jobhistory-daemon.sh start historyserver
hstop:
	sbin/mr-jobhistory-daemon.sh stop historyserver
	sbin/stop-yarn.sh
	sbin/stop-dfs.sh
sarita:
	bin/hadoop dfs -mkdir -p /user/sarita
	bin/hadoop dfs -mkdir -p /user/sarita/input
inputformat:
	gunzip all/*.gz
copydataset:
	bin/hadoop dfs -put all/*.csv /user/sarita/input
pseudo:
	hdfs namenode -format
	sbin/start-dfs.sh
	sbin/start-yarn.sh
	sbin/mr-jobhistory-daemon.sh start historyserver
	bin/hadoop dfs -mkdir -p /user/HW02
	bin/hadoop dfs -mkdir -p /user/HW02/input
	gunzip all/*.gz
	bin/hadoop dfs -put all/*.csv /user/HW02/input
	hadoop jar HW02.jar HW02 /user/HW02/input /user/HW02/output
	bin/hadoop dfs -get /user/HW02/output
emr:
	aws emr create-cluster --name "HW2_Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge --steps Type=CUSTOM_JAR,Name="Main program JAR",ActionOnFailure=CONTINUE,Jar=s3://mybucketcs6420/HW02.jar,MainClass=com/Main,Args=[s3://mybucketcs6420/input,s3://mybucketcs6420/output] --auto-terminate --log-uri s3://mybucketcs6420/logs --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a --enable-debugging {"ClusterId":"1234"}
	aws emr describe-cluster --cluster-id "1234" | jq '.Cluster.Status.State'
	aws s3 ls s3://mybucketcs6420/output --recursive
	aws s3 cp s3://mybucketcs6420/output/part-r-00000
	aws emr terminate-clusters --cluster-ids "1234"


	
