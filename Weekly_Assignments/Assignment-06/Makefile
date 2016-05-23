# Author : Sharmo and Sarita

variable_bucket_name = ${variable_bucket_name}  # please replace variable_bucket_name with your AWS bucket name
i = test1.csv #${input_dir}
o = out #${output_dir}

jar:
	sbt package
	cp target/scala-*/missed*.jar Missed.jar

runscala:jar
	rm -rf out
	sbt "run $i $o"
runpython:
	rm -rf out
	/usr/local/hadoop/spark-1.2.0-bin-hadoop2.4/bin/pyspark Missed_python.py $i $o
	
clean:
	rm -rf out derby.log metastore_db project target
	rm -rf *.class *.jar

aws:	jar
	aws s3 cp Missed.jar s3://${variable_bucket_name}/job/
	aws s3 rm s3://${variable_bucket_name}/output --recursive
	aws emr create-cluster \
	--name "Missed_Connection_Spark" \
	--release-label emr-4.3.0 \
	--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=c1.medium \
                      InstanceGroupType=CORE,InstanceCount=4,InstanceType=c1.medium \
	--steps Type=CUSTOM_JAR,Name="Missed JAR Step",ActionOnFailure=CONTINUE,Jar=s3://${variable_bucket_name}/Missed.jar,MainClass=Missed,Args=[s3://${variable_bucket_name}/input,s3://${variable_bucket_name}/output] \
	--auto-terminate \
	--log-uri s3://${variable_bucket_name}/logs \
	--service-role EMR_DefaultRole \
	--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a \
	--enable-debugging > response.json




