export AWS_PROFILE=dev_acct
aws emr create-cluster \
 --name "dev_cluster" \
 --log-uri "s3n://aws-logs-341966982503-us-east-1/elasticmapreduce/" \
 --release-label "emr-6.5.0" \
 --service-role "EMR_DefaultRole" \
 --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","EmrManagedMasterSecurityGroup":"sg-006fd4680d89f7a1e","EmrManagedSlaveSecurityGroup":"sg-0f24caabca7daf52b","KeyName":"sanjeeb_ec2","SubnetId":"subnet-0072c61000057a25e"}' \
 --applications Name=Hadoop Name=Hive Name=Spark Name=Tez \
 --configurations '[{"Classification":"hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}},{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' \
 --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","Name":"Master - 1","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}},{"InstanceCount":1,"InstanceGroupType":"CORE","Name":"Core - 2","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}},{"BidPrice":"0.154","InstanceCount":2,"InstanceGroupType":"TASK","Name":"Task - 3","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}}]' \
 --auto-scaling-role "EMR_AutoScaling_DefaultRole" \
 --scale-down-behavior "TERMINATE_AT_TASK_COMPLETION" \
 --ebs-root-volume-size "10" \
 --auto-termination-policy '{"IdleTimeout":3600}' \
 --region "us-east-1"


# aws emr terminate-clusters --cluster-ids j-CMTA99HJ7MBR