> This code is for production purposes
> 1. After build the file and the code is running very good:
> 2. We run the command:
>> spark-submit --master local[*] --class sparkPack.SparkObj
> /home/cloudera/jarpath/SparkDeployment-0.0.1-SNAPSHOT.jar
>
- Usually the code is running using YARN in the cluster, but because we are running in
single node cluster, we can use it local system
- >> NOTE: sparkPAck.SparkObj is for " spark package . spark object name".