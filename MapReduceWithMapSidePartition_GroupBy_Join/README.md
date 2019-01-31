# This is an MapSide Join with the help of Partitioning and Grouping Different Keys into same group with 
#  the help of setGroupByValuesComparator
# This also has source code to provide example of Secondary Sorting in hadoop

#Steps to run the job on Yarn

mvn clean
mvn install -> Jar will be generated

# Copy the jar and run on yarn by specifying the jar name, main class, input and output path.

$HADOOP_HOME/bin/yarn jar mapsidejoin-0.0.1-SNAPSHOT.jar /user/pradeep/DeptName.txt /user/pradeep/DeptStrength.txt /user/pradeep/mapsidejoin
#MR job should be executed successfully.

#Validate the output file data by executing the below command
$HADOOP_HOME/bin/hdfs dfs -cat /user/pradeep/output/part-r-00000