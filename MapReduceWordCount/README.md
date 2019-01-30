# This is an Sample Map Reduce WordCount Project which will read the text file from HDFS and outputs the MR result to output file.
#Steps to run the job on Yarn

mvn clean
mvn install -> Jar will be generated

# Copy the jar and run on yarn by specifying the jar name, main class, input and output path.

$HADOOP_HOME/bin/yarn jar <mapreduce-0.0.1-SNAPSHOT.jar> /user/pradeep/simplielearn.txt /user/pradeep/output
#MR job should be executed successfully.

#Validate the output file data by executing the below command
$HADOOP_HOME/bin/hdfs dfs -cat /user/pradeep/output/part-r-00000