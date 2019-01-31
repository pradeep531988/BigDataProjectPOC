# This is an Sample Map Reduce CountryWiseSalesFinder Project which will read the text file from HDFS and outputs the MR result to output file.
#Steps to run the job on Yarn

mvn clean
mvn install -> Jar will be generated

# Copy the jar and run on yarn by specifying the jar name, main class, input and output path.

$HADOOP_HOME/bin/yarn jar salescountrywise-0.0.1-SNAPSHOT /user/pradeep/sales.txt /user/pradeep/output
#MR job should be executed successfully.

#Validate the output file data by executing the below command
$HADOOP_HOME/bin/hdfs dfs -cat /user/pradeep/output/part-r-00000