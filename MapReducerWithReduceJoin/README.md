# This is an Sample MapReduce program which performs Reducer side join to find the no of transaction per person by joining Person Customer Id with 
# Transaction File CustomerID


#Reference: https://www.edureka.co/blog/mapreduce-example-reduce-side-join/

#Mapper Cust:
// Key  :4000001value value:cust	Kristina
//Key :4000002value value:cust	Paige
			
#Mapper transaction:
// Key: 4000002Tran value:tnxn	198.44
// Key: 4000002Tran value:tnxn	005.58

#ReducerJoin:
	/*Key :4000001Value:cust	Kristina
			Key :4000001Value:tnxn	021.43
			Key :4000001Value:tnxn	052.29
			Key :4000001Value:tnxn	090.04
			Key :4000001Value:tnxn	135.37
			Key :4000001Value:tnxn	047.05
			Key :4000001Value:tnxn	126.90
			Key :4000001Value:tnxn	137.64
			Key :4000001Value:tnxn	040.33 */
			

#Steps to run the job on Yarn

* mvn clean
* mvn install -> Jar will be generated

# Copy the jar and run on yarn by specifying the jar name, main class, input and output path.

$HADOOP_HOME/bin/yarn jar reducejoin-0.0.1-SNAPSHOT.jar /user/pradeep/cust_details /user/pradeep/transaction_details /user/pradeep/reducer_join

#MR job should be executed successfully.

#Validate the output file data by executing the below command
* $HADOOP_HOME/bin/hdfs dfs -cat /user/pradeep/output/part-r-00000
