package com.dsp.mapjoin;

/**
 * Hello world!
 *
 */
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.dsp.mapjoin.mapper.DeptEmpStrengthMapper;
import com.dsp.mapjoin.mapper.DeptNameMapper;
import com.dsp.mapjoin.model.TextPair;
import com.dsp.mapjoin.reducer.JoinReducer;


public class MapSideJoin extends Configured implements Tool {

	public static class KeyPartitioner implements Partitioner<TextPair, Text> {
		public void configure(JobConf job) {}
		// This Partition is done to put the same key to same group 
		// ie put Dept_ID column data and Dept_ID column data of both DeptName and DeptStrength to same group
		/*
		 * 
		 * 
		 * */


		/*
		 * Partitioner decides which mapper output goes
		 *  to which reduer based on mapper output key. In general, different key is in 
		 *  different group (Iterator at the reducer side). But sometimes, 
		 *  we want different key is in the same group. 
		 *  This is the time for Output Value Grouping Comparator, 
		 *  which  is used to group mapper output. For easy understanding, think this is the group by condition in SQL. 
		 *  Output Key Comparator is used during sort stage for the mapper output key.
    	   	remember: 
    	 	if you use setOutputValueGroupingComparator, all the key in the same group at reducer side will be 
    	 	same now even they are not the same at the mapper output.
		 */
		
		/* 
		 Group1:
		 A11	Finance
		 A11	50

		 Group2:
		 B12	HR
		 B12	100

		 Group3:
		 C13	Manufacturing
		 C13	250

		 * */
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.println("Usage: <Department Emp Strength input> <Department Name input> <output>");
			return -1;
		}

		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("Join 'Department Emp Strength input' with 'Department Name input'");

		Path AInputPath = new Path(args[0]);
		Path BInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		MultipleInputs.addInputPath(conf, AInputPath, TextInputFormat.class, DeptNameMapper.class);
		MultipleInputs.addInputPath(conf, BInputPath, TextInputFormat.class, DeptEmpStrengthMapper.class);

		FileOutputFormat.setOutputPath(conf, outputPath);

		//https://autofei.wordpress.com/2012/10/18/setpartitionerclass-setoutputkeycomparatorclass-and-setoutputvaluegroupingcomparator/
		conf.setPartitionerClass(KeyPartitioner.class);
		conf.setOutputValueGroupingComparator(TextPair.FirstComparator.class);

		conf.setMapOutputKeyClass(TextPair.class);

		conf.setReducerClass(JoinReducer.class);

		conf.setOutputKeyClass(Text.class);

		JobClient.runJob(conf);

		return 0;
	}

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new MapSideJoin(), args);
		System.exit(exitCode);
	}
}
