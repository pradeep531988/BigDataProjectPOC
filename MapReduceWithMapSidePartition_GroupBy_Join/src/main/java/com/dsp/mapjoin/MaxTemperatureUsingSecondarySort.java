package com.dsp.mapjoin;


//MaxTemperatureUsingSecondarySort Application to find the maximum temperature by sorting temperatures in the key
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.dsp.mapjoin.model.IntPair;

public class MaxTemperatureUsingSecondarySort extends Configured implements
		Tool {

	/*
	 * Partitioner decides which mapper output goes
	 *  to which reduer based on mapper output key. In general, different key is in 
	 *  different group (Iterator at the reducer side). But sometimes, 
	 *  we want different key is in the same group. 
	 *  This is the time for Output Value Grouping Comparator, 
	 *  which  is used to group mapper output. For easy understanding, think this is the group by condition in SQL. 
	 *  We use this since we have IntPair as OutputKey and we need to group it by First value in output
	 *  key.
	 *  Output Key Comparator is used during sort stage for the mapper output key.
   	remember: 
 	if you use setOutputValueGroupingComparator, all the key in the same group at reducer side will be 
 	same now even they are not the same at the mapper output.
	 */
	static class MaxTemperatureMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, IntPair, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<IntPair, Text> output, Reporter reporter)
				throws IOException {

			String[] strs = value.toString().split("\\t");

			output.collect(
					new IntPair(Integer.parseInt(strs[0]), Integer
							.parseInt(strs[1])), new Text(strs[2]));
		}
	}

	static class MaxTemperatureReducer extends MapReduceBase implements
			Reducer<IntPair, Text, IntPair, Text> {

		public void reduce(IntPair key, Iterator<Text> values,
				OutputCollector<IntPair, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				output.collect(key, values.next());
			}
		}
	}

	public static class FirstPartitioner implements
			Partitioner<IntPair, Text> {

		
		public void configure(JobConf job) {
		}

		
		public int getPartition(IntPair key, Text value,
				int numPartitions) {
			return Math.abs(key.getFirst() * 127) % numPartitions;
		}
	}

	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(IntPair.class, true);
		}

		
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntPair ip1 = (IntPair) w1;
			IntPair ip2 = (IntPair) w2;
			int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
			if (cmp != 0) {
				return cmp;
			}
			return -IntPair.compare(ip1.getSecond(), ip2.getSecond()); // reverse
		}
	}

	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(IntPair.class, true);
		}

		
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntPair ip1 = (IntPair) w1;
			IntPair ip2 = (IntPair) w2;
			return IntPair.compare(ip1.getFirst(), ip2.getFirst());
		}
	}

	public int run(String[] args) throws IOException {
		JobConf conf = new JobConf(getConf(), getClass());

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		FileInputFormat.addInputPath(conf, input);
		FileOutputFormat.setOutputPath(conf, output);
		conf.setMapperClass(MaxTemperatureMapper.class);
		conf.setPartitionerClass(FirstPartitioner.class);
		conf.setOutputKeyComparatorClass(KeyComparator.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);
		conf.setReducerClass(MaxTemperatureReducer.class);
		conf.setOutputKeyClass(IntPair.class);
		conf.setOutputValueClass(Text.class);

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(),
				args);
		System.exit(exitCode);
	}
}
//^^ MaxTemperatureUsingSecondarySort