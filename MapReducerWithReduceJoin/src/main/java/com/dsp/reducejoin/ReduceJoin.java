package com.dsp.reducejoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoin {
	public static class CustsMapper extends
	Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			System.out.println("Key Customer :"+new Text(parts[0]) + "value :"+ new Text("cust\t" + parts[1]));
			// Key Customer :4000001value :cust	Kristina
			//Key Customer :4000002value :cust	Paige
			context.write(new Text(parts[0]), new Text("cust\t" + parts[1]));
		}
	}

	public static class TxnsMapper extends
	Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			//Transaction Key4000002Tran value:tnxn	198.44
			//Transaction Key4000002Tran value:tnxn	005.58
			System.out.println("Transaction Key" + new Text(parts[2]) + "Tran value:" +new Text("tnxn\t" + parts[3]));

			context.write(new Text(parts[2]), new Text("tnxn\t" + parts[3]));
		}
	}

	public static class ReduceJoinReducer extends
	Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String name = "";
			double total = 0.0;
			int count = 0;
			System.out.println("Reducer Key:" + key + " value:" +values);
			//Reducer Key: 4000001 value:org.apache.hadoop.mapreduce.task.ReduceContextImpl$ValueIterable@488eb7f2
			for (Text t : values) {
			System.out.println("Key :"+key + "Value:"+t);
			/*Key :4000001Value:cust	Kristina
			Key :4000001Value:tnxn	021.43
			Key :4000001Value:tnxn	052.29
			Key :4000001Value:tnxn	090.04
			Key :4000001Value:tnxn	135.37
			Key :4000001Value:tnxn	047.05
			Key :4000001Value:tnxn	126.90
			Key :4000001Value:tnxn	137.64
			Key :4000001Value:tnxn	040.33 */
			
				String parts[] = t.toString().split("\t");
				if (parts[0].equals("tnxn")) {
					count++;
					total += Float.parseFloat(parts[1]);
				} else if (parts[0].equals("cust")) {
					name = parts[1];
				}
			}
			String str = String.format("%d\t%f", count, total);
			context.write(new Text(name), new Text(str));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Reduce-side join");
		job.setJarByClass(ReduceJoin.class);
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);


		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CustsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TxnsMapper.class);
		Path outputPath = new Path(args[2]);


		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
