package com.dsp.sales;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.dsp.sales.comparator.MyKeyComparator;
import com.dsp.sales.mapper.SalesMapper;
import com.dsp.sales.reducer.SalesCountryReducer;

public class SalesCountryDriver {
	public static void main(String[] args) {
		// Create a configuration object for the job
		JobConf job_conf = new JobConf(SalesCountryDriver.class);

		// Set a name of the Job
		job_conf.setJobName("SalePerCountry");

		// Specify data type of output key and value
		job_conf.setOutputKeyClass(Text.class);
		job_conf.setOutputValueClass(IntWritable.class);

		// Specify names of Mapper and Reducer Class
		job_conf.setMapperClass(SalesMapper.class);
		job_conf.setReducerClass(SalesCountryReducer.class);

		// Specify formats of the data type of Input and output
		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);

		// Comparator
		job_conf.setOutputKeyComparatorClass(MyKeyComparator.class);
		
		//We can store common configuration and files to cache
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job_conf);

     // Get the cached archives/files
      //  File f = new File("./cachedfile/zip.txt");
      //  File f = new File("./map.zip/some/file/in/zip.txt");
        
		// Set input and output directories using command line arguments, 
		//arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.
		
		FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));

		try {
			// Run the job 
			Job my_client = new Job(job_conf, "Test Job");

			my_client.waitForCompletion(true);
			 if(my_client.isSuccessful()) {
		            System.out.println("Job was successful");
		        } else if(!my_client.isSuccessful()) {
		            System.out.println("Job was not successful");           
		        }
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
