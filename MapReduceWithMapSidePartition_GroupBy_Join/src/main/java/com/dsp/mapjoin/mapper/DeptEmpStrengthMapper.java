package com.dsp.mapjoin.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.dsp.mapjoin.model.TextPair;

public class DeptEmpStrengthMapper extends MapReduceBase implements Mapper<LongWritable, Text, TextPair, Text> {

	public void map(LongWritable key, Text value, OutputCollector<TextPair, Text> output, Reporter reporter) 
			throws IOException 
	{	
	
		String valueString = value.toString();
		String[] SingleNodeData = valueString.split("\t");
		output.collect(new TextPair(SingleNodeData[0], "1"), new Text(SingleNodeData[1]));
	}
}

