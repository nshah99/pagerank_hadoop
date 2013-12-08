package com.xebia.sandbox.hadoop.job1.xmlhakker;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WikiJsonMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text>{

	public void map(LongWritable key, Text value,
			OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		@SuppressWarnings("unused")
		String[] values = value.toString().split(",");
		output.collect(key, new Text("test"));
	}

}
