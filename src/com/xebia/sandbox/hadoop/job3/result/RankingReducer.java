package com.xebia.sandbox.hadoop.job3.result;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RankingReducer extends MapReduceBase implements Reducer<FloatWritable, Text, Text, FloatWritable> {
	
	public void reduce(FloatWritable key, Iterator<Text> values, OutputCollector<Text, FloatWritable> output, Reporter arg3) throws IOException {
		
		while(values.hasNext()) {
			output.collect(values.next(), key);
		}
	}
	
}

