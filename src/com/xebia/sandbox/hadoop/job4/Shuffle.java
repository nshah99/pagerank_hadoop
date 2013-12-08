package com.xebia.sandbox.hadoop.job4;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Shuffle extends Configured implements Tool {
	
    public static String OUT = "outfile";
    public static String IN = "inputlarger";
    public static int count = 1;

public static class ShuffleMapper extends Mapper<LongWritable,Text,LongWritable,Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] values = value.toString().split("1.0");
		int len = values.length;
		int val = value.getLength();
		context.write(new LongWritable(count++),new Text(values[0]));
		
	}
	
	
}

public int run(String[] args) throws Exception {
	// TODO Auto-generated method stub
    //set in and out to args.
    IN = "wiki/ranking/iter00/part-00000";
    OUT = "wiki/shuffle";

    String infile = IN;
    String outputfile = OUT + System.nanoTime();

    boolean isdone = false;
    boolean success = false;

    HashMap <Integer, Integer> _map = new HashMap<Integer, Integer>();

    

        Job job = new Job(getConf());
        job.setJarByClass(Shuffle.class);
        job.setJobName("Shuffle");
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(ShuffleMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(infile));
        FileOutputFormat.setOutputPath(job, new Path(outputfile));

        success = job.waitForCompletion(true);
        
	return 0;
}

public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Shuffle(), args));
}


}
