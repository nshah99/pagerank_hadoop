package com.xebia.sandbox.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Comparator;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.xebia.sandbox.hadoop.job1.xmlhakker.WikiJsonMapper;
import com.xebia.sandbox.hadoop.job1.xmlhakker.WikiLinksReducer;
import com.xebia.sandbox.hadoop.job1.xmlhakker.WikiPageLinksMapper;
import com.xebia.sandbox.hadoop.job1.xmlhakker.XmlInputFormat;
import com.xebia.sandbox.hadoop.job2.calculate.RankCalculateMapper;
import com.xebia.sandbox.hadoop.job2.calculate.RankCalculateReduce;
import com.xebia.sandbox.hadoop.job3.result.RankingMapper;
import com.xebia.sandbox.hadoop.job3.result.RankingReducer;


public class WikiPageRanking {
    
    private static NumberFormat nf = new DecimalFormat("00");
    
    public static void main(String[] args) throws Exception {
        WikiPageRanking pageRanking = new WikiPageRanking();
        
        pageRanking.runXmlParsing(args[0], args[1]);
        
        int runs = 0;
        Integer noOfRuns = Integer.parseInt(args[2]);
        for (; runs < noOfRuns; runs++) {
            //pageRanking.runRankCalculation("wiki/ranking/iter"+nf.format(runs), "wiki/ranking/iter"+nf.format(runs + 1));
            pageRanking.runRankCalculation("s3n://nshah99/wiki/ranking/iter"+nf.format(runs), "s3n://nshah99/wiki/ranking/iter"+nf.format(runs + 1));

        }
        
        //pageRanking.runRankOrdering("wiki/ranking/iter"+nf.format(runs), "wiki/result");
        pageRanking.runRankOrdering("s3n://nshah99/wiki/ranking/iter"+nf.format(runs), "s3n://nshah99/wiki/result");

        
    }
    
    public void runXmlParsing(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(WikiPageRanking.class);
        
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
        
        // Input / Mapper
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        conf.setInputFormat(XmlInputFormat.class);
        conf.setMapperClass(WikiPageLinksMapper.class);
        
        //conf.setInputFormat(TextInputFormat.class);
        //conf.setMapperClass(WikiJsonMapper.class);
        
        // Output / Reducer
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        
        //conf.setOutputKeyClass(LongWritable.class);
        
        conf.setOutputValueClass(Text.class);
        conf.setReducerClass(WikiLinksReducer.class);
        
        JobClient.runJob(conf);
    }

    private void runRankCalculation(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(WikiPageRanking.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        conf.setMapperClass(RankCalculateMapper.class);
        conf.setReducerClass(RankCalculateReduce.class);
        
        JobClient.runJob(conf);
    }
    
    private void runRankOrdering(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(WikiPageRanking.class);
        
        conf.setOutputKeyClass(FloatWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        conf.setMapperClass(RankingMapper.class);
        conf.setReducerClass(RankingReducer.class);
        conf.setOutputKeyComparatorClass(LongWritable.DecreasingComparator.class);
        
        JobClient.runJob(conf);
    }
    
}

class ReverseRankComparator extends WritableComparator {

	public ReverseRankComparator() {
		super((Class<? extends WritableComparable>) ReverseRankComparator.class,false);
		// TODO Auto-generated constructor stub
	}
	
	
	@Override
    public int compare(Object o1, Object o2) {
			
			FloatWritable n1 = (FloatWritable) o1;
			FloatWritable n2 = (FloatWritable) o2;
			
			return n1.compareTo(n2);
	
	}


}
