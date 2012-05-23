package com.songrec;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;


public class PopularSongsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text triplet, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
       String[] tokens = triplet.toString().split("\\s+");
       String songId = tokens[1];
       int playCount = Integer.parseInt(tokens[2]);
       outputCollector.collect(new Text(songId), new IntWritable(playCount));
    }
}
