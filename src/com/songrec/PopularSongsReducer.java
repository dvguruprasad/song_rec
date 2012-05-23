package com.songrec;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class PopularSongsReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text songId, Iterator<IntWritable> playCounts, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int sum = 0;
        while(playCounts.hasNext())
           sum += playCounts.next().get();
        outputCollector.collect(songId, new IntWritable(sum));
    }
}