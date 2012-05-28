package com.songrec.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;


public class PopularSongsMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text triplet, Context context) throws IOException, InterruptedException {
       String[] tokens = triplet.toString().split("\\s+");
       String songId = tokens[1];
       int playCount = Integer.parseInt(tokens[2]);
       context.write(new Text(songId), new IntWritable(playCount));
    }
}
