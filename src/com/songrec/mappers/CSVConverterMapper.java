package com.songrec.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CSVConverterMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        String userId = tokens[0];
        String songId = tokens[1];
        String playCount = tokens[2];
        String result = userId + "," + songId + "," + playCount;
        context.write(new Text(result), new Text(""));
    }
}
