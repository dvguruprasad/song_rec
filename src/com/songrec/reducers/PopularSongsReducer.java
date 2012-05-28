package com.songrec.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PopularSongsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text songId, Iterable<IntWritable> playCounts, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable playCount : playCounts) sum += playCount.get();
        context.write(songId, new IntWritable(sum));
    }
}