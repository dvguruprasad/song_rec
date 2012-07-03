package com.songrec.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserIdHashReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable hashedUserId, Iterable<Text> userIds, Context context) throws IOException, InterruptedException {
        context.write(hashedUserId, userIds.iterator().next());
    }
}
