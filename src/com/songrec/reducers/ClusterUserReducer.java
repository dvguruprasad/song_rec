package com.songrec.reducers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ClusterUserReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable clusterId, Iterable<Text> userIds, Context context) throws IOException, InterruptedException {
        String result = "";
        for(Text userId : userIds) result += userId + ",";
        context.write(clusterId, new Text(result));
    }
}
