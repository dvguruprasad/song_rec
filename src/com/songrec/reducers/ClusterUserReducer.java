package com.songrec.reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ClusterUserReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text clusterId, Iterable<Text> userIds, Context context) throws IOException, InterruptedException {
        int count = 0;
        String result = "";
        for(Text userId : userIds){
            result += userId + ",";
            count++;
        }
        if(count < 2)
           return;
        context.write(clusterId, new Text(result));
    }
}
