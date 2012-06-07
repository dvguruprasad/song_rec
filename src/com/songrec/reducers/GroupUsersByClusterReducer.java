package com.songrec.reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupUsersByClusterReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text clusterId, Iterable<Text> userIds, Context context) throws IOException, InterruptedException {
        String commaSeperatedUserIds = "";
        for (Text userId : userIds)
            commaSeperatedUserIds += userId.toString() + ",";
        context.write(clusterId, new Text(commaSeperatedUserIds));
    }
}
