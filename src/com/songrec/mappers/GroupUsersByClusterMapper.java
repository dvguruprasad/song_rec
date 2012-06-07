package com.songrec.mappers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupUsersByClusterMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text clusterId, Text userId, Context context) throws IOException, InterruptedException {
        context.write(clusterId, userId);
    }
}
