package com.songrec.jobs;

import com.songrec.mappers.GroupUsersByClusterMapper;
import com.songrec.reducers.GroupUsersByClusterReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.io.IOException;

public class GroupUsersByClusterJob extends AbstactJob {
    public GroupUsersByClusterJob(String inputPath, String outputPath) {
        super(inputPath, outputPath);
    }

    @Override
    public void prepare(Job job) throws IOException {
        KeyValueTextInputFormat.setInputPaths(job, inputPath);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(GroupUsersByClusterMapper.class);
        job.setReducerClass(GroupUsersByClusterReducer.class);

        job.setNumReduceTasks(10);
    }
}
