package com.songrec.jobs;

import com.songrec.mappers.UserIdHashMapper;
import com.songrec.reducers.UserIdHashReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class UserIdHashJob extends AbstactJob {
    public UserIdHashJob(String inputPath, String outputPath) {
        super(outputPath, inputPath);
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setMapperClass(UserIdHashMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(UserIdHashReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setJarByClass(UserIdHashJob.class);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new UserIdHashJob(args[0], args[1]), args);
        System.exit(res);
    }
}
