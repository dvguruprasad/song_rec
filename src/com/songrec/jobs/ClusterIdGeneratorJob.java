package com.songrec.jobs;

import com.songrec.mappers.ClusterIdGeneratorMapper;
import com.songrec.reducers.ClusterIdGeneratorReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ClusterIdGeneratorJob extends AbstactJob {

    public ClusterIdGeneratorJob(String inputPath, String outputPath) {
        super(inputPath, outputPath);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new ClusterIdGeneratorJob(args[0], args[1]), args);
        System.exit(res);
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ClusterIdGeneratorMapper.class);
        job.setReducerClass(ClusterIdGeneratorReducer.class);

        job.setNumReduceTasks(10);
    }
}
