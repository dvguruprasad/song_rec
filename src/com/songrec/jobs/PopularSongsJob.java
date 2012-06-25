package com.songrec.jobs;

import com.songrec.mappers.PopularSongsMapper;
import com.songrec.reducers.PopularSongsReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PopularSongsJob extends AbstactJob {
    public PopularSongsJob(String inputPath, String outputPath) {
        super(outputPath, inputPath);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PopularSongsJob(args[0], args[1]), args);
        System.exit(res);
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setJarByClass(PopularSongsJob.class);

        job.setMapperClass(PopularSongsMapper.class);
        job.setCombinerClass(PopularSongsReducer.class);
        job.setReducerClass(PopularSongsReducer.class);
    }
}
