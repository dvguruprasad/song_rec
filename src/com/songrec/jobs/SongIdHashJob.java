package com.songrec.jobs;

import com.songrec.mappers.SongIdHashMapper;
import com.songrec.reducers.SongIdHashReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SongIdHashJob extends AbstactJob {
    public SongIdHashJob(String inputPath, String outputPath) {
        super(inputPath, outputPath);
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(SongIdHashJob.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(SongIdHashMapper.class);
        job.setReducerClass(SongIdHashReducer.class);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new SongIdHashJob(args[0], args[1]), args);
        System.exit(res);
    }
}
