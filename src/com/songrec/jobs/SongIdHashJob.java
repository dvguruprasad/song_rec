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
    public static final String SONG_ID_HASH_PATH = "songIdHashPath";

    public SongIdHashJob(String inputPath, String outputPath) {
        super(outputPath, inputPath);
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setMapperClass(SongIdHashMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SongIdHashReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setJarByClass(SongIdHashJob.class);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new SongIdHashJob(args[0], args[1]), args);
        System.exit(res);
    }
}
