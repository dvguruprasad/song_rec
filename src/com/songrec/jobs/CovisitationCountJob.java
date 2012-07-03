package com.songrec.jobs;

import com.songrec.dto.SongPair;
import com.songrec.mappers.CovisitationCountMapper;
import com.songrec.reducers.CovisitationCountReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class CovisitationCountJob extends AbstactJob {
    private String songIdHashOutputPath;

    public CovisitationCountJob(String inputPath, String outputPath, String songIdHashOutputPath) {
        super(outputPath, inputPath);
        this.songIdHashOutputPath = songIdHashOutputPath;
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(CovisitationCountMapper.class);
        job.setMapOutputKeyClass(SongPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(CovisitationCountReducer.class);
        job.setOutputValueClass(SongPair.class);
        job.setOutputValueClass(IntWritable.class);

        job.getConfiguration().set(SongIdHashJob.SONG_ID_HASH_PATH, songIdHashOutputPath);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new CovisitationCountJob(args[0], args[1], args[2]), args);
        System.exit(res);
    }
}
