package com.songrec.jobs;

import com.songrec.dto.SongVectorAndSimilarityScores;
import com.songrec.dto.SongVectorOrSimilarityScores;
import com.songrec.reducers.SongVectorAndSimilaritiesReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SongVectorAndSimilaritiesGeneratorJob extends AbstactJob {
    public SongVectorAndSimilaritiesGeneratorJob(String songVectorPath, String songSimilarityVectorPath, String outputPath) {
        super(outputPath, songVectorPath, songSimilarityVectorPath);
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SongVectorOrSimilarityScores.class);

        job.setReducerClass(SongVectorAndSimilaritiesReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(SongVectorAndSimilarityScores.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setNumReduceTasks(10);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new SongVectorAndSimilaritiesGeneratorJob(args[0], args[1], args[2]), args);
        System.exit(res);
    }
}

