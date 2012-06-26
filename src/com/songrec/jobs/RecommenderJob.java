package com.songrec.jobs;

import com.songrec.dto.SongRecommendations;
import com.songrec.dto.UserVectorAndSimilarityScores;
import com.songrec.mappers.RecommenderMapper;
import com.songrec.reducers.RecommenderReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class RecommenderJob extends AbstactJob {
    public RecommenderJob(String inputPath, String outputPath) {
        super(outputPath, inputPath);
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(RecommenderMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(UserVectorAndSimilarityScores.class);

        job.setReducerClass(RecommenderReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(SongRecommendations.class);

        job.setNumReduceTasks(10);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new RecommenderJob(args[0], args[1]), args);
        System.exit(res);
    }
}
