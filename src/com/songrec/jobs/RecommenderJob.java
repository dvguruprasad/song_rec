package com.songrec.jobs;

import com.songrec.dto.SongRecommendations;
import com.songrec.dto.UserVectorAndSimilarityScores;
import com.songrec.mappers.RecommenderMapper;
import com.songrec.reducers.RecommenderReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class RecommenderJob extends AbstactJob {
    private String hashedSongIdsPath;
    private String hashedUserIdsPath;

    public RecommenderJob(String inputPath, String outputPath, String hashedSongIdsPath, String hashedUserIdsPath) {
        super(outputPath, inputPath);
        this.hashedSongIdsPath = hashedSongIdsPath;
        this.hashedUserIdsPath = hashedUserIdsPath;
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(RecommenderMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(UserVectorAndSimilarityScores.class);

        job.setReducerClass(RecommenderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SongRecommendations.class);

        job.getConfiguration().set("songs_hash", hashedSongIdsPath);
        job.getConfiguration().set("users_hash", hashedUserIdsPath);

    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new RecommenderJob(args[0], args[1], args[2], args[3]), args);
        System.exit(res);
    }
}
