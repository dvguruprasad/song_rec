package com.songrec.jobs;

import com.songrec.dto.UserPlayCountsAndSimilarities;
import com.songrec.reducers.Song_PlayCountAndSimilaritiesReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Song_PlayCountAndSimilaritiesGeneratorJob extends AbstactJob {
    public Song_PlayCountAndSimilaritiesGeneratorJob(String song_userVectorPath, String songSimilarityVectorPath, String outputPath) {
        super(outputPath, song_userVectorPath, songSimilarityVectorPath);
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Song_PlayCountAndSimilaritiesReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(UserPlayCountsAndSimilarities.class);
    }
}
