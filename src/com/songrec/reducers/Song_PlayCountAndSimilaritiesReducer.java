package com.songrec.reducers;

import com.songrec.dto.SongSimilarityScores;
import com.songrec.dto.SongVector;
import com.songrec.dto.UserPlayCounts;
import com.songrec.dto.UserPlayCountsAndSimilarities;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Song_PlayCountAndSimilaritiesReducer extends Reducer<IntWritable, SongVector, IntWritable, UserPlayCountsAndSimilarities> {
    @Override
    protected void reduce(IntWritable songId, Iterable<SongVector> values, Context context) throws IOException, InterruptedException {
        SongSimilarityScores similarityScores = null;
        UserPlayCounts userPlayCounts = null;
                
        for(SongVector vector : values){
            if(vector instanceof SongSimilarityScores){
                similarityScores = (SongSimilarityScores) vector;
            } else{
                userPlayCounts = (UserPlayCounts)vector;
            }
        }
        context.write(songId,  new UserPlayCountsAndSimilarities(similarityScores.getMap(), userPlayCounts.getMap()));
    }
}