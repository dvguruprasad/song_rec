package com.songrec.reducers;

import com.songrec.dto.SongVectorAndSimilarityScores;
import com.songrec.dto.SongVectorOrSimilarityScores;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SongVectorAndSimilaritiesReducer extends Reducer<IntWritable, SongVectorOrSimilarityScores, IntWritable, SongVectorAndSimilarityScores> {
    @Override
    protected void reduce(IntWritable songId, Iterable<SongVectorOrSimilarityScores> values, Context context) throws IOException, InterruptedException {
        SongVectorAndSimilarityScores result = new SongVectorAndSimilarityScores();
        for (SongVectorOrSimilarityScores songVectorOrSimilarityScores : values) {
            if (songVectorOrSimilarityScores.hasSimilarityScores()) {
                result.setSimilarityScores(songVectorOrSimilarityScores.getSimilarityScores());
            } else {
                result.setUserPlayCounts(songVectorOrSimilarityScores.getPlayCounts());
            }
        }
        if (!result.hasBoth()) return;
        context.write(songId, result);
    }
}