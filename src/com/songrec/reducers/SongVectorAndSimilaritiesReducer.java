package com.songrec.reducers;

import com.songrec.dto.SimilarityScores;
import com.songrec.dto.SongVectorAndSimilarityScores;
import com.songrec.dto.SongVectorOrSimilarityScores;
import com.songrec.dto.UserPlayCounts;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SongVectorAndSimilaritiesReducer extends Reducer<IntWritable, SongVectorOrSimilarityScores, IntWritable, SongVectorAndSimilarityScores> {
    @Override
    protected void reduce(IntWritable songId, Iterable<SongVectorOrSimilarityScores> values, Context context) throws IOException, InterruptedException {
        int counter = 0;
        String types = "(";
        SongVectorAndSimilarityScores result = new SongVectorAndSimilarityScores();
        for (SongVectorOrSimilarityScores songVectorOrSimilarityScores : values) {
            if (songVectorOrSimilarityScores.hasSimilarityScores()) {
                result.setSimilarityScores(songVectorOrSimilarityScores.getSimilarityScores());
                types += SimilarityScores.class.getSimpleName() + ",";
            } else {
                result.setUserPlayCounts(songVectorOrSimilarityScores.getPlayCounts());
                types += UserPlayCounts.class.getSimpleName() + ",";
            }
            counter++;
            types += songVectorOrSimilarityScores.toString();
        }
        if(!result.hasBoth()) return;
        types += ")";
        System.out.println("Number of iterables for song id " + songId.get() + ": " + counter + "| types: " + types);
        context.write(songId, result);
    }
}