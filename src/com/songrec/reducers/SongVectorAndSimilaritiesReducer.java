package com.songrec.reducers;

import com.songrec.dto.SongVectorAndSimilarityScores;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SongVectorAndSimilaritiesReducer extends Reducer<IntWritable, SongVectorAndSimilarityScores, IntWritable, SongVectorAndSimilarityScores> {
    @Override
    protected void reduce(IntWritable songId, Iterable<SongVectorAndSimilarityScores> values, Context context) throws IOException, InterruptedException {
        int counter = 0;
        String types = "(";
        SongVectorAndSimilarityScores result = new SongVectorAndSimilarityScores();
        for (SongVectorAndSimilarityScores songVectorAndSimilarityScores : values) {
//            if (songVectorAndSimilarityScores.hasSimilarityScores()) {
//                result.setSimilarityScores(new SongSimilarityScores(songVectorAndSimilarityScores.getSimilarityScores()));
//                types += SongSimilarityScores.class.getSimpleName() + ",";
//            } else {
//                result.setUserPlayCounts(new UserPlayCounts(songVectorAndSimilarityScores.getPlayCounts()));
//                types += UserPlayCounts.class.getSimpleName() + ",";
//            }
            counter++;
            types += songVectorAndSimilarityScores.toString();
        }
        types += ")";
        System.out.println("Number of iterables for song id " + songId.get() + ": " + counter + "| types: " + types);
        context.write(songId, result);
    }
}