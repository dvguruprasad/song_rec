package com.songrec.reducers;

import com.songrec.PlayCountPair;
import com.songrec.SongPair;
import com.songrec.algorithms.PearsonCorrelationSimilarity;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class ItemSimilarityReducer extends Reducer<SongPair, PlayCountPair, SongPair, DoubleWritable> {
    @Override
    protected void reduce(SongPair songPair, Iterable<PlayCountPair> playCountVectors, Context context) throws IOException, InterruptedException {
        ArrayList<Float> firstItemVector = new ArrayList<Float>();
        ArrayList<Float> secondItemVector = new ArrayList<Float>();
        for(PlayCountPair pair : playCountVectors){
            firstItemVector.add((float) pair.firstSongPlayCount());
            secondItemVector.add((float) pair.secondSongPlayCount());
        }

        if(firstItemVector.size() < 3)
            return;

        double similarityScore = PearsonCorrelationSimilarity.score(firstItemVector, secondItemVector);
        context.write(songPair, new DoubleWritable(similarityScore));
    }
}
