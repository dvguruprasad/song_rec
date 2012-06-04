package com.songrec.reducers;

import com.songrec.algorithms.PearsonCorrelationSimilarity;
import com.songrec.dto.PlayCountPair;
import com.songrec.dto.SongPair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class ItemSimilarityReducer extends Reducer<SongPair, PlayCountPair, SongPair, DoubleWritable> {
    @Override
    protected void reduce(SongPair songPair, Iterable<PlayCountPair> playCountVectors, Context context) throws IOException, InterruptedException {
        long then = System.nanoTime();
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
        context.getCounter(com.songrec.Counters.TIME_SPENT_IN_COMPUTAION_OF_SIMILARITIES_REDUCER).increment(System.nanoTime() - then);
    }
}
