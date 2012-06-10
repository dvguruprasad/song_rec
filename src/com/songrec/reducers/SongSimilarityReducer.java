package com.songrec.reducers;

import com.songrec.algorithms.PearsonCorrelationSimilarity;
import com.songrec.dto.PlayCountPair;
import com.songrec.dto.SongPair;
import com.songrec.utils.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class SongSimilarityReducer extends Reducer<SongPair, PlayCountPair, Text, DoubleWritable> {
    private static Map<Integer,String> itemIdToHashMap;
    @Override

    protected void setup(Context context) throws IOException, InterruptedException {
        itemIdToHashMap = FileUtils.getItemIdToHashMap(new Path("/u/guru/mr-output/SongIdHashJob"), context.getConfiguration());
    }

    @Override
    protected void reduce(SongPair songPair, Iterable<PlayCountPair> playCountVectors, Context context) throws IOException, InterruptedException {
        long then = System.nanoTime();
        ArrayList<Short> firstItemVector = new ArrayList<Short>();
        ArrayList<Short> secondItemVector = new ArrayList<Short>();
        for(PlayCountPair pair : playCountVectors){
            firstItemVector.add(pair.firstSongPlayCount());
            secondItemVector.add(pair.secondSongPlayCount());
        }

        if(firstItemVector.size() < 3)
            return;

        double similarityScore = PearsonCorrelationSimilarity.score(firstItemVector, secondItemVector);
        String songPairText = itemIdToHashMap.get(songPair.firstSongId()) + "," + itemIdToHashMap.get(songPair.secondSongId());
        context.write(new Text(songPairText), new DoubleWritable(similarityScore));
        context.getCounter(com.songrec.Counters.TIME_SPENT_IN_COMPUTAION_OF_SIMILARITIES).increment(System.nanoTime() - then);
    }
}