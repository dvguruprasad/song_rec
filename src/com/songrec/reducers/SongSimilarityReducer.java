package com.songrec.reducers;

import com.songrec.algorithms.PearsonCorrelationSimilarity;
import com.songrec.dto.PlayCountPair;
import com.songrec.dto.PlayCountPairsMap;
import com.songrec.dto.SongSimilarityScores;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class SongSimilarityReducer extends Reducer<IntWritable, PlayCountPairsMap, IntWritable, SongSimilarityScores> {

    public static final int SIMILARITY_SCORE_THRESHOLD = 0;
    public static final int MINIMUM_NUMBER_OF_PLAYCOUNTS = 3;

    @Override
    protected void reduce(IntWritable songId, Iterable<PlayCountPairsMap> playCountVectors, Context context) throws IOException, InterruptedException {

        int counter = 0;
        int relatedNumberOfSongs = 0;
        Iterator<PlayCountPairsMap> iterator = playCountVectors.iterator();
        HashMap<Integer, List<PlayCountPair>> map = new HashMap<Integer, List<PlayCountPair>>();

        while (iterator.hasNext()) {
            counter++;
            PlayCountPairsMap playCountVector = iterator.next();
            for (Map.Entry<Integer, List<PlayCountPair>> entry : playCountVector.entrySet()) {
                if (map.containsKey(entry.getKey())) {
                    map.get(entry.getKey()).addAll(entry.getValue());
                } else {
                    map.put(entry.getKey(), entry.getValue());
                }
                System.out.println("[map " + counter + "] song related to " + songId.get() + " => " + entry.getKey());
            }
            relatedNumberOfSongs += playCountVector.size();
        }
        PlayCountPairsMap aggregatedMap = new PlayCountPairsMap(map);
        System.out.println("Number of related songs in aggregated map for song id " + songId + "= " + aggregatedMap.size());
        System.out.println("Number of Playcount PairMaps for song + " + songId + "= " + counter);
        System.out.println("Number of songs related to " + songId + " => " + relatedNumberOfSongs);

        SongSimilarityScores similarityScores = new SongSimilarityScores();

        for (Map.Entry<Integer, List<PlayCountPair>> playCountVector : aggregatedMap.entrySet()) {
            if (playCountVector.getValue().size() < MINIMUM_NUMBER_OF_PLAYCOUNTS)
                continue;

            double similarityScore = similarityScore(playCountVector.getValue());
            if (similarityScore > SIMILARITY_SCORE_THRESHOLD)
                similarityScores.put(playCountVector.getKey(), similarityScore);
        }

        if (!similarityScores.isEmpty())
            context.write(songId, similarityScores);
    }

    private double similarityScore(List<PlayCountPair> playCountPairs) {
        ArrayList<Short> firstSongPlayCounts = new ArrayList<Short>();
        ArrayList<Short> secondSongPlayCounts = new ArrayList<Short>();
        for (int i = 0; i < playCountPairs.size(); i++) {
            PlayCountPair pair = playCountPairs.get(i);
            firstSongPlayCounts.add(pair.firstSongPlayCount());
            secondSongPlayCounts.add(pair.secondSongPlayCount());

        }
        return PearsonCorrelationSimilarity.score(firstSongPlayCounts, secondSongPlayCounts);
    }
}