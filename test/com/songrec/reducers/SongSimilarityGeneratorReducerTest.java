package com.songrec.reducers;

import com.songrec.Counters;
import com.songrec.dto.PlayCountPair;
import com.songrec.dto.PlayCountPairsMap;
import com.songrec.dto.SimilarityScore;
import com.songrec.dto.SimilarityScores;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.*;

public class SongSimilarityGeneratorReducerTest {
    @Test
    public void shouldGenerateSimilarityVectorForGivenSong() throws IOException, InterruptedException {
        SongSimilarityGeneratorReducer reducer = new SongSimilarityGeneratorReducer();
        Reducer.Context mockContext = mock(Reducer.Context.class);
        when(mockContext.getCounter(Counters.TIME_SPENT_IN_COMPUTAION_OF_SIMILARITIES)).thenReturn(mock(Counter.class));
        ArrayList<PlayCountPairsMap> playCountVectors = new ArrayList<PlayCountPairsMap>();
        playCountVectors.add(songPlayCountVector_20());
        playCountVectors.add(songPlayCountVector_30());

        IntWritable songId = new IntWritable(10);
        reducer.reduce(songId, playCountVectors, mockContext);
        verify(mockContext).write(songId, expectedSimilarityScoresVector());
    }

    private SimilarityScores expectedSimilarityScoresVector() {
        SimilarityScores vector = new SimilarityScores();
        vector.add(new SimilarityScore(20, 0.7321617151746577));
        vector.add(new SimilarityScore(30, 0.8737200675525434));
        return vector;
    }

    private PlayCountPairsMap songPlayCountVector_20() {
        HashMap<Integer, List<PlayCountPair>> map = new HashMap<Integer, List<PlayCountPair>>();
        ArrayList<PlayCountPair> playCountPairs = new ArrayList<PlayCountPair>();
        playCountPairs.add(new PlayCountPair((short)5, (short)4));
        playCountPairs.add(new PlayCountPair((short)9, (short)3));
        playCountPairs.add(new PlayCountPair((short)4, (short)9));
        playCountPairs.add(new PlayCountPair((short)20, (short)15));
        map.put(20, playCountPairs);
        return new PlayCountPairsMap(map);
    }

    private PlayCountPairsMap songPlayCountVector_30() {
        HashMap<Integer, List<PlayCountPair>> map = new HashMap<Integer, List<PlayCountPair>>();
        ArrayList<PlayCountPair> playCountPairs = new ArrayList<PlayCountPair>();
        playCountPairs.add(new PlayCountPair((short)10, (short)4));
        playCountPairs.add(new PlayCountPair((short)1, (short)3));
        playCountPairs.add(new PlayCountPair((short)20, (short)30));
        playCountPairs.add(new PlayCountPair((short)5, (short)10));
        map.put(30, playCountPairs);
        return new PlayCountPairsMap(map);
    }
}
