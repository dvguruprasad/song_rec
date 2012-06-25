package com.songrec.reducers;

import com.songrec.dto.SongSimilarityScores;
import com.songrec.dto.SongVector;
import com.songrec.dto.UserPlayCounts;
import com.songrec.dto.UserPlayCountPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.mockito.Mockito.mock;

public class Song_PlayCountAndSimilaritiesReducerTest {
    @Test
    public void shouldConsolidateUserPlayCountAndSimilarityScoresForGivenSong() throws IOException, InterruptedException {
        Song_PlayCountAndSimilaritiesReducer reducer = new Song_PlayCountAndSimilaritiesReducer();
        IntWritable songId = new IntWritable(10);
        Reducer.Context mockContext = mock(Reducer.Context.class);

        ArrayList<SongVector> vectors = new ArrayList<SongVector>();

        ArrayList<UserPlayCountPair> userPlayCounts = new ArrayList<UserPlayCountPair>();
        userPlayCounts.add(new UserPlayCountPair(1, (short)6));
        vectors.add(new UserPlayCounts(userPlayCounts));

        vectors.add(new SongSimilarityScores());


        reducer.reduce(songId, vectors, mockContext);
        
    }

}
