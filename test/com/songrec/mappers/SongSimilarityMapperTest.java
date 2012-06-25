package com.songrec.mappers;

import com.songrec.Counters;
import com.songrec.dto.PlayCountPair;
import com.songrec.dto.PlayCountPairsMap;
import com.songrec.dto.SongPlayCountPair;
import com.songrec.dto.SongPlayCountPairs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.*;

public class SongSimilarityMapperTest {
    @Test
    public void generatesASongVectorForEverySongUserHasListenedTo() throws IOException, InterruptedException {
        SongSimilarityMapper mapper = new SongSimilarityMapper();
        Mapper.Context mockContext = mock(Mapper.Context.class);

        ArrayList<SongPlayCountPair> list = new ArrayList<SongPlayCountPair>();
        list.add(songPlayCountPair(11, 4));
        list.add(songPlayCountPair(33, 8));
        list.add(songPlayCountPair(66, 7));
        list.add(songPlayCountPair(99, 2));
        SongPlayCountPairs songPlayCountPairs = new SongPlayCountPairs(list);
        when(mockContext.getCounter(Counters.TIME_SPENT_IN_COMPUTAION_OF_SIMILARITIES)).thenReturn(mock(Counter.class));

        mapper.map(new Text("userId"), songPlayCountPairs, mockContext);

        HashMap<Integer, List<PlayCountPair>> map_11 = new HashMap<Integer, List<PlayCountPair>>();
        map_11.put(33, Arrays.asList(playCountPair(4, 8)));
        map_11.put(66, Arrays.asList(playCountPair(4, 7)));
        map_11.put(99, Arrays.asList(playCountPair(4, 2)));
        PlayCountPairsMap playCountVector_11 = new PlayCountPairsMap(map_11);

        HashMap<Integer, List<PlayCountPair>> map_33 = new HashMap<Integer, List<PlayCountPair>>();
        map_33.put(66, Arrays.asList(playCountPair(8, 7)));
        map_33.put(99, Arrays.asList(playCountPair(8, 2)));
        PlayCountPairsMap playCountVector_33 = new PlayCountPairsMap(map_33);

        HashMap<Integer, List<PlayCountPair>> map_66 = new HashMap<Integer, List<PlayCountPair>>();
        map_66.put(99, Arrays.asList(playCountPair(7, 2)));
        PlayCountPairsMap playCountVector_66 = new PlayCountPairsMap(map_66);

        verify(mockContext).write(new IntWritable(33), playCountVector_33);
        verify(mockContext).write(new IntWritable(66), playCountVector_66);
        verify(mockContext).write(new IntWritable(11), playCountVector_11);
    }

    private PlayCountPair playCountPair(int firstPlayCount, int secondPlayCount) {
        return new PlayCountPair((short) firstPlayCount, (short) secondPlayCount);
    }

    private SongPlayCountPair songPlayCountPair(int songId, int playCountPair) {
        return new SongPlayCountPair(songId, (short) playCountPair);
    }
}
