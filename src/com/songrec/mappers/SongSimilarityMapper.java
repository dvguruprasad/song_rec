package com.songrec.mappers;

import com.songrec.Counters;
import com.songrec.dto.PlayCountPairsMap;
import com.songrec.dto.SongPlayCountPair;
import com.songrec.dto.SongPlayCountPairs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SongSimilarityMapper extends Mapper<Text, SongPlayCountPairs, IntWritable, PlayCountPairsMap> {

    private static HashMap<Integer, PlayCountPairsMap> playCountsPerSong = new HashMap<Integer, PlayCountPairsMap>();

    @Override
    protected void map(Text userId, SongPlayCountPairs songPlayCountpairs, Context context) throws IOException, InterruptedException {
        long then = System.nanoTime();
        long size = songPlayCountpairs.size();
        for (int i = 0; i < size; i++) {
            SongPlayCountPair firstSong = songPlayCountpairs.get(i);
            PlayCountPairsMap mappings = new PlayCountPairsMap();
            for (int j = i + 1; j < size; j++) {
                SongPlayCountPair secondSong = songPlayCountpairs.get(j);
                mappings.add(firstSong, secondSong);
            }

            if (mappings.size() > 0) {
                System.out.println("Number of songs related to " + firstSong.songId() + " =>" + mappings.size());
                context.write(new IntWritable(firstSong.songId()), mappings);
            }
        }
        context.getCounter(Counters.TIME_SPENT_IN_COMPUTAION_OF_SIMILARITIES).increment(System.nanoTime() - then);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, PlayCountPairsMap> entry : playCountsPerSong.entrySet()) {
            System.out.println("Number of songs related to " + entry.getKey() + " =>" + entry.getValue().size());
            context.write(new IntWritable(entry.getKey()), entry.getValue());
        }
    }

    private void addOrUpdate(SongPlayCountPair firstSong, SongPlayCountPair secondSong) {
        if (playCountsPerSong.containsKey(firstSong.songId())) {
            PlayCountPairsMap playCountPairsMap = playCountsPerSong.get(firstSong.songId());
            if (playCountPairsMap.size() < 10) {
                playCountPairsMap.add(firstSong, secondSong);
            }
        } else {
            PlayCountPairsMap vector = new PlayCountPairsMap();
            vector.add(firstSong, secondSong);
            playCountsPerSong.put(firstSong.songId(), vector);
        }
    }
}