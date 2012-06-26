package com.songrec.mappers;

import com.songrec.Counters;
import com.songrec.dto.PlayCountPairsMap;
import com.songrec.dto.SongPlayCountPair;
import com.songrec.dto.SongPlayCountPairs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SongSimilarityGeneratorMapper extends Mapper<Text, SongPlayCountPairs, IntWritable, PlayCountPairsMap> {
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
                context.write(new IntWritable(firstSong.songId()), mappings);
            }
        }
        context.getCounter(Counters.TIME_SPENT_IN_COMPUTAION_OF_SIMILARITIES).increment(System.nanoTime() - then);
    }
}