package com.songrec.mappers;

import com.songrec.Counters;
import com.songrec.dto.PlayCountPairsMap;
import com.songrec.dto.SongPlayCount;
import com.songrec.dto.SongPlayCounts;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SongSimilarityGeneratorMapper extends Mapper<Text, SongPlayCounts, IntWritable, PlayCountPairsMap> {
    @Override
    protected void map(Text userId, SongPlayCounts songPlayCountpairs, Context context) throws IOException, InterruptedException {
        long then = System.nanoTime();
        long size = songPlayCountpairs.size();
        for (int i = 0; i < size; i++) {
            SongPlayCount firstSong = songPlayCountpairs.get(i);
            PlayCountPairsMap mappings = new PlayCountPairsMap();
            for (int j = i + 1; j < size; j++) {
                SongPlayCount secondSong = songPlayCountpairs.get(j);
                mappings.addOrUpdate(firstSong, secondSong);
            }
            context.write(new IntWritable(firstSong.songId()), mappings);
        }
        context.getCounter(Counters.TIME_SPENT_IN_COMPUTAION_OF_SIMILARITIES).increment(System.nanoTime() - then);
    }
}