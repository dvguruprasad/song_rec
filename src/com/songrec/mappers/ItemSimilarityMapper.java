package com.songrec.mappers;

import com.songrec.Counters;
import com.songrec.dto.PlayCountPair;
import com.songrec.dto.SongPair;
import com.songrec.dto.SongPlayCountPair;
import com.songrec.dto.SongPlayCountPairs;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ItemSimilarityMapper extends Mapper<Text, SongPlayCountPairs, SongPair, PlayCountPair> {
    @Override
    protected void map(Text userId, SongPlayCountPairs songPlayCountpairs, Context context) throws IOException, InterruptedException {
        long then = System.nanoTime();
        long size = songPlayCountpairs.size();
        for(int i = 0; i < size; i++){
            for(int j = i+1; j< size; j++){
                SongPlayCountPair firstSong = songPlayCountpairs.get(i);
                SongPlayCountPair secondSong = songPlayCountpairs.get(j);
                SongPair key = new SongPair(firstSong.songId(), secondSong.songId());
                PlayCountPair value = new PlayCountPair(firstSong.playCount(), secondSong.playCount());
                context.write(key, value);
            }
        }
        context.getCounter(Counters.TIME_SPENT_IN_COMPUTAION_OF_SIMILARITIES_MAPPER).increment(System.nanoTime() - then);
    }
}
