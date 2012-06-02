package com.songrec.mappers;

import com.songrec.PlayCountPair;
import com.songrec.SongPair;
import com.songrec.SongPlayCountPair;
import com.songrec.SongPlayCountPairs;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ItemSimilarityMapper extends Mapper<Text, SongPlayCountPairs, SongPair, PlayCountPair> {
    @Override
    protected void map(Text userId, SongPlayCountPairs songPlayCountpairs, Context context) throws IOException, InterruptedException {
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
    }
}
