package com.songrec.mappers;

import com.songrec.dto.SongPair;
import com.songrec.dto.SongPlayCountPairs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CovisitationCountMapper extends Mapper<Text, SongPlayCountPairs, SongPair, IntWritable> {
    @Override
    protected void map(Text userId, SongPlayCountPairs songPlayCountPairs, Context context) throws IOException, InterruptedException {
        long size = songPlayCountPairs.size();
        for (int i = 0; i < size; i++) {
            for (int j = i + 1; j < size; j++) {
                context.write(new SongPair(songPlayCountPairs.get(i).songId(), songPlayCountPairs.get(j).songId()), new IntWritable(1));
            }
        }
    }
}
