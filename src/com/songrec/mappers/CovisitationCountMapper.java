package com.songrec.mappers;

import com.songrec.dto.SongPair;
import com.songrec.dto.SongPlayCounts;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CovisitationCountMapper extends Mapper<Text, SongPlayCounts, SongPair, IntWritable> {
    @Override
    protected void map(Text userId, SongPlayCounts songPlayCounts, Context context) throws IOException, InterruptedException {
        long size = songPlayCounts.size();
        for (int i = 0; i < size; i++) {
            for (int j = i + 1; j < size; j++) {
                context.write(new SongPair(songPlayCounts.get(i).songId(), songPlayCounts.get(j).songId()), new IntWritable(1));
            }
        }
    }
}
