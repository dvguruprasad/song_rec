package com.songrec.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SongIdHashReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable hashedSongId, Iterable<Text> songIds, Context context) throws IOException, InterruptedException {
        context.write(hashedSongId, songIds.iterator().next());
    }
}
