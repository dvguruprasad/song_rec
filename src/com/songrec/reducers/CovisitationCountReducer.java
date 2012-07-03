package com.songrec.reducers;

import com.songrec.dto.SongPair;
import com.songrec.jobs.SongIdHashJob;
import com.songrec.utils.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

public class CovisitationCountReducer extends Reducer<SongPair, IntWritable, SongPair, IntWritable> {
    private static Map<Integer,String> itemIdToHashMap;

    protected void setup(Context context) throws IOException, InterruptedException {
        itemIdToHashMap = FileUtils.songIdHashMap(new Path(context.getConfiguration().get(SongIdHashJob.SONG_ID_HASH_PATH)), context.getConfiguration());
    }

    @Override
    protected void reduce(SongPair songPair, Iterable<IntWritable> covisitedCounts, Context context) throws IOException, InterruptedException {
        int result = 0;
        for(IntWritable count : covisitedCounts){
            result += count.get();
        }
        context.write(songPair, new IntWritable(result));
    }
}
