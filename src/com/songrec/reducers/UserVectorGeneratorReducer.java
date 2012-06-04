package com.songrec.reducers;

import com.songrec.dto.SongPlayCountPair;
import com.songrec.dto.SongPlayCountPairs;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserVectorGeneratorReducer extends Reducer<Text, SongPlayCountPair, Text, SongPlayCountPairs> {
    @Override
    protected void reduce(Text userId, Iterable<SongPlayCountPair> songPlayCountPairs, Context context) throws IOException, InterruptedException {
        context.write(userId, new SongPlayCountPairs(songPlayCountPairs));
    }
}
