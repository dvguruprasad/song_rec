package com.songrec.reducers;

import com.songrec.SongPlayCountPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserVectorGeneratorReducer extends Reducer<Text, SongPlayCountPair, Text, Text> {
    @Override
    protected void reduce(Text userId, Iterable<SongPlayCountPair> songPlayCountPairs, Context context) throws IOException, InterruptedException {
        String songPlayCountText = "";
        for(SongPlayCountPair pair : songPlayCountPairs)
            songPlayCountText += pair.songId() + "," + pair.playCount() + ";";
        context.write(userId, new Text(songPlayCountText));
    }
}
