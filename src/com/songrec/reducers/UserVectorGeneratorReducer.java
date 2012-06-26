package com.songrec.reducers;

import com.songrec.dto.SongPlayCountPair;
import com.songrec.dto.SongPlayCountPairs;
import com.songrec.utils.BoundedPriorityQueue;
import com.songrec.workflows.Thresholds;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;

public class UserVectorGeneratorReducer extends Reducer<Text, SongPlayCountPair, Text, SongPlayCountPairs> {
    @Override
    protected void reduce(Text userId, Iterable<SongPlayCountPair> songPlayCountPairs, Context context) throws IOException, InterruptedException {
        BoundedPriorityQueue<SongPlayCountPair> queue = new BoundedPriorityQueue<SongPlayCountPair>(Thresholds.MAXIMUM_NUMBER_OF_SONGS, songPlayCountComparator());
        for(SongPlayCountPair pair : songPlayCountPairs){
            queue.add(new SongPlayCountPair(pair.songId(),  pair.playCount()));
        }
        if(queue.size() < Thresholds.MINIMUM_NUMBER_OF_SONGS)
            return;
        context.write(userId, new SongPlayCountPairs(queue.retrieve()));
    }

    private Comparator<SongPlayCountPair> songPlayCountComparator() {
        return new Comparator<SongPlayCountPair>() {
            @Override
            public int compare(SongPlayCountPair one, SongPlayCountPair another) {
                return one.playCount() > another.playCount() ? 1 : 
                        (one.playCount() < another.playCount() ? -1 : 0);
            }
        };
    }
}
