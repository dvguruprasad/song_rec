package com.songrec.reducers;

import com.songrec.dto.SongPlayCount;
import com.songrec.dto.SongPlayCounts;
import com.songrec.utils.BoundedPriorityQueue;
import com.songrec.workflows.Thresholds;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;

public class UserVectorGeneratorReducer extends Reducer<Text, SongPlayCount, Text, SongPlayCounts> {
    @Override
    protected void reduce(Text userId, Iterable<SongPlayCount> songPlayCountPairs, Context context) throws IOException, InterruptedException {
        BoundedPriorityQueue<SongPlayCount> queue = new BoundedPriorityQueue<SongPlayCount>(Thresholds.MAXIMUM_NUMBER_OF_SONGS, songPlayCountComparator());
        for(SongPlayCount pair : songPlayCountPairs){
            queue.add(new SongPlayCount(pair.songId(),  pair.playCount()));
        }
        if(queue.size() < Thresholds.MINIMUM_NUMBER_OF_SONGS)
            return;
        context.write(userId, new SongPlayCounts(queue.retrieve()));
    }

    private Comparator<SongPlayCount> songPlayCountComparator() {
        return new Comparator<SongPlayCount>() {
            @Override
            public int compare(SongPlayCount one, SongPlayCount another) {
                return one.playCount() > another.playCount() ? 1 : 
                        (one.playCount() < another.playCount() ? -1 : 0);
            }
        };
    }
}
