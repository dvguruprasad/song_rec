package com.songrec.reducers;

import com.songrec.dto.SongVectorOrSimilarityScores;
import com.songrec.dto.UserPlayCount;
import com.songrec.dto.UserPlayCounts;
import com.songrec.utils.BoundedPriorityQueue;
import com.songrec.workflows.Thresholds;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;

public class SongVectorGeneratorReducer extends Reducer<IntWritable, UserPlayCount, IntWritable, SongVectorOrSimilarityScores> {
    @Override
    protected void reduce(IntWritable songId, Iterable<UserPlayCount> pairs, Context context) throws IOException, InterruptedException {
        BoundedPriorityQueue<UserPlayCount> queue = new BoundedPriorityQueue<UserPlayCount>(Thresholds.MAXIMUM_NUMBER_OF_USERS, userPlayCountComparator());
        for (UserPlayCount pair : pairs) {
            queue.add(new UserPlayCount(pair.userId(), pair.playCount()));
        }

        UserPlayCounts userPlayCounts = new UserPlayCounts(queue.retrieve());
        if (userPlayCounts.size() < Thresholds.MINIMUM_NUMBER_OF_USERS)
            return;
        context.write(songId, new SongVectorOrSimilarityScores(userPlayCounts));
    }

    private Comparator<UserPlayCount> userPlayCountComparator() {
        return new Comparator<UserPlayCount>() {
            @Override
            public int compare(UserPlayCount one, UserPlayCount another) {
                return one.playCount() > another.playCount() ? 1 :
                        (one.playCount() < another.playCount() ? -1 : 0);
            }
        };
    }
}
