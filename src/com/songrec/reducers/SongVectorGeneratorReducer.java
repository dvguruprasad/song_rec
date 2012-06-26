package com.songrec.reducers;

import com.songrec.dto.SongVectorAndSimilarityScores;
import com.songrec.dto.UserPlayCountPair;
import com.songrec.dto.UserPlayCounts;
import com.songrec.utils.BoundedPriorityQueue;
import com.songrec.workflows.Thresholds;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;

public class SongVectorGeneratorReducer extends Reducer<IntWritable, UserPlayCountPair, IntWritable, SongVectorAndSimilarityScores> {
    @Override
    protected void reduce(IntWritable songId, Iterable<UserPlayCountPair> pairs, Context context) throws IOException, InterruptedException {
        BoundedPriorityQueue<UserPlayCountPair> queue = new BoundedPriorityQueue<UserPlayCountPair>(Thresholds.MAXIMUM_NUMBER_OF_USERS, userPlayCountComparator());
        for (UserPlayCountPair pair : pairs) {
            queue.add(new UserPlayCountPair(pair.userId(), pair.playCount()));
        }

        UserPlayCounts userPlayCounts = new UserPlayCounts(queue.retrieve());
        if (userPlayCounts.size() < Thresholds.MINIMUM_NUMBER_OF_USERS)
            return;
        context.write(songId, new SongVectorAndSimilarityScores(userPlayCounts));
    }

    private Comparator<UserPlayCountPair> userPlayCountComparator() {
        return new Comparator<UserPlayCountPair>() {
            @Override
            public int compare(UserPlayCountPair one, UserPlayCountPair another) {
                return one.playCount() > another.playCount() ? 1 :
                        (one.playCount() < another.playCount() ? -1 : 0);
            }
        };
    }
}
