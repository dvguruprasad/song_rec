package com.songrec.reducers;

import com.songrec.dto.SongVector;
import com.songrec.dto.UserPlayCounts;
import com.songrec.dto.UserPlayCountPair;
import com.songrec.utils.BoundedPriorityQueue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;

public class SongVectorGeneratorReducer extends Reducer<IntWritable, UserPlayCountPair, IntWritable, SongVector> {
    @Override
    protected void reduce(IntWritable itemId, Iterable<UserPlayCountPair> pairs, Context context) throws IOException, InterruptedException {
        BoundedPriorityQueue<UserPlayCountPair> queue = new BoundedPriorityQueue<UserPlayCountPair>(70, userPlayCountComparator());
        for (UserPlayCountPair pair : pairs) {
            queue.add(new UserPlayCountPair(pair.userId(), pair.playCount()));
        }

        UserPlayCounts vector = new UserPlayCounts(queue.retrieve());
        if (vector.size() > 2) {
            System.out.println(vector);
            context.write(itemId, vector);
        }
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
