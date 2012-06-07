package com.songrec.reducers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ClusterIdGeneratorReducer extends Reducer<Text, LongWritable, LongWritable, Text> {
    @Override
    protected void reduce(Text userId, Iterable<LongWritable> hashValues, Context context) throws IOException, InterruptedException {
        long min = Long.MAX_VALUE;
        for (LongWritable hashValue : hashValues){
            if(hashValue.get() < min)
                min = hashValue.get();
        }

        context.write(new LongWritable(min), new Text(userId));
    }
}
