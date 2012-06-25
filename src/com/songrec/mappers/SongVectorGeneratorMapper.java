package com.songrec.mappers;

import com.songrec.dto.UserPlayCountPair;
import com.songrec.utils.HashingX;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SongVectorGeneratorMapper extends Mapper<LongWritable, Text, IntWritable, UserPlayCountPair> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");
        short playCount = Short.valueOf(tokens[2]);

        if (playCount < 3)
            return;

        int userId = HashingX.hash(tokens[0]);
        int itemId = HashingX.hash(tokens[1]);
        context.write(new IntWritable(itemId), new UserPlayCountPair(userId, playCount));
    }
}
