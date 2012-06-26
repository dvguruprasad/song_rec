package com.songrec.mappers;

import com.songrec.dto.SongPlayCountPair;
import com.songrec.utils.HashingX;
import com.songrec.workflows.Thresholds;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserVectorGeneratorMapper extends Mapper<LongWritable, Text, Text, SongPlayCountPair> {
    @Override
    protected void map(LongWritable key, Text triplet, Context context) throws IOException, InterruptedException {
        String[] tokens = triplet.toString().split("\t");
        String userId = tokens[0];
        Short playCount = Short.valueOf(tokens[2]);
        if (playCount < Thresholds.MINIMUM_PLAYCOUNT)
            return;
        int songId = HashingX.hash(tokens[1]);
        context.write(new Text(userId), new SongPlayCountPair(songId, playCount));
    }
}