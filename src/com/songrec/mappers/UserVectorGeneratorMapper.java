package com.songrec.mappers;

import com.google.common.hash.Hashing;
import com.songrec.dto.SongPlayCountPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserVectorGeneratorMapper extends Mapper<LongWritable, Text, Text, SongPlayCountPair> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        String userId = tokens[0];
        int songId = Hashing.sha1().newHasher().putString(tokens[1]).hash().asInt();
        String playCount = tokens[2];
        context.write(new Text(userId), new SongPlayCountPair(songId, Short.valueOf(playCount)));
    }
}
