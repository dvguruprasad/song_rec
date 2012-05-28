package com.songrec.mappers;

import com.google.common.hash.Hashing;
import com.songrec.SongPlayCountPair;
import com.songrec.SongPlayCountPairs;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ClusterUserMapper extends Mapper<Text, SongPlayCountPairs, LongWritable, Text> {
//    @Override
//    protected void map(LongWritable key, Text triplet, Context context) throws IOException, InterruptedException {
//        String[] tokens = triplet.toString().split("\t");
//        String userId = tokens[0];
//        String songId = tokens[1];
//        context.write(new Text(userId), new LongWritable(hash(songId)));
//    }

    @Override
    protected void map(Text userId, SongPlayCountPairs pairs, Context context) throws IOException, InterruptedException {
        long minHash = Long.MIN_VALUE;
        for(SongPlayCountPair pair : pairs.get()){
            long hash = hash(pair.songId());
            if(hash < minHash)
                minHash = hash;
        }
        context.write(new LongWritable(minHash), new Text(userId));
    }


    private long hash(String value){
        return Hashing.sha1().newHasher().putString(value).hash().asLong();
    }
}
