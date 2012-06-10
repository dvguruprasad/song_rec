package com.songrec.mappers;

import com.songrec.utils.HashingX;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterIdGeneratorMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static HashMap<String, List<Long>> hashesPerUser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        hashesPerUser = new HashMap<String, List<Long>>();
    }

    @Override
    protected void map(LongWritable key, Text triplet, Context context) throws IOException, InterruptedException {
        String[] tokens = triplet.toString().split("\t");
        String userId = tokens[0];
        String songId = tokens[1];

        long hashValue = HashingX.hash_sha1(songId) + HashingX.hash_goodFastHash(songId);
        addOrUpdate(userId, hashValue);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, List<Long>> entry : hashesPerUser.entrySet()) {
            String userId = entry.getKey();
            long minimumHash = minimum(entry.getValue());
            context.write(new Text(userId), new LongWritable(minimumHash));
        }
    }

    private void addOrUpdate(String userId, long hashValue) {
        if(hashesPerUser.containsKey(userId)){
            hashesPerUser.get(userId).add(hashValue);
        }
        else {
            ArrayList<Long> hashes = new ArrayList<Long>();
            hashes.add(hashValue);
            ClusterIdGeneratorMapper.hashesPerUser.put(userId, hashes);
        }
    }

    private long minimum(List<Long> hashes) {
        long min = Long.MAX_VALUE;
        for(long hash : hashes)
            if(hash < min){
                min = hash;
            }
        return min;
    }
}