package com.songrec.mappers;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.songrec.Counters;
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

        long hashValue = hash(songId, Hashing.sha1()) + hash(songId, Hashing.goodFastHash(64));
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

    private long hash(String songId, HashFunction hashFunction) {
        return 0x7FFFFFFF & hashFunction.newHasher().putString(songId).hash().asLong();
    }

    private String sha1MinHash(ArrayList<String> songIds) {
        return String.valueOf(minHash(songIds, Hashing.sha1()));
    }

    private String md5MinHash(ArrayList<String> songIds) {
        return String.valueOf(minHash(songIds, Hashing.md5()));
    }

    private String goodFastMinHash(ArrayList<String> songIds) {
        return String.valueOf(minHash(songIds, Hashing.goodFastHash(64)));
    }

    private long minHash(ArrayList<String> songIds, HashFunction hashFunction) {
        long minHash = Long.MAX_VALUE;
        for (String songId : songIds) {
            long hash = Math.abs(hashFunction.newHasher().putString(songId).hash().asLong());
            if (hash < minHash)
                minHash = hash;
        }
        return minHash;
    }
}