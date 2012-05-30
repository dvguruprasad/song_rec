package com.songrec.mappers;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class ClusterUserMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text userId, Text songPlayCounts, Context context) throws IOException, InterruptedException {
        String[] songs = songPlayCounts.toString().split(";");
        ArrayList<String> songIds = new ArrayList<String>(); 
        for(String song : songs){
            songIds.add(song.split(",")[0]);
        }
        String clusterId = sha1MinHash(songIds) + "_" + md5MinHash(songIds) + "_" + goodFastMinHash(songIds);
        context.write(new Text(clusterId), new Text(userId));
    }

    private String sha1MinHash(ArrayList<String> songIds){
        return String.valueOf(minHash(songIds, Hashing.sha1()));
    }

    private String md5MinHash(ArrayList<String> songIds){
        return String.valueOf(minHash(songIds, Hashing.md5()));
    }

    private String goodFastMinHash(ArrayList<String> songIds){
        return String.valueOf(minHash(songIds, Hashing.goodFastHash(64)));
    }

    private long minHash(ArrayList<String> songIds, HashFunction hashFunction) {
        long minHash = Long.MAX_VALUE;
        for(String songId : songIds){
            long hash = Math.abs(hashFunction.newHasher().putString(songId).hash().asLong());
            if(hash < minHash)
                minHash = hash;
        }
        return minHash;
    }
}