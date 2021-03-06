package com.songrec.mappers;

import com.songrec.dto.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

public class RecommenderMapper extends Mapper<IntWritable, SongVectorAndSimilarityScores, IntWritable, UserVectorAndSimilarityScores> {
    @Override
    protected void map(IntWritable songId, SongVectorAndSimilarityScores songVectorAndSimilarityScores, Context context) throws IOException, InterruptedException {
        UserPlayCounts playCounts = songVectorAndSimilarityScores.getPlayCounts();
        for(Map.Entry<Integer, Short> playCount : playCounts.entrySet()){
            SongPlayCount songPlayCount = new SongPlayCount(songId.get(), playCount.getValue());
            UserVectorAndSimilarityScores userVectorAndSimilarityScores = new UserVectorAndSimilarityScores(songPlayCount, songVectorAndSimilarityScores.getSimilarityScores());
            Integer userId = playCount.getKey();
            context.write(new IntWritable(userId), userVectorAndSimilarityScores);
        }
    }
}
