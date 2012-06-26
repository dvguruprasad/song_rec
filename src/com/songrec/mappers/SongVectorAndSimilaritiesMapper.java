package com.songrec.mappers;

import com.songrec.dto.SongVectorAndSimilarityScores;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SongVectorAndSimilaritiesMapper extends Mapper<IntWritable, SongVectorAndSimilarityScores, IntWritable, SongVectorAndSimilarityScores> {
    @Override
    protected void map(IntWritable key, SongVectorAndSimilarityScores value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
