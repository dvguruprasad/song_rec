package com.songrec.jobs;

import com.songrec.dto.SongVectorOrSimilarityScores;
import com.songrec.dto.UserPlayCount;
import com.songrec.mappers.SongVectorGeneratorMapper;
import com.songrec.reducers.SongVectorGeneratorReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SongVectorGeneratorJob extends AbstactJob {
    public SongVectorGeneratorJob(String inputPath, String outputPath) {
        super(outputPath, inputPath);
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setMapperClass(SongVectorGeneratorMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(UserPlayCount.class);

        job.setReducerClass(SongVectorGeneratorReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(SongVectorOrSimilarityScores.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new SongVectorGeneratorJob(args[0], args[1]), args);
        System.exit(res);
    }
}
