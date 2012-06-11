package com.songrec.jobs;


import com.songrec.dto.PlayCountPair;
import com.songrec.dto.SongPair;
import com.songrec.mappers.SongSimilarityMapper;
import com.songrec.reducers.SongSimilarityReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SongSimilarityGeneratorJob extends AbstactJob {
    public SongSimilarityGeneratorJob(String inputPath, String outputPath) {
        super(inputPath, outputPath);
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(SongSimilarityMapper.class);
        job.setMapOutputKeyClass(SongPair.class);
        job.setMapOutputValueClass(PlayCountPair.class);

        job.setReducerClass(SongSimilarityReducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(10);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new SongSimilarityGeneratorJob(args[0], args[1]), args);
        System.exit(res);
    }
}
