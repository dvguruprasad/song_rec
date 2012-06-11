package com.songrec.jobs;

import com.songrec.mappers.SortMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SortJob extends AbstactJob {
    public SortJob(String inputPath, String outputPath) {
        super(inputPath, outputPath);
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(Reducer.class);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new SortJob(args[0], args[1]), args);
        System.exit(res);
    }
}
