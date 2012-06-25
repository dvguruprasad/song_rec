package com.songrec.jobs;

import com.songrec.mappers.CSVConverterMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class CSVConverterJob extends AbstactJob {
    public CSVConverterJob(String inputPath, String outputPath) {
        super(outputPath, inputPath);
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(CSVConverterMapper.class);
        job.setReducerClass(Reducer.class);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new CSVConverterJob(args[0], args[1]), args);
        System.exit(res);
    }
}
