package com.songrec.jobs;

import com.songrec.dto.SongPlayCountPair;
import com.songrec.dto.SongPlayCountPairs;
import com.songrec.mappers.UserVectorGeneratorMapper;
import com.songrec.reducers.UserVectorGeneratorReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class UserVectorGeneratorJob extends AbstactJob {
    public UserVectorGeneratorJob(String inputPath, String outputPath) {
        super(inputPath, outputPath);
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setMapperClass(UserVectorGeneratorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SongPlayCountPair.class);

        job.setReducerClass(UserVectorGeneratorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SongPlayCountPairs.class);

        job.setNumReduceTasks(10);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new UserVectorGeneratorJob(args[0], args[1]), args);
        System.exit(res);
    }
}