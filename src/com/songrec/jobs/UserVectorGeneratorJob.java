package com.songrec.jobs;

import com.songrec.SongPlayCountPair;
import com.songrec.SongPlayCountPairs;
import com.songrec.mappers.UserVectorGeneratorMapper;
import com.songrec.reducers.UserVectorGeneratorReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class UserVectorGeneratorJob extends AbstactJob {
    private String inputPath;
    private String outputPath;

    public UserVectorGeneratorJob(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "UserVectorGeneratorJob");
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SongPlayCountPair.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SongPlayCountPairs.class);

        job.setMapperClass(UserVectorGeneratorMapper.class);
        job.setReducerClass(UserVectorGeneratorReducer.class);

        job.setJarByClass(UserVectorGeneratorJob.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new UserVectorGeneratorJob(args[0], args[1]), args);
        System.exit(res);
    }
}