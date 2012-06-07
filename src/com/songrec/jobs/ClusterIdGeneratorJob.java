package com.songrec.jobs;

import com.songrec.mappers.ClusterIdGeneratorMapper;
import com.songrec.reducers.ClusterIdGeneratorReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class ClusterIdGeneratorJob extends AbstactJob {
    private String inputPath;
    private String outputPath;

    public ClusterIdGeneratorJob(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPathForJob(ClusterIdGeneratorJob.class.getSimpleName(), outputPath);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "ClusterIdGeneratorJob");
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ClusterIdGeneratorMapper.class);
        job.setReducerClass(ClusterIdGeneratorReducer.class);

        job.setNumReduceTasks(10);

        job.setJarByClass(ClusterIdGeneratorJob.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new ClusterIdGeneratorJob(args[0], args[1]), args);
        System.exit(res);
    }
}
