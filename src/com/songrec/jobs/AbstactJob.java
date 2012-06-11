package com.songrec.jobs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public abstract class AbstactJob extends Configured implements Tool {
    protected String inputPath;
    protected String outputPath;

    public AbstactJob(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPathForJob(name(), outputPath);
    }

    public String outputPath() {
        return outputPath;
    }
    
    public String name(){
        return getClass().getSimpleName();
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJob();
        prepare(job);
        job.setJarByClass(getClass());
        return job.waitForCompletion(true) ? 0 : 1;
    }

    protected Job getJob() throws IOException {
        Job job = new Job(getConf(), name());
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }

    protected static String outputPathForJob(String jobName, String outputPath) {
        return outputPath + "/" + jobName;
    }

    public abstract void prepare(Job job) throws IOException;
}
