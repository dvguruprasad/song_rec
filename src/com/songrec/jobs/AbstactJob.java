package com.songrec.jobs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public abstract class AbstactJob extends Configured implements Tool {
    protected String[] inputPaths;
    private String outputPath;

    public AbstactJob(String outputPath, String... inputPaths) {
        this.inputPaths = inputPaths;
        this.outputPath = outputPathForJob(name(), outputPath);
    }

    public String outputPath() {
        return outputPath;
    }

    public String name() {
        return getClass().getSimpleName();
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJob();
        prepare(job);
        job.setJarByClass(getClass());
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private Job getJob() throws IOException {
        Job job = new Job(getConf(), name());
        FileInputFormat.setInputPaths(job, toPaths(inputPaths));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }

    private Path[] toPaths(String[] inputPaths) {
        Path[] paths = new Path[inputPaths.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = new Path(inputPaths[i]);
        }
        return paths;
    }

    private static String outputPathForJob(String jobName, String outputPath) {
        return outputPath + "/" + jobName;
    }

    public abstract void prepare(Job job) throws IOException;
}
