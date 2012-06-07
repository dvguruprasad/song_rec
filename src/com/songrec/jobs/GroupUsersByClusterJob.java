package com.songrec.jobs;

import com.songrec.mappers.GroupUsersByClusterMapper;
import com.songrec.reducers.GroupUsersByClusterReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GroupUsersByClusterJob extends AbstactJob{
    private String inputPath;
    private String outputPath;

    public GroupUsersByClusterJob(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPathForJob(GroupUsersByClusterJob.class.getSimpleName(), outputPath);
    }

    @Override
    public int run(String[] strings) throws Exception {

        Job job = new Job(getConf(), "GroupUsersByClusterJob");

        KeyValueTextInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(GroupUsersByClusterMapper.class);
        job.setReducerClass(GroupUsersByClusterReducer.class);

        job.setNumReduceTasks(10);

        job.setJarByClass(GroupUsersByClusterJob.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
