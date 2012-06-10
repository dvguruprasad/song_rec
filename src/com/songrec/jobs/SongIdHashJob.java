package com.songrec.jobs;

import com.songrec.mappers.SongIdHashMapper;
import com.songrec.reducers.SongIdHashReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class SongIdHashJob extends AbstactJob {
    private String inputPath;
    private String outputPath;

    public SongIdHashJob(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPathForJob(SongIdHashJob.class.getSimpleName(), outputPath);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "SongIdHashJob");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(SongIdHashJob.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(SongIdHashMapper.class);
        job.setReducerClass(SongIdHashReducer.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new SongIdHashJob(args[0], args[1]), args);
        System.exit(res);
    }
}
