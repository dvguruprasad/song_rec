package com.songrec.jobs;

import com.songrec.mappers.ClusterUserMapper;
import com.songrec.reducers.ClusterUserReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class ClusterUserJob extends AbstactJob {
    private String inputPath;
    private String outputPath;

    public ClusterUserJob(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "UserVectorGeneratorJob");
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(ClusterUserMapper.class);
        job.setReducerClass(ClusterUserReducer.class);

        job.setJarByClass(ClusterUserJob.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new ClusterUserJob(args[0], args[1]), args);
        System.exit(res);
    }
}
