package com.songrec.jobs;

import com.songrec.mappers.SortMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class SortJob extends AbstactJob {
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "SortJob");
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(outputPath("SortJob")));

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(Reducer.class);

        job.setJarByClass(UserVectorGeneratorJob.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new SortJob(), args);
        System.exit(res);
    }
}
