package com.songrec.jobs;

import com.songrec.mappers.PopularSongsMapper;
import com.songrec.reducers.PopularSongsReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class PopularSongsJob extends AbstactJob {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PopularSongsJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "PopularSongsJob");

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setJarByClass(PopularSongsJob.class);

        job.setMapperClass(PopularSongsMapper.class);
        job.setCombinerClass(PopularSongsReducer.class);
        job.setReducerClass(PopularSongsReducer.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
