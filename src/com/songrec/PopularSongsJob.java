package com.songrec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


import java.io.IOException;

public class PopularSongsJob {
    public static void main(String[] args) throws IOException {
        JobConf job = new JobConf(new Configuration(), PopularSongsJob.class);
        job.setJobName("PopularSongsJob");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(PopularSongsMapper.class);
        job.setCombinerClass(PopularSongsReducer.class);
        job.setReducerClass(PopularSongsReducer.class);

        JobClient.runJob(job);
    }
}
