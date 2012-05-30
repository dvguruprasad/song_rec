package com.songrec.jobs;

import com.songrec.mappers.CSVConverterMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class CSVConverterJob extends AbstactJob {
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "CSVConverterJob");

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setJarByClass(CSVConverterJob.class);

        job.setMapperClass(CSVConverterMapper.class);
        job.setReducerClass(Reducer.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new CSVConverterJob(), args);
        System.exit(res);
    }
}
