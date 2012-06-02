package com.songrec.jobs;


import com.songrec.PlayCountPair;
import com.songrec.SongPair;
import com.songrec.mappers.ItemSimilarityMapper;
import com.songrec.reducers.ItemSimilarityReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class ItemSimilarityGeneratorJob extends AbstactJob {
    private String inputPath;
    private String outputPath;

    public ItemSimilarityGeneratorJob(String inputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath(ItemSimilarityGeneratorJob.class.getSimpleName());
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = new Job(getConf(), "ItemSimilarityGenerator");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(ItemSimilarityMapper.class);
        job.setMapOutputKeyClass(SongPair.class);
        job.setMapOutputValueClass(PlayCountPair.class);

        job.setReducerClass(ItemSimilarityReducer.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setJarByClass(ItemSimilarityGeneratorJob.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new ItemSimilarityGeneratorJob(args[0]), args);
        System.exit(res);
    }
}
