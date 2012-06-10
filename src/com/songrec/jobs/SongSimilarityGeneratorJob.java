package com.songrec.jobs;


import com.songrec.dto.PlayCountPair;
import com.songrec.dto.SongPair;
import com.songrec.mappers.SongSimilarityMapper;
import com.songrec.reducers.SongSimilarityReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class SongSimilarityGeneratorJob extends AbstactJob {
    private String inputPath;
    private String outputPath;

    public SongSimilarityGeneratorJob(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPathForJob(SongSimilarityGeneratorJob.class.getSimpleName(), outputPath);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = new Job(getConf(), "ItemSimilarityGenerator");

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(SongSimilarityMapper.class);
        job.setMapOutputKeyClass(SongPair.class);
        job.setMapOutputValueClass(PlayCountPair.class);

        job.setReducerClass(SongSimilarityReducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(10);
        job.setJarByClass(SongSimilarityGeneratorJob.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new SongSimilarityGeneratorJob(args[0], args[1]), args);
        System.exit(res);
    }
}
