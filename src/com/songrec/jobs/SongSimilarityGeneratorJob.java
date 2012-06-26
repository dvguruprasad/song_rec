package com.songrec.jobs;

import com.songrec.dto.PlayCountPairsMap;
import com.songrec.dto.SongVectorOrSimilarityScores;
import com.songrec.mappers.SongSimilarityGeneratorMapper;
import com.songrec.reducers.SongSimilarityGeneratorReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SongSimilarityGeneratorJob extends AbstactJob {

    private String songIdHashPath;

    public SongSimilarityGeneratorJob(String userVectorPath, String songIdHashPath, String outputPath) {
        super(outputPath, userVectorPath);
        this.songIdHashPath = songIdHashPath;
    }

    @Override
    public void prepare(Job job) throws IOException {
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(SongSimilarityGeneratorMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PlayCountPairsMap.class);

        job.setReducerClass(SongSimilarityGeneratorReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(SongVectorOrSimilarityScores.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.getConfiguration().set(SongIdHashJob.SONG_ID_HASH_PATH, songIdHashPath);

        job.setNumReduceTasks(10);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new SongSimilarityGeneratorJob(args[0], args[1], args[2]), args);
        System.exit(res);
    }
}
