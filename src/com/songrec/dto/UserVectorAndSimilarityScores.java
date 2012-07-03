package com.songrec.dto;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserVectorAndSimilarityScores implements Writable {
    SongPlayCount song;
    SimilarityScores similarityScores;

    public UserVectorAndSimilarityScores() {
    }

    public UserVectorAndSimilarityScores(SongPlayCount songPlayCount, SimilarityScores similarityScores) {
        this.song = songPlayCount;
        this.similarityScores = similarityScores;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        song.write(out);
        similarityScores.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        song = new SongPlayCount();
        song.readFields(in);
        similarityScores = new SimilarityScores();
        similarityScores.readFields(in);
    }

    public SongPlayCount getSong() {
        return song;
    }

    public SimilarityScores getSimilarityScores() {
        return similarityScores;
    }
}
