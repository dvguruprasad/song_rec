package com.songrec.dto;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SongVectorAndSimilarityScores implements Writable {
    private SimilarityScores similarityScores;
    private UserPlayCounts userPlayCounts;

    public SongVectorAndSimilarityScores() {
    }

    public SongVectorAndSimilarityScores(UserPlayCounts userPlayCounts, SimilarityScores similarityScores) {
        this.userPlayCounts = userPlayCounts;
        this.similarityScores = similarityScores;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        similarityScores.write(out);
        userPlayCounts.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.similarityScores = new SimilarityScores();
        this.similarityScores.readFields(in);
        this.userPlayCounts = new UserPlayCounts();
        this.userPlayCounts.readFields(in);
    }

    @Override
    public String toString() {
        String result = "[";
        if ((null != similarityScores) && !similarityScores.isEmpty())
            result += SimilarityScores.class.getSimpleName() + similarityScores.hashCode() + " ";
        if (null != userPlayCounts && !userPlayCounts.isEmpty())
            result += UserPlayCounts.class.getSimpleName() + userPlayCounts.hashCode();
        result += "]";
        return result;
    }

    public SimilarityScores getSimilarityScores() {
        return similarityScores;
    }

    public void setSimilarityScores(SimilarityScores similarityScores) {
        this.similarityScores = similarityScores;
    }

    public UserPlayCounts getPlayCounts() {
        return userPlayCounts;
    }

    public void setUserPlayCounts(UserPlayCounts userPlayCounts) {
        this.userPlayCounts = userPlayCounts;
    }

    public boolean hasBoth() {
        return null != similarityScores && null != userPlayCounts;
    }
}
