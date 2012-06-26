package com.songrec.dto;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SongVectorOrSimilarityScores implements Writable {
    private SimilarityScores similarityScores;
    private UserPlayCounts userPlayCounts;

    public SongVectorOrSimilarityScores() {
    }

    public SongVectorOrSimilarityScores(SimilarityScores similarityScores) {
        this.similarityScores = similarityScores;
    }

    public SongVectorOrSimilarityScores(UserPlayCounts userPlayCounts) {
        this.userPlayCounts = userPlayCounts;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (null != similarityScores) {
            out.writeBoolean(true);
            similarityScores.write(out);
        } else {
            out.writeBoolean(false);
            userPlayCounts.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.similarityScores = null;
        this.userPlayCounts = null;
        if (in.readBoolean()) {
            this.similarityScores = new SimilarityScores();
            this.similarityScores.readFields(in);
        } else {
            this.userPlayCounts = new UserPlayCounts();
            this.userPlayCounts.readFields(in);
        }
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

    public boolean hasSimilarityScores() {
        return null != similarityScores;
    }

    public SimilarityScores getSimilarityScores() {
        return similarityScores;
    }

    public boolean hasBoth() {
        return null != similarityScores && null != userPlayCounts;
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
}
