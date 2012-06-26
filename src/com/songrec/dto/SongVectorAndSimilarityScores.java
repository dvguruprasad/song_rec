package com.songrec.dto;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SongVectorAndSimilarityScores implements Writable {
    private SongSimilarityScores similarityScores;
    private UserPlayCounts userPlayCounts;

    public SongVectorAndSimilarityScores() {
    }

    public SongVectorAndSimilarityScores(SongSimilarityScores similarityScores) {
        this.similarityScores = similarityScores;
    }

    public SongVectorAndSimilarityScores(UserPlayCounts userPlayCounts) {
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
        if (in.readBoolean()) {
            this.similarityScores = new SongSimilarityScores();
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
            result += SongSimilarityScores.class.getSimpleName() + " ";
        if (null != userPlayCounts && !userPlayCounts.isEmpty())
            result += UserPlayCounts.class.getSimpleName();
        result += "]";
        return result;
    }

    public boolean hasSimilarityScores() {
        return null != similarityScores;
    }

    public SongSimilarityScores getSimilarityScores() {
        return similarityScores;
    }

    public void setSimilarityScores(SongSimilarityScores similarityScores) {
        this.similarityScores = similarityScores;
    }

    public UserPlayCounts getPlayCounts() {
        return userPlayCounts;
    }

    public void setUserPlayCounts(UserPlayCounts userPlayCounts) {
        this.userPlayCounts = userPlayCounts;
    }
    
    
}
