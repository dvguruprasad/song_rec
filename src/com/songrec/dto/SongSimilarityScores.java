package com.songrec.dto;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SongSimilarityScores implements Writable {
    List<SimilarityScore> list;

    public SongSimilarityScores() {
        list = new ArrayList<SimilarityScore>();
    }

    public SongSimilarityScores(List<SimilarityScore> list) {
        this.list = list;
    }

    public SongSimilarityScores(SongSimilarityScores similarityScores) {
        this.list = similarityScores.list;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(list.size());
        for (SimilarityScore similarityScore : list) {
            out.writeInt(similarityScore.songId());
            out.writeDouble(similarityScore.score());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        while (size > 0) {
            list.add(new SimilarityScore(in.readInt(),  in.readDouble()));
            size--;
        }
    }

    public void add(SimilarityScore score) {
        list.add(score);
    }

    @Override
    public String toString() {
        String result = "";
        for (SimilarityScore similarityScore : list) {
            result += "(" + similarityScore.songId() + ":" + similarityScore.score() + "),";
        }
        return result;
    }

    public boolean isEmpty() {
        return list.isEmpty();
    }
}
