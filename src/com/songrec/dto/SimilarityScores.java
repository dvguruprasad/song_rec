package com.songrec.dto;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SimilarityScores implements Writable, Iterable<SimilarityScore> {
    List<SimilarityScore> list;

    public SimilarityScores() {
        list = new ArrayList<SimilarityScore>();
    }

    public SimilarityScores(List<SimilarityScore> list) {
        this.list = list;
    }

    public SimilarityScores(SimilarityScores similarityScores) {
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

    @Override
    public Iterator<SimilarityScore> iterator() {
        return list.iterator();
    }
}
