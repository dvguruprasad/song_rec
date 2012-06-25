package com.songrec.dto;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UserPlayCountsOrSimilarityScores implements Writable {
    Map<Integer, Double> similarityScoresMap = new HashMap<Integer, Double>();

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(similarityScoresMap.size());
        for (Map.Entry<Integer, Double> entry : similarityScoresMap.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeDouble(entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        while(size > 0) {
            similarityScoresMap.put(in.readInt(), in.readDouble());
            size--;
        }
    }

    public void addSimilarityScore(Integer songId, double similarityScore) {
        similarityScoresMap.put(songId, similarityScore);
    }
}
