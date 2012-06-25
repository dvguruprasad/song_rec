package com.songrec.dto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SongSimilarityScores extends SongVector {
    Map<Integer, Double> map;

    public SongSimilarityScores() {
        map = new HashMap<Integer, Double>();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<Integer, Double> entry : map.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeDouble(entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        while(size > 0) {
            map.put(in.readInt(), in.readDouble());
            size--;
        }
    }

    public void put(Integer songId, double similarityScore) {
        map.put(songId, similarityScore);
    }

    public Map<Integer, Double> getMap() {
        return map;
    }

    @Override
    public String toString() {
        String result = "";
        for(Map.Entry<Integer, Double> entry : map.entrySet()){
            result += "(" + entry.getKey() + ":" + entry.getValue() + "),";
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SongSimilarityScores that = (SongSimilarityScores) o;

        if (map != null ? !map.equals(that.map) : that.map != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return map != null ? map.hashCode() : 0;
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }
}
