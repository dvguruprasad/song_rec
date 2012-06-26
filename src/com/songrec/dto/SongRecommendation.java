package com.songrec.dto;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SongRecommendation implements Writable, Comparable<SongRecommendation> {
    private int songId;
    private double predictedPlayCount;

    public SongRecommendation() {
    }

    public SongRecommendation(int songId, double predictedPlayCount) {
        this.songId = songId;
        this.predictedPlayCount = predictedPlayCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(songId);
        out.writeDouble(predictedPlayCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.songId = in.readInt();
        this.predictedPlayCount = in.readDouble();
    }

    @Override
    public int compareTo(SongRecommendation songRecommendation) {
        return this.predictedPlayCount < songRecommendation.predictedPlayCount ? 1 : (this.predictedPlayCount >  songRecommendation.predictedPlayCount ? -1 : 0);
    }

    @Override
    public String toString() {
        String result = "";
        result += "SongID: " + songId + ", PredictedPlayCount: " + predictedPlayCount;
        return result;
    }
}
