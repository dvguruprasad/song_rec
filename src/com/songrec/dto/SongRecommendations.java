package com.songrec.dto;

import com.songrec.workflows.Thresholds;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SongRecommendations implements Writable {
    List<SongRecommendation> recommendations;

    public SongRecommendations() {
        recommendations = new ArrayList<SongRecommendation>();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(recommendations.size());
        for (SongRecommendation recommendation : recommendations) {
            recommendation.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = recommendations.size();
        recommendations = new ArrayList<SongRecommendation>();
        while (size > 0) {
            SongRecommendation recommendation = new SongRecommendation();
            recommendation.readFields(in);
            recommendations.add(recommendation);
        }
    }

    @Override
    public String toString() {
        String result = "";
        for (SongRecommendation recommendation : recommendations) {
            result += "(" + recommendation + ")";
        }
        return result;
    }

    public void add(SongRecommendation songRecommendation) {
        recommendations.add(songRecommendation);
    }

    public void sort() {
        Collections.sort(recommendations);
    }
}
