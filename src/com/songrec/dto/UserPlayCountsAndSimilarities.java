package com.songrec.dto;

import java.util.HashMap;
import java.util.Map;

public class UserPlayCountsAndSimilarities {
    private Map<Integer, Double> similarityScoresMap;
    private HashMap<Integer, Short> userPlayCountsMap;

    public UserPlayCountsAndSimilarities(Map<Integer, Double> similarityScoresMap, HashMap<Integer, Short> userPlayCountsMap) {
        this.similarityScoresMap = similarityScoresMap;
        this.userPlayCountsMap = userPlayCountsMap;
    }

    @Override
    public String toString() {
        String result = "";
        result += "SimilarityScores: " + similarityScores(similarityScoresMap) + ", User playcounts: " + userPlayCounts();
        return result;
    }

    private String userPlayCounts() {
        String result = "";
        for(Map.Entry<Integer, Short> entry : userPlayCountsMap.entrySet()){
            result += "(" + entry.getKey() + "," + entry.getValue() + ")";
        }
        return result;
    }

    private String similarityScores(Map<Integer, Double> map) {
        String result = "";
        for(Map.Entry<Integer, Double> entry : map.entrySet()){
            result += "(" + entry.getKey() + "," + entry.getValue() + ")";
        }
        return result;
    }
}
