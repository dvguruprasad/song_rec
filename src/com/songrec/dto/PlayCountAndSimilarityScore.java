package com.songrec.dto;

public class PlayCountAndSimilarityScore{
    private short playCount;
    private double similarityScore;

    public PlayCountAndSimilarityScore(short playCount, double similarityScore) {
        this.playCount = playCount;
        this.similarityScore = similarityScore;
    }

    public short playCount() {
        return playCount;
    }

    public double similarityScore() {
        return similarityScore;
    }
}
