package com.songrec.dto;

public class SimilarityScore implements Comparable<SimilarityScore>{
    private int songId;
    private double similarityScore;

    public SimilarityScore(int songId, double similarityScore) {
        this.songId = songId;
        this.similarityScore = similarityScore;
    }

    public int songId() {
        return songId;
    }

    public double score() {
        return similarityScore;
    }

    @Override
    public int compareTo(SimilarityScore similarityScore) {
        return score() > similarityScore.score() ? 1 : (score() < similarityScore.score() ? -1 : 0);
    }
}
