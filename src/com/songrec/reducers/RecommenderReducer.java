package com.songrec.reducers;

import com.songrec.dto.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecommenderReducer extends Reducer<IntWritable, UserVectorAndSimilarityScores, IntWritable, SongRecommendations> {
    @Override
    protected void reduce(IntWritable userId, Iterable<UserVectorAndSimilarityScores> userVectorAndSimilarityScores, Context context) throws IOException, InterruptedException {
        HashMap<Integer, List<PlayCountAndSimilarityScore>> candidateSongs = new HashMap<Integer, List<PlayCountAndSimilarityScore>>();
        int songCount = 0;
        for (UserVectorAndSimilarityScores userVectorAndSimilarityScore : userVectorAndSimilarityScores) {
            songCount++;
            SimilarityScores similarityScores = userVectorAndSimilarityScore.getSimilarityScores();
            for (SimilarityScore similarityScore : similarityScores) {
                if (candidateSongs.containsKey(similarityScore.songId())) {
                    candidateSongs.get(similarityScore.songId()).add(new PlayCountAndSimilarityScore(userVectorAndSimilarityScore.getSong().playCount(), similarityScore.score()));
                } else {
                    ArrayList<PlayCountAndSimilarityScore> playCountAndSimilarityScores = new ArrayList<PlayCountAndSimilarityScore>();
                    playCountAndSimilarityScores.add(new PlayCountAndSimilarityScore(userVectorAndSimilarityScore.getSong().playCount(), similarityScore.score()));
                    candidateSongs.put(similarityScore.songId(), playCountAndSimilarityScores);
                }
            }
        }

        SongRecommendations recommendations = new SongRecommendations();
        System.out.println("userId => " + userId.get());
        System.out.println("songCount => " + songCount);
        System.out.println("----------------------------------------------------------------------");
        for (Map.Entry<Integer, List<PlayCountAndSimilarityScore>> entry : candidateSongs.entrySet()) {
            String playCountsAndSimilarityScores = "";
            for (PlayCountAndSimilarityScore ps : entry.getValue()) {
                playCountsAndSimilarityScores += "(PC: " + ps.playCount() + "," + "SS: " + ps.similarityScore() + ")";
            }
            System.out.println("songId => " + entry.getKey() + ", data => [" + playCountsAndSimilarityScores + "]");
            recommendations.add(new SongRecommendation(entry.getKey(), calculatePredictedRating(entry.getValue())));
        }
        System.out.println("----------------------------------------------------------------------");
        recommendations.sort();
        context.write(userId, recommendations);
    }

    private double calculatePredictedRating(List<PlayCountAndSimilarityScore> data) {
        double numerator = 0;
        double denominator = 0;
        for (PlayCountAndSimilarityScore playCountAndSimilarityScore : data) {
            numerator += playCountAndSimilarityScore.playCount() * playCountAndSimilarityScore.similarityScore();
            denominator += playCountAndSimilarityScore.similarityScore();
        }
        return numerator / denominator;
    }
}
