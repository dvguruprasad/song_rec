package com.songrec.reducers;

import com.songrec.dto.*;
import com.songrec.utils.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecommenderReducer extends Reducer<IntWritable, UserVectorAndSimilarityScores, Text, SongRecommendations> {
    private static Map<Integer, String> songIdHash;
    private static Map<Integer, String> userIdHash;

    protected void setup(Context context) throws IOException, InterruptedException {
        songIdHash = FileUtils.songIdHashMap(new Path(context.getConfiguration().get("songs_hash")), context.getConfiguration());
        userIdHash = FileUtils.songIdHashMap(new Path(context.getConfiguration().get("users_hash")), context.getConfiguration());
    }

    @Override
    protected void reduce(IntWritable userId, Iterable<UserVectorAndSimilarityScores> userVectorAndSimilarityScores, Context context) throws IOException, InterruptedException {
        HashMap<Integer, List<PlayCountAndSimilarityScore>> candidateSongs = new HashMap<Integer, List<PlayCountAndSimilarityScore>>();
        for (UserVectorAndSimilarityScores userVectorAndSimilarityScore : userVectorAndSimilarityScores) {
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
        for (Map.Entry<Integer, List<PlayCountAndSimilarityScore>> entry : candidateSongs.entrySet()) {
            String originalSongId = songIdHash.get(entry.getKey());
            recommendations.add(new SongRecommendation(originalSongId, calculatePredictedRating(entry.getValue())));
        }
        String originalUserId = userIdHash.get(userId.get());
        context.write(new Text(originalUserId), recommendations);
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