package com.songrec.workflows;

import com.songrec.jobs.*;

public class RecommenderWorkFlow extends WorkFlow {
    public static void main(String args[]) throws Exception {
        String baseInputPath = args[0];
        String baseOutputPath = args[1];

        String hashedSongIdsPath = runJob(new SongIdHashJob(baseInputPath, baseOutputPath));
        String hashedUserIdsPath = runJob(new UserIdHashJob(baseInputPath, baseOutputPath));

        String songSimilaritiesVectorPath = songsSimilaritiesPath(baseInputPath, baseOutputPath);
        String songVectorPath = runJob(new SongVectorGeneratorJob(baseInputPath, baseOutputPath));

        String songVectorAndSimilarityScoresPath = runJob(new SongVectorAndSimilaritiesGeneratorJob(songVectorPath, songSimilaritiesVectorPath, baseOutputPath));
        runJob(new RecommenderJob(songVectorAndSimilarityScoresPath, baseOutputPath, hashedSongIdsPath, hashedUserIdsPath));
    }

    private static String songsSimilaritiesPath(String baseInputPath, String baseOutputPath) throws Exception {
        String userVectorPath = runJob(new UserVectorGeneratorJob(baseInputPath, baseOutputPath));
        return runJob(new SongSimilarityGeneratorJob(userVectorPath, "", baseOutputPath));
    }
}
