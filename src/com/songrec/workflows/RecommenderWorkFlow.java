package com.songrec.workflows;

import com.songrec.jobs.*;

public class RecommenderWorkFlow extends WorkFlow {
    public static void main(String args[]) throws Exception {
        String baseInputPath = args[0];
        String baseOutputPath = args[1];

        String songVectorPath = runJob(new SongVectorGeneratorJob(baseInputPath, baseOutputPath));
        String userVectorPath = runJob(new UserVectorGeneratorJob(baseInputPath, baseOutputPath));
//        String songIdHashPath = runJob(new SongIdHashJob(baseInputPath, baseOutputPath));
        String songSimilarityVectorPath = runJob(new SongSimilarityGeneratorJob(userVectorPath, "", baseOutputPath));
        String songVectorAndSimilarityScoresPath = runJob(new SongVectorAndSimilaritiesGeneratorJob(songVectorPath, songSimilarityVectorPath, baseOutputPath));
        runJob(new RecommenderJob(songVectorAndSimilarityScoresPath, baseOutputPath));
    }
}
