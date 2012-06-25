package com.songrec.workflows;

import com.songrec.jobs.SongSimilarityGeneratorJob;
import com.songrec.jobs.Song_UserVectorGeneratorJob;
import com.songrec.jobs.Song_PlayCountAndSimilaritiesGeneratorJob;
import com.songrec.jobs.UserVectorGeneratorJob;

public class RecommenderWorkFlow extends WorkFlow {
    public static void main(String args[]) throws Exception {
        String baseInputPath = args[0];
        String baseOutputPath = args[1];

        String song_UserVectorPath = runJob(new Song_UserVectorGeneratorJob(baseInputPath, baseOutputPath));

        String userVectorPath = runJob(new UserVectorGeneratorJob(baseInputPath, baseOutputPath));
        String songSimilarityVectorPath = runJob(new SongSimilarityGeneratorJob(userVectorPath, "", baseOutputPath));
        runJob(new Song_PlayCountAndSimilaritiesGeneratorJob(song_UserVectorPath, songSimilarityVectorPath, baseOutputPath));
    }
}
