package com.songrec.workflows;

import com.songrec.jobs.SongSimilarityGeneratorJob;
import com.songrec.jobs.UserVectorGeneratorJob;

public class SongSimilarityWorkFlow extends WorkFlow {
    public static void main(String args[]) throws Exception {
        String baseInputPath = args[0];
        String baseOutputPath = args[1];

//        String songIdHashPath = runJob(new SongIdHashJob(baseInputPath, baseOutputPath));
        String userVectorPath = runJob(new UserVectorGeneratorJob(baseInputPath, baseOutputPath));
        runJob(new SongSimilarityGeneratorJob(userVectorPath, "", baseOutputPath));
    }
}
