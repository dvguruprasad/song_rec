package com.songrec.workflows;

import com.songrec.jobs.SongIdHashJob;
import com.songrec.jobs.SongSimilarityGeneratorJob;
import com.songrec.jobs.UserVectorGeneratorJob;

public class SongSimilarityWorkFlow extends WorkFlow {
    public static void main(String args[]) throws Exception {
        runJob(new SongIdHashJob(args[0], args[1]));
        String userVectorPath = runJob(new UserVectorGeneratorJob(args[0], args[1]));
        runJob(new SongSimilarityGeneratorJob(userVectorPath, args[1]));
    }
}
