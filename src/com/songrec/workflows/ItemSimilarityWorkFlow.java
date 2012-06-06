package com.songrec.workflows;

import com.songrec.jobs.ItemSimilarityGeneratorJob;
import com.songrec.jobs.UserVectorGeneratorJob;

public class ItemSimilarityWorkFlow extends WorkFlow {
    public static void main(String args[]) throws Exception {
        String userVectorPath = runJob(new UserVectorGeneratorJob(args[0], args[1]));
        runJob(new ItemSimilarityGeneratorJob(userVectorPath, args[1]));
    }
}
