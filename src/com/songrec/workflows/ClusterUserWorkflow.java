package com.songrec.workflows;

import com.songrec.jobs.ClusterUserJob;
import com.songrec.jobs.UserVectorGeneratorJob;

public class ClusterUserWorkflow extends WorkFlow{
    public static void main(String args[]) throws Exception {
        String inputPath = args[0];
        String outputPath = args[0];
        String userVectorPath = runJob(new UserVectorGeneratorJob(inputPath, outputPath));
        runJob(new ClusterUserJob(userVectorPath, outputPath));
    }
}
