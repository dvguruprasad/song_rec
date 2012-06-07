package com.songrec.workflows;

import com.songrec.jobs.ClusterIdGeneratorJob;
import com.songrec.jobs.GroupUsersByClusterJob;

public class ClusterUserWorkFlow extends WorkFlow {
    public static void main(String args[]) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        String clusterIdsPath = runJob(new ClusterIdGeneratorJob(inputPath, outputPath));
        runJob(new GroupUsersByClusterJob(clusterIdsPath, outputPath));
    }
}
