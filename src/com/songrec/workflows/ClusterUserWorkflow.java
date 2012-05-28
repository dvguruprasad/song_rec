package com.songrec.workflows;

import com.songrec.jobs.ClusterUserJob;
import com.songrec.jobs.UserVectorGeneratorJob;
import org.apache.hadoop.util.ToolRunner;

public class ClusterUserWorkflow {

    public static final String USER_VECTOR_INPUT = "/u/guru/gutenberg/user-vector-input";
    public static final String USER_VECTOR_OUTPUT = "/u/guru/gutenberg-output/user-vector";
    public static final String CLUSTER_USER_OUTPUT = "/u/guru/gutenberg-output/cluster-user-output";

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new UserVectorGeneratorJob(USER_VECTOR_INPUT, USER_VECTOR_OUTPUT), args);
        if(res != 0)
            throw new Exception("Failed creating user vector");

        res = ToolRunner.run(new ClusterUserJob(USER_VECTOR_OUTPUT, CLUSTER_USER_OUTPUT), args);
        if(res != 0)
            throw new Exception("Failed clustering users");
    }
}
