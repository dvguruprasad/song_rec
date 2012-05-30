package com.songrec.workflows;

import com.songrec.jobs.ClusterUserJob;
import com.songrec.jobs.UserVectorGeneratorJob;
import org.apache.hadoop.util.ToolRunner;

public class ClusterUserWorkflow {

    public static final String USER_VECTOR_OUTPUT = "user-vector-output";
    public static final String CLUSTER_USER_OUTPUT = "cluster-user-output";

    public static void main(String args[]) throws Exception {

        int res = ToolRunner.run(new UserVectorGeneratorJob(args[0], args[1] + "/" + USER_VECTOR_OUTPUT), args);
        if(res != 0)
            throw new Exception("Failed creating user vector");

        res = ToolRunner.run(new ClusterUserJob(args[1] + "/" +  USER_VECTOR_OUTPUT, args[1] + "/" +  CLUSTER_USER_OUTPUT), args);
        if(res != 0)
            throw new Exception("Failed clustering users");
    }
}
