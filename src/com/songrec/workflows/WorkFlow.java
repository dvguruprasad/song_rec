package com.songrec.workflows;

import com.songrec.jobs.AbstactJob;
import org.apache.hadoop.util.ToolRunner;

public abstract class WorkFlow {
    protected static String runJob(AbstactJob job) throws Exception {
        int res = ToolRunner.run(job, new String[]{});
        if(res != 0)
            throw new Exception("Failed running " + job.getClass().getSimpleName());
        return "/u/guru/mr-output" + "/" + job.getClass().getSimpleName();
    }
}
