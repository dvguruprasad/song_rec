package com.songrec.jobs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public abstract class AbstactJob extends Configured implements Tool {
    protected static String outputPathForJob(String jobName, String outputPath) {
        return outputPath + "/" + jobName;
    }
}
