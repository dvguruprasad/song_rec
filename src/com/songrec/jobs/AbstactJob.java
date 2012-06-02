package com.songrec.jobs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public abstract class AbstactJob extends Configured implements Tool {
    private static final String BASE_OUTPUT_PATH = "/u/guru/mr-output";

    protected static String outputPath(String jobName) {
        return BASE_OUTPUT_PATH + "/" + jobName;
    }
}
