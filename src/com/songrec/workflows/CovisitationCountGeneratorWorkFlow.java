package com.songrec.workflows;

import com.songrec.jobs.CovisitationCountJob;
import com.songrec.jobs.SongIdHashJob;
import com.songrec.jobs.UserVectorGeneratorJob;

public class CovisitationCountGeneratorWorkFlow extends WorkFlow {
    public static void main(String args[]) throws Exception {
        String baseInputPath = args[0];
        String baseOutputPath = args[1];

        String songIdHashOutputPath = runJob(new SongIdHashJob(baseInputPath, baseOutputPath));
        String userVectorPath = runJob(new UserVectorGeneratorJob(baseInputPath, baseOutputPath));
        runJob(new CovisitationCountJob(userVectorPath, baseOutputPath, songIdHashOutputPath));
    }
}
