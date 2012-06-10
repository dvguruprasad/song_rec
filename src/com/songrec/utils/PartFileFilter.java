package com.songrec.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class PartFileFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
        return path.getName().startsWith("part-r-");
    }
}
