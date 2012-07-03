package com.songrec.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FileUtils {

    private static final PartFileFilter filter = new PartFileFilter();
    private static Map<Integer, String> songIdHashMap;
    private static Map<Integer, String> userIdHashMap;

    public static Map<Integer, String> songIdHashMap(Path hashFilePath, Configuration configuration) throws IOException {
        if(null != songIdHashMap) return songIdHashMap;
        return create(hashFilePath, configuration);
    }

    public static Map<Integer, String> userIdHashMap(Path hashFilePath, Configuration configuration) throws IOException {
        if(null != userIdHashMap) return userIdHashMap;
        return create(hashFilePath, configuration);
    }

    private static Map<Integer, String> create(Path hashFilePath, Configuration configuration) throws IOException {
        HashMap<Integer, String> map = new HashMap<Integer, String>();
        FileSystem fileSystem = hashFilePath.getFileSystem(configuration);
        FileStatus[] fss = fileSystem.listStatus(hashFilePath, filter);
        for (FileStatus status : fss) {
            Path path = status.getPath();
            SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, path, configuration);
            IntWritable key = new IntWritable();
            Text value = new Text();
            while (reader.next(key, value)) {
                map.put(key.get(), value.toString());
            }
            reader.close();
        }
        return map;
    }
}
