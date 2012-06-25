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
    private static Map<Integer, String> map;

    public static Map<Integer, String> getItemIdToHashMap(Path itemIdHashFilesPath, Configuration configuration) throws IOException {
        if(null != map) return map;

        map = new HashMap<Integer, String>();
        FileSystem fileSystem = itemIdHashFilesPath.getFileSystem(configuration);
        FileStatus[] fss = fileSystem.listStatus(itemIdHashFilesPath, filter);
        for (FileStatus status : fss) {
            Path path = status.getPath();
            SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, path, configuration);
            IntWritable key = new IntWritable();
            Text value = new Text();
            while (reader.next(key, value)) {
                map.put(key.get(),  value.toString());
            }
            reader.close();
        }
        return map;
    }
}
