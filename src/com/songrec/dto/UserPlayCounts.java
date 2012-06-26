package com.songrec.dto;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserPlayCounts implements Writable{
    private HashMap<Integer, Short> map;

    public UserPlayCounts() {
        map = new HashMap<Integer, Short>();
    }

    public UserPlayCounts(List<UserPlayCountPair> list) {
        map = new HashMap<Integer, Short>();
        for (UserPlayCountPair pair : list) {
            map.put(pair.userId(), pair.playCount());
        }
    }

    public UserPlayCounts(UserPlayCounts playCounts) {
        this.map = playCounts.map;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<Integer, Short> entry : map.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeShort(entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        while (size > 0) {
            map.put(in.readInt(), in.readShort());
            size--;
        }
    }

    @Override
    public String toString() {
        String result = "";
        for (Map.Entry<Integer, Short> entry : map.entrySet()) {
            result += entry.getKey() + ":" + entry.getValue() + ";";
        }
        return result;
    }

    public HashMap<Integer, Short> getMap() {
        return map;
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }
}