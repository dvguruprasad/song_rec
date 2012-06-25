package com.songrec.dto;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CovisitationVector implements WritableComparable {
    Map<Integer, Short> map = new HashMap<Integer, Short>();

    @Override
    public int compareTo(Object o) {
        return 0;
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

    public void incrementCount(int songId) {
        if (map.containsKey(songId)) {
            short count = (short) (map.get(songId) + 1);
            map.put(songId, count);
        } else {
            map.put(songId, (short) 1);
        }
    }

    public void merge(CovisitationVector vector) {
        for (Map.Entry<Integer, Short> entry : vector.map.entrySet()) {
            if (map.containsKey(entry.getKey())) {
                addToCount(entry.getKey(), entry.getValue());
            } else {
                map.put(entry.getKey(), entry.getValue());
            }

        }
    }

    private void addToCount(Integer songId, Short covisitationCount) {
        short count = map.get(songId);
        map.put(songId, (short) (count + covisitationCount));
    }

    public String toString(Map<Integer, String> songIdHashMap) {
        String result = "";
        for(Map.Entry<Integer, Short> entry : map.entrySet()){
            result += "(" + songIdHashMap.get(entry.getKey()) + ":" + entry.getValue() + ")";
        }
        return result;
    }
}
