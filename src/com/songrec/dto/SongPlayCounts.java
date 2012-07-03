package com.songrec.dto;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class SongPlayCounts implements Writable, Iterable<SongPlayCount> {
    private ArrayList<SongPlayCount> pairs;

    public SongPlayCounts() {
    }

    public SongPlayCounts(ArrayList<SongPlayCount> songPlayCounts) {
        this.pairs = songPlayCounts;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(pairs.size());
        for(SongPlayCount pair : pairs){
            out.writeInt(pair.songId());
            out.writeShort(pair.playCount());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        pairs = new ArrayList<SongPlayCount>();
        for(int i = 0;i < count; i++){
            pairs.add(new SongPlayCount(in.readInt(), in.readShort()));
        }
    }

    @Override
    public Iterator<SongPlayCount> iterator() {
        return pairs.iterator();
    }
    
    public long size(){
        return pairs.size();
    }

    public SongPlayCount get(int index){
        return pairs.get(index);
    }

    @Override
    public String toString() {
        String result = "";
        for(SongPlayCount pair : pairs)
            result += "(" + pair.songId() + "," + pair.playCount() + "),";
        return result;
    }
}
