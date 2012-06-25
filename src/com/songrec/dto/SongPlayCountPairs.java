package com.songrec.dto;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SongPlayCountPairs implements Writable, Iterable<SongPlayCountPair> {
    private ArrayList<SongPlayCountPair> pairs;

    public SongPlayCountPairs() {
    }

    public SongPlayCountPairs(ArrayList<SongPlayCountPair> songPlayCountPairs) {
        this.pairs = songPlayCountPairs;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(pairs.size());
        for(SongPlayCountPair pair : pairs){
            out.writeInt(pair.songId());
            out.writeShort(pair.playCount());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        pairs = new ArrayList<SongPlayCountPair>();
        for(int i = 0;i < count; i++){
            pairs.add(new SongPlayCountPair(in.readInt(), in.readShort()));
        }
    }

    @Override
    public Iterator<SongPlayCountPair> iterator() {
        return pairs.iterator();
    }
    
    public long size(){
        return pairs.size();
    }

    public SongPlayCountPair get(int index){
        return pairs.get(index);
    }

    @Override
    public String toString() {
        String result = "";
        for(SongPlayCountPair pair : pairs)
            result += "(" + pair.songId() + "," + pair.playCount() + "),";
        return result;
    }
}
