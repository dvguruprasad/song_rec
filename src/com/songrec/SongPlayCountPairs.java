package com.songrec;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class SongPlayCountPairs implements Writable {
    private ArrayList<SongPlayCountPair> pairs;

    public SongPlayCountPairs() {
    }

    public SongPlayCountPairs(Iterable<SongPlayCountPair> songPlayCountPairs) {
        pairs = new ArrayList<SongPlayCountPair>();
        for(SongPlayCountPair pair : songPlayCountPairs){
            pairs.add(new SongPlayCountPair(pair.songId(), pair.playCount()));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(pairs.size());
        for(SongPlayCountPair pair : pairs){
            out.writeChars(pair.songId());
            out.writeChars(pair.playCount());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        pairs = new ArrayList<SongPlayCountPair>();
        for(int i = 0;i < count; i++){
            pairs.add(new SongPlayCountPair(in.readUTF(), in.readUTF()));
        }
    }

    public ArrayList<SongPlayCountPair> get(){
        return pairs;
    }
}
