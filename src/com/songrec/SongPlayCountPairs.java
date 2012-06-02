package com.songrec;

import com.songrec.utils.DataInputX;
import com.songrec.utils.DataOutputX;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class SongPlayCountPairs implements Writable, Iterable<SongPlayCountPair> {
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
            DataOutputX.writeString(out, pair.songId());
            out.writeLong(pair.playCount());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        pairs = new ArrayList<SongPlayCountPair>();
        for(int i = 0;i < count; i++){
            pairs.add(new SongPlayCountPair(DataInputX.readString(in), in.readShort()));
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
}
