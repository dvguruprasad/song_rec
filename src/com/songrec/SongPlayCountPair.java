package com.songrec;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SongPlayCountPair implements WritableComparable {
    private String songId;
    private String playCount;

    public SongPlayCountPair() {
    }

    public SongPlayCountPair(String songId, String playCount) {
        this.songId = songId;
        this.playCount = playCount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(songId);
        dataOutput.writeUTF(playCount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        songId = dataInput.readUTF();
        playCount = dataInput.readUTF();
    }

    public String songId(){
        return songId;
    }

    public String playCount(){
        return playCount;
    }

    @Override
    public int compareTo(Object o) {
        return songId.compareTo(songId);
    }

    @Override
    public String toString() {
        return "(" + songId + "," + playCount + "),";
    }
}
