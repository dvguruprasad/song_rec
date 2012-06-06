package com.songrec.dto;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SongPlayCountPair implements WritableComparable<SongPlayCountPair> {
    private int songId;
    private short playCount;

    public SongPlayCountPair() {
    }

    public SongPlayCountPair(int songId, short playCount) {
        this.songId = songId;
        this.playCount = playCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(songId);
        out.writeShort(playCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        songId = in.readInt();
        playCount = in.readShort();
    }

    public int songId() {
        return songId;
    }

    public short playCount() {
        return playCount;
    }

    @Override
    public String toString() {
        return "(" + songId + "," + playCount + "),";
    }

    @Override
    public int compareTo(SongPlayCountPair songPlayCountPair) {
        int result = songId > songPlayCountPair.songId() ? 1 : (songId < songPlayCountPair.songId() ? -1 : 0);
        if (result != 0) return result;
        return playCount > songPlayCountPair.playCount() ? 1 : (playCount < songPlayCountPair.playCount() ? -1 : 0);
    }
}
