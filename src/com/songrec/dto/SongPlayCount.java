package com.songrec.dto;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SongPlayCount implements WritableComparable<SongPlayCount> {
    private int songId;
    private short playCount;

    public SongPlayCount() {
    }

    public SongPlayCount(int songId, short playCount) {
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
    public int compareTo(SongPlayCount songPlayCount) {
        int result = songId > songPlayCount.songId() ? 1 : (songId < songPlayCount.songId() ? -1 : 0);
        if (result != 0) return result;
        return playCount > songPlayCount.playCount() ? 1 : (playCount < songPlayCount.playCount() ? -1 : 0);
    }
}
