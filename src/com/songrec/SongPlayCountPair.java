package com.songrec;

import com.songrec.utils.DataInputX;
import com.songrec.utils.DataOutputX;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SongPlayCountPair implements WritableComparable<SongPlayCountPair> {
    private String songId;
    private short playCount;

    public SongPlayCountPair() {
    }

    public SongPlayCountPair(String songId, short playCount) {
        this.songId = songId;
        this.playCount = playCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        DataOutputX.writeString(out, songId);
        out.writeShort(playCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        songId = DataInputX.readString(in);
        playCount = in.readShort();
    }

    public String songId() {
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
        int result = songId.compareTo(songPlayCountPair.songId());
        if (result != 0) return result;
        return playCount > songPlayCountPair.playCount() ? 1 : (playCount < songPlayCountPair.playCount() ? -1 : 0);
    }
}
