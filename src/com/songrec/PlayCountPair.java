package com.songrec;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PlayCountPair implements WritableComparable<PlayCountPair> {
    private short firstSongPlayCount;
    private short secondSongPlayCount;

    public PlayCountPair() {
    }

    public long firstSongPlayCount() {
        return firstSongPlayCount;
    }

    public long secondSongPlayCount() {
        return secondSongPlayCount;
    }

    public PlayCountPair(short firstSongPlayCount, short secondSongPlayCount) {
        this.firstSongPlayCount = firstSongPlayCount;
        this.secondSongPlayCount = secondSongPlayCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(firstSongPlayCount);
        out.writeShort(secondSongPlayCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstSongPlayCount = in.readShort();
        secondSongPlayCount = in.readShort();
    }

    @Override
    public int compareTo(PlayCountPair playCountPair) {
        int result = compare(firstSongPlayCount, playCountPair.firstSongPlayCount);
        if(result != 0) return result;
        return compare(secondSongPlayCount, playCountPair.secondSongPlayCount);
    }

    private int compare(long a, long b) {
        return a > b ? 1 : ((a < b)? -1 : 0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PlayCountPair that = (PlayCountPair) o;

        if (firstSongPlayCount != that.firstSongPlayCount) return false;
        if (secondSongPlayCount != that.secondSongPlayCount) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) firstSongPlayCount;
        result = 31 * result + (int) secondSongPlayCount;
        return result;
    }
}
