package com.songrec.dto;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.songrec.utils.DataInputX;
import com.songrec.utils.DataOutputX;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SongPair implements WritableComparable<SongPair> {
    private String firstSongId;
    private String secondSongId;

    public SongPair() {
    }

    public SongPair(String firstSongId, String secondSongId) {
        this.firstSongId = firstSongId;
        this.secondSongId = secondSongId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SongPair songPair = (SongPair) o;

        return firstSongId.equals(songPair.firstSongId) && secondSongId.equals(songPair.secondSongId);
    }

    @Override
    public int hashCode() {
        int result = sha1Hashcode(firstSongId).asInt();
        result = 31 * result + sha1Hashcode(secondSongId).asInt();
        return result;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        DataOutputX.writeString(out, firstSongId);
        DataOutputX.writeString(out, secondSongId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstSongId = DataInputX.readString(in);
        secondSongId = DataInputX.readString(in);
    }

    @Override
    public int compareTo(SongPair songPair) {
        int result = firstSongId.compareTo(songPair.firstSongId);
        if(result != 0) return result;
        return secondSongId.compareTo(songPair.secondSongId) ;
    }

    @Override
    public String toString() {
        return firstSongId + "," + secondSongId;
    }

    private HashCode sha1Hashcode(String str) {
        return Hashing.sha1().newHasher().putString(str).hash();
    }
}
