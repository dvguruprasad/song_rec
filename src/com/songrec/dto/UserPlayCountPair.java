package com.songrec.dto;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserPlayCountPair implements WritableComparable<UserPlayCountPair> {
    private int userId;
    private short playCount;

    public UserPlayCountPair() {
    }

    public UserPlayCountPair(int userId, short playCount) {
        this.userId = userId;
        this.playCount = playCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(userId);
        out.writeShort(playCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId = in.readInt();
        playCount = in.readShort();
    }

    public int userId() {
        return userId;
    }

    public short playCount() {
        return playCount;
    }

    @Override
    public String toString() {
        return "(" + userId + "," + playCount + "),";
    }

    @Override
    public int compareTo(UserPlayCountPair userPlayCountPair) {
        int result = userId > userPlayCountPair.userId() ? 1 : (userId < userPlayCountPair.userId() ? -1 : 0);
        if (result != 0) return result;
        return playCount > userPlayCountPair.playCount() ? 1 : (playCount < userPlayCountPair.playCount() ? -1 : 0);
    }
}
