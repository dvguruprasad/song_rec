package com.songrec.dto;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserPlayCount implements WritableComparable<UserPlayCount> {
    private int userId;
    private short playCount;

    public UserPlayCount() {
    }

    public UserPlayCount(int userId, short playCount) {
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
    public int compareTo(UserPlayCount userPlayCount) {
        int result = userId > userPlayCount.userId() ? 1 : (userId < userPlayCount.userId() ? -1 : 0);
        if (result != 0) return result;
        return playCount > userPlayCount.playCount() ? 1 : (playCount < userPlayCount.playCount() ? -1 : 0);
    }
}
