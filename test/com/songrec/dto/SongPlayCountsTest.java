package com.songrec.dto;

import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;

public class SongPlayCountsTest {
    @Test
    public void shouldReadFields() throws IOException {
        ArrayList pairs = new ArrayList<SongPlayCount>();
        pairs.add(new SongPlayCount(1, (short) 5));
        pairs.add(new SongPlayCount(2, (short) 8));
        SongPlayCounts songPlayCounts = new SongPlayCounts(pairs);
        songPlayCounts.write(new DataOutputStream(new FileOutputStream("/tmp/foobar")));

        DataInput in = new DataInputStream(new FileInputStream("/tmp/foobar"));
        SongPlayCounts deserializedPairs = new SongPlayCounts();
        deserializedPairs.readFields(in);
        Assert.assertEquals(2, deserializedPairs.size());
        SongPlayCount expectedPairOne = deserializedPairs.get(0);
        Assert.assertEquals("song1", expectedPairOne.songId());
        Assert.assertEquals((short) 5, expectedPairOne.playCount());

        SongPlayCount expectedPairTwo = deserializedPairs.get(1);
        Assert.assertEquals("song2", expectedPairTwo.songId());
        Assert.assertEquals((short) 8, expectedPairTwo.playCount());
    }
}
