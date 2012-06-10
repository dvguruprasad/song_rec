package com.songrec.dto;

import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;

public class SongPlayCountPairsTest {
    @Test
    public void shouldReadFields() throws IOException {
        ArrayList pairs = new ArrayList<SongPlayCountPair>();
        pairs.add(new SongPlayCountPair(1, (short) 5));
        pairs.add(new SongPlayCountPair(2, (short) 8));
        SongPlayCountPairs songPlayCountPairs = new SongPlayCountPairs(pairs);
        songPlayCountPairs.write(new DataOutputStream(new FileOutputStream("/tmp/foobar")));

        DataInput in = new DataInputStream(new FileInputStream("/tmp/foobar"));
        SongPlayCountPairs deserializedPairs = new SongPlayCountPairs();
        deserializedPairs.readFields(in);
        Assert.assertEquals(2, deserializedPairs.size());
        SongPlayCountPair expectedPairOne = deserializedPairs.get(0);
        Assert.assertEquals("song1", expectedPairOne.songId());
        Assert.assertEquals((short) 5, expectedPairOne.playCount());

        SongPlayCountPair expectedPairTwo = deserializedPairs.get(1);
        Assert.assertEquals("song2", expectedPairTwo.songId());
        Assert.assertEquals((short) 8, expectedPairTwo.playCount());
    }
    
    @Test
    public void foo() throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream("/tmp/blah");
        fileOutputStream.write(500);
        fileOutputStream.close();
    }
}
