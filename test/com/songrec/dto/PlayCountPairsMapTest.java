package com.songrec.dto;

import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PlayCountPairsMapTest {
    @Test
    public void writeAndRead() throws IOException {
        HashMap<Integer, List<PlayCountPair>> map = new HashMap<Integer, List<PlayCountPair>>();
        ArrayList<PlayCountPair> playCountPairs = new ArrayList<PlayCountPair>();
        playCountPairs.add(new PlayCountPair((short)4, (short)6));
        playCountPairs.add(new PlayCountPair((short)8, (short)9));
        playCountPairs.add(new PlayCountPair((short)2, (short)10));
        map.put(11, playCountPairs);

        ArrayList<PlayCountPair> playCountPairs2 = new ArrayList<PlayCountPair>();
        playCountPairs2.add(new PlayCountPair((short)5, (short)4));
        playCountPairs2.add(new PlayCountPair((short)1, (short)1));
        playCountPairs2.add(new PlayCountPair((short)9, (short)2));
        map.put(22, playCountPairs2);
        PlayCountPairsMap playCountPairsMap = new PlayCountPairsMap(map);

        DataOutputStream out = new DataOutputStream(new FileOutputStream("/tmp/foo"));
        playCountPairsMap.write(out);

        DataInputStream in = new DataInputStream(new FileInputStream("/tmp/foo"));
        PlayCountPairsMap result = new PlayCountPairsMap();
        result.readFields(in);
        Assert.assertEquals(2, result.size());
    }
    
    @Test
    public void merge(){
        HashMap<Integer, List<PlayCountPair>> map_1 = new HashMap<Integer, List<PlayCountPair>>();
        ArrayList<PlayCountPair> playCountPairs_1 = new ArrayList<PlayCountPair>();
        playCountPairs_1.add(new PlayCountPair((short) 1, (short) 2));
        playCountPairs_1.add(new PlayCountPair((short) 3, (short) 4));
        map_1.put(11, playCountPairs_1);
        
        PlayCountPairsMap mapOne = new PlayCountPairsMap(map_1);

        HashMap<Integer, List<PlayCountPair>> map_2 = new HashMap<Integer, List<PlayCountPair>>();
        ArrayList<PlayCountPair> playCountPairs_2 = new ArrayList<PlayCountPair>();
        playCountPairs_2.add(new PlayCountPair((short) 8, (short) 10));
        map_2.put(22, playCountPairs_2);
        ArrayList<PlayCountPair> playCountPairs_3 = new ArrayList<PlayCountPair>();
        playCountPairs_3.add(new PlayCountPair((short)5, (short)7));
        map_2.put(33, playCountPairs_3);
        
        PlayCountPairsMap mapTwo = new PlayCountPairsMap(map_2);
        
        mapOne.merge(mapTwo);
        
        Assert.assertEquals(3, mapOne.size());
    }
}
