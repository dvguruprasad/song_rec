package com.songrec.dto;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class PlayCountPairsMap implements Writable{
    HashMap<Integer, List<PlayCountPair>> map;

    public PlayCountPairsMap() {
        map = new HashMap<Integer, List<PlayCountPair>>();
    }

    public PlayCountPairsMap(HashMap<Integer, List<PlayCountPair>> map) {
        this.map = map;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<Integer, List<PlayCountPair>> entry : map.entrySet()) {
            out.writeInt(entry.getKey());
            writePlayCountPairs(out, entry);
        }
    }

    private void writePlayCountPairs(DataOutput out, Map.Entry<Integer, List<PlayCountPair>> entry) throws IOException {
        List<PlayCountPair> pairs = entry.getValue();
        out.writeInt(pairs.size());
        for (PlayCountPair pair : pairs) {
            out.writeShort(pair.firstSongPlayCount());
            out.writeShort(pair.secondSongPlayCount());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        map.clear();
        int size = in.readInt();
        while (size > 0) {
            map.put(in.readInt(), readPlayCountPairs(in));
            size--;
        }
    }

    private List<PlayCountPair> readPlayCountPairs(DataInput in) throws IOException {
        ArrayList<PlayCountPair> list = new ArrayList<PlayCountPair>();
        int size = in.readInt();
        while (size > 0) {
            list.add(new PlayCountPair(in.readShort(), in.readShort()));
            size--;
        }
        return list;
    }

    public void addOrUpdate(SongPlayCount firstSong, SongPlayCount secondSong) {
        if (map.containsKey(secondSong.songId())) {
            List<PlayCountPair> playCountPairs = map.get(secondSong.songId());
            if(playCountPairs.size() < 10)
                playCountPairs.add(new PlayCountPair(firstSong.playCount(), secondSong.playCount()));
        } else {
            ArrayList<PlayCountPair> playCountPairs = new ArrayList<PlayCountPair>();
            playCountPairs.add(new PlayCountPair(firstSong.playCount(), secondSong.playCount()));
            map.put(secondSong.songId(), playCountPairs);
        }
    }

    public void merge(PlayCountPairsMap vector) {
        for (Map.Entry<Integer, List<PlayCountPair>> entry : vector.map.entrySet()) {
            if (map.containsKey(entry.getKey())) {
                map.get(entry.getKey()).addAll(entry.getValue());
            } else {
                map.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public Set<Map.Entry<Integer, List<PlayCountPair>>> entrySet() {
        return map.entrySet();
    }

    @Override
    public String toString() {
        String result = "";
        for (Map.Entry<Integer, List<PlayCountPair>> entry : map.entrySet()) {
            result += "[" + entry.getKey() + ":" + playCountsAsString(entry.getValue()) + "]";
        }
        return result;
    }

    public int size(){
        return map.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PlayCountPairsMap that = (PlayCountPairsMap) o;

        if (!map.equals(that.map)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    private String playCountsAsString(List<PlayCountPair> pairs) {
        String result = "";
        for (PlayCountPair pair : pairs) {
            result += "(" + pair.firstSongPlayCount() + "," + pair.secondSongPlayCount() + "),";
        }
        return result;
    }

    public HashMap<Integer, List<PlayCountPair>> getMap() {
        return map;
    }

    public static PlayCountPairsMap merge(Iterator<PlayCountPairsMap> iterator) {
        HashMap<Integer, List<PlayCountPair>> map = new HashMap<Integer, List<PlayCountPair>>();

        while (iterator.hasNext()) {
            PlayCountPairsMap playCountVector = iterator.next();
            for (Map.Entry<Integer, List<PlayCountPair>> entry : playCountVector.entrySet()) {
                if (map.containsKey(entry.getKey())) {
                    map.get(entry.getKey()).addAll(entry.getValue());
                } else {
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return new PlayCountPairsMap(map);
    }
}
