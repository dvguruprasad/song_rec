package com.songrec.utils;

import java.io.DataOutput;
import java.io.IOException;

public class DataOutputX {
    public static void writeString(DataOutput out, String str) throws IOException {
        out.writeInt(str.length());
        out.writeChars(str);
    }
}
