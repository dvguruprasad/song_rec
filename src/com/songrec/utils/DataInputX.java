package com.songrec.utils;

import java.io.DataInput;
import java.io.IOException;

public class DataInputX {
    public static String readString(DataInput in) throws IOException {
        int index = 0;
        int length = in.readInt();
        char [] chars = new char[length];
        while(index < length) chars[index++] = in.readChar();
        return String.valueOf(chars);
    }
}
