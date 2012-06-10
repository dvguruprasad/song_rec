package com.songrec.utils;

import com.google.common.hash.Hashing;

public class HashingX {
    public static int hash(int value){
        return 0x7FFFFFFF & Hashing.sha1().newHasher().putInt(value).hash().asInt();
    }

    public static int hash(String value){
        return 0x7FFFFFFF & Hashing.sha1().newHasher().putString(value).hash().asInt();
    }

    public static int hash_sha1(String value){
        return 0x7FFFFFFF & Hashing.sha1().newHasher().putString(value).hash().asInt();
    }

    public static int hash_goodFastHash(String value){
        return 0x7FFFFFFF & Hashing.goodFastHash(64).newHasher().putString(value).hash().asInt();
    }
}
