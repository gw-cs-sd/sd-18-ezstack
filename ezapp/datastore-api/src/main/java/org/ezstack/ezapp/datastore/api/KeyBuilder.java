package org.ezstack.ezapp.datastore.api;

import com.google.common.hash.Hashing;

import java.nio.charset.Charset;

public class KeyBuilder {
    public static String hashKey(String table, String key) {
        return hash(table) + hash(key);
    }

    private static String hash(String input) {
        return Hashing.murmur3_128().hashString(input, Charset.defaultCharset()).toString();
    }
}
