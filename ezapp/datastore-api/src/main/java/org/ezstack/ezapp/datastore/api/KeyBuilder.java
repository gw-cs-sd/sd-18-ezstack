package org.ezstack.ezapp.datastore.api;

import com.google.common.hash.Hashing;

import java.nio.charset.Charset;

public class KeyBuilder {
    public static String hashKey(String table, String key) {
        StringBuilder sb = new StringBuilder(table).append("~").append(key);
        return Hashing.murmur3_128().hashString(sb.toString(), Charset.defaultCharset()).toString();
    }
}
