package org.ezstack.ezapp.datastore.api;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

public class KeyBuilder {
    public static String hashKey(String table, String key) {
        StringBuilder sb = new StringBuilder(table).append("~").append(key);
        return Hashing.murmur3_128().hashString(sb.toString(), Charsets.UTF_8).toString();
    }
}
