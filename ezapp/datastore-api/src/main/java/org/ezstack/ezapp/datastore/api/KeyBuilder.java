package org.ezstack.ezapp.datastore.api;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

public class KeyBuilder {
    public static String hashKey(String table, String key) {
        return hash(table, key);
    }

    private static String hash(String... inputs) {
        StringBuilder builder = new StringBuilder();
        for (String input : inputs) {
            builder.append(Hashing.murmur3_128().hashString(input, Charsets.UTF_8).toString());
        }
        return builder.toString();
    }
}
