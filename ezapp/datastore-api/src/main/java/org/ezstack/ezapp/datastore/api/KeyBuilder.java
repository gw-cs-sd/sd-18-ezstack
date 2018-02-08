package org.ezstack.ezapp.datastore.api;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

public class KeyBuilder {
    public static String hashKey(String table, String key) {
        // These hash keys do not need to be clusterable by table, as there are no range queries,
        // hence, it makes more sense to put the key hash first as it will likely provide better distribution
        return hash(key, table);
    }

    public static String hash(String... inputs) {
        StringBuilder builder = new StringBuilder();
        for (String input : inputs) {
            builder.append(Hashing.murmur3_128().hashString(input, Charsets.UTF_8).toString());
        }
        return builder.toString();
    }
}
