package org.ezstack.ezapp.datastore.api;

import com.google.common.base.CharMatcher;

public class Names {

    private static final CharMatcher TABLE_NAME_ALLOWED =
            CharMatcher.inRange('a', 'z')
                    .or(CharMatcher.inRange('0', '9'))
                    .or(CharMatcher.anyOf("-.:_"))
                    .precomputed();

    public static boolean isLegalTableName(String table) {
        return table != null &&
                table.length() > 0 && table.length() <= 255 &&
                table.charAt(0) != '_' &&
                !(table.charAt(0) == '.' && (".".equals(table) || "..".equals(table))) &&
                TABLE_NAME_ALLOWED.matchesAllOf(table);
    }

    public static boolean isLegalKey(String key) {
        return key != null && !key.isEmpty() && key.charAt(0) != '_';
    }
}
