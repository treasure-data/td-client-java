package com.treasuredata.client;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class UrlPathSegmentEscaper
{
    private UrlPathSegmentEscaper()
    {
    }

    private static String replaceWithMap(String text, Pattern pattern, Map<String, String> map)
    {
        Matcher matcher = pattern.matcher(text);
        if (!matcher.find()) {
            return text;
        }
        StringBuilder sb = new StringBuilder();
        int previousEnd = 0;
        do {
            sb.append(text.subSequence(previousEnd, matcher.start()));
            sb.append(map.get(matcher.toMatchResult().group()));
            previousEnd = matcher.end();
        } while (matcher.find());
        sb.append(text.subSequence(previousEnd, text.length()));
        return sb.toString();
    }

    private static final Pattern NO_ESCAPE_CHARS = Pattern.compile("\\+|%(?:2[146789BC]|3[ABD]|7E|40)");
    private static final Map<String, String> REPLACEMENT_TABLE;
    static {
        Map<String, String> m = new HashMap<>();
        m.put("+", "%20");
        m.put("%21", "!");
        m.put("%24", "$");
        m.put("%26", "&");
        m.put("%27", "'");
        m.put("%28", "(");
        m.put("%29", ")");
        m.put("%2B", "+");
        m.put("%2C", ",");
        m.put("%3A", ":");
        m.put("%3B", ";");
        m.put("%3D", "=");
        m.put("%7E", "~");
        m.put("%40", "@");
        REPLACEMENT_TABLE = Collections.unmodifiableMap(m);
    }

    static String escape(String s)
    {
        try {
            String encoded = URLEncoder.encode(s, "UTF-8");
            return replaceWithMap(encoded, NO_ESCAPE_CHARS, REPLACEMENT_TABLE);
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
