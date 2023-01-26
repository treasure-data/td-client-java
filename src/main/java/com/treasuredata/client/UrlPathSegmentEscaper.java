package com.treasuredata.client;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Escape URL path segment in a compatible way with com.google.common.net.UrlEscapers.urlPathSegmentEscaper;
 */
final class UrlPathSegmentEscaper
{
    private UrlPathSegmentEscaper()
    {
    }

    private static String replaceAllMatched(String text, Pattern pattern)
    {
        Matcher matcher = pattern.matcher(text);
        if (!matcher.find()) {
            return text;
        }
        StringBuilder sb = new StringBuilder();
        int previousEnd = 0;
        do {
            sb.append(text.subSequence(previousEnd, matcher.start()));
            sb.append(replace(matcher.group()));
            previousEnd = matcher.end();
        } while (matcher.find());
        sb.append(text.subSequence(previousEnd, text.length()));
        return sb.toString();
    }

    private static final Pattern GUAVA_INCOMPATIBLE = Pattern.compile("\\+|%(?:2[146789BC]|3[ABD]|7E|40)");

    private static String replace(String token)
    {
        switch (token) {
            case "+":   return "%20";
            case "%21": return "!";
            case "%24": return "$";
            case "%26": return "&";
            case "%27": return "'";
            case "%28": return "(";
            case "%29": return ")";
            case "%2B": return "+";
            case "%2C": return ",";
            case "%3A": return ":";
            case "%3B": return ";";
            case "%3D": return "=";
            case "%7E": return "~";
            case "%40": return "@";
            default:    throw new IllegalStateException("Unknown token: " + token);
        }
    }

    static String escape(String s)
    {
        try {
            String encoded = URLEncoder.encode(s, "UTF-8");
            return replaceAllMatched(encoded, GUAVA_INCOMPATIBLE);
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
