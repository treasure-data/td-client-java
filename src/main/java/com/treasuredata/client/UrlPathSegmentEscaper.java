package com.treasuredata.client;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

final class UrlPathSegmentEscaper
{
    private UrlPathSegmentEscaper()
    {
    }

    static String escape(String s)
    {
        try {
            String encoded = URLEncoder.encode(s, "UTF-8");
            return encoded
                    .replaceAll("\\+", "%20")
                    .replaceAll("%21", "!")
                    .replaceAll("%24", "\\$")
                    .replaceAll("%26", "&")
                    .replaceAll("%27", "'")
                    .replaceAll("%28", "(")
                    .replaceAll("%29", ")")
                    .replaceAll("%2B", "+")
                    .replaceAll("%2C", ",")
                    .replaceAll("%3A", ":")
                    .replaceAll("%3B", ";")
                    .replaceAll("%3D", "=")
                    .replaceAll("%7E", "~")
                    .replaceAll("%40", "@");
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
