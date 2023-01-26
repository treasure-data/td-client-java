/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.treasuredata.client;

import org.junit.Test;

import static com.google.common.net.UrlEscapers.urlPathSegmentEscaper;
import static org.junit.Assert.assertEquals;

/**
 * Ensure compatibility with com.google.common.net.UrlEscapers.urlPathSegmentEscaper;
 */
public class TestUrlEscaper
{
    @Test
    public void nonAscii()
    {
        String v = urlPathSegmentEscaper().escape("あ漢ü");
        assertEquals("%E3%81%82%E6%BC%A2%C3%BC", v);
    }

    @Test
    public void emoji()
    {
        String v = urlPathSegmentEscaper().escape("🇸🇨");
        assertEquals("%F0%9F%87%B8%F0%9F%87%A8", v);
    }

    @Test
    public void specialChars()
    {
        String v = urlPathSegmentEscaper().escape("-_.~!*'();:@&=+$,/?%#[]^¥|{}\\<> ");
        assertEquals("-_.~!*'();:@&=+$,%2F%3F%25%23%5B%5D%5E%C2%A5%7C%7B%7D%5C%3C%3E%20", v);
    }

    @Test
    public void alphaNum()
    {
        String v = urlPathSegmentEscaper().escape("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
        assertEquals("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", v);
    }
}
