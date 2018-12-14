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

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An abstraction of TD API request, which will be translated to Jetty's http client request.
 * We need this abstraction to create multiple http request objects upon API call retry since
 * Jetty's Request instances are not reusable.
 */
public class TDApiRequest
{
    private static Logger logger = LoggerFactory.getLogger(TDApiRequest.class);
    private final TDHttpMethod method;
    private final String path;
    private final Map<String, String> queryParams;
    private final Multimap<String, String> headerParams;
    private final Optional<String> postJson;
    private final Optional<File> putFile;
    private final Optional<byte[]> content;
    private final Optional<Boolean> followRedirects;
    private final int contentOffset;
    private final int contentLength;

    TDApiRequest(
            TDHttpMethod method,
            String path,
            Map<String, String> queryParams,
            Multimap<String, String> headerParams,
            Optional<String> postJson,
            Optional<File> putFile,
            Optional<byte[]> content,
            int contentOffset,
            int contentLength,
            Optional<Boolean> followRedirects
    )
    {
        this.method = checkNotNull(method, "method is null");
        this.path = checkNotNull(path, "uri is null");
        this.queryParams = checkNotNull(queryParams, "queryParms is null");
        this.headerParams = checkNotNull(headerParams, "headerParams is null");
        this.postJson = checkNotNull(postJson, "postJson is null");
        this.putFile = checkNotNull(putFile, "putFile is null");
        this.content = checkNotNull(content, "content is null");
        this.contentOffset = contentOffset;
        this.contentLength = contentLength;
        this.followRedirects = checkNotNull(followRedirects, "followRedirects is null");
    }

    public TDApiRequest withUri(String uri)
    {
        return new TDApiRequest(method, uri, ImmutableMap.copyOf(queryParams), ImmutableMultimap.copyOf(headerParams), postJson, putFile, content, contentOffset, contentLength, followRedirects);
    }

    public String getPath()
    {
        return path;
    }

    public TDHttpMethod getMethod()
    {
        return method;
    }

    public Map<String, String> getQueryParams()
    {
        return queryParams;
    }

    public Multimap<String, String> getHeaderParams()
    {
        return headerParams;
    }

    public Optional<String> getPostJson()
    {
        return postJson;
    }

    public Optional<File> getPutFile()
    {
        return putFile;
    }

    public Optional<byte[]> getContent()
    {
        return content;
    }

    public int getContentOffset()
    {
        return contentOffset;
    }

    public int getContentLength()
    {
        return contentLength;
    }

    public Optional<Boolean> getFollowRedirects()
    {
        return followRedirects;
    }

    public static class Builder
    {
        private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
        private static final Multimap<String, String> EMPTY_HEADERS = ImmutableMultimap.of();
        private TDHttpMethod method;
        private String path;
        private Map<String, String> queryParams;
        private ImmutableMultimap.Builder<String, String> headerParams;
        private Optional<String> postJson = Optional.absent();
        private Optional<File> file = Optional.absent();
        private Optional<byte[]> content = Optional.absent();
        private int contentOffset;
        private int contentLength;
        private Optional<Boolean> followRedirects = Optional.absent();

        Builder(TDHttpMethod method, String path)
        {
            this.method = method;
            this.path = path;
        }

        public static Builder GET(String uri)
        {
            return new Builder(TDHttpMethod.GET, uri);
        }

        public static Builder POST(String uri)
        {
            return new Builder(TDHttpMethod.POST, uri);
        }

        public static Builder PUT(String uri)
        {
            return new Builder(TDHttpMethod.PUT, uri);
        }

        public static Builder DELETE(String uri)
        {
            return new Builder(TDHttpMethod.DELETE, uri);
        }

        public Builder addHeader(String key, String value)
        {
            if (headerParams == null) {
                headerParams = ImmutableMultimap.builder();
            }
            headerParams.put(key, value);
            return this;
        }

        public Builder addHeaders(Multimap<String, String> headers)
        {
            if (headerParams == null) {
                headerParams = ImmutableMultimap.builder();
            }
            headerParams.putAll(headers);
            return this;
        }

        public Builder addQueryParam(String key, String value)
        {
            if (queryParams == null) {
                queryParams = new HashMap<>();
            }
            queryParams.put(key, value);
            return this;
        }

        public Builder setPostJson(String json)
        {
            this.postJson = Optional.of(json);
            return this;
        }

        public Builder setFile(File file)
        {
            this.file = Optional.of(file);
            return this;
        }

        public Builder setContent(byte[] content, int offset, int length)
        {
            this.content = Optional.of(content);
            this.contentOffset = offset;
            this.contentLength = length;
            return this;
        }

        public Builder setFollowRedirects(boolean followRedirects)
        {
            this.followRedirects = Optional.of(followRedirects);
            return this;
        }

        public TDApiRequest build()
        {
            return new TDApiRequest(
                    method,
                    path,
                    queryParams != null ? queryParams : EMPTY_MAP,
                    headerParams != null ? headerParams.build() : EMPTY_HEADERS,
                    postJson,
                    file,
                    content,
                    contentOffset,
                    contentLength,
                    followRedirects
            );
        }
    }

    public static String urlEncode(String value)
    {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
    }
}
