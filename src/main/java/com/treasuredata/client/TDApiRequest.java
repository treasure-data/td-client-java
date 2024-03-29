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

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Collections;

import static java.util.Objects.requireNonNull;

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
    private final Map<String, Collection<String>> headerParams;
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
            Map<String, Collection<String>> headerParams,
            Optional<String> postJson,
            Optional<File> putFile,
            Optional<byte[]> content,
            int contentOffset,
            int contentLength,
            Optional<Boolean> followRedirects
    )
    {
        this.method = requireNonNull(method, "method is null");
        this.path = requireNonNull(path, "uri is null");
        this.queryParams = requireNonNull(queryParams, "queryParms is null");
        this.headerParams = requireNonNull(headerParams, "headerParams is null");
        this.postJson = requireNonNull(postJson, "postJson is null");
        this.putFile = requireNonNull(putFile, "putFile is null");
        this.content = requireNonNull(content, "content is null");
        this.contentOffset = contentOffset;
        this.contentLength = contentLength;
        this.followRedirects = requireNonNull(followRedirects, "followRedirects is null");
    }

    public TDApiRequest withUri(String uri)
    {
        return new TDApiRequest(method, uri, Collections.unmodifiableMap(new HashMap<>(queryParams)), Collections.unmodifiableMap(new HashMap<>(headerParams)), postJson, putFile, content, contentOffset, contentLength, followRedirects);
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

    /**
     * @deprecated Use {@link #getAllHeaders()} instead.
     * @return
     */
    @Deprecated
    public Multimap<String, String> getHeaderParams()
    {
        ImmutableMultimap.Builder<String, String> builder = new ImmutableMultimap.Builder<>();
        for (Map.Entry<String, Collection<String>> e : headerParams.entrySet()) {
            builder.putAll(e.getKey(), e.getValue());
        }
        return builder.build();
    }

    public Map<String, Collection<String>> getAllHeaders()
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
        private static final Map<String, String> EMPTY_MAP = Collections.emptyMap();
        private static final Map<String, Collection<String>> EMPTY_HEADERS = Collections.emptyMap();
        private TDHttpMethod method;
        private String path;
        private Map<String, String> queryParams;
        private HashMap<String, Collection<String>> headerParams;
        private Optional<String> postJson = Optional.empty();
        private Optional<File> file = Optional.empty();
        private Optional<byte[]> content = Optional.empty();
        private int contentOffset;
        private int contentLength;
        private Optional<Boolean> followRedirects = Optional.empty();

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
            return addHeaders(key, Collections.singletonList(value));
        }

        /**
         * @deprecated Use {@link #addHeaders(Map)} or {@link #addHeaders(String, Collection)} instead.
         * @param headers
         * @return
         */
        @Deprecated
        public Builder addHeaders(Multimap<String, String> headers)
        {
            return this.addHeaders(headers.asMap());
        }

        public Builder addHeaders(String key, Collection<String> values)
        {
            if (headerParams == null) {
                headerParams = new HashMap<>();
            }
            addHeaderValues(key, values);
            return this;
        }

        public Builder addHeaders(Map<String, ? extends Collection<String>> headers)
        {
            if (headerParams == null) {
                headerParams = new HashMap<>();
            }
            for (Map.Entry<String, ? extends Collection<String>> e : headers.entrySet()) {
                addHeaderValues(e.getKey(), e.getValue());
            }
            return this;
        }

        private void addHeaderValues(String key, Collection<String> values)
        {
            headerParams.compute(key, (unused, list) -> {
                if (list == null) {
                    return new ArrayList<>(values);
                }
                else {
                    list.addAll(values);
                    return list;
                }
            });
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
                    headerParams != null ? Collections.unmodifiableMap(new HashMap<>(headerParams)) : EMPTY_HEADERS,
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
            throw new RuntimeException(e);
        }
    }
}
