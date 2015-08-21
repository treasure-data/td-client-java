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
import org.eclipse.jetty.http.HttpMethod;
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
    private final HttpMethod method;
    private final String path;
    private final Map<String, String> queryParams;
    private final Map<String, String> headerParams;
    private final Optional<File> putFile;

    TDApiRequest(
            HttpMethod method,
            String path,
            Map<String, String> queryParams,
            Map<String, String> headerParams,
            Optional<File> putFile
    )
    {
        this.method = checkNotNull(method, "method is null");
        this.path = checkNotNull(path, "uri is null");
        this.queryParams = checkNotNull(queryParams, "queryParms is null");
        this.headerParams = checkNotNull(headerParams, "headerParams is null");
        this.putFile = checkNotNull(putFile, "putFile is null");
    }

    public String getPath()
    {
        return path;
    }

    public HttpMethod getMethod()
    {
        return method;
    }

    public Map<String, String> getQueryParams()
    {
        return queryParams;
    }

    public Map<String, String> getHeaderParams()
    {
        return headerParams;
    }

    public Optional<File> getPutFile()
    {
        return putFile;
    }

    public static class Builder
    {
        private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
        private HttpMethod method;
        private String path;
        private Map<String, String> queryParams;
        private Map<String, String> headerParams;
        private Optional<File> file = Optional.absent();

        Builder(HttpMethod method, String path)
        {
            this.method = method;
            this.path = path;
        }

        public static Builder GET(String uri)
        {
            return new Builder(HttpMethod.GET, uri);
        }

        public static Builder POST(String uri)
        {
            return new Builder(HttpMethod.POST, uri);
        }

        public static Builder PUT(String uri)
        {
            return new Builder(HttpMethod.PUT, uri);
        }

        public static Builder DELETE(String uri)
        {
            return new Builder(HttpMethod.DELETE, uri);
        }

        public Builder addHeader(String key, String value)
        {
            if (headerParams == null) {
                headerParams = new HashMap<>();
            }
            headerParams.put(urlEncode(key), urlEncode(value));
            return this;
        }

        public Builder addQueryParam(String key, String value)
        {
            if (queryParams == null) {
                queryParams = new HashMap<>();
            }
            queryParams.put(urlEncode(key), urlEncode(value));
            return this;
        }

        public Builder setFile(File file)
        {
            this.file = Optional.of(file);
            return this;
        }

        public TDApiRequest build()
        {
            return new TDApiRequest(
                    method,
                    path,
                    queryParams != null ? queryParams : EMPTY_MAP,
                    headerParams != null ? headerParams : EMPTY_MAP,
                    file
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
