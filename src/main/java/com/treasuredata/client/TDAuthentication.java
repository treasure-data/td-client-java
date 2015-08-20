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

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Authentication;
import org.eclipse.jetty.client.api.AuthenticationStore;
import org.eclipse.jetty.client.util.BasicAuthentication;

import java.net.URI;

/**
 *
 */
public abstract class TDAuthentication
{
    public abstract void configure(HttpClient client, URI uri);

    public static class PasswordAuthentication extends TDAuthentication {

        private final String user;
        private final String password;

        public PasswordAuthentication(String user, String password)
        {
            this.user = user;
            this.password = password;
        }

        @Override
        public void configure(HttpClient client, URI uri)
        {
            AuthenticationStore store = client.getAuthenticationStore();
            store.addAuthentication(new BasicAuthentication(
                    uri, "", user, password
            ));
        }
    }

}
