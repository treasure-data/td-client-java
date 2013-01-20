//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2013 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.treasure_data.model;

import com.treasure_data.auth.TreasureDataCredentials;

public abstract class AbstractRequest<T extends AbstractModel> implements Request<T> {
    private TreasureDataCredentials credentials;

    private T model;

    protected AbstractRequest(T model) {
        this.model = model;
    }

    public TreasureDataCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(TreasureDataCredentials credentials) {
        this.credentials = credentials;
    }

    protected T get() {
        return model;
    }
}
