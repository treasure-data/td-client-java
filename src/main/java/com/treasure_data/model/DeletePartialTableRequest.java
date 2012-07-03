//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2012 Muga Nishizawa
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

public class DeletePartialTableRequest extends TableSpecifyRequest<Table> {

    private long from;

    private long to;

    public DeletePartialTableRequest(Table table, long from, long to) {
        super(table);
        this.from = from;
        this.to = to;
        validateParameters();
    }

    private void validateParameters() {
        if (to <= 0) {
            throw new IllegalArgumentException(String.format(
                    "'to' must be a positive number: to=%d", to));
        } else if (to % 3600 != 0) {
            throw new IllegalArgumentException(String.format(
                    "'to' parameter must be a multiple of 3600: to=%d", to));
        }

        if (from < 0) {
            throw new IllegalArgumentException(String.format(
                    "'from' must be a positive number (>= 0): from=%d", from));
        } else if (to - from <= 0) {
            throw new IllegalArgumentException(String.format(
                    "'from' must be less than 'to': from=%d", from));
        } else if (from % 3600 != 0) {
            throw new IllegalArgumentException(String.format(
                    "'from' parameter must be a multiple of 3600: from=%d", from));
        }
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }
}
