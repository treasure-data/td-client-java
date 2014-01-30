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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Job extends AbstractModel {

    public static enum Type {
        HIVE("hive"), MAPRED("mapred"), IMPALA("impala"), PRESTO("presto"), UNKNOWN("none");

        private String type;

        Type(String type) {
            this.type = type;
        }

        public String type() {
            return this.type;
        }

        public static class StringToType {
            private static final Map<String, Type> REVERSE_DICTIONARY;

            static {
                Map<String, Type> map = new HashMap<String, Type>();
                for (Type elem : Type.values()) {
                    map.put(elem.type(), elem);
                }
                REVERSE_DICTIONARY = Collections.unmodifiableMap(map);
            }

            static Type get(String key) {
                return REVERSE_DICTIONARY.get(key);
            }
        }
    }

    public static Type toType(String typeName) {
        return Type.StringToType.get(typeName);
    }

    public static String toTypeName(Type type) {
        return type.type();
    }

    public static enum Priority {
        VERYLOW(-2), LOW(-1), NORMAL(0), HIGH(1), VERYHIGH(2);

        private int priority;

        Priority(int priority) {
            this.priority = priority;
        }

        public int getPriority() {
            return priority;
        }

        public static Priority fromInt(int p) {
            return IntToPriority.get(p);
        }

        private static class IntToPriority {
            private static final Map<Integer, Priority> REVERSE_DICTIONARY;
            static {
                Map<Integer, Priority> map = new HashMap<Integer, Priority>();
                for (Priority p : Priority.values()) {
                    map.put(p.getPriority(), p);
                }
                REVERSE_DICTIONARY = Collections.unmodifiableMap(map);
            }

            static Priority get(int p) {
                return REVERSE_DICTIONARY.get(p);
            }
        }
    }

    private Type type;

    private Database database;

    private String url;

    private String query;

    private String resultTable;

    private Priority priority = Priority.NORMAL;

    private int retryLimit = 0;

    public Job(String jobID) {
        this(jobID, Job.Type.HIVE, null, null, null);
    }

    public Job(Database database, String query) {
        this(database, query, null);
    }

    public Job(Database database, String query, Priority priority, int retryLimit) {
        this(null, Job.Type.HIVE, database, null, null);
        setQuery(query);
        setPriority(priority);
        setRetryLimit(retryLimit);
    }

    public Job(Database database, String query, String resultTable) {
        this(null, Job.Type.HIVE, database, null, null);
        setQuery(query);
    }

    public Job(Database database, Job.Type type, String query, String resultTable) {
        this(null, type, database, null, null);
        setQuery(query);
    }

    public Job(String jobID, Job.Type type, Database database, String url, String resultTable) {
        super(jobID);
        setType(type);
        setDatabase(database);
        setURL(url);
        setResultTable(resultTable);
    }

    public void setJobID(String jobID) {
        super.setName(jobID);
    }

    public String getJobID() {
        return super.getName();
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Job.Type getType() {
        return type;
    }

    public void setDatabase(Database database) {
        this.database = database;
    }

    public Database getDatabase() {
        return database;
    }

    public void setURL(String url) {
        this.url = url;
    }

    public String getURL() {
        return url;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    public void setPriority(Priority priority) {
        this.priority = priority;
    }

    public Priority getPriority() {
        return priority;
    }

    public void setRetryLimit(int retryLimit) {
        this.retryLimit = retryLimit;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public void setResultTable(String resultTable) {
        this.resultTable = resultTable;
    }

    public String getResultTable() {
        return resultTable;
    }
}
