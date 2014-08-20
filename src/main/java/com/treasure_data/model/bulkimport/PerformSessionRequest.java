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
package com.treasure_data.model.bulkimport;

import com.treasure_data.model.Job;

public class PerformSessionRequest extends BulkImportSpecifyRequest<Session> {
    private Job.Priority priority;

    public PerformSessionRequest(Session sess) {
        this(sess, Job.Priority.NORMAL);
    }

    public PerformSessionRequest(Session sess, Job.Priority priority) {
        super(sess);
        this.priority = priority;
    }

    public void setPriority(Job.Priority priority) {
        this.priority = priority;
    }

    public Job.Priority getPriority() {
        return priority;
    }

}
