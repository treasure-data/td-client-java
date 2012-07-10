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
package com.treasure_data.model.bulkimport;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class UploadPartRequest extends BulkImportSpecifyRequest<Session> {

    public static enum Format {
        MSGPACKGZ, UNKNOWN,
    }

    public static String toFormatName(Format format) {
        switch (format) {
        case MSGPACKGZ:
            return "msgpack.gz";
        default:
            return "unknown";
        }
    }

    public static Format toFormat(String formatName) {
        if (formatName.equals("msgpack.gz")) {
            return Format.MSGPACKGZ;
        } else {
            return Format.UNKNOWN;
        }
    }

    private String partID;

    private Format format;

    private InputStream in;

    private int size;

    public UploadPartRequest(Session sess, String partID, byte[] bytes) {
        this(sess, partID, new ByteArrayInputStream(bytes), bytes.length);
    }

    public UploadPartRequest(Session sess, String partID, InputStream in, int size) {
        super(sess);
        this.partID = partID;
        this.format = Format.MSGPACKGZ;
        this.in = in;
        this.size = size;
    }

    public String getPartID() {
        return partID;
    }

    public Format getFormat() {
        return format;
    }

    public InputStream getInputStream() {
        return in;
    }

    public int getSize() {
        return size;
    }
}
