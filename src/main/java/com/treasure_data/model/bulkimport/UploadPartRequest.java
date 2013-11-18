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

public class UploadPartRequest extends BulkImportSpecifyRequest<Session> {

    public static enum Format {
        MSGPACKGZ,
    }

    public static String toFormatName(Format format) {
        switch (format) {
        case MSGPACKGZ:
            return "msgpack.gz";
        default:
            return "msgpack.gz";
        }
    }

    public static Format toFormat(String formatName) {
        if (formatName.equals("msgpack.gz")) {
            return Format.MSGPACKGZ;
        } else {
            return Format.MSGPACKGZ;
        }
    }

    private String partID;

    private Format format;

    protected boolean isMemoryData;
    protected byte[] memoryData;
    protected String partFileName;

    public UploadPartRequest(Session sess, String partID, byte[] bytes) {
        this(sess, partID, true, bytes, null);
    }

    public UploadPartRequest(Session sess, String partID, String partFileName) {
        this(sess, partID, false, null, partFileName);
    }

    private UploadPartRequest(Session sess, String partID, boolean isMemoryData, byte[] bytes, String partFileName) {
        super(sess);
        this.isMemoryData = isMemoryData;
        this.partID = partID;
        this.memoryData = bytes;
        this.partFileName = partFileName;
    }

    public Format getFormat() {
        return format;
    }

    public boolean isMemoryData() {
        return isMemoryData;
    }

    public String getPartID() {
        return partID;
    }

    public byte[] getMemoryData() {
        return memoryData;
    }

    public String getPartFileName() {
        return partFileName;
    }
}
