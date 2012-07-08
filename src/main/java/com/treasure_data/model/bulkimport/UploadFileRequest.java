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

public class UploadFileRequest extends BulkImportSpecifyRequest<Session> {

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

    private String fileID;

    private Format format;

    private byte[] bytes;

    public UploadFileRequest(Session sess, String fileID, byte[] bytes) {
        super(sess);
        this.fileID = fileID;
        this.format = Format.MSGPACKGZ;
        this.bytes = bytes;
    }

    public String getFileID() {
        return fileID;
    }

    public Format getFormat() {
        return format;
    }

    public byte[] getBytes() {
        return bytes;
    }
}
