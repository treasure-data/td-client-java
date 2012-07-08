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
