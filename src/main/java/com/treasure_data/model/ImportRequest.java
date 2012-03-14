package com.treasure_data.model;

public class ImportRequest extends AbstractRequest<Table> {

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

    private Table table;

    private Format format;

    private byte[] bytes;

    public ImportRequest(Table table, Format format, byte[] bytes) {
        this.table = table;
        this.format = format;
        this.bytes = bytes;
    }

    public Table getTable() {
        return table;
    }

    public Format getFormat() {
        return format;
    }

    public byte[] getBytes() {
        return bytes;
    }
}
