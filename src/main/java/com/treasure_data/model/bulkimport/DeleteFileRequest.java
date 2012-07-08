package com.treasure_data.model.bulkimport;

public class DeleteFileRequest extends BulkImportSpecifyRequest<Session> {

    private String fileID;

    public DeleteFileRequest(Session sess, String fileID) {
        super(sess);
        this.fileID = fileID;
    }

    public String getFileID() {
        return fileID;
    }
}
