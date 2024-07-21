package com.kegdb.storage;

import java.io.Serial;
import java.io.Serializable;

public class DataFile {
    private final String fileDir;
    private final int fileId;
    private final boolean sync;
    private final boolean isWriter;

    public DataFile(String fileDir, int fileId, boolean sync, boolean isWriter) {
        this.fileDir = fileDir;
        this.fileId = fileId;
        this.sync = sync;
        this.isWriter = isWriter;
    }

    private record DataFileEntry(long checksum, long timestamp, int keySize, int valueSize, String key, String value) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }
}
