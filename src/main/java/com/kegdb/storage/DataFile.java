package com.kegdb.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static com.kegdb.Constants.*;

public class DataFile {
    private final String fileDir;
    private final boolean sync;
    //private final boolean isWriter;
    private int fileId;
    private File dataFile;

    public DataFile(boolean sync) {
        this.fileDir = DATAFILES_DIR;
        this.sync = sync;
        //this.isWriter = isWriter;
        this.fileId = getNewFileId();
        this.dataFile = createDataFile();
    }

    public ValueMetadata write(byte[] buffer) throws IOException {
        checkFileSize();
        try (RandomAccessFile file = new RandomAccessFile(dataFile, "rw")) {
            long offset = file.length();
            file.seek(offset);
            file.write(buffer);
            return new ValueMetadata(fileId, offset, buffer.length, System.currentTimeMillis());
        }
    }

    public DataFileEntry read(int fileId, long valuePosition, int valueSize) {
        byte[] buffer = new byte[valueSize];
        try (RandomAccessFile file = new RandomAccessFile(fileDir + "/" + String.format(DATAFILE_NAME, fileId), "r")) {
            file.seek(valuePosition);
            file.read(buffer, 0, valueSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return DataFileEntry.convertFromBytes(buffer);
    }

    private int getNewFileId() {
        int fileCount = 0;
        File directory = new File(fileDir);
        if (!(directory.exists() || directory.isDirectory())) {
            directory.mkdir();
            return -1;
        }
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    fileCount++;
                }
            }
        }
        return fileCount - 1;
    }

    private void checkFileSize() {
        if (dataFile.length() > MAX_FILE_SIZE) {
            this.dataFile = createDataFile();
        }
    }

    private File createDataFile() {
        return new File(fileDir + "/" + String.format(DATAFILE_NAME, ++fileId));
    }


}
