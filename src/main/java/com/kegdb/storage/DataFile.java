package com.kegdb.storage;

import com.kegdb.Constants;
import com.kegdb.exception.KegException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataFile {
    private static final Logger logger = LoggerFactory.getLogger(DataFile.class);
    private final String fileDir;
    private final boolean sync;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    //private final boolean isWriter;
    private int fileId;
    private File dataFile;

    public DataFile(boolean sync, String dataDirectory) {
        this.fileDir = dataDirectory;
        this.sync = sync;
        initialize();
    }

    private void initialize() {
        File directory = new File(fileDir);
        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (!created) {
                logger.error("Failed to create data directory at {}", fileDir);
                throw new KegException("Failed to create data directory.");
            }
            logger.info("Data directory created at {}", fileDir);
        }

        this.fileId = getNewFileId();
        this.dataFile = createDataFile();
        logger.info("Initialized DataFile with fileId {}", fileId);
    }

    private int getNewFileId() {
        int maxId = -1;
        File directory = new File(fileDir);
        File[] files = directory.listFiles((dir, name) -> name.matches("keg_\\d+\\.db"));
        if (files != null) {
            for (File file : files) {
                String name = file.getName();
                int id = extractFileId(name);
                if (id > maxId) {
                    maxId = id;
                }
            }
        }
        return maxId + 1;
    }

    private int extractFileId(String fileName) {
        String idStr = fileName.replace("keg_", "").replace(".db", "");
        try {
            return Integer.parseInt(idStr);
        } catch (NumberFormatException e) {
            logger.warn("Invalid file name format: {}", fileName);
            return -1;
        }
    }

    private File createDataFile() {
        String fileName = String.format(Constants.DATAFILE_NAME, fileId);
        File file = new File(fileDir, fileName);
        try {
            if (!file.createNewFile()) {
                logger.error("Data file {} already exists.", file.getAbsolutePath());
                throw new KegException("Data file already exists: " + file.getAbsolutePath());
            }
            logger.info("Created new data file: {}", file.getAbsolutePath());
        } catch (IOException e) {
            logger.error("Failed to create data file {}: {}", file.getAbsolutePath(), e.getMessage());
            throw new KegException("Failed to create data file.", e);
        }
        return file;
    }

    public ValueMetadata write(byte[] buffer) throws IOException {
        checkFileSize();
        rwLock.writeLock().lock();
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
            long offset = raf.length();
            raf.seek(offset);
            raf.write(buffer);
            ValueMetadata meta = new ValueMetadata(fileId, offset, buffer.length, System.currentTimeMillis());
            logger.debug("Written data to fileId {}, offset {}, size {}", fileId, offset, buffer.length);
            if (sync) {
                raf.getFD().sync();
            }

            return meta;
        } catch (IOException e) {
            logger.error("Failed to write to data file {}: {}", dataFile.getName(), e.getMessage());
            throw e;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public DataFileEntry read(int fileId, long valuePosition, int valueSize) {
        rwLock.readLock().lock();
        String targetFileName = String.format(Constants.DATAFILE_NAME, fileId);
        File targetFile = new File(fileDir, targetFileName);

        if (!targetFile.exists()) {
            logger.error("Data file not found: {}", targetFile.getAbsolutePath());
            throw new KegException("Data file not found: " + targetFile.getAbsolutePath());
        }

        byte[] buffer = new byte[valueSize];
        try (RandomAccessFile raf = new RandomAccessFile(targetFile, "r")) {
            if (valuePosition + valueSize > raf.length()) {
                logger.error("Attempt to read beyond file length in file {}.", targetFile.getName());
                throw new KegException("Attempt to read beyond file length.");
            }
            raf.seek(valuePosition);
            int bytesRead = raf.read(buffer, 0, valueSize);
            if (bytesRead != valueSize) {
                logger.error("Incomplete read operation in file {}.", targetFile.getName());
                throw new KegException("Incomplete read operation.");
            }
            logger.debug("Read data from fileId {}, offset {}, size {}", fileId, valuePosition, valueSize);
            return DataFileEntry.convertFromBytes(buffer);
        } catch (IOException e) {
            logger.error("Failed to read from data file {}: {}", targetFile.getName(), e.getMessage());
            throw new KegException("Failed to read from data file.", e);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void checkFileSize() {
        if (dataFile.length() > Constants.MAX_FILE_SIZE) {
            logger.info("Data file {} exceeds max size ({} bytes). Creating a new data file.", dataFile.getName(), Constants.MAX_FILE_SIZE);
            this.dataFile = createDataFile();
            this.fileId++;
        }
    }
}
