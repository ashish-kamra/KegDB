package com.kegdb.storage;

import com.kegdb.Config;
import com.kegdb.resp.RESPDataType;
import com.kegdb.resp.RESPObject;
import com.kegdb.util.Checksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.kegdb.Constants.ARGS_ERROR_MESSAGE;
import static com.kegdb.Constants.CHECKSUM_ERROR_MESSAGE;

public class Keg {
    private static final Logger logger = LoggerFactory.getLogger(Keg.class);
    private final ConcurrentHashMap<String, ValueMetadata> keyDir;
    private final DataFile dataFile;

    public Keg() {
        String dataDir = Config.getDataDirectory();
        this.dataFile = new DataFile(false, dataDir);
        this.keyDir = new ConcurrentHashMap<>();
        logger.info("Keg initialized with data directory {}", dataDir);
    }

    public RESPObject storeOrRetrieve(RESPObject respObject) {
        List<RESPObject> args = (List<RESPObject>) respObject.getValue();
        String command = (String) args.getFirst().getValue();
        return switch (command.toUpperCase()) {
            case "PING" -> ping(args);
            case "GET" -> get(args);
            case "SET" -> set(args);
            case "DEL", "DELETE" -> delete(args);
            default -> new RESPObject(RESPDataType.SIMPLE_STRING, "Invalid command");
        };
    }

    private RESPObject ping(List<RESPObject> respObjects) {
        if (respObjects.size() == 1) {
            return new RESPObject(RESPDataType.SIMPLE_STRING, "PONG");
        } else if (respObjects.size() == 2) {
            return new RESPObject(RESPDataType.SIMPLE_STRING, respObjects.get(1).getValue());
        }
        return new RESPObject(RESPDataType.ERROR, String.format(ARGS_ERROR_MESSAGE, "PING"));
    }

    private RESPObject get(List<RESPObject> respObjects) {
        if (respObjects.size() != 2) {
            logger.warn("GET command received with incorrect number of arguments.");
            return new RESPObject(RESPDataType.ERROR, String.format(ARGS_ERROR_MESSAGE, "GET"));
        }
        var key = (String) respObjects.get(1).getValue();
        logger.debug("Fetching key {}", key);
        if (!keyDir.containsKey(key)) {
            return new RESPObject(RESPDataType.NULL, null);
        }
        var meta = keyDir.get(key);
        var dataFileEntry = dataFile.read(meta.fileId(), meta.recordPosition(), meta.recordSize());
        if (Checksum.verifyChecksum(dataFileEntry.value(), dataFileEntry.checksum())) {
            logger.debug("Checksum verified for key '{}'.", key);
            return new RESPObject(RESPDataType.BULK_STRING, dataFileEntry.value());
        }
        return new RESPObject(RESPDataType.ERROR, CHECKSUM_ERROR_MESSAGE);
    }

    private RESPObject set(List<RESPObject> respObjects) {
        if (respObjects.size() != 3) {
            return new RESPObject(RESPDataType.ERROR, String.format(ARGS_ERROR_MESSAGE, "SET"));
        }

        String key = (String) respObjects.get(1).getValue();
        String value = (String) respObjects.get(2).getValue();

        try {
            storeValue(key, value);
            logger.debug("SET operation successful for key '{}'.", key);
        } catch (IOException e) {
            logger.error("SET operation failed: {}", e.getMessage(), e);
            return new RESPObject(RESPDataType.ERROR, "ERR internal error");
        }

        return new RESPObject(RESPDataType.SIMPLE_STRING, "OK");
    }

    private void storeValue(String key, String value) throws IOException {
        long checksum = Checksum.generateChecksum(value);
        byte[] dataFileEntry = DataFileEntry.getDataFileEntry(checksum, key, value);
        ValueMetadata meta = dataFile.write(dataFileEntry);
        keyDir.put(key, meta);
    }

    private RESPObject delete(List<RESPObject> respObjects) {
        if (respObjects.size() != 2) {
            logger.warn("DELETE command received with incorrect number of arguments.");
            return new RESPObject(RESPDataType.ERROR, String.format(ARGS_ERROR_MESSAGE, "DELETE"));
        }

        String key = (String) respObjects.get(1).getValue();
        ValueMetadata entry = keyDir.remove(key);

        if (entry == null) {
            return new RESPObject(RESPDataType.INTEGER, 0);
        }

        try {
            String deletedValue = "";
            long checksum = Checksum.generateChecksum(deletedValue);
            byte[] dataFileEntry = DataFileEntry.getDataFileEntry(checksum, key, deletedValue);
            dataFile.write(dataFileEntry);
            logger.debug("DELETE operation successful for key '{}'.", key);
        } catch (IOException e) {
            logger.error("Error writing delete marker for key '{}': {}", key, e.getMessage());
            return new RESPObject(RESPDataType.ERROR, "ERR internal error");
        }

        return new RESPObject(RESPDataType.INTEGER, 1);
    }

}
