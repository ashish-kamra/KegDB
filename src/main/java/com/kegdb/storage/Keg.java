package com.kegdb.storage;

import com.kegdb.resp.RESPDataType;
import com.kegdb.resp.RESPObject;
import com.kegdb.resp.RESPParser;
import com.kegdb.util.Checksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.kegdb.Constants.ARGS_ERROR_MESSAGE;
import static com.kegdb.Constants.CHECKSUM_ERROR_MESSAGE;

public class Keg {
    private static final Logger logger = LoggerFactory.getLogger(Keg.class);
    private final Map<String, ValueMetadata> keyDir;
    private final DataFile dataFile;

    public Keg() {
        this.keyDir = new HashMap<>();
        this.dataFile = new DataFile(false);
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
            return new RESPObject(RESPDataType.BULK_STRING, dataFileEntry.value());
        }
        return new RESPObject(RESPDataType.ERROR, CHECKSUM_ERROR_MESSAGE);
    }

    private RESPObject set(List<RESPObject> respObjects) {
        try {
            if (respObjects.size() != 3) {
                return new RESPObject(RESPDataType.ERROR, String.format(ARGS_ERROR_MESSAGE, "SET"));
            }
            var key = (String) respObjects.get(1).getValue();
            var value = (String) respObjects.get(2).getValue();
            var checksum = Checksum.generateChecksum(value);
            byte[] dataFileEntry = DataFileEntry.getDataFileEntry(checksum, key, value);
            var meta = dataFile.write(dataFileEntry);
            keyDir.put(key, meta);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new RESPObject(RESPDataType.SIMPLE_STRING, "OK");
    }

    private RESPObject delete(List<RESPObject> respObjects) {
        return null;
    }

}
