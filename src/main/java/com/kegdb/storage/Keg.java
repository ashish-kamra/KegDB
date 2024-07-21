package com.kegdb.storage;

import com.kegdb.resp.RESPDataType;
import com.kegdb.resp.RESPObject;
import com.kegdb.resp.RESPParser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.kegdb.Constants.ARGS_ERROR_MESSAGE;

public class Keg {
    /*private final Map<String, ValueMetadata> keydir;
    private final DataFile dataFile;
    private final RESPParser respParser;

    public Keg(DataFile dataFile) {
        this.keydir = new HashMap<>();
        this.dataFile = dataFile;
        this.respParser = new RESPParser();
    }*/

    public RESPObject storeOrRetrieve(RESPObject respObject) {
        List<RESPObject> args = (List<RESPObject>) respObject.getValue();
        String command = (String) args.get(0).getValue();
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
        return null;
    }

    private RESPObject set(List<RESPObject> respObjects) {
        return null;
    }

    private RESPObject delete(List<RESPObject> respObjects) {
        return null;
    }

    private record ValueMetadata(int fileId, int valueSize, int valuePosition, long timestamp) {
    }
}
