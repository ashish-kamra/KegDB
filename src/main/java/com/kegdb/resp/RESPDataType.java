package com.kegdb.resp;

public enum RESPDataType {
    SIMPLE_STRING('+'), ERROR('-'), INTEGER(':'), BULK_STRING('$'), ARRAY('*');

    public final char firstByte;

    RESPDataType(char firstByte) {
        this.firstByte = firstByte;
    }
}
