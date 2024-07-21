package com.kegdb.resp;

public class RESPObject {

    RESPDataType RESPDataType;
    Object value;

    public RESPObject(RESPDataType RESPDataType, Object value) {
        this.RESPDataType = RESPDataType;
        this.value = value;
    }

    public RESPDataType getType() {
        return RESPDataType;
    }

    public void setType(RESPDataType RESPDataType) {
        this.RESPDataType = RESPDataType;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "RESPObject{" +
                "type=" + RESPDataType +
                ", value=" + value +
                '}';
    }
}
