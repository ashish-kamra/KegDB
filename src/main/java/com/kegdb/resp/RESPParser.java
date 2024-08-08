package com.kegdb.resp;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.kegdb.Constants.CRLF;

public class RESPParser {

    public String serialize(RESPObject obj) {
        StringBuilder sb = new StringBuilder();
        switch (obj.RESPDataType) {
            case SIMPLE_STRING:
                sb.append("+").append(obj.value).append(CRLF);
                break;
            case ERROR:
                sb.append("-").append(obj.value).append(CRLF);
                break;
            case INTEGER:
                sb.append(":").append(obj.value).append(CRLF);
                break;
            case BULK_STRING:
                String str = (String) obj.value;
                sb.append("$").append(str.length()).append(CRLF);
                sb.append(str).append(CRLF);
                break;
            case NULL:
                sb.append("_").append(CRLF);
                break;
            case ARRAY:
                List<RESPObject> list = (List<RESPObject>) obj.value;
                sb.append("*").append(list.size()).append(CRLF);
                for (RESPObject item : list) {
                    sb.append(serialize(item));
                }
                break;
        }
        return sb.toString();
    }

    public RESPObject deserialize(BufferedReader reader) throws IOException {
        int firstByte = reader.read();
        if (firstByte == -1) {
            return null;
        }

        char type = (char) firstByte;
        String line = reader.readLine();
        switch (type) {
            case '+':
                return new RESPObject(RESPDataType.SIMPLE_STRING, line);
            case '-':
                return new RESPObject(RESPDataType.ERROR, line);
            case ':':
                return new RESPObject(RESPDataType.INTEGER, Long.parseLong(line));
            case '$':
                int length = Integer.parseInt(line);
                if (length == -1) {
                    return new RESPObject(RESPDataType.BULK_STRING, null);
                }
                char[] bulk = new char[length];
                reader.read(bulk, 0, length);
                reader.readLine(); // consume CRLF \r\n
                return new RESPObject(RESPDataType.BULK_STRING, new String(bulk));
            case '*':
                int count = Integer.parseInt(line);
                List<RESPObject> array = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    array.add(deserialize(reader));
                }
                return new RESPObject(RESPDataType.ARRAY, array);
            default:
                throw new IOException("Unknown RESP type: " + type);
        }
    }
}