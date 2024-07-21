package com.kegdb.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.zip.CRC32;

public class Checksum {

    public long generateChecksum(Serializable obj) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(obj);
        objectOutputStream.flush();

        byte[] data = byteArrayOutputStream.toByteArray();
        CRC32 crc = new CRC32();
        crc.update(data);

        return crc.getValue();
    }

    public boolean verifyChecksum(Serializable obj, long expectedChecksum) throws IOException {
        long actualChecksum = generateChecksum(obj);
        return actualChecksum == expectedChecksum;
    }
}
