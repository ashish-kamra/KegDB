package com.kegdb.util;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

public final class Checksum {
    private Checksum() {
    }

    public static long generateChecksum(String obj) {
        CRC32 crc = new CRC32();
        crc.update(obj.getBytes(StandardCharsets.UTF_8));
        return crc.getValue();
    }

    public static boolean verifyChecksum(String obj, long expectedChecksum) {
        long actualChecksum = generateChecksum(obj);
        return actualChecksum == expectedChecksum;
    }
}
