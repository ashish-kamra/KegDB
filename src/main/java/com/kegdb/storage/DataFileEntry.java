package com.kegdb.storage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record DataFileEntry(long checksum, long timestamp, int keySize, int valueSize, String key, String value) {

    public static DataFileEntry convertFromBytes(byte[] buffer) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);

        long checksum = byteBuffer.getLong();
        long timestamp = byteBuffer.getLong();
        int keySize = byteBuffer.getInt();
        int valueSize = byteBuffer.getInt();

        byte[] keyBytes = new byte[keySize];
        byteBuffer.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        byte[] valueBytes = new byte[valueSize];
        byteBuffer.get(valueBytes);
        String value = new String(valueBytes, StandardCharsets.UTF_8);

        return new DataFileEntry(checksum, timestamp, keySize, valueSize, key, value);
    }

    public static byte[] getDataFileEntry(long checksum, String key, String value) {
        int keySize = key.getBytes(StandardCharsets.UTF_8).length;
        int valueSize = value.getBytes(StandardCharsets.UTF_8).length;
        return new DataFileEntry(checksum, System.currentTimeMillis(), keySize, valueSize, key, value).convertToBytes();
    }

    public byte[] convertToBytes() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8 + 8 + 4 + 4 + keySize + valueSize);

        byteBuffer.putLong(checksum);
        byteBuffer.putLong(timestamp);
        byteBuffer.putInt(keySize);
        byteBuffer.putInt(valueSize);
        byteBuffer.put(key.getBytes(StandardCharsets.UTF_8));
        byteBuffer.put(value.getBytes(StandardCharsets.UTF_8));

        return byteBuffer.array();
    }
}
