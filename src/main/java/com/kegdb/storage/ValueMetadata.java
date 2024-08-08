package com.kegdb.storage;

public record ValueMetadata(int fileId, long recordPosition, int recordSize, long timestamp) {
}
