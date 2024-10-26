package com.kegdb.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KegException extends RuntimeException {
    private static final Logger logger = LoggerFactory.getLogger(KegException.class);

    public KegException(String message) {
        super(message);
        logger.error(message);
    }

    public KegException(String message, Throwable cause) {
        super(message, cause);
        logger.error(message, cause);
    }
}
