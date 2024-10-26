package com.kegdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static com.kegdb.Constants.PROPERTIES_FILE_LOCATION;

public class Config {
    private static final Logger log = LoggerFactory.getLogger(Config.class);
    private static final Properties properties;

    static {
        properties = new Properties();
        try (FileInputStream fis = new FileInputStream(System.getProperty("user.dir") + PROPERTIES_FILE_LOCATION)) {
            properties.load(fis);
        } catch (IOException e) {
            log.error("Error reading {} : {}", PROPERTIES_FILE_LOCATION, e.getMessage());
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    public static int getPort() {
        return Integer.parseInt(properties.getProperty("port", "6379"));
    }

    public static int getThreadPoolSize() {
        return Integer.parseInt(properties.getProperty("thread_pool_size", "10"));
    }

    public static String getDataDirectory() {
        return properties.getProperty("data_directory", "./db");
    }

}