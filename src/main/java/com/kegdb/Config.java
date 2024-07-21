package com.kegdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.kegdb.Constants.PROPERTIES_FILE;

public class Config {

    private static final Logger log = LoggerFactory.getLogger(Config.class);

    public Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
            if (inputStream == null) {
                log.error("{} not found", PROPERTIES_FILE);
                return null;
            }
            properties.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;
    }

}
