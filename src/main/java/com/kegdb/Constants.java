package com.kegdb;

public final class Constants {

    public static final String PROPERTIES_FILE_LOCATION = "/src/main/resources/db.properties";
    public static final String ARGS_ERROR_MESSAGE = "ERR wrong number of arguments for '%s' command";
    public static final String CHECKSUM_ERROR_MESSAGE = "ERR Invalid value: Checksum does not match";
    public static final String CRLF = "\r\n";
    public static final Long MAX_FILE_SIZE = 10000000L; //10MB
    public static final String DATAFILES_DIR = "db";
    public static final String DATAFILE_NAME = "keg_%d.db";

    private Constants() {
    }

}
