package org.apache.beam.sdk.schemas.io;

/** Exception thrown when the request for a table is invalid, such as invalid metadata. */
public class InvalidConfigurationException extends IllegalArgumentException {
    public InvalidConfigurationException(String msg) {
        super(msg);
    }

    public InvalidConfigurationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public InvalidConfigurationException(Throwable cause) {
        super(cause);
    }
}