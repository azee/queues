package com.example.error;

/**
 * Created by azee on 09.12.16.
 */
public class QueueException extends RuntimeException {
    public QueueException() {
    }

    public QueueException(String message) {
        super(message);
    }

    public QueueException(String message, Throwable cause) {
        super(message, cause);
    }
}
