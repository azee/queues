package com.example.beans;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by azee on 09.12.16.
 */
public class Message<T> implements Serializable {

    private T body;
    private final String uuid = UUID.randomUUID().toString();

    public Message(T body) {
        this.body = body;
    }

    public T getBody() {
        return body;
    }

    public void setBody(T body) {
        this.body = body;
    }

    public String getUuid() {
        return uuid;
    }
}
