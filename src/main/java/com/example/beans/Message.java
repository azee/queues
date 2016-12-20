package com.example.beans;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by azee on 09.12.16.
 */
public class Message<T> implements Serializable {

    private T body;
    private String uuid;
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public Message(T body) {
        this.body = body;
        uuid = UUID.randomUUID().toString();
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

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public Message<T> withUuid(String uuid){
        setUuid(uuid);
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }
}
