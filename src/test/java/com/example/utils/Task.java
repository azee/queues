package com.example.utils;

import com.example.QueueService;

import java.util.concurrent.Callable;

/**
 * Created by azee on 09.12.16.
 */
public abstract class Task implements Callable<Boolean> {
    private final QueueService service;

    public Task(QueueService service) {
        this.service = service;
    }
}
