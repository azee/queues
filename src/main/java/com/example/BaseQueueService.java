package com.example;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by azee on 09.12.16.
 */
public abstract class BaseQueueService implements QueueService {

    private final long timeout;

    /**
     * Creates a scheduled cleanup thread that will move delayed pending
     * messages to a queue again
     * @param timeout
     */
    public BaseQueueService(long timeout) {
        this.timeout = timeout;
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                clearPending();
            }
        }, timeout, timeout, TimeUnit.MILLISECONDS);
    }

    protected abstract void clearPending();

    @Override
    public long getTimeout() {
        return timeout;
    }

}
