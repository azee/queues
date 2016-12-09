package com.example;

import org.junit.Before;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Created by azee on 09.12.16.
 */
public class CommonBaseTest {
    protected final QueueService service;
    protected final String QUEUE_NAME = "testQueueName";

    public CommonBaseTest(QueueService service) {
        this.service = service;
    }

    @Parameterized.Parameters(name = "{index}: queue {1}")
    public static Collection getServices() {
        return Arrays.asList(new Object[][]{
                {new InMemoryQueueService(10)},
                {new InMemoryQueueService(300)},
                {new InMemoryQueueService(1000)},
                {new InMemoryQueueService(3000)},
                {new FileQueueService(300, "/tmp/queues")},
                {new FileQueueService(1000, "/tmp/queues")}
        });
    }

    @Before
    public void setUp(){
        service.clearMessages(QUEUE_NAME);
    }

    protected String getMessage(String message){
        return String.format("[%s]: %s", service.getClass().getSimpleName(), message);
    }
}
