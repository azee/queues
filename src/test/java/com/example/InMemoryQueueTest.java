package com.example;

import com.example.beans.Message;
import com.example.utils.Task;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by azee on 24.12.16.
 */
public class InMemoryQueueTest {

    private final String QUEUE_NAME = "InmenQueueName";

    @Test
    public void concurrentPullDeleteTest() throws InterruptedException {
        final int limit = 1000;
        QueueService service = new InMemoryQueueService(30000);
        final AtomicInteger counter = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
        for (int i = 0; i < limit; i++) {
            Runnable worker = new Runnable() {
                @Override
                public void run() {
                    counter.incrementAndGet();
                    service.push(QUEUE_NAME, new Message("msg"));
                    service.delete(QUEUE_NAME, service.pull(QUEUE_NAME));
                }
            };
            executor.execute(worker);
        }
        executor.shutdown();
        executor.awaitTermination(30000, TimeUnit.SECONDS);
        assertThat("Messages queue is not empty", service.messagesInQueue(QUEUE_NAME), is(0L));
        assertThat("Pending messages container is not empty", service.pendingMessages(QUEUE_NAME), is(0L));
        assertThat("Incorrect number of processed messages", counter.get(), is(limit));
    }


    @Test
    public void messageNotRemovedOnPopTest() {
        QueueService service = new InMemoryQueueService(100);
        service.push(QUEUE_NAME, new Message("Message1"));
        service.push(QUEUE_NAME, new Message("Message2"));

        assertThat(service.pull(QUEUE_NAME).getBody(), is("Message1"));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Boolean> future = executor.submit(new Task(service) {
            @Override
            public Boolean call() throws Exception {
                while (service.pendingMessages(QUEUE_NAME) != 0 && service.messagesInQueue(QUEUE_NAME) != 2) {
                }
                return true;
            }
        });

        try {
            assertTrue(future.get(service.getTimeout() * 2, TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            future.cancel(true);
            throw new RuntimeException(e);
        }
        executor.shutdownNow();

        assertThat(service.messagesInQueue(QUEUE_NAME), is(2L));
        assertThat(service.pendingMessages(QUEUE_NAME), is(0L));
        assertThat(service.pull(QUEUE_NAME).getBody(), is("Message1"));
    }
}
