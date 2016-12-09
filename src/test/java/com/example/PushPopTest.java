package com.example;

import com.example.beans.Message;
import com.example.utils.Task;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.concurrent.*;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class PushPopTest extends CommonBaseTest{

    public PushPopTest(QueueService service) {
        super(service);
    }

    @Test
    public void simplePushPopTest(){
        Message message1 = new Message("msg1");
        service.push(QUEUE_NAME, message1);
        Message message2 = new Message("msg2");
        service.push(QUEUE_NAME, message2);

        assertThat(service.pull(QUEUE_NAME).getBody(), is("msg1"));
        assertThat(service.pull(QUEUE_NAME).getBody(), is("msg2"));

        assertThat(service.messagesInQueue(QUEUE_NAME), is(0L));
        assertThat(service.pendingMessages(QUEUE_NAME), is(2L));

        service.delete(QUEUE_NAME, message1);
        assertThat(service.messagesInQueue(QUEUE_NAME), is(0L));
        assertThat(service.pendingMessages(QUEUE_NAME), is(1L));

        service.delete(QUEUE_NAME, message2);
        assertThat(service.messagesInQueue(QUEUE_NAME), is(0L));
        assertThat(service.pendingMessages(QUEUE_NAME), is(0L));
    }

    @Test
    public void emptyListTest(){
        Message message = new Message("msg1");
        service.push(QUEUE_NAME, message);

        assertThat(service.pull(QUEUE_NAME).getBody(), is("msg1"));
        service.delete(QUEUE_NAME, message);

        assertNull(service.pull(QUEUE_NAME));
    }


    @Test
    public void concurrentPullDeleteTest() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
        for (int i = 0; i < 1; i++) {
            service.push(QUEUE_NAME, new Message("msg" + i));
            Runnable worker = new Runnable() {
                @Override
                public void run() {
                    service.delete(QUEUE_NAME, service.pull(QUEUE_NAME));
                }
            };
            executor.execute(worker);
        }
        executor.shutdown();
        executor.awaitTermination(30000, TimeUnit.SECONDS);
        assertThat(getMessage("Messages queue is not empty"), service.messagesInQueue(QUEUE_NAME), is(0L));
        assertThat(getMessage("Pending messages container is not empty"), service.pendingMessages(QUEUE_NAME), is(0L));
    }

    @Test
    public void messageNotRemovedOnPopTest(){
        service.push(QUEUE_NAME, new Message("Message1"));
        service.push(QUEUE_NAME, new Message("Message2"));

        assertThat(service.pull(QUEUE_NAME).getBody(), is("Message1"));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Boolean> future = executor.submit(new Task(service) {
            @Override
            public Boolean call() throws Exception {
                while (service.pendingMessages(QUEUE_NAME) != 0 && service.messagesInQueue(QUEUE_NAME) != 2) {}
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
