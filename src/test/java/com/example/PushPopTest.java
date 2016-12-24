package com.example;

import com.example.beans.Message;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

}
