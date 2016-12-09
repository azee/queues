package com.example;

import com.example.beans.Message;
import com.example.beans.PendingMessage;

import java.util.Deque;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;


public class InMemoryQueueService extends BaseQueueService{

    //Using Deque to be able to place messages at the beginning of the queue
    //Using ConcurrentLinkedDeque to avoid concurrent modification exceptions and locking
    private final Map<String, Deque<Message>> queues = new ConcurrentHashMap<>();

    //Pending map is used as a container for "invisible" (prefetched) messages
    private final Map<Message, PendingMessage> pendings = new ConcurrentHashMap<Message, PendingMessage>();

    public InMemoryQueueService(long timeout) {
        super(timeout);
    }

    /**
     * Just add a message to a concurrent queue
     */
    @Override
    public void push(String queueName, Message message) {
        getQueue(queueName).add(message);
    }


    /**
     * Pull a message will remove it from the queue and place into a "pending confirmation prefetched" container
     */
    @Override
    public Message pull(String queueName) {
        Message message;
        try {
            message = getQueue(queueName).removeFirst();
        } catch (NoSuchElementException e){
            return null;
        }
        if (message != null){
            pendings.put(message, new PendingMessage(queueName));
        }
        return message;
    }

    /**
     * Remove a message from pending - confirm from a consumer
     */
    @Override
    public void delete(String queueName, Message message) {
        pendings.remove(message);
    }

    /**
     * Get number of messages in queue
     * Will be useful during exploitation and for TDD
     */
    @Override
    public long messagesInQueue(String queueName) {
        return getQueue(queueName).size();
    }

    /**
     * Get number of pending confirmation messages
     * Will be useful during exploitation and for TDD
     */
    @Override
    public long pendingMessages(String queueName) {
        return pendings.size();
    }

    /**
     * Drop a queue
     */
    @Override
    public void clearMessages(String queueName) {
        queues.clear();
        pendings.clear();
    }

    /**
     * Goes through pending container to define if ttl of eny is exceeded
     * If so moves messages to the head of the queue
     */
    protected void clearPending() {
        pendings.entrySet().removeIf(entry -> {
            if (entry.getValue().isVisited()){
                getQueue(entry.getValue().getQueuName()).addFirst(entry.getKey());
                return true;
            } else {
                entry.getValue().setVisited(true);
            }
            return false;
        });
    }

    /**
     * Get a queue by name
     * Will act synchronized if a new queue is created
     */
    private Deque<Message> getQueue(String queueName){
        queues.putIfAbsent(queueName, new ConcurrentLinkedDeque());
        return queues.get(queueName);
    }

}
