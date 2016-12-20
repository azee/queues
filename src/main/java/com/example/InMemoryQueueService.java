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
    private final Map<String, Deque<Message>> queues = new ConcurrentHashMap<>();

    //Pending map is used as a container for "invisible" (prefetched) messages
    private final Map<Message, PendingMessage> pendings = new ConcurrentHashMap<Message, PendingMessage>();

    public InMemoryQueueService(long timeout) {
        super(timeout);
    }

    /**
     * Add a message to a concurrent queue
     * There is a possibility to add a message to a deleted queue if
     * queue was removed in the middle of operation by another thread
     */
    @Override
    public void push(String queueName, Message message) {
        getQueue(queueName).add(message);
    }


    /**
     * Pull a message. Message will be removed from the queue
     * and placed into a "pending confirmation pre-fetched" container
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
     */
    @Override
    public long messagesInQueue(String queueName) {
        return getQueue(queueName).size();
    }

    /**
     * Get number of pending confirmation messages
     */
    @Override
    public long pendingMessages(String queueName) {
        return pendings.size();
    }

    /**
     * Drop all messages from the queue
     * Skipping synchronisation. Supposed to be used at startup, shutdown or TDD.
     */
    @Override
    public void clearMessages(String queueName) {
        getQueue(queueName).clear();
        pendings.values().removeIf(message -> message.getQueuName().equals(queueName));
    }

    /**
     * Goes through pending container. Mark unvisited messages as visited.
     * Move previously visited to a queue again (GC-like timeout implementation)
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
     */
    private Deque<Message> getQueue(String queueName){
        //Using ConcurrentLinkedDeque to avoid concurrent modification exceptions and locking
        queues.putIfAbsent(queueName, new ConcurrentLinkedDeque());
        return queues.get(queueName);
    }

}
