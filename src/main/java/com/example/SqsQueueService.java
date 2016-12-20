package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import com.example.beans.Message;
import com.example.error.QueueException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ru.greatbit.utils.serialize.JsonSerializer.marshal;
import static ru.greatbit.utils.serialize.Serializer.unmarshal;

/**
 * Created by azee on 09.12.16.
 */
public class SqsQueueService implements QueueService {

    private final AmazonSQSClient sqsClient;
    private final long timeout;

    private final String RECEIPT_HANDLE_KEY = "receiptHandle";

    public SqsQueueService(AmazonSQSClient sqsClient, long timeout) {
        this.timeout = timeout;
        this.sqsClient = sqsClient;
    }

    @Override
    public void push(String queueName, Message message) {
        sqsClient.sendMessage(getQueueUri(queueName), serializeMessage(message));
    }

    /**
     * Receive a message from a queue. Place it into a message decorator
     * @param queueName
     * @return Wrapped message
     */
    @Override
    public Message pull(String queueName) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsClient.getQueueUrl(queueName).getQueueUrl());
        receiveMessageRequest.setMaxNumberOfMessages(1);
        List<com.amazonaws.services.sqs.model.Message> messages = sqsClient.receiveMessage(queueName).getMessages();
        return messages.size() < 1 ? null : deserializeMessage(messages.get(0));
    }

    /**
     * Removes a message using Receipt handler in attributes
     * @param queueName
     * @param message
     */
    @Override
    public void delete(String queueName, Message message) {
        sqsClient.deleteMessage(new DeleteMessageRequest(sqsClient.getQueueUrl(queueName).getQueueUrl(),
                (String) message.getAttributes().get(RECEIPT_HANDLE_KEY)));
    }

    @Override
    public long messagesInQueue(String queueName) {
        return Long.parseLong(getAttribute(queueName, QueueAttributeName.ApproximateNumberOfMessages));
    }

    @Override
    public long pendingMessages(String queueName) {
        return Long.parseLong(getAttribute(queueName, QueueAttributeName.ApproximateNumberOfMessagesNotVisible));
    }

    @Override
    public void clearMessages(String queueName) {
        sqsClient.listQueues().getQueueUrls().forEach(url ->
            sqsClient.deleteQueue(url)
        );
    }

    @Override
    public long getTimeout() {
        return timeout;
    }

    /**
     * Get queue attribute
     * @param queueName
     * @param attributeName
     * @return String value of an attribute
     */
    private String getAttribute(String queueName, QueueAttributeName attributeName){
        return sqsClient.getQueueAttributes(queueName, Arrays.asList(attributeName.toString()))
                .getAttributes().get(attributeName.toString());
    }

    /**
     * Get a Queue url or create if absent
     * @param queueName
     * @return Existing or created queue url
     */
    private String getQueueUri(String queueName){
        GetQueueUrlResult queueUrl = sqsClient.getQueueUrl(queueName);
        return queueUrl != null ? queueUrl.getQueueUrl() : createQueue(queueName);
    }

    /**
     * Safely creates a queue if absent
     * @param queueName
     * @return Queue url
     */
    private String createQueue(String queueName){
        String uri;
        Map<String, String> attributes = new HashMap<>();
        attributes.put("VisibilityTimeout", Long.toString(timeout / 1000));
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(queueName).withAttributes(attributes);
        try {
            uri = sqsClient.createQueue(createQueueRequest).getQueueUrl();
        } catch (QueueNameExistsException e){
            uri = sqsClient.getQueueUrl(queueName).getQueueUrl();
        }
        return uri;
    }

    /**
     * Transform SQS message to Message decorator
     * @param message
     * @return Wrapped message
     */
    private Message deserializeMessage(com.amazonaws.services.sqs.model.Message message) {
        try {
            Message result = unmarshal(message.getBody(), Message.class).withUuid(message.getMessageId());
            result.getAttributes().put(RECEIPT_HANDLE_KEY, message.getReceiptHandle());
            return result;
        } catch (Exception e) {
            throw new QueueException("Can't deserialize message", e);
        }
    }

    /**
     * Transform an Message decorator to an SQS message body
     * @param message
     * @return serialized message body
     */
    private String serializeMessage(Message message) {
        try {
            return marshal(message);
        } catch (Exception e) {
            throw new QueueException("Can't serialize message", e);
        }
    }

}
