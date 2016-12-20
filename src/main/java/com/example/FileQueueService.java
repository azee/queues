package com.example;

import com.example.beans.Message;
import com.example.error.QueueException;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileQueueService extends BaseQueueService{
    private final String baseDir;
    private final String PENDING_PATH = "pending";
    private final String LOCK_DIR_PATH = ".lock";
    private final String MESSAGES_PATH = "messages";

    private final long LOCK_ITERATION_TIMEOUT = 100;

    /**
     * Constructor
     * Creates a base directory if needed
     */
    public FileQueueService(long timeout, String baseDir) {
        super(timeout);
        this.baseDir = baseDir;
        File file = new File(baseDir);
        if (!file.exists()){
            file.mkdir();
        }
    }


    @Override
    public void push(String queueName, Message message) {
        //We keep reception time of the message to use it as index for FIFO
        //will be a part of a filename
        long now = System.nanoTime();

        //Create a directory for the queue if a new queue name received
        File queueDir = getQueueBaseDir(queueName);
        if (!queueDir.exists()){
            queueDir.mkdir();
        }

        //Lock is implemented thorough a hidden directory
        //Each queue has its own lock
        File lock = getLock(queueName);
        try {
            lock(lock);
            //Serialize message
            writeToFile(queueName, message, now);
        } catch (IOException e){
            throw new QueueException("Error occurred while performing file operations", e);
        }finally {
            unlock(lock);
        }
    }

    @Override
    public Message pull(String queueName) {
        File lock = getLock(queueName);
        try {
            lock(lock);

            //Pull will move file to a pending messages directory
            //Create a directory if does not exist
            File pendings = getQueuePendingDir(queueName);
            if (!pendings.exists()){
                pendings.mkdir();
            }
            File queueDir = getQueueDir(queueName);
            File[] files = queueDir.listFiles();
            if (files.length == 0){
                return null;
            }

            //Select the file with lowest receive timestamp
            File messageFile = null;
            for (File file : files){
                if (messageFile == null || file.getName().compareTo(messageFile.getName()) < 0){
                    messageFile = file;
                }
            }

            //Deserialize
            Object message = readFromFile(messageFile);

            //Move message file to pendings
            messageFile.renameTo(new File(getQueuePendingDir(queueName) + File.separator + messageFile.getName()));
            return (Message)message;
        } finally {
            unlock(lock);
        }
    }

    @Override
    public void delete(String queueName, Message message) {
        File lock = getLock(queueName);
        try {
            lock(lock);
            //Each message has an UUID
            //Find a file with corresponding uuid in name and remove it
            for (File file : getQueuePendingDir(queueName).listFiles()){
                if (file.getName().contains(message.getUuid())){
                    file.delete();
                    return;
                }
            }
        } finally {
            unlock(lock);
        }
    }

    @Override
    public long messagesInQueue(String queueName) {
        return getQueueDir(queueName).listFiles().length;
    }

    @Override
    public long pendingMessages(String queueName) {
        return getQueuePendingDir(queueName).listFiles().length;
    }

    @Override
    protected void clearPending() {
        for (File pendingDir : getPendingDirs()) {
            for (File file : pendingDir.listFiles()) {
                if (System.currentTimeMillis() - file.lastModified() > getTimeout()) {
                    file.renameTo(new File(getQueueDir(pendingDir.getParentFile().getName()) + File.separator + file.getName()));
                }
            }
        }
    }

    @Override
    public void clearMessages(String queueName) {
        File queueDir = getQueueBaseDir(queueName);
        if (queueDir.exists()){
            try {
                FileUtils.deleteDirectory(queueDir);
            } catch (IOException e) {
                throw new QueueException(String.format("Couldn't clear file queue [%s]", queueName), e);
            }
        }
    }

    /**
     * Locking a queue by creating a directory
     * @param lock
     */
    private void lock(File lock){
        while (!lock.mkdir()) {
            try {
                Thread.sleep(LOCK_ITERATION_TIMEOUT);
            } catch (InterruptedException e) {
                throw new QueueException("Thread was interrupted", e);
            }
        }
    }

    /**
     * Removes the locking file
     * @param lock
     */
    private void unlock(File lock) {
        lock.delete();
    }

    /////////////Directory path methods///////////////
    private File getLock(String queueName){
        return new File(getQueueBaseDirPath(queueName + File.separator + LOCK_DIR_PATH));
    }

    private String getQueueBaseDirPath(String queueName){
        return baseDir + File.separator + queueName;
    }

    private File getQueueBaseDir(String queueName) {
        return new File(getQueueBaseDirPath(queueName));
    }

    private String getQueueDirPath(String queueName){
        return getQueueBaseDirPath(queueName) + File.separator + MESSAGES_PATH;
    }

    private File getQueueDir(String queueName){
        return new File(getQueueDirPath(queueName));
    }

    private String getQueuePendingDirPath(String queueName){
        return getQueueBaseDirPath(queueName) + File.separator + PENDING_PATH;
    }

    private File getQueuePendingDir(String queueName){
        return new File(getQueuePendingDirPath(queueName));
    }

    private String getMessageDirPath(String queueName, String filename){
        return getQueueDirPath(queueName) + File.separator + filename;
    }
    ////////////////////////////////////////////////

    /**
     * Serialize a message
     */
    private void writeToFile(String queueName, Message message, long time) throws IOException {
        File messagesDir = getQueueDir(queueName);
        if (!messagesDir.exists()){
            messagesDir.mkdir();
        }
        String fileName = time + message.getUuid();
        FileOutputStream fout = new FileOutputStream(getMessageDirPath(queueName, fileName));
        ObjectOutputStream oos = new ObjectOutputStream(fout);
        oos.writeObject(message);
    }

    /**
     * Deserialize a message
     */
    private Message readFromFile(File file) {
        try {
            FileInputStream fin = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fin);
            Object message = ois.readObject();
            ois.close();
            return (Message)message;
        } catch (Exception e){
            throw new QueueException("Couldn't read file", e);
        }
    }

    /**
     * Get all pending directories for all queues
     */
    private List<File> getPendingDirs() {
        List<File> dirs = new ArrayList<>();
        for (File queueDir : new File(baseDir).listFiles()){
            if (queueDir.isDirectory()){
                for (File dir : queueDir.listFiles()){
                    if (dir.isDirectory() && dir.getName().equals(PENDING_PATH)){
                        dirs.add(dir);
                    }
                    continue;
                }
            }
        }
        return dirs;
    }
}
