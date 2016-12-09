package com.example.beans;

/**
 * Created by azee on 09.12.16.
 */
public class PendingMessage {
    private final String queuName;
    private boolean visited;

    public PendingMessage(String queuName) {
        this.queuName = queuName;
    }

    public boolean isVisited() {
        return visited;
    }

    public void setVisited(boolean visited) {
        this.visited = visited;
    }

    public String getQueuName() {
        return queuName;
    }
}
