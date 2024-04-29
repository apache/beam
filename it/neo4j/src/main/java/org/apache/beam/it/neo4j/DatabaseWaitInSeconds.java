package org.apache.beam.it.neo4j;

class DatabaseWaitInSeconds implements DatabaseWaitOption {
    private final int seconds;

    private DatabaseWaitInSeconds(int seconds) {
        this.seconds = seconds;
    }

    public static DatabaseWaitOption wait(int seconds) {
        return new DatabaseWaitInSeconds(seconds);
    }

    public int getSeconds() {
        return seconds;
    }
}
