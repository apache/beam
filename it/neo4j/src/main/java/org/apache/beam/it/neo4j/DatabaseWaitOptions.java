package org.apache.beam.it.neo4j;

public class DatabaseWaitOptions {

    public static DatabaseWaitOption waitDatabase() {
            return DatabaseWait.WAIT;
    }

    public static DatabaseWaitOption waitDatabase(int seconds) {
        return DatabaseWaitInSeconds.wait(seconds);
    }

    public static DatabaseWaitOption noWaitDatabase() {
        return DatabaseNoWait.NO_WAIT;
    }
}

