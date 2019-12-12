package org.apache.beam.sdk.io.gcp.firestore;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WriteBatcherTest {
    @Test
    public void testWriteBatcherWithoutData() {
        WriteBatcher writeBatcher = new WriteBatcherImpl();
        writeBatcher.start();
        assertEquals(WriteBatcherImpl.FIRESTORE_BATCH_UPDATE_ENTITIES_START, writeBatcher.nextBatchSize(0));
    }

    @Test
    public void testWriteBatcherFastQueries() {
        WriteBatcher writeBatcher = new WriteBatcherImpl();
        writeBatcher.start();
        writeBatcher.addRequestLatency(0, 1000, 200);
        writeBatcher.addRequestLatency(0, 1000, 200);
        assertEquals(WriteBatcherImpl.FIRESTORE_BATCH_UPDATE_ENTITIES_LIMIT, writeBatcher.nextBatchSize(0));
    }

    @Test
    public void testWriteBatcherSlowQueries() {
        WriteBatcher writeBatcher = new WriteBatcherImpl();
        writeBatcher.start();
        writeBatcher.addRequestLatency(0, 10000, 200);
        writeBatcher.addRequestLatency(0, 10000, 200);
        assertEquals(100, writeBatcher.nextBatchSize(0));
    }

    @Test
    public void testWriteBatcherSizeNotBelowMinimum() {
        WriteBatcher writeBatcher = new WriteBatcherImpl();
        writeBatcher.start();
        writeBatcher.addRequestLatency(0, 30000, 50);
        writeBatcher.addRequestLatency(0, 30000, 50);
        assertEquals(WriteBatcherImpl.FIRESTORE_BATCH_UPDATE_ENTITIES_MIN, writeBatcher.nextBatchSize(0));
    }

    @Test
    public void testWriteBatcherSlidingWindow() {
        WriteBatcher writeBatcher = new WriteBatcherImpl();
        writeBatcher.start();
        writeBatcher.addRequestLatency(0, 30000, 50);
        writeBatcher.addRequestLatency(50000, 5000, 200);
        writeBatcher.addRequestLatency(100000, 5000, 200);
        assertEquals(200, writeBatcher.nextBatchSize(150000));
    }
}
