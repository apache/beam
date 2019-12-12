package org.apache.beam.sdk.io.gcp.firestore;

/**
 * A WriteBatcher for unit tests, which does no timing-based adjustments (so unit tests have
 * consistent results).
 */
class FakeWriteBatcher implements WriteBatcher {
    @Override
    public void start() {
    }

    @Override
    public void addRequestLatency(long timeSinceEpochMillis, long latencyMillis, int numMutations) {
    }

    @Override
    public int nextBatchSize(long timeSinceEpochMillis) {
        return WriteBatcherImpl.FIRESTORE_BATCH_UPDATE_ENTITIES_START;
    }
}
