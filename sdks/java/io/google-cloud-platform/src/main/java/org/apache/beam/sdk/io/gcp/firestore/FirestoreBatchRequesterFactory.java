package org.apache.beam.sdk.io.gcp.firestore;

import com.google.cloud.firestore.Firestore;

public class FirestoreBatchRequesterFactory<T> implements BatchRequesterFactory<T> {
    @Override
    public FirestoreBatchRequester<T> create(Firestore firestore) {
        return new FirestoreBatchRequester<>(firestore);
    }
}