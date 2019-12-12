package org.apache.beam.sdk.io.gcp.firestore;

import com.google.cloud.firestore.Firestore;

import java.io.Serializable;

public interface BatchRequesterFactory<T> extends Serializable {
    FirestoreBatchRequester<T> create(Firestore firestore);
}