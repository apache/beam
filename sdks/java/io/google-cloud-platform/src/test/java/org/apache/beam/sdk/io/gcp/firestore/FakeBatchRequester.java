package org.apache.beam.sdk.io.gcp.firestore;

import com.google.cloud.firestore.Firestore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FakeBatchRequester<T> extends FirestoreBatchRequester<T> {
    private HashMap<String, List<T>> fakeStorage;

    public FakeBatchRequester(Firestore firestoreClient) {
        super(firestoreClient);
        fakeStorage = new HashMap<>();
    }

    @Override
    public void commit(List<T> input, String collection, String documentId) {
        for (T value : input) {
            String key = collection + documentId;
            if (!fakeStorage.containsKey(key)) {
                fakeStorage.put(key, new ArrayList<T>());
            }
            fakeStorage.get(key).add(value);
        }
    }

    public HashMap<String, List<T>> getStorage() {
        return fakeStorage;
    }
}