package org.apache.beam.sdk.io.gcp.firestore;

import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.WriteBatch;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

public class FirestoreBatchRequesterTest {
    @Mock
    private Firestore stubFirestore;

    @Mock
    private WriteBatch stubWriteBatch;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(stubFirestore.batch()).thenReturn(stubWriteBatch);
        when(stubWriteBatch.set(any(DocumentReference.class), anyString())).thenReturn(null);
    }

    @Test
    public void FirestoreBatchInvokedOnce() {
        FirestoreBatchRequester<String> requester = new FirestoreBatchRequester<>(stubFirestore);

        try {
            requester.commit(new ArrayList<>(), "collection", null);
        } catch (ExecutionException | InterruptedException | NullPointerException ignored) {

        }

        verify(stubFirestore, times(1)).batch();
    }

    @Test
    public void FirestoreBatchCommitInvokedOnce() {
        FirestoreBatchRequester<String> requester = new FirestoreBatchRequester<>(stubFirestore);

        try {
            requester.commit(new ArrayList<>(), "collection", null);
        } catch (ExecutionException | InterruptedException | NullPointerException ignored) {

        }

        verify(stubWriteBatch, times(1)).commit();
    }
}
