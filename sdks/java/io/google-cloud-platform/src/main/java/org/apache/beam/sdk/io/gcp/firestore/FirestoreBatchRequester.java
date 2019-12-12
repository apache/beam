/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.io.gcp.firestore;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class FirestoreBatchRequester<T> {
    private Firestore firestoreClient;
    private static final Logger LOG = LoggerFactory.getLogger(FirestoreBatchRequester.class);

    public FirestoreBatchRequester(Firestore firestoreClient) {
        this.firestoreClient = firestoreClient;
    }

    public void commit(List<T> input, String collection, String documentId) throws ExecutionException, InterruptedException {
        WriteBatch batch = firestoreClient.batch();

        for (T object : input) {
            DocumentReference docRef = getDocRef(collection, documentId);
            batch.set(docRef, object);
        }

        ApiFuture<List<WriteResult>> commit = batch.commit();

        for (WriteResult result : commit.get()) {
            LOG.info("Update time : " + result.getUpdateTime() + " Firestore key : " + documentId);
        }
    }

    private DocumentReference getDocRef(String collection, String key) {
        return key == null ? firestoreClient.collection(collection).document() : firestoreClient.collection(collection).document(key);
    }
}