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

import org.apache.beam.sdk.options.ValueProvider;

import javax.annotation.Nullable;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class FirestoreIO {

    public Write write() {
        return new Write(null, null, null);
    }

    public static class Write extends Mutate {
        Write(@Nullable ValueProvider<String> projectId, String collectionId, @Nullable String keyId) {
            super(projectId, collectionId, keyId);
        }

        /**
         * Returns a new {@link Write} that writes to the Cloud Firestore for the specified project.
         */
        public Write withProjectId(String projectId) {
            checkArgument(projectId != null, "projectId can not be null");
            return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
        }

        /**
         * Same as {@link Write#withProjectId(String)} but with a {@link ValueProvider}.
         */
        public Write withProjectId(ValueProvider<String> projectId) {
            checkArgument(projectId != null, "projectId can not be null");
            return new Write(projectId, collectionId, keyId);
        }

        /**
         * Returns a new {@link Write} that writes to the Cloud Firestore for the specified collection.
         */
        public Write withCollectionId(String collectionId) {
            checkArgument(collectionId != null, "collectionId can not be null");
            return new Write(projectId, collectionId, keyId);
        }

        /**
         * Returns a new {@link Write} that writes to the Cloud Firestore for the specified collection key.
         */
        public Write withKeyId(String keyId) {
            checkArgument(keyId != null, "keyId can not be null");
            return new Write(projectId, collectionId, keyId);
        }

        public Write build() {
            return new Write(projectId, collectionId, keyId);
        }
    }
}
