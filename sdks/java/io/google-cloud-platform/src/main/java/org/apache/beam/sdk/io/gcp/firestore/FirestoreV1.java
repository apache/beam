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

import com.google.auto.value.AutoValue;
import com.google.cloud.firestore.FirestoreOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import javax.annotation.Nullable;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class FirestoreV1 {

    // A package-private constructor to prevent direct instantiation from outside of this package
    FirestoreV1() {
    }

    public Write write() {
        return new AutoValue_FirestoreV1_Write.Builder().build();
    }

    @AutoValue
    public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
        @Nullable
        public abstract ValueProvider<String> getProjectId();

        @Nullable
        public abstract String getCollectionId();

        @Nullable
        public abstract String getDocumentId();

        @Override
        public abstract String toString();

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T> {
            abstract Builder<T> setProjectId(ValueProvider<String> projectId);

            abstract Builder<T> setCollectionId(String collectionId);

            abstract Builder<T> setDocumentId(String documentId);

            abstract Write<T> build();
        }

        /**
         * Returns a new {@link Write} that reads from the Cloud Firestore for the specified project.
         */
        public Write<T> withProjectId(String projectId) {
            checkArgument(projectId != null, "projectId can not be null");
            return toBuilder().setProjectId(ValueProvider.StaticValueProvider.of(projectId)).build();
        }

        /**
         * Same as {@link Write#withProjectId(String)} but with a {@link ValueProvider}.
         */
        public Write<T> withProjectId(ValueProvider<String> projectId) {
            checkArgument(projectId != null, "projectId can not be null");
            return toBuilder().setProjectId(projectId).build();
        }

        /**
         * Returns a new {@link Write} that reads from the Cloud Firestore for the specified collection identifier.
         */
        public Write<T> to(String collectionId) {
            checkArgument(collectionId != null, "collectionId can not be null");
            return toBuilder().setCollectionId(collectionId).build();
        }

        /**
         * Returns a new {@link Write} that reads from the Cloud Firestore for the specified document identifier.
         */
        public Write<T> withDocumentId(String documentId) {
            checkArgument(documentId != null, "documentId can not be null");
            return toBuilder().setDocumentId(documentId).build();
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder
                    .addIfNotNull(DisplayData.item("projectId", getProjectId()).withLabel("ProjectId"))
                    .addIfNotNull(DisplayData.item("collectionId", getCollectionId()).withLabel("CollectionId"))
                    .addIfNotNull(DisplayData.item("documentId", getDocumentId()).withLabel("DocumentId"));
        }

        @Override
        public PDone expand(PCollection<T> input) {
            input.apply("Write to Firestore", ParDo.of(
                    new FirestoreWriterFn<T>(
                            getProjectId().get(),
                            getCollectionId(),
                            getDocumentId(),
                            new FirestoreOptions.DefaultFirestoreFactory(),
                            new WriteBatcherImpl(),
                            new FirestoreBatchRequesterFactory<T>())));
            return PDone.in(input.getPipeline());
        }
    }

//    public static class Write extends Mutate {
//        Write(@Nullable ValueProvider<String> projectId, ValueProvider<String> collectionId, @Nullable ValueProvider<String> documentId) {
//            super(projectId, collectionId, documentId);
//        }
//
//        /**
//         * Returns a new {@link Write} that writes to the Cloud Firestore for the specified project.
//         */
//        public Write withProjectId(String projectId) {
//            checkArgument(projectId != null, "projectId can not be null");
//            return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
//        }
//
//        /**
//         * Same as {@link Write#withProjectId(String)} but with a {@link ValueProvider}.
//         */
//        public Write withProjectId(ValueProvider<String> projectId) {
//            checkArgument(projectId != null, "projectId can not be null");
//            return new Write(projectId, collectionId, documentId);
//        }
//
//        /**
//         * Returns a new {@link Write} that writes to the Cloud Firestore for the specified [collection](https://cloud.google.com/firestore/docs/data-model).
//         */
//        public Write withCollectionId(String collectionId) {
//            checkArgument(collectionId != null, "collectionId can not be null");
//            return new Write(projectId, ValueProvider.StaticValueProvider.of(collectionId), documentId);
//        }
//
//        /**
//         * Returns a new {@link Write} that writes to the Cloud Firestore for the specified [document identifier](https://cloud.google.com/firestore/docs/data-model).
//         */
//        public Write withDocumentId(String documentId) {
//            checkArgument(documentId != null, "documentId can not be null");
//            return new Write(projectId, collectionId, ValueProvider.StaticValueProvider.of(documentId));
//        }
//
//        public Write build() {
//            return new Write(projectId, collectionId, documentId);
//        }
//    }
}
