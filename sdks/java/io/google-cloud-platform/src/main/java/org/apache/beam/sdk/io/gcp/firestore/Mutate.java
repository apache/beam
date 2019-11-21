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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import javax.annotation.Nullable;

public abstract class Mutate<T> extends PTransform<PCollection<T>, PDone> {
    @Nullable
    protected ValueProvider<String> projectId;
    protected String collectionId;
    @Nullable
    protected String keyId;

    Mutate(ValueProvider<String> projectId, String collectionId, String keyId) {
        this.projectId = projectId;
        this.collectionId = collectionId;
        this.keyId = keyId;
    }

    @Override
    public PDone expand(PCollection<T> input) {
        input.apply("Write to Firestore", ParDo.of(new FirestoreWriterFn(collectionId, keyId)));
        return PDone.in(input.getPipeline());
    }
}

