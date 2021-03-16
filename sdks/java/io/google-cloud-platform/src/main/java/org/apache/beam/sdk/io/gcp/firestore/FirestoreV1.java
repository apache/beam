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

import javax.annotation.concurrent.Immutable;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * {@link FirestoreV1} provides an API which provides lifecycle managed {@link PTransform}s for
 * <a href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1">Cloud
 * Firestore v1 API</a>.
 * <p/>
 * This class is part of the Firestore Connector DSL and should be accessed via {@link
 * FirestoreIO#v1()}.
 * <p/>
 * All {@link PTransform}s provided by this API use {@link org.apache.beam.sdk.extensions.gcp.options.GcpOptions
 * GcpOptions} on {@link org.apache.beam.sdk.options.PipelineOptions PipelineOptions} for
 * credentials access and projectId resolution. As such, the lifecycle of gRPC clients and project
 * information is scoped to the bundle level, not the worker level.
 * <p/>
 *
 * <h3>Permissions</h3>
 *
 * Permission requirements depend on the {@code PipelineRunner} that is used to execute the
 * pipeline. Please refer to the documentation of corresponding {@code PipelineRunner}s for more
 * details.
 *
 * <p>Please see <a href="https://cloud.google.com/firestore/docs/quickstart-servers#create_a_in_native_mode_database">Create
 * a Firestore in Native mode database
 * </a>for security and permission related information specific to Cloud Firestore.
 *
 * <p>Optionally, Cloud Firestore V1 Emulator, running locally, could be used for testing purposes
 * by providing the host port information vi {@link FirestoreIOOptions#setEmulatorHostPort(String)}.
 * In such a case, all the Cloud Firestore API calls are directed to the Emulator.
 *
 * @see FirestoreIO#v1()
 * @see org.apache.beam.sdk.PipelineRunner
 * @see org.apache.beam.sdk.options.PipelineOptions
 * @see org.apache.beam.sdk.extensions.gcp.options.GcpOptions
 * @see <a href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1">Cloud
 * Firestore v1 API</a>
 */
@Immutable
public final class FirestoreV1 {
  static final FirestoreV1 INSTANCE = new FirestoreV1();

  private FirestoreV1() {}

}
