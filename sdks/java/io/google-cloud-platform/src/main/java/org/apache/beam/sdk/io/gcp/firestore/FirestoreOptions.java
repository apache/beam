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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

@Description("Options used to configure Cloud Firestore IO")
public interface FirestoreOptions extends PipelineOptions {

  /**
   * A host port pair to allow connecting to a Cloud Firestore emulator instead of the live service.
   * The value passed to this method will take precedent if the {@code FIRESTORE_EMULATOR_HOST}
   * environment variable is also set.
   *
   * @return the string representation of a host and port pair to be used when constructing Cloud
   *     Firestore clients.
   * @see com.google.cloud.firestore.FirestoreOptions.Builder#setEmulatorHost(java.lang.String)
   */
  @Nullable
  String getEmulatorHost();

  /**
   * Define a host port pair to allow connecting to a Cloud Firestore emulator instead of the live
   * service. The value passed to this method will take precedent if the {@code
   * FIRESTORE_EMULATOR_HOST} environment variable is also set.
   *
   * @param host the emulator host and port to connect to
   * @see com.google.cloud.firestore.FirestoreOptions.Builder#setEmulatorHost(java.lang.String)
   */
  void setEmulatorHost(String host);

  /**
   * The Firestore database ID to connect to. Note: named database is currently an internal feature
   * in Firestore. Do not set this to anything other than "(default)".
   */
  @Description("Firestore database ID")
  @Default.String("(default)")
  String getFirestoreDb();

  /** Set the Firestore database ID to connect to. */
  void setFirestoreDb(String firestoreDb);

  /**
   * A host port pair to allow connecting to a Cloud Firestore instead of the default live service.
   *
   * @return the string representation of a host and port pair to be used when constructing Cloud
   *     Firestore clients.
   */
  @Description("Firestore endpoint (host and port)")
  @Default.String("batch-firestore.googleapis.com:443")
  String getFirestoreHost();

  /**
   * Define a host port pair to allow connecting to a Cloud Firestore instead of the default live
   * service.
   *
   * @param host the host and port to connect to
   */
  void setFirestoreHost(String host);
}
