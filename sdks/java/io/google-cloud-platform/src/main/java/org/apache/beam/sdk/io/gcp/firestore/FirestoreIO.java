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

/**
 * {@link FirestoreIO} provides an API for reading from and writing to <a target="_blank"
 * rel="noopener noreferrer" href="https://cloud.google.com/firestore/docs">Google Cloud
 * Firestore</a>.
 *
 * <p>For documentation see {@link FirestoreV1}.
 */
public final class FirestoreIO {

  private FirestoreIO() {}

  public static FirestoreV1 v1() {
    return FirestoreV1.INSTANCE;
  }
}
