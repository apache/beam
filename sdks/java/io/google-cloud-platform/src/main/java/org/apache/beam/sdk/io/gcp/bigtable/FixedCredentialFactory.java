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
package org.apache.beam.sdk.io.gcp.bigtable;

import com.google.auth.Credentials;
import org.apache.beam.sdk.extensions.gcp.auth.CredentialFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A fixed credential factory to return the credential set by users. This class is for backward
 * compatibility if a user is setting credentials through BigtableOptions.
 */
class FixedCredentialFactory implements CredentialFactory {

  private Credentials credentials;

  private FixedCredentialFactory(Credentials credentials) {
    this.credentials = credentials;
  }

  static FixedCredentialFactory create(Credentials credentials) {
    return new FixedCredentialFactory(credentials);
  }

  @Override
  public @Nullable Credentials getCredential() {
    return credentials;
  }
}
