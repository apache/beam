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
package org.apache.beam.sdk.util;

import java.io.Serializable;

/**
 * A secret management interface used for handling sensitive data.
 *
 * <p>This interface provides a generic way to handle secrets. Implementations of this interface
 * should handle fetching secrets from a secret management system. The underlying secret management
 * system should be able to return a valid byte array representing the secret.
 */
public interface Secret extends Serializable {
  /**
   * Returns the secret as a byte array.
   *
   * @return The secret as a byte array.
   */
  byte[] getSecretBytes();
}
