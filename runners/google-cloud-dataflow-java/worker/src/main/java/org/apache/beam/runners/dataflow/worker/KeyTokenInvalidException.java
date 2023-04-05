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
package org.apache.beam.runners.dataflow.worker;

/** Indicates that the key token was invalid when data was attempted to be fetched. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class KeyTokenInvalidException extends RuntimeException {
  public KeyTokenInvalidException(String key) {
    super("Unable to fetch data due to token mismatch for key " + key);
  }

  /** Returns whether an exception was caused by a {@link KeyTokenInvalidException}. */
  public static boolean isKeyTokenInvalidException(Throwable t) {
    while (t != null) {
      if (t instanceof KeyTokenInvalidException) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }
}
