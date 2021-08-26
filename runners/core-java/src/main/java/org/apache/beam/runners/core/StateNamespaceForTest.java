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
package org.apache.beam.runners.core;

import java.io.IOException;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A simple {@link StateNamespace} used for testing. */
public class StateNamespaceForTest implements StateNamespace {
  private String key;

  public StateNamespaceForTest(String key) {
    this.key = key;
  }

  @Override
  public String stringKey() {
    return key;
  }

  @Override
  public Object getCacheKey() {
    return key;
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof StateNamespaceForTest)) {
      return false;
    }

    return Objects.equals(this.key, ((StateNamespaceForTest) obj).key);
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public void appendTo(Appendable sb) throws IOException {
    sb.append(key);
  }
}
