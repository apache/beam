/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util.state;

import java.io.IOException;
import java.util.Objects;

/**
 * A simple {@link StateNamespace} used for testing.
 */
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
  public boolean equals(Object obj) {
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
