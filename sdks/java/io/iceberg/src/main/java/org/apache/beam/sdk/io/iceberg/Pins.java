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
package org.apache.beam.sdk.io.iceberg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

/** The pinned columns of a {@link SchemaEvolutionConfig}, as path segments. */
final class Pins {
  private final List<List<String>> segments;
  private final List<String> dotted;

  Pins(Collection<String> requiredColumns) {
    this.dotted = new ArrayList<>(requiredColumns);
    Collections.sort(dotted);
    this.segments = new ArrayList<>();
    for (String column : dotted) {
      segments.add(Arrays.asList(column.split("\\.", -1)));
    }
  }

  /** Whether {@code dottedPath} itself is pinned. */
  boolean isPinned(String dottedPath) {
    return dotted.contains(dottedPath);
  }

  /**
   * Returns the pinned column strictly below {@code dottedPath} (the lexicographically first when
   * several are), or null when there is none. Columns below a pin, or beside it, have none.
   */
  @Nullable String pinnedColumnBeneath(String dottedPath) {
    List<String> path = Arrays.asList(dottedPath.split("\\.", -1));
    for (int i = 0; i < segments.size(); i++) {
      List<String> pin = segments.get(i);
      if (pin.size() > path.size() && pin.subList(0, path.size()).equals(path)) {
        return dotted.get(i);
      }
    }
    return null;
  }
}
