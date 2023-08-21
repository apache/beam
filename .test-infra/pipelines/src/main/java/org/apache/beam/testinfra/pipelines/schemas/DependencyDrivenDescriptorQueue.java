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
package org.apache.beam.testinfra.pipelines.schemas;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Prevents stack overflow errors when parsing a {@link Descriptor}. Orders {@link Descriptor} by
 * their interdependencies such that the first has no dependencies i.e. only primitive types and
 * subsequent {@link Descriptor}s contain {@link JavaType#MESSAGE} types.
 */
@Internal
class DependencyDrivenDescriptorQueue implements Iterable<Descriptor>, Comparator<Descriptor> {
  private final Map<@NonNull String, @NonNull Descriptor> descriptorMap = new HashMap<>();
  private final Map<@NonNull String, @NonNull Set<String>> dependencyMap = new HashMap<>();

  private final Map<@NonNull String, @NonNull Integer> messageFieldCounts = new HashMap<>();

  /**
   * Enqueues a {@link Descriptor}. Walks down its dependency tree of any nested {@link
   * JavaType#MESSAGE} types, further enqueuing these types' {@link Descriptor}s.
   */
  void enqueue(@NonNull Descriptor descriptor) {
    List<@NonNull Descriptor> descriptorStack = new ArrayList<>();
    descriptorStack.add(descriptor);
    while (!descriptorStack.isEmpty()) {
      Descriptor fromStack = descriptorStack.remove(0);
      if (descriptorMap.containsKey(fromStack.getFullName())) {
        checkState(dependencyMap.containsKey(fromStack.getFullName()));
        checkState(messageFieldCounts.containsKey(fromStack.getFullName()));
        continue;
      }
      int messageFieldCounts = 0;
      for (FieldDescriptor field : fromStack.getFields()) {
        if (!field.getJavaType().equals(JavaType.MESSAGE)) {
          continue;
        }
        messageFieldCounts++;
        Descriptor fieldDescriptor = field.getMessageType();
        if (!dependencyMap.containsKey(fieldDescriptor.getFullName())) {
          dependencyMap.put(fieldDescriptor.getFullName(), new HashSet<>());
        }
        Set<String> dependents = dependencyMap.get(fieldDescriptor.getFullName());
        dependents.add(descriptor.getFullName());
        descriptorStack.add(fieldDescriptor);
      }
      descriptorMap.put(fromStack.getFullName(), fromStack);
      this.messageFieldCounts.put(fromStack.getFullName(), messageFieldCounts);
      if (!dependencyMap.containsKey(fromStack.getFullName())) {
        dependencyMap.put(fromStack.getFullName(), new HashSet<>());
      }
    }
  }

  /**
   * Returns an iteration of {@link Descriptor}s ordered by increasing dependency such that the
   * first has no nested {@link JavaType#MESSAGE} types.
   */
  @Override
  public Iterator<Descriptor> iterator() {
    return descriptorMap.values().stream().sorted(this).iterator();
  }

  @Override
  public int compare(@NonNull Descriptor a, @NonNull Descriptor b) {
    boolean aDependsOnB =
        checkStateNotNull(dependencyMap.get(b.getFullName())).contains(a.getFullName());
    boolean bDependsOnA =
        checkStateNotNull(dependencyMap.get(a.getFullName())).contains(b.getFullName());
    if (aDependsOnB) {
      return 1;
    }
    if (bDependsOnA) {
      return -1;
    }
    Integer aMessageFieldCount = checkStateNotNull(messageFieldCounts.get(a.getFullName()));
    Integer bMessageFieldCount = checkStateNotNull(messageFieldCounts.get(b.getFullName()));
    return aMessageFieldCount.compareTo(bMessageFieldCount);
  }
}
