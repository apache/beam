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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.dataflow.v1beta3.Job;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import java.util.Iterator;
import org.junit.jupiter.api.Test;

/** Tests for {@link DependencyDrivenDescriptorQueue}. */
class DependencyDrivenDescriptorQueueTest {
  @Test
  void iterator_Job_isInDependencyOrder() {
    DependencyDrivenDescriptorQueue queue = new DependencyDrivenDescriptorQueue();
    queue.enqueue(Job.getDescriptor());
    Iterator<Descriptor> itr = queue.iterator();
    Descriptor previous = itr.next();
    int previousMessageCount = messageCount(previous);
    assertTrue(itr.hasNext());
    while (itr.hasNext()) {
      Descriptor current = itr.next();
      int currentMessageCount = messageCount(current);
      assertTrue(previousMessageCount <= currentMessageCount);
      boolean previousDependsOnCurrent =
          previous.getFields().stream()
              .anyMatch(field -> field.getFullName().equals(current.getFullName()));
      assertFalse(previousDependsOnCurrent);
    }
  }

  private static int messageCount(Descriptor descriptor) {
    return (int)
        descriptor.getFields().stream()
            .filter(field -> field.getJavaType().equals(JavaType.MESSAGE))
            .count();
  }
}
