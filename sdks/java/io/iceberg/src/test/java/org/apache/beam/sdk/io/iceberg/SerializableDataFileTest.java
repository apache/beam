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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Test;

/**
 * Test for {@link SerializableDataFile}. More tests can be found in {@link
 * org.apache.beam.sdk.io.iceberg.RecordWriterManagerTest}.
 */
public class SerializableDataFileTest {
  static final Set<String> FIELDS_SET =
      ImmutableSet.<String>builder()
          .add("path")
          .add("fileFormat")
          .add("recordCount")
          .add("fileSizeInBytes")
          .add("partitionPath")
          .add("partitionSpecId")
          .add("keyMetadata")
          .add("splitOffsets")
          .add("columnSizes")
          .add("valueCounts")
          .add("nullValueCounts")
          .add("nanValueCounts")
          .add("lowerBounds")
          .add("upperBounds")
          .build();

  @Test
  public void testFieldsInEqualsMethodInSyncWithGetterFields() {
    List<String> getMethodNames =
        Arrays.stream(SerializableDataFile.class.getDeclaredMethods())
            .map(Method::getName)
            .filter(methodName -> methodName.startsWith("get"))
            .collect(Collectors.toList());

    List<String> lowerCaseFields =
        FIELDS_SET.stream().map(String::toLowerCase).collect(Collectors.toList());
    List<String> extras = new ArrayList<>();
    for (String field : getMethodNames) {
      if (!lowerCaseFields.contains(field.substring(3).toLowerCase())) {
        extras.add(field);
      }
    }
    if (!extras.isEmpty()) {
      throw new IllegalStateException(
          "Detected new field(s) added to SerializableDataFile: "
              + extras
              + "\nPlease include the new field(s) in SerializableDataFile's equals() and hashCode() methods, then add them "
              + "to this test class's FIELDS_SET.");
    }
  }
}
