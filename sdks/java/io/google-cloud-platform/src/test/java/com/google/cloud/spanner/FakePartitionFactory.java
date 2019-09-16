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
package com.google.cloud.spanner;

import com.google.protobuf.ByteString;
import java.util.Arrays;

/**
 * Creates fake {@link Partition} objects.
 *
 * <p>TODO: We cannot use mockito in tests because its serialization does not preserve the equals
 * semantics. Move this fake object to the Cloud Spanner library.
 */
public class FakePartitionFactory {

  private FakePartitionFactory() {}

  public static Partition createFakeQueryPartition(ByteString token) {
    return Partition.createQueryPartition(
        token, PartitionOptions.getDefaultInstance(), Statement.of(""), Options.fromQueryOptions());
  }

  public static Partition createFakeReadPartition(ByteString token) {
    return Partition.createReadPartition(
        token,
        PartitionOptions.getDefaultInstance(),
        "",
        "",
        KeySet.all(),
        Arrays.asList(),
        Options.fromReadOptions());
  }
}
