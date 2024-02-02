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
package org.apache.beam.runners.flink.translation.types.compat;

import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.core.io.VersionedIOReadableWritable;

/** A legacy snapshot which does not care about schema compatibility. */
@SuppressWarnings("allcheckers")
public class UnversionedTypeSerializerSnapshot<T> extends TypeSerializerConfigSnapshot<T> {

  /** Needs to be public to work with {@link VersionedIOReadableWritable}. */
  public UnversionedTypeSerializerSnapshot() {}

  @SuppressWarnings("initialization")
  public UnversionedTypeSerializerSnapshot(CoderTypeSerializer<T> serializer) {
    super.setPriorSerializer(serializer);
  }

  @Override
  public int getVersion() {
    // We always return the same version
    return 1;
  }

  @Override
  public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
      TypeSerializer<T> newSerializer) {
    // We assume compatibility because we don't have a way of checking schema compatibility
    return TypeSerializerSchemaCompatibility.compatibleAsIs();
  }
}
