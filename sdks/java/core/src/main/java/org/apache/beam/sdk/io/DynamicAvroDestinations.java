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
package org.apache.beam.sdk.io;

import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A specialization of {@link DynamicDestinations} for {@link AvroIO}. In addition to dynamic file
 * destinations, this allows specifying other AVRO properties (schema, metadata, codec, datum
 * writer) per destination.
 */
public abstract class DynamicAvroDestinations<UserT, DestinationT, OutputT>
    extends DynamicDestinations<UserT, DestinationT, OutputT> {
  /** Return an AVRO schema for a given destination. */
  public abstract Schema getSchema(DestinationT destination);

  /** Return AVRO file metadata for a given destination. */
  public Map<String, Object> getMetadata(DestinationT destination) {
    return ImmutableMap.of();
  }

  /** Return an AVRO codec for a given destination. */
  public CodecFactory getCodec(DestinationT destination) {
    return AvroIO.TypedWrite.DEFAULT_CODEC;
  }

  /**
   * Return a {@link AvroSink.DatumWriterFactory} for a given destination. If provided, it will be
   * used to created {@link org.apache.avro.io.DatumWriter} instances as required.
   */
  public AvroSink.@Nullable DatumWriterFactory<OutputT> getDatumWriterFactory(
      DestinationT destinationT) {
    return null;
  }
}
