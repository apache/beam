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
package org.apache.beam.sdk.io.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;

/**
 * A {@link Coder} for {@link Metadata}.
 *
 * <p>The {@link Metadata#lastModifiedMillis()} field was added after this coder was already
 * deployed, so this class decodes a default value for backwards compatibility. See {@link
 * MetadataCoderV2} for retaining timestamp information.
 */
public class MetadataCoder extends AtomicCoder<Metadata> {
  private static final MetadataCoder INSTANCE = new MetadataCoder();
  private static final ResourceIdCoder RESOURCE_ID_CODER = ResourceIdCoder.of();
  private static final VarIntCoder INT_CODER = VarIntCoder.of();
  private static final VarLongCoder LONG_CODER = VarLongCoder.of();

  private MetadataCoder() {}

  /** Returns the singleton {@link MetadataCoder} instance. */
  public static MetadataCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(Metadata value, OutputStream os) throws IOException {
    RESOURCE_ID_CODER.encode(value.resourceId(), os);
    INT_CODER.encode(value.isReadSeekEfficient() ? 1 : 0, os);
    LONG_CODER.encode(value.sizeBytes(), os);
  }

  @Override
  public Metadata decode(InputStream is) throws IOException {
    return decodeBuilder(is).build();
  }

  Metadata.Builder decodeBuilder(InputStream is) throws IOException {
    ResourceId resourceId = RESOURCE_ID_CODER.decode(is);
    boolean isReadSeekEfficient = INT_CODER.decode(is) == 1;
    long sizeBytes = LONG_CODER.decode(is);
    return Metadata.builder()
        .setResourceId(resourceId)
        .setIsReadSeekEfficient(isReadSeekEfficient)
        .setSizeBytes(sizeBytes);
  }

  @Override
  public boolean consistentWithEquals() {
    return true;
  }
}
