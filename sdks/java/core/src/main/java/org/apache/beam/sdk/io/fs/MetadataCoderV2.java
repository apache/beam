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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata.Builder;

/** A {@link Coder} for {@link Metadata} that includes {@link Metadata#lastModifiedMillis()}. */
@Experimental(Kind.FILESYSTEM)
public class MetadataCoderV2 extends AtomicCoder<Metadata> {
  private static final MetadataCoderV2 INSTANCE = new MetadataCoderV2();
  private static final MetadataCoder V1_CODER = MetadataCoder.of();
  private static final VarLongCoder LONG_CODER = VarLongCoder.of();

  private MetadataCoderV2() {}

  /** Returns the singleton {@link MetadataCoderV2} instance. */
  public static MetadataCoderV2 of() {
    return INSTANCE;
  }

  @Override
  public void encode(Metadata value, OutputStream os) throws IOException {
    V1_CODER.encode(value, os);
    LONG_CODER.encode(value.lastModifiedMillis(), os);
  }

  @Override
  public Metadata decode(InputStream is) throws IOException {
    Builder builder = V1_CODER.decodeBuilder(is);
    long lastModifiedMillis = LONG_CODER.decode(is);
    return builder.setLastModifiedMillis(lastModifiedMillis).build();
  }

  @Override
  public boolean consistentWithEquals() {
    return true;
  }
}
