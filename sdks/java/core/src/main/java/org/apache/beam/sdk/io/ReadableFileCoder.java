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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MetadataCoder;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

/** A {@link Coder} for {@link org.apache.beam.sdk.io.FileIO.ReadableFile}. */
public class ReadableFileCoder extends StructuredCoder<FileIO.ReadableFile> {

  private final Coder<Metadata> metadataCoder;

  public static ReadableFileCoder of(Coder<Metadata> metadataCoder) {
    return new ReadableFileCoder(metadataCoder);
  }

  public static ReadableFileCoder of() {
    return new ReadableFileCoder(MetadataCoder.of());
  }

  public Coder<Metadata> getMetadataCoder() {
    return metadataCoder;
  }

  private ReadableFileCoder(Coder<Metadata> metadataCoder) {
    this.metadataCoder = metadataCoder;
  }

  @Override
  public void encode(
      FileIO.ReadableFile value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
      throws CoderException, IOException {
    getMetadataCoder().encode(value.getMetadata(), outStream);
    VarIntCoder.of().encode(value.getCompression().ordinal(), outStream);
  }

  @Override
  public FileIO.ReadableFile decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream)
      throws CoderException, IOException {
    MatchResult.Metadata metadata = getMetadataCoder().decode(inStream);
    Compression compression = Compression.values()[VarIntCoder.of().decode(inStream)];
    return new FileIO.ReadableFile(metadata, compression);
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<? extends Coder<?>> getCoderArguments() {
    return Collections.singletonList(metadataCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    // ignore the default Metadata coder for backward compatible
    if (!getMetadataCoder().equals(MetadataCoder.of())) {
      verifyDeterministic(this, "Metadata coder must be deterministic", getMetadataCoder());
    }
  }
}
