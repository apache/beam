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
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.transforms.SerializableBiConsumer;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class MetadataDynamicCoder extends StructuredCoder<MatchResult.Metadata> {

  private static final MetadataCoder V1_CODER = MetadataCoder.of();

  private List<MetadataFieldCoderDescription> fieldCoders = new ArrayList<>();

  public MetadataDynamicCoder() {}

  public <T> MetadataDynamicCoder withCoderForField(
      Coder<T> coder,
      SerializableFunction<? super MatchResult.Metadata, T> getter,
      SerializableBiConsumer<? super MatchResult.Metadata.Builder, T> setter) {
    MetadataFieldCoderDescription metadataFieldCoderDescription =
        new MetadataFieldCoderDescription(coder, getter, setter);
    fieldCoders.add(metadataFieldCoderDescription);
    return this;
  }

  @Override
  public void encode(MatchResult.Metadata metadata, OutputStream outStream) throws IOException {
    V1_CODER.encode(metadata, outStream);
    for (MetadataFieldCoderDescription fieldCoderDescription : fieldCoders) {
      SerializableFunction<? super MatchResult.Metadata, ?> getter =
          fieldCoderDescription.getGetter();
      Coder coder = fieldCoderDescription.getCoder();
      try {
        coder.encode(getter.apply(metadata), outStream);
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to encode " + getter + " with coder " + coder.getClass());
      }
    }
  }

  @Override
  public MatchResult.Metadata decode(InputStream inStream) throws IOException {
    MatchResult.Metadata.Builder builder = V1_CODER.decodeBuilder(inStream);

    for (MetadataFieldCoderDescription metadataFieldCoderDescription : fieldCoders) {
      Coder coder = metadataFieldCoderDescription.getCoder();
      BiConsumer setter = metadataFieldCoderDescription.getSetter();

      try {
        setter.accept(builder, coder.decode(inStream));
      } catch (Exception e) {
        throw new RuntimeException("Failed to decode with coder " + coder.getClass());
      }
    }
    return builder.build();
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return fieldCoders.stream()
        .map(MetadataFieldCoderDescription::getCoder)
        .collect(Collectors.toList());
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    for (Coder<?> coder : getCoderArguments()) {
      verifyDeterministic(this, "Coder must be deterministic " + coder.getClass(), coder);
    }
  }
}
