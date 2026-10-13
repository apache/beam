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
package org.apache.beam.runners.core;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.values.CausedByDrain;

/**
 * Encapsulates metadata that propagates with elements in the pipeline.
 *
 * <p>This metadata is sent along with elements. It currently includes fields like {@link
 * CausedByDrain}, and is designed to be extensible to support future metadata fields such as
 * OpenTelemetry context or CDC (Change Data Capture) kind.
 *
 * <p>The purpose of this class is to group targeted metadata fields together. This makes it easier
 * to define combination strategies (e.g., when accumulating state in {@code ReduceFnRunner}) when
 * multiple elements are merged or grouped, without having to extend method signatures or state
 * handling for every new metadata field.
 */
@AutoValue
public abstract class CombinedMetadata {
  public abstract CausedByDrain causedByDrain();

  public static CombinedMetadata create(CausedByDrain causedByDrain) {
    return new AutoValue_CombinedMetadata(causedByDrain);
  }

  public static CombinedMetadata createDefault() {
    return create(CausedByDrain.NORMAL);
  }

  public static class Coder extends AtomicCoder<CombinedMetadata> {
    private static final Coder INSTANCE = new Coder();

    public static Coder of() {
      return INSTANCE;
    }

    @Override
    public void encode(CombinedMetadata value, OutputStream outStream) throws IOException {
      BeamFnApi.Elements.ElementMetadata proto =
          BeamFnApi.Elements.ElementMetadata.newBuilder()
              .setDrain(
                  value.causedByDrain() == CausedByDrain.CAUSED_BY_DRAIN
                      ? BeamFnApi.Elements.DrainMode.Enum.DRAINING
                      : BeamFnApi.Elements.DrainMode.Enum.NOT_DRAINING)
              .build();
      proto.writeDelimitedTo(outStream);
    }

    @Override
    public CombinedMetadata decode(InputStream inStream) throws IOException {
      BeamFnApi.Elements.ElementMetadata proto =
          BeamFnApi.Elements.ElementMetadata.parseDelimitedFrom(inStream);
      if (proto == null) {
        return CombinedMetadata.createDefault();
      }

      CausedByDrain causedByDrain =
          proto.getDrain() == BeamFnApi.Elements.DrainMode.Enum.DRAINING
              ? CausedByDrain.CAUSED_BY_DRAIN
              : CausedByDrain.NORMAL;

      return CombinedMetadata.create(causedByDrain);
    }
  }
}
