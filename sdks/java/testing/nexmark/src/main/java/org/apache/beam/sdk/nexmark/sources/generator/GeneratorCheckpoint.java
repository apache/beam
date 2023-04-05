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
package org.apache.beam.sdk.nexmark.sources.generator;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.toStringHelper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.UnboundedSource;

/** Just enough state to be able to restore a generator back to where it was checkpointed. */
public class GeneratorCheckpoint implements UnboundedSource.CheckpointMark {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();

  /** Coder for this class. */
  public static final Coder<GeneratorCheckpoint> CODER_INSTANCE =
      new CustomCoder<GeneratorCheckpoint>() {
        @Override
        public void encode(GeneratorCheckpoint value, OutputStream outStream)
            throws CoderException, IOException {
          LONG_CODER.encode(value.numEvents, outStream);
          LONG_CODER.encode(value.wallclockBaseTime, outStream);
        }

        @Override
        public GeneratorCheckpoint decode(InputStream inStream) throws CoderException, IOException {
          long numEvents = LONG_CODER.decode(inStream);
          long wallclockBaseTime = LONG_CODER.decode(inStream);
          return new GeneratorCheckpoint(numEvents, wallclockBaseTime);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {}
      };

  private final long numEvents;
  private final long wallclockBaseTime;

  GeneratorCheckpoint(long numEvents, long wallclockBaseTime) {
    this.numEvents = numEvents;
    this.wallclockBaseTime = wallclockBaseTime;
  }

  public Generator toGenerator(GeneratorConfig config) {
    return new Generator(config, numEvents, wallclockBaseTime);
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
    // Nothing to finalize.
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("numEvents", numEvents)
        .add("wallclockBaseTime", wallclockBaseTime)
        .toString();
  }
}
