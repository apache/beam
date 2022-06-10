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
package org.apache.beam.sdk.io.synthetic;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;

/**
 * Holds enough state to resume generating synthetic records back from where it was checkpointed.
 */
class SyntheticRecordsCheckpoint implements UnboundedSource.CheckpointMark {
  private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();

  /** Coder for this class. */
  public static final Coder<SyntheticRecordsCheckpoint> CODER =
      new AtomicCoder<SyntheticRecordsCheckpoint>() {

        @Override
        public void encode(SyntheticRecordsCheckpoint value, OutputStream outStream)
            throws IOException {
          LONG_CODER.encode(value.currentCheckMarkPosition, outStream);
        }

        @Override
        public SyntheticRecordsCheckpoint decode(InputStream inStream) throws IOException {
          long currentCheckMarkPosition = LONG_CODER.decode(inStream);

          return new SyntheticRecordsCheckpoint(currentCheckMarkPosition);
        }
      };

  private final long currentCheckMarkPosition;

  public SyntheticRecordsCheckpoint(long startPosition) {
    this.currentCheckMarkPosition = startPosition;
  }

  @Override
  public void finalizeCheckpoint() {
    // Nothing to finalize.
  }

  public long getCurrentCheckMarkPosition() {
    return currentCheckMarkPosition;
  }
}
