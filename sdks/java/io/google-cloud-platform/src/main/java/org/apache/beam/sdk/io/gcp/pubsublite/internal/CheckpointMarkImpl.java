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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.cloud.pubsublite.Offset;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointMarkImpl implements CheckpointMark {

  private final Logger logger = LoggerFactory.getLogger(CheckpointMarkImpl.class);

  final Offset offset;

  private final Optional<Supplier<BlockingCommitter>> committer;

  CheckpointMarkImpl(Offset offset, Supplier<BlockingCommitter> committer) {
    this.offset = offset;
    this.committer = Optional.of(committer);
  }

  /** Internal-only constructor for deserialization. */
  private CheckpointMarkImpl(Offset offset) {
    this.offset = offset;
    this.committer = Optional.empty();
  }

  static Coder<CheckpointMarkImpl> coder() {
    return new AtomicCoder<CheckpointMarkImpl>() {
      @Override
      public void encode(CheckpointMarkImpl value, OutputStream outStream) throws IOException {
        VarLongCoder.of().encode(value.offset.value(), outStream);
      }

      @Override
      public CheckpointMarkImpl decode(InputStream inStream) throws IOException {
        return new CheckpointMarkImpl(Offset.of(VarLongCoder.of().decode(inStream)));
      }
    };
  }

  @Override
  public void finalizeCheckpoint() {
    try {
      checkState(committer.isPresent());
      committer.get().get().commitOffset(offset);
    } catch (Exception e) {
      logger.warn("Failed to finalize checkpoint.", e);
    }
  }
}
