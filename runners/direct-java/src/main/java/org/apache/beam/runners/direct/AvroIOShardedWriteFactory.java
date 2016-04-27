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
package org.apache.beam.runners.direct;

import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

class AvroIOShardedWriteFactory implements PTransformOverrideFactory {
  @Override
  public <InputT extends PInput, OutputT extends POutput> PTransform<InputT, OutputT> override(
      PTransform<InputT, OutputT> transform) {
    if (transform instanceof AvroIO.Write.Bound) {
      @SuppressWarnings("unchecked")
      AvroIO.Write.Bound<InputT> originalWrite = (AvroIO.Write.Bound<InputT>) transform;
      if (originalWrite.getNumShards() > 1
          || (originalWrite.getNumShards() == 1
              && !"".equals(originalWrite.getShardNameTemplate()))) {
        @SuppressWarnings("unchecked")
        PTransform<InputT, OutputT> override =
            (PTransform<InputT, OutputT>) new AvroIOShardedWrite<InputT>(originalWrite);
        return override;
      }
    }
    return transform;
  }

  private class AvroIOShardedWrite<InputT> extends ShardControlledWrite<InputT> {
    private final AvroIO.Write.Bound<InputT> initial;

    private AvroIOShardedWrite(AvroIO.Write.Bound<InputT> initial) {
      this.initial = initial;
    }

    @Override
    int getNumShards() {
      return initial.getNumShards();
    }

    @Override
    PTransform<? super PCollection<InputT>, PDone> getSingleShardTransform(int shardNum) {
      String shardName =
          IOChannelUtils.constructName(
              initial.getFilenamePrefix(),
              initial.getShardNameTemplate(),
              initial.getFilenameSuffix(),
              shardNum,
              getNumShards());
      return initial.withoutSharding().to(shardName).withSuffix("");
    }

    @Override
    protected PTransform<PCollection<InputT>, PDone> delegate() {
      return initial;
    }
  }
}
