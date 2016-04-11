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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.TextIO.Write.Bound;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;

class TextIOShardedWriteFactory implements PTransformOverrideFactory {

  @Override
  public <InputT extends PInput, OutputT extends POutput> PTransform<InputT, OutputT> override(
      PTransform<InputT, OutputT> transform) {
    if (transform instanceof TextIO.Write.Bound) {
      @SuppressWarnings("unchecked")
      TextIO.Write.Bound<InputT> originalWrite = (TextIO.Write.Bound<InputT>) transform;
      if (originalWrite.getNumShards() > 1
          || (originalWrite.getNumShards() == 1
              && !"".equals(originalWrite.getShardNameTemplate()))) {
        @SuppressWarnings("unchecked")
        PTransform<InputT, OutputT> override =
            (PTransform<InputT, OutputT>) new TextIOShardedWrite<InputT>(originalWrite);
        return override;
      }
    }
    return transform;
  }

  private static class TextIOShardedWrite<InputT> extends ShardControlledWrite<InputT> {
    private final TextIO.Write.Bound<InputT> initial;

    private TextIOShardedWrite(Bound<InputT> initial) {
      this.initial = initial;
    }

    @Override
    int getNumShards() {
      return initial.getNumShards();
    }

    @Override
    PTransform<PCollection<InputT>, PDone> getSingleShardTransform(int shardNum) {
      String shardName =
          IOChannelUtils.constructName(
              initial.getFilenamePrefix(),
              initial.getShardTemplate(),
              initial.getFilenameSuffix(),
              shardNum,
              getNumShards());
      return TextIO.Write.withCoder(initial.getCoder()).to(shardName).withoutSharding();
    }

    @Override
    protected PTransform<PCollection<InputT>, PDone> delegate() {
      return initial;
    }
  }
}
