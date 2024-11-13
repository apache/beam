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
package org.apache.beam.sdk.io.solace.write;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * This class a pseudo-key with a given cardinality. The downstream steps will use state {@literal
 * &} timers to distribute the data and control for the number of parallel workers used for writing.
 */
@Internal
public class AddShardKeyDoFn extends DoFn<Solace.Record, KV<Integer, Solace.Record>> {
  private final int shardCount;
  private int shardKey;

  public AddShardKeyDoFn(int shardCount) {
    this.shardCount = shardCount;
    shardKey = -1;
  }

  @ProcessElement
  public void processElement(
      @Element Solace.Record record, OutputReceiver<KV<Integer, Solace.Record>> c) {
    shardKey = (shardKey + 1) % shardCount;
    c.output(KV.of(shardKey, record));
  }
}
