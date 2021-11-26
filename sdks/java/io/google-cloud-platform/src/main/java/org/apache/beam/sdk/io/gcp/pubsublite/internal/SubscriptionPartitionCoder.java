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

import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;

public class SubscriptionPartitionCoder extends AtomicCoder<SubscriptionPartition> {
  private static final Coder<SubscriptionPartition> CODER =
      DelegateCoder.of(
          KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()),
          SubscriptionPartitionCoder::toKv,
          SubscriptionPartitionCoder::fromKv);

  private static KV<String, Long> toKv(SubscriptionPartition value) {
    return KV.of(value.subscription().toString(), value.partition().value());
  }

  private static SubscriptionPartition fromKv(KV<String, Long> kv) {
    return SubscriptionPartition.of(
        SubscriptionPath.parse(kv.getKey()), Partition.of(kv.getValue()));
  }

  @Override
  public void encode(SubscriptionPartition value, OutputStream outStream) throws IOException {
    CODER.encode(value, outStream);
  }

  @Override
  public SubscriptionPartition decode(InputStream inStream) throws IOException {
    return CODER.decode(inStream);
  }

  public static CoderProvider getCoderProvider() {
    return CoderProviders.forCoder(
        TypeDescriptor.of(SubscriptionPartition.class), new SubscriptionPartitionCoder());
  }
}
