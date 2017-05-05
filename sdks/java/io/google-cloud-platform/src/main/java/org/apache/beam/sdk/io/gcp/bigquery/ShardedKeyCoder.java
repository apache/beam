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

package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;


/**
 * A {@link Coder} for {@link ShardedKey}, using a wrapped key {@link Coder}.
 */
@VisibleForTesting
class ShardedKeyCoder<KeyT>
    extends StructuredCoder<ShardedKey<KeyT>> {
  public static <KeyT> ShardedKeyCoder<KeyT> of(Coder<KeyT> keyCoder) {
    return new ShardedKeyCoder<>(keyCoder);
  }

  private final Coder<KeyT> keyCoder;
  private final VarIntCoder shardNumberCoder;

  protected ShardedKeyCoder(Coder<KeyT> keyCoder) {
    this.keyCoder = keyCoder;
    this.shardNumberCoder = VarIntCoder.of();
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(keyCoder);
  }

  @Override
  public void encode(ShardedKey<KeyT> key, OutputStream outStream)
      throws IOException {
    keyCoder.encode(key.getKey(), outStream);
    shardNumberCoder.encode(key.getShardNumber(), outStream);
  }

  @Override
  public ShardedKey<KeyT> decode(InputStream inStream)
      throws IOException {
    return new ShardedKey<>(
        keyCoder.decode(inStream),
        shardNumberCoder.decode(inStream));
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    keyCoder.verifyDeterministic();
  }
}
