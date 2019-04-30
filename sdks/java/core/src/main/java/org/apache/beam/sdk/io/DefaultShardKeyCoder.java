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
package org.apache.beam.sdk.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;

/** A {@link Coder} for {@link DefaultShardKey}. */
class DefaultShardKeyCoder extends Coder<DefaultShardKey> {

  public static DefaultShardKeyCoder of() {
    return INSTANCE;
  }

  private static final DefaultShardKeyCoder INSTANCE = new DefaultShardKeyCoder();
  private final VarIntCoder intCoder;

  protected DefaultShardKeyCoder() {
    this.intCoder = VarIntCoder.of();
  }

  @Override
  public void encode(DefaultShardKey key, OutputStream outStream) throws IOException {
    intCoder.encode(key.getKey(), outStream);
    intCoder.encode(key.getShardNumber(), outStream);
  }

  @Override
  public DefaultShardKey decode(InputStream inStream) throws IOException {
    return DefaultShardKey.of(intCoder.decode(inStream), intCoder.decode(inStream));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return ImmutableList.of();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    intCoder.verifyDeterministic();
  }
}
