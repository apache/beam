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
package org.apache.beam.sdk.schemas;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

@Experimental(Kind.SCHEMAS)
public class LogicalTypeCoder<InputT, BaseT> extends CustomCoder<InputT> {

  private final Schema.LogicalType<InputT, BaseT> logicalType;
  private final Coder<BaseT> baseCoder;

  private LogicalTypeCoder(Schema.LogicalType<InputT, BaseT> logicalType, Coder<BaseT> baseCoder) {
    this.logicalType = logicalType;
    this.baseCoder = baseCoder;
  }

  public static <InputT, BaseT> LogicalTypeCoder<InputT, BaseT> of(
      Schema.LogicalType<InputT, BaseT> logicalType, Coder<BaseT> baseCoder) {
    return new LogicalTypeCoder<>(logicalType, baseCoder);
  }

  public Schema.LogicalType<InputT, BaseT> getLogicalType() {
    return logicalType;
  }

  public Coder<BaseT> getBaseCoder() {
    return baseCoder;
  }

  @Override
  public void encode(InputT value, OutputStream outStream) throws IOException {
    baseCoder.encode(logicalType.toBaseType(value), outStream);
  }

  @Override
  public InputT decode(InputStream inStream) throws IOException {
    return logicalType.toInputType(baseCoder.decode(inStream));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return super.getCoderArguments();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    baseCoder.verifyDeterministic();
  }

  @Override
  public boolean consistentWithEquals() {
    // we can't assume that InputT is consistent with equals
    return false;
  }

  @Override
  public Object structuralValue(InputT value) {
    if (baseCoder.consistentWithEquals()) {
      return logicalType.toBaseType(value);
    } else {
      return baseCoder.structuralValue(logicalType.toBaseType(value));
    }
  }

  @Override
  public boolean isRegisterByteSizeObserverCheap(InputT value) {
    return baseCoder.isRegisterByteSizeObserverCheap(logicalType.toBaseType(value));
  }

  @Override
  public void registerByteSizeObserver(InputT value, ElementByteSizeObserver observer)
      throws Exception {
    baseCoder.registerByteSizeObserver(logicalType.toBaseType(value), observer);
  }
}
