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
package org.apache.beam.sdk.fn.data;

import java.io.InputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;

/** A receiver of encoded data, decoding it and passing it onto a downstream consumer. */
public class DecodingFnDataReceiver<T> implements FnDataReceiver<ByteString> {

  private final Coder<T> coder;
  private final FnDataReceiver<T> consumer;

  public DecodingFnDataReceiver(Coder<T> coder, FnDataReceiver<T> consumer) {
    this.coder = coder;
    this.consumer = consumer;
  }

  public static <T> DecodingFnDataReceiver<T> create(Coder<T> coder, FnDataReceiver<T> consumer) {
    return new DecodingFnDataReceiver<T>(coder, consumer);
  }

  @Override
  public void accept(ByteString input) throws Exception {
    InputStream inputStream = input.newInput();
    while (inputStream.available() > 0) {
      consumer.accept(coder.decode(inputStream));
    }
  }
}
