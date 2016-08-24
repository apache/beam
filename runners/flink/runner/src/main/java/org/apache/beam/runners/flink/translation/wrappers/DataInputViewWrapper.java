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
package org.apache.beam.runners.flink.translation.wrappers;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.flink.core.memory.DataInputView;

/**
 * Wrapper for {@link DataInputView}. We need this because Flink reads data using a
 * {@link org.apache.flink.core.memory.DataInputView} while
 * Dataflow {@link org.apache.beam.sdk.coders.Coder}s expect an
 * {@link java.io.InputStream}.
 */
public class DataInputViewWrapper extends InputStream {

  private DataInputView inputView;

  public DataInputViewWrapper(DataInputView inputView) {
    this.inputView = inputView;
  }

  public void setInputView(DataInputView inputView) {
    this.inputView = inputView;
  }

  @Override
  public int read() throws IOException {
    try {
      return inputView.readUnsignedByte();
    } catch (EOFException e) {
      // translate between DataInput and InputStream,
      // DataInput signals EOF by exception, InputStream does it by returning -1
      return -1;
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return inputView.read(b, off, len);
  }
}
