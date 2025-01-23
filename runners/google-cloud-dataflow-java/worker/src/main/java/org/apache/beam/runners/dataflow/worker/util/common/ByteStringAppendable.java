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
package org.apache.beam.runners.dataflow.worker.util.common;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.Preconditions;

/*
 * An Appendable implementation that appends CharSequence in UTF-8
 * to the passed in ByteStringOutputStream. This can be used over
 * new OutputStreamWriter(stream, StandardCharsets.UTF_8) when thread safety of
 * OutputStreamWriter is not required.
 */
@NotThreadSafe
public class ByteStringAppendable implements Appendable {

  private final ByteStringOutputStream stream;

  public ByteStringAppendable(ByteStringOutputStream stream) {
    this.stream = stream;
  }

  @Override
  public Appendable append(@Nullable CharSequence csq) throws IOException {
    Preconditions.checkArgumentNotNull(csq);
    stream.write(csq.toString().getBytes(StandardCharsets.UTF_8));
    return this;
  }

  @Override
  public Appendable append(@Nullable CharSequence csq, int start, int end) throws IOException {
    Preconditions.checkArgumentNotNull(csq);
    stream.write(csq.subSequence(start, end).toString().getBytes(StandardCharsets.UTF_8));
    return this;
  }

  @Override
  public Appendable append(char c) throws IOException {
    stream.write(String.valueOf(c).getBytes(StandardCharsets.UTF_8));
    return this;
  }
}
