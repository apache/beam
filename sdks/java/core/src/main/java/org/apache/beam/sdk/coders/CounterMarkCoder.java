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
package org.apache.beam.sdk.coders;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import org.apache.beam.sdk.io.CountingSource;
import org.joda.time.Instant;

public class CounterMarkCoder extends CustomCoder<CountingSource.CounterMark> {

  @Override
  public void encode(CountingSource.CounterMark value, OutputStream outStream)
      throws CoderException, IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null CounterMark");
    }

    DataOutputStream stream = new DataOutputStream(outStream);
    stream.writeLong(value.getLastEmitted());
    InstantCoder.of().encode(value.getStartTime(), stream);
  }

  @Override
  public CountingSource.CounterMark decode(InputStream inStream)
      throws CoderException, IOException {
    try {
      DataInputStream stream = new DataInputStream(inStream);
      long lastEmitted = stream.readLong();
      Instant startTime = InstantCoder.of().decode(stream);
      return new CountingSource.CounterMark(lastEmitted, startTime);
    } catch (EOFException | UTFDataFormatException exn) {
      throw new CoderException(exn);
    }
  }
}
