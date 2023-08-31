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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class DoubleDeltaCoder extends AtomicCoder<List<Double>> {
  private static final DoubleDeltaCoder INSTANCE = new DoubleDeltaCoder();

  public static DoubleDeltaCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(List<Double> value, OutputStream outStream)
      throws CoderException, IOException {
    VarIntCoder.of().encode(value.size(), outStream);
    long previous = 0;
    for (Double d : value) {
      long bits = Double.doubleToLongBits(d);
      VarLongCoder.of().encode(bits ^ previous, outStream);
      previous = bits;
    }
  }

  @Override
  public List<Double> decode(InputStream inStream) throws CoderException, IOException {
    int size = VarIntCoder.of().decode(inStream);
    ArrayList<Double> result = new ArrayList<>(size);
    long previous = 0;
    for (int i = 0; i < size; ++i) {
      long current = VarLongCoder.of().decode(inStream) ^ previous;
      result.add(Double.longBitsToDouble(current));
      previous = current;
    }
    return result;
  }
}
