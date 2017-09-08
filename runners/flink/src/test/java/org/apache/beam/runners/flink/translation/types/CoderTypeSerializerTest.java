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
package org.apache.beam.runners.flink.translation.types;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CoderTypeSerializerTest {

  @Test
  public void shouldBeAbleToWriteSnapshotForAnonymousClassCoder() throws Exception {
    AtomicCoder<String> anonymousClassCoder = new AtomicCoder<String>() {

      @Override public void encode(String value, OutputStream outStream)
          throws CoderException, IOException {

      }

      @Override public String decode(InputStream inStream) throws CoderException, IOException {
        return "";
      }
    };

    CoderTypeSerializer<String> serializer = new CoderTypeSerializer<>(anonymousClassCoder);

    TypeSerializerConfigSnapshot configSnapshot = serializer.snapshotConfiguration();
    configSnapshot.write(new ComparatorTestBase.TestOutputView());
  }

  @Test
  public void shouldBeAbleToWriteSnapshotForConcreteClassCoder() throws Exception {
    Coder<String> concreteClassCoder = StringUtf8Coder.of();
    CoderTypeSerializer<String> coderTypeSerializer = new CoderTypeSerializer<>(concreteClassCoder);
    TypeSerializerConfigSnapshot typeSerializerConfigSnapshot = coderTypeSerializer
        .snapshotConfiguration();
    typeSerializerConfigSnapshot.write(new ComparatorTestBase.TestOutputView());
  }
}

