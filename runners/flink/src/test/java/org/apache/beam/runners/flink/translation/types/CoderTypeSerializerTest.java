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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.junit.Test;

/** Tests {@link CoderTypeSerializer}. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class CoderTypeSerializerTest implements Serializable {

  @Test
  public void shouldWriteAndReadSnapshotForAnonymousClassCoder() throws Exception {
    AtomicCoder<String> anonymousClassCoder =
        new AtomicCoder<String>() {

          @Override
          public void encode(String value, OutputStream outStream) {}

          @Override
          public String decode(InputStream inStream) {
            return "";
          }
        };

    testWriteAndReadConfigSnapshot(anonymousClassCoder);
  }

  @Test
  public void shouldWriteAndReadSnapshotForConcreteClassCoder() throws Exception {
    Coder<String> concreteClassCoder = StringUtf8Coder.of();
    testWriteAndReadConfigSnapshot(concreteClassCoder);
  }

  private void testWriteAndReadConfigSnapshot(Coder<String> coder) throws IOException {
    CoderTypeSerializer<String> serializer =
        new CoderTypeSerializer<>(
            coder, new SerializablePipelineOptions(PipelineOptionsFactory.create()));

    TypeSerializerSnapshot writtenSnapshot = serializer.snapshotConfiguration();
    ComparatorTestBase.TestOutputView outView = new ComparatorTestBase.TestOutputView();
    writtenSnapshot.writeSnapshot(outView);

    TypeSerializerSnapshot readSnapshot = new UnversionedTypeSerializerSnapshot();
    readSnapshot.readSnapshot(
        writtenSnapshot.getCurrentVersion(), outView.getInputView(), getClass().getClassLoader());

    assertThat(readSnapshot.restoreSerializer(), is(serializer));
  }
}
