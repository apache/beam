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

package org.apache.beam.runners.core.construction;

import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.Components;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for {@link Coders}.
 */
@RunWith(Parameterized.class)
public class CodersTest {
  @Parameters(name = "{index}: {0}")
  public static Iterable<Coder<?>> data() {
    return ImmutableList.<Coder<?>>of(
        StringUtf8Coder.of(),
        IterableCoder.of(VarLongCoder.of()),
        KvCoder.of(StringUtf8Coder.of(), ListCoder.of(VarLongCoder.of())),
        SerializableCoder.of(Record.class),
        new RecordCoder(),
        KvCoder.of(new RecordCoder(), AvroCoder.of(Record.class)));
  }

  @Parameter(0)
  public Coder<?> coder;

  @Test
  public void toAndFromProto() throws Exception {
    SdkComponents componentsBuilder = SdkComponents.create();
    RunnerApi.Coder coderProto = Coders.toProto(coder, componentsBuilder);

    Components encodedComponents = componentsBuilder.toComponents();
    Coder<?> decodedCoder = Coders.fromProto(coderProto, encodedComponents);
    assertThat(decodedCoder, Matchers.<Coder<?>>equalTo(coder));
  }

  static class Record implements Serializable {
  }

  private static class RecordCoder extends CustomCoder<Record> {
    @Override
    public void encode(Record value, OutputStream outStream, Context context)
        throws CoderException, IOException {}

    @Override
    public Record decode(InputStream inStream, Context context) throws CoderException, IOException {
      return new Record();
    }

    @Override
    public boolean equals(Object other) {
      return other != null && getClass().equals(other.getClass());
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }
}
