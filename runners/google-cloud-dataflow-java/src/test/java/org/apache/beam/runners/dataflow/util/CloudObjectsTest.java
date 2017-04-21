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

package org.apache.beam.runners.dataflow.util;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.util.CloudObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for {@link CloudObjects}.
 */
@RunWith(Parameterized.class)
public class CloudObjectsTest {
  @Parameters(name = "{index}: {0}")
  public static Iterable<Coder<?>> data() {
    return ImmutableList.<Coder<?>>builder().add(new RecordCoder()).build();
  }

  @Parameter(0)
  public Coder<?> coder;

  @Test
  public void toAndFromCloudObject() throws Exception {
    CloudObject cloudObject = CloudObjects.asCloudObject(coder);
    Coder<?> reconstructed = CloudObjects.coderFromCloudObject(cloudObject);

    assertEquals(coder.getClass(), reconstructed.getClass());
    assertEquals(coder, reconstructed);
  }

  static class Record implements Serializable {}

  private static class RecordCoder extends CustomCoder<Record> {
    @Override
    public void encode(Record value, OutputStream outStream, Context context)
        throws CoderException, IOException {}

    @Override
    public Record decode(InputStream inStream, Context context)
        throws CoderException, IOException {
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
