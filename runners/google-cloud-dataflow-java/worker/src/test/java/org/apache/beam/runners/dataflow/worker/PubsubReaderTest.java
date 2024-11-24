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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link PubsubReader}. */
@RunWith(JUnit4.class)
public class PubsubReaderTest {
  @Mock StreamingModeExecutionContext mockContext;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  private void testReadWith(String parseFn) throws Exception {
    when(mockContext.getWorkItem())
        .thenReturn(
            Windmill.WorkItem.newBuilder()
                .setKey(ByteString.copyFromUtf8("key"))
                .setWorkToken(0)
                .addMessageBundles(
                    Windmill.InputMessageBundle.newBuilder()
                        .setSourceComputationId("pubsub")
                        .addMessages(
                            Windmill.Message.newBuilder()
                                .setTimestamp(0)
                                .setData(ByteString.copyFromUtf8("e0")))
                        .addMessages(
                            Windmill.Message.newBuilder()
                                .setTimestamp(1000)
                                .setData(ByteString.copyFromUtf8("e1")))
                        .addMessages(
                            Windmill.Message.newBuilder()
                                .setTimestamp(2000)
                                .setData(ByteString.copyFromUtf8("e2"))))
                .build());

    Map<String, Object> spec = new HashMap<>();
    spec.put(PropertyNames.OBJECT_TYPE_NAME, "");
    if (parseFn != null) {
      spec.put(PropertyNames.PUBSUB_SERIALIZED_ATTRIBUTES_FN, parseFn);
    }
    CloudObject cloudSourceSpec = CloudObject.fromSpec(spec);
    PubsubReader.Factory factory = new PubsubReader.Factory();
    PubsubReader<String> reader =
        (PubsubReader<String>)
            factory.create(
                cloudSourceSpec,
                WindowedValue.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder()),
                null,
                mockContext,
                null);

    NativeReader.NativeReaderIterator<WindowedValue<String>> iter = reader.iterator();
    assertTrue(iter.start());
    assertEquals(
        iter.getCurrent(), WindowedValue.timestampedValueInGlobalWindow("e0", new Instant(0)));
    assertTrue(iter.advance());
    assertEquals(
        iter.getCurrent(), WindowedValue.timestampedValueInGlobalWindow("e1", new Instant(1)));
    assertTrue(iter.advance());
    assertEquals(
        iter.getCurrent(), WindowedValue.timestampedValueInGlobalWindow("e2", new Instant(2)));
    assertFalse(iter.advance());
  }

  @Test
  public void testBasic() throws Exception {
    testReadWith(null /* No ParseFn */);
  }

  @Test
  public void testEmptyParseFn() throws Exception {
    testReadWith("");
  }
}
