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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WindmillReaderIteratorBase}. */
@RunWith(JUnit4.class)
public class WindmillReaderIteratorBaseTest {
  private static class TestWindmillReaderIterator extends WindmillReaderIteratorBase<Long> {
    protected TestWindmillReaderIterator(Windmill.WorkItem work) {
      super(work);
    }

    @Override
    protected WindowedValue<Long> decodeMessage(Windmill.Message message) {
      return WindowedValue.valueInGlobalWindow(message.getTimestamp());
    }
  }

  @Test
  public void testBasic() throws IOException {
    testForMessageBundleCounts();
    testForMessageBundleCounts(0);
    testForMessageBundleCounts(0, 0);
    testForMessageBundleCounts(1);
    testForMessageBundleCounts(2);
    testForMessageBundleCounts(1, 1);
    testForMessageBundleCounts(0, 1);
    testForMessageBundleCounts(1, 0);
    testForMessageBundleCounts(0, 0, 1, 3, 0, 1, 0, 0, 0, 1);
    testForMessageBundleCounts(0, 0, 1, 3, 0, 1, 0, 0, 0, 0);
  }

  private void testForMessageBundleCounts(int... messageBundleCounts) throws IOException {
    List<Windmill.InputMessageBundle> bundles = new ArrayList<>();
    long numTotalMessages = 0;
    for (int count : messageBundleCounts) {
      Windmill.InputMessageBundle.Builder bundle =
          Windmill.InputMessageBundle.newBuilder().setSourceComputationId("foo");
      for (int i = 0; i < count; ++i) {
        bundle.addMessages(
            Windmill.Message.newBuilder()
                .setTimestamp(numTotalMessages++)
                .setData(ByteString.EMPTY)
                .build());
      }
      bundles.add(bundle.build());
    }
    Windmill.WorkItem workItem =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.EMPTY)
            .setWorkToken(0L)
            .addAllMessageBundles(bundles)
            .build();
    try (TestWindmillReaderIterator iter = new TestWindmillReaderIterator(workItem)) {
      List<Long> actual =
          ReaderTestUtils.windowedValuesToValues(
              ReaderUtils.readRemainingFromIterator(iter, false));
      assertFalse(iter.advance());
      List<Long> expected = new ArrayList<>();
      for (int i = 0; i < numTotalMessages; ++i) {
        expected.add((long) i);
      }
      assertEquals(Arrays.toString(messageBundleCounts), expected, actual);
    }
  }
}
