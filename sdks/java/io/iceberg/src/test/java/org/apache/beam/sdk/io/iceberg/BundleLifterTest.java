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
package org.apache.beam.sdk.io.iceberg;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

import org.apache.beam.sdk.io.iceberg.BundleLifter.BundleLiftDoFn;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BundleLifterTest {

  private static final TupleTag<Integer> INTEGER_SMALL = new TupleTag<Integer>() {};
  private static final TupleTag<Integer> INTEGER_LARGE = new TupleTag<Integer>() {};
  private static final TupleTag<String> STRING_SMALL = new TupleTag<String>() {};
  private static final TupleTag<String> STRING_LARGE = new TupleTag<String>() {};

  @Test
  public void testSmallBundle() throws Exception {
    DoFnTester<Integer, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 3, x -> 1));

    tester.startBundle();
    tester.processElement(1);
    tester.processElement(2);
    tester.finishBundle();

    assertThat(tester.peekOutputElements(INTEGER_SMALL), containsInAnyOrder(1, 2));
    assertThat(tester.peekOutputElements(INTEGER_LARGE), empty());
  }

  @Test
  public void testLargeBundle() throws Exception {
    DoFnTester<Integer, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 3, x -> 1));

    tester.startBundle();
    tester.processElement(1);
    tester.processElement(2);
    tester.processElement(3);
    tester.finishBundle();

    assertThat(tester.peekOutputElements(INTEGER_SMALL), empty());
    assertThat(tester.peekOutputElements(INTEGER_LARGE), containsInAnyOrder(1, 2, 3));
  }

  @Test
  public void testSmallBundleWithSizer() throws Exception {
    DoFnTester<String, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(STRING_SMALL, STRING_LARGE, 10, e -> e.length()));

    tester.startBundle();
    tester.processElement("123");
    tester.processElement("456");
    tester.processElement("789");
    tester.finishBundle();

    assertThat(tester.peekOutputElements(STRING_SMALL), containsInAnyOrder("123", "456", "789"));
    assertThat(tester.peekOutputElements(STRING_LARGE), empty());
  }

  @Test
  public void testLargeBundleWithSizer() throws Exception {
    DoFnTester<String, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(STRING_SMALL, STRING_LARGE, 10, e -> e.length()));

    tester.startBundle();
    tester.processElement("123");
    tester.processElement("456");
    tester.processElement("789");
    tester.processElement("0");
    tester.finishBundle();

    assertThat(tester.peekOutputElements(STRING_SMALL), empty());
    assertThat(
        tester.peekOutputElements(STRING_LARGE), containsInAnyOrder("123", "456", "789", "0"));
  }
}
