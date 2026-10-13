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
package org.apache.beam.runners.dataflow.worker.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SimpleByteHistogram}. */
@RunWith(JUnit4.class)
public class SimpleByteHistogramTest {

  @Test
  public void testHistogram() {
    SimpleByteHistogram histogram = new SimpleByteHistogram();
    histogram.add(10); // <128B
    histogram.add(127); // <128B
    histogram.add(128); // <256B
    histogram.add(255); // <256B
    histogram.add(256); // <512B
    histogram.add(511); // <512B
    histogram.add(512); // <1KB
    histogram.add(1023); // <1KB
    histogram.add(1024); // <10KB
    histogram.add(10240 - 1); // <10KB
    histogram.add(10240); // <1MB
    histogram.add(1048576 - 1); // <1MB
    histogram.add(1048576); // >=1MB
    histogram.add(2000000); // >=1MB

    String expected = "[<128B:2, <256B:2, <512B:2, <1KB:2, <10KB:2, <1MB:2, >=1MB:2]";
    assertEquals(expected, histogram.format());
  }
}
