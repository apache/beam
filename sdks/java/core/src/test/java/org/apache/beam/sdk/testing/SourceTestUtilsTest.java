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
package org.apache.beam.sdk.testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * Tests for {@link SourceTestUtils}.
 */
@RunWith(JUnit4.class)
public class SourceTestUtilsTest {

  @Test
  public void testToUnsplittableSource() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    BoundedSource<Long> baseSource = CountingSource.upTo(100);
    BoundedSource<Long> unsplittableSource = SourceTestUtils.toUnsplittableSource(baseSource);
    List<?> splits = unsplittableSource.splitIntoBundles(1, options);
    assertEquals(splits.size(), 1);
    assertEquals(splits.get(0), unsplittableSource);

    BoundedReader<Long> unsplittableReader = unsplittableSource.createReader(options);
    assertNull(unsplittableReader.splitAtFraction(0.5));

    Assert.assertEquals(0, unsplittableReader.getFractionConsumed(), 1e-15);
    SourceTestUtils.readNItemsFromUnstartedReader(unsplittableReader, 50);
    SourceTestUtils.readRemainingFromReader(unsplittableReader, true);
    Assert.assertEquals(1, unsplittableReader.getFractionConsumed(), 1e-15);
  }
}
