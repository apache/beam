/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * Tests code common to all offset-based sources.
 */
@RunWith(JUnit4.class)
public class ByteOffsetBasedSourceTest {

  class TestByteOffsetBasedSource extends ByteOffsetBasedSource<String> {

    private static final long serialVersionUID = 85539250;

    public TestByteOffsetBasedSource(long startOffset, long endOffset, long minBundleSize) {
      super(startOffset, endOffset, minBundleSize);
    }

    @Override
    public ByteOffsetBasedSource<String> createSourceForSubrange(long start, long end) {
      return new TestByteOffsetBasedSource(start, end, 1024);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0;
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public void validate() {}

    @Override
    public Coder<String> getDefaultOutputCoder() {
      return null;
    }

    @Override
    public long getMaxEndOffset(PipelineOptions options) {
      return getEndOffset();
    }
  }

  public static void assertSplitsAre(List<? extends ByteOffsetBasedSource<String>> splits,
      long[] expectedBoundaries) {
    assertEquals(splits.size(), expectedBoundaries.length - 1);
    int i = 0;
    for (ByteOffsetBasedSource<String> split : splits) {
      assertEquals(split.getStartOffset(), expectedBoundaries[i]);
      assertEquals(split.getEndOffset(), expectedBoundaries[i + 1]);
      i++;
    }
  }

  @Test
  public void testSplitPositionsZeroStart() throws Exception {
    long start = 0;
    long end = 1000;
    long minBundleSize = 50;
    long desiredBundleSize = 150;
    TestByteOffsetBasedSource testSource = new TestByteOffsetBasedSource(start, end, minBundleSize);
    long[] boundaries = {0, 150, 300, 450, 600, 750, 900, 1000};
    assertSplitsAre(testSource.splitIntoBundles(desiredBundleSize, null), boundaries);
  }

  @Test
  public void testSplitPositionsNonZeroStart() throws Exception {
    long start = 300;
    long end = 1000;
    long minBundleSize = 50;
    long desiredBundleSize = 150;
    TestByteOffsetBasedSource testSource = new TestByteOffsetBasedSource(start, end, minBundleSize);
    long[] boundaries = {300, 450, 600, 750, 900, 1000};
    assertSplitsAre(testSource.splitIntoBundles(desiredBundleSize, null), boundaries);
  }

  @Test
  public void testMinBundleSize() throws Exception {
    long start = 300;
    long end = 1000;
    long minBundleSize = 150;
    long desiredBundleSize = 100;
    TestByteOffsetBasedSource testSource = new TestByteOffsetBasedSource(start, end, minBundleSize);
    long[] boundaries = {300, 450, 600, 750, 1000};
    assertSplitsAre(testSource.splitIntoBundles(desiredBundleSize, null), boundaries);
  }

  @Test
  public void testSplitPositionsCollapseEndBundle() throws Exception {
    long start = 0;
    long end = 1000;
    long minBundleSize = 50;
    long desiredBundleSize = 110;
    TestByteOffsetBasedSource testSource = new TestByteOffsetBasedSource(start, end, minBundleSize);
    // Last 10 bytes should collapse to the previous bundle.
    long[] boundaries = {0, 110, 220, 330, 440, 550, 660, 770, 880, 1000};
    assertSplitsAre(testSource.splitIntoBundles(desiredBundleSize, null), boundaries);
  }
}
