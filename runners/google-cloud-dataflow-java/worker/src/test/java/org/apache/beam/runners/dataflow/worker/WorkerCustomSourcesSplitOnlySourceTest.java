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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;

import com.google.api.services.dataflow.model.DerivedSource;
import com.google.api.services.dataflow.model.SourceSplitResponse;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.worker.WorkerCustomSources.SplittableOnlyBoundedSource;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.storage.NoopPathValidator;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Combinatorial tests for {@link WorkerCustomSources#limitNumberOfBundles} which checks that all
 * splits were returned.
 */
@RunWith(Parameterized.class)
public class WorkerCustomSourcesSplitOnlySourceTest {
  @Parameters
  public static Iterable<Object[]> data() {
    ImmutableList.Builder<Object[]> ret = ImmutableList.builder();
    for (int i = 1; i <= 1000; i += 137) {
      ret.add(new Object[] {i});
    }
    return ret.build();
  }

  @Parameter public int numberOfSplits;

  @Test
  public void testAllSplitsAreReturned() throws Exception {
    final long apiSizeLimitForTest = 500 * 1024;
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setAppName("TestAppName");
    options.setProject("test-project");
    options.setRegion("some-region1");
    options.setTempLocation("gs://test/temp/location");
    options.setGcpCredential(new TestCredential());
    options.setRunner(DataflowRunner.class);
    options.setPathValidatorClass(NoopPathValidator.class);
    // Generate a CountingSource and split it into the desired number of splits
    // (desired size = 1 byte), triggering the re-split with a larger bundle size.
    // Thus below we expect to produce 'numberOfSplits' splits.
    com.google.api.services.dataflow.model.Source source =
        WorkerCustomSourcesTest.translateIOToCloudSource(
            CountingSource.upTo(numberOfSplits), options);
    SourceSplitResponse split =
        WorkerCustomSourcesTest.performSplit(
            source, options, 1L, null /* numBundles limit */, apiSizeLimitForTest);
    assertThat(
        split.getBundles().size(),
        lessThanOrEqualTo(WorkerCustomSources.DEFAULT_NUM_BUNDLES_LIMIT));

    List<OffsetBasedSource<?>> originalSplits = new ArrayList<>(numberOfSplits);
    // Collect all the splits
    for (DerivedSource derivedSource : split.getBundles()) {
      Object deserializedSource =
          WorkerCustomSources.deserializeFromCloudSource(derivedSource.getSource().getSpec());
      if (deserializedSource instanceof SplittableOnlyBoundedSource) {
        SplittableOnlyBoundedSource<?> splittableOnlySource =
            (SplittableOnlyBoundedSource<?>) deserializedSource;
        originalSplits.addAll((List) splittableOnlySource.split(1L, options));
      } else {
        originalSplits.add((OffsetBasedSource<?>) deserializedSource);
      }
    }

    assertEquals(numberOfSplits, originalSplits.size());
    for (int i = 0; i < originalSplits.size(); i++) {
      OffsetBasedSource<?> offsetBasedSource = (OffsetBasedSource<?>) originalSplits.get(i);
      assertEquals(i, offsetBasedSource.getStartOffset());
      assertEquals(i + 1, offsetBasedSource.getEndOffset());
    }
  }
}
