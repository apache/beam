/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.beam.runners.dataflow.internal;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@see DataflowUnboundedReadFromBoundedSource}.
 */
@RunWith(JUnit4.class)
public class DataflowUnboundedReadFromBoundedSourceTest {
  @Test
  public void testKind() {
    DataflowUnboundedReadFromBoundedSource<?> read = new
        DataflowUnboundedReadFromBoundedSource<>(new NoopNamedSource());

    assertEquals("Read(NoopNamedSource)", read.getKindString());
  }

  @Test
  public void testKindAnonymousSource() {
    NoopNamedSource anonSource = new NoopNamedSource() {};
    DataflowUnboundedReadFromBoundedSource<?> read = new
        DataflowUnboundedReadFromBoundedSource<>(anonSource);

    assertEquals("Read(AnonymousSource)", read.getKindString());
  }

  /** Source implementation only useful for its identity. */
  static class NoopNamedSource extends BoundedSource<String> {
    @Override
    public List<? extends BoundedSource<String>> splitIntoBundles(long desiredBundleSizeBytes,
        PipelineOptions options) throws Exception {
      return null;
    }
    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0;
    }
    @Override
    public BoundedReader<String> createReader(
        PipelineOptions options) throws IOException {
      return null;
    }
    @Override
    public void validate() {

    }
    @Override
    public Coder<String> getDefaultOutputCoder() {
      return null;
    }
  }
}
