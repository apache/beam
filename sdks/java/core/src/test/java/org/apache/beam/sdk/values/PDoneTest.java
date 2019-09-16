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
package org.apache.beam.sdk.values;

import static org.apache.beam.sdk.TestUtils.LINES;

import java.io.File;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for PDone. */
@RunWith(JUnit4.class)
public class PDoneTest {

  @Rule public final TestPipeline p = TestPipeline.create();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  /** A PTransform that just returns a fresh PDone. */
  static class EmptyTransform extends PTransform<PBegin, PDone> {
    @Override
    public PDone expand(PBegin begin) {
      return PDone.in(begin.getPipeline());
    }
  }

  /** A PTransform that's composed of something that returns a PDone. */
  static class SimpleTransform extends PTransform<PBegin, PDone> {
    private final String filename;

    public SimpleTransform(String filename) {
      this.filename = filename;
    }

    @Override
    public PDone expand(PBegin begin) {
      return begin.apply(Create.of(LINES)).apply(TextIO.write().to(filename));
    }
  }

  // TODO: This test doesn't work, because we can't handle composite
  // transforms that contain no nested transforms.
  @Ignore
  @Test
  @Category(ValidatesRunner.class)
  public void testEmptyTransform() {
    p.begin().apply(new EmptyTransform());

    p.run();
  }

  // Cannot run on the service, unless we allocate a GCS temp file
  // instead of a local temp file.  Or switch to applying a different
  // transform that returns PDone.
  @Test
  @Category(NeedsRunner.class)
  public void testSimpleTransform() throws Exception {
    File tmpFile = tmpFolder.newFile("file.txt");
    String filename = tmpFile.getPath();

    p.begin().apply(new SimpleTransform(filename));

    p.run();
  }
}
