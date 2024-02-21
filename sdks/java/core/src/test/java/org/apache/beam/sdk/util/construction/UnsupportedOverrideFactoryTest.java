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
package org.apache.beam.sdk.util.construction;

import java.util.Collections;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PDone;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link UnsupportedOverrideFactory}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class UnsupportedOverrideFactoryTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private final String message = "my_error_message";
  private TestPipeline pipeline = TestPipeline.create();
  private UnsupportedOverrideFactory factory = UnsupportedOverrideFactory.withMessage(message);

  @Test
  public void getReplacementTransformThrows() {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(message);
    factory.getReplacementTransform(null);
  }

  @Test
  public void mapOutputThrows() {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(message);
    factory.mapOutputs(Collections.emptyMap(), PDone.in(pipeline));
  }
}
