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
package org.apache.beam.runners.gearpump;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

/** Tests for {@link GearpumpRunnerRegistrar}. */
public class GearpumpRunnerRegistrarTest {

  @Test
  public void testFullName() {
    String[] args = new String[] {String.format("--runner=%s", GearpumpRunner.class.getName())};
    PipelineOptions opts = PipelineOptionsFactory.fromArgs(args).create();
    assertEquals(opts.getRunner(), GearpumpRunner.class);
  }

  @Test
  public void testClassName() {
    String[] args =
        new String[] {String.format("--runner=%s", GearpumpRunner.class.getSimpleName())};
    PipelineOptions opts = PipelineOptionsFactory.fromArgs(args).create();
    assertEquals(opts.getRunner(), GearpumpRunner.class);
  }

  @Test
  public void testOptions() {
    assertEquals(
        ImmutableList.of(GearpumpPipelineOptions.class),
        new GearpumpRunnerRegistrar.Options().getPipelineOptions());
  }
}
