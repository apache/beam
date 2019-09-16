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
package org.apache.beam.runners.apex;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

/** Tests the proper registration of the Apex runner. */
public class ApexRunnerRegistrarTest {

  @Test
  public void testFullName() {
    String[] args = new String[] {String.format("--runner=%s", ApexRunner.class.getName())};
    PipelineOptions opts = PipelineOptionsFactory.fromArgs(args).create();
    assertEquals(opts.getRunner(), ApexRunner.class);
  }

  @Test
  public void testClassName() {
    String[] args = new String[] {String.format("--runner=%s", ApexRunner.class.getSimpleName())};
    PipelineOptions opts = PipelineOptionsFactory.fromArgs(args).create();
    assertEquals(opts.getRunner(), ApexRunner.class);
  }
}
