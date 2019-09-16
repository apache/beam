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
package org.apache.beam.sdk.nexmark;

import org.junit.Test;

/**
 * Test of {@link Main}; using {@link NexmarkOptions} since we don't want to test option parsing
 * here.
 */
public class MainTest {
  @Test
  public void testSmokeSuiteOnDirectRunner() throws Exception {
    // Default for SMOKE is 100k or 10k for heavier queries - way overkill for "smoke" test
    final String[] pipelineArgs = {"--numEvents=500", "--suite=SMOKE", "--manageResources=false"};
    new Main().runAll(pipelineArgs);
  }
}
