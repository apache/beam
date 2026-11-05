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
package org.apache.beam.sdk.io.gcp.bigquery;

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs {@link BigQueryIOWriteTest} for Storage Write API streaming modes in a separate test class
 * so Gradle {@code maxParallelForks} can execute them in parallel with other modes.
 */
@RunWith(Parameterized.class)
public class BigQueryIOWriteStorageApiStreamTest extends BigQueryIOWriteTest {
  @Parameters(name = "useStorageApi={0}, useStorageApiApproximate={1}, useStreaming={2}")
  public static Iterable<Object[]> data() {
    return ImmutableList.of(new Object[] {true, false, true}, new Object[] {true, true, true});
  }
}
