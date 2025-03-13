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
package org.apache.beam.sdk.io.hbase;

import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link HBaseCoderProviderRegistrar}. */
@RunWith(JUnit4.class)
public class HBaseCoderProviderRegistrarTest {
  @Test
  public void testResultCoderIsRegistered() throws Exception {
    CoderRegistry.createDefault().getCoder(Result.class);
  }

  @Test
  public void testMutationCoderIsRegistered() throws Exception {
    CoderRegistry.createDefault().getCoder(Mutation.class);
    CoderRegistry.createDefault().getCoder(Put.class);
    CoderRegistry.createDefault().getCoder(Delete.class);
  }

  @Test
  public void testRowMutationsCoderIsRegistered() throws Exception {
    CoderRegistry.createDefault().getCoder(RowMutations.class);
  }
}
