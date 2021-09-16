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
package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for UniqueIdGenerator. */
@RunWith(JUnit4.class)
public class UniqueIdGeneratorTest {
  @Test
  public void testGetUniqueId() {
    ImmutableList.Builder<CompletableFuture<String[]>> futureBuilder = ImmutableList.builder();
    for (int i = 0; i < 10; i++) {
      futureBuilder.add(
          CompletableFuture.supplyAsync(
              () -> {
                String[] array = new String[1000];
                for (int j = 0; j < 1000; j++) {
                  array[j] = UniqueIdGenerator.getUniqueId().toString();
                }
                return array;
              }));
    }
    List<CompletableFuture<String[]>> futures = futureBuilder.build();
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    Set<String> resultSet =
        futures.stream().flatMap(x -> Stream.of(x.join())).collect(Collectors.toSet());

    assertEquals(10000, resultSet.size());
  }
}
