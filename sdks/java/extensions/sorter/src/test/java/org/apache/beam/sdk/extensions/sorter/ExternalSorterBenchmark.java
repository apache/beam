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
package org.apache.beam.sdk.extensions.sorter;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.UUID;
import org.apache.beam.sdk.extensions.sorter.ExternalSorter.Options.SorterType;
import org.apache.beam.sdk.values.KV;

/** {@link ExternalSorter} benchmarks. */
public class ExternalSorterBenchmark {
  private static final int N = 1000 * 1000; // 1m * (36 * 2) ~= 72MB per 1 million KVs

  public static void main(String[] args) throws IOException {
    File tempDirectory = Files.createTempDirectory("sorter").toFile();
    tempDirectory.deleteOnExit();

    ExternalSorter.Options options =
        new ExternalSorter.Options().setMemoryMB(32).setTempLocation(tempDirectory.toString());

    options.setSorterType(SorterType.HADOOP);
    benchmark(ExternalSorter.create(options));

    options.setSorterType(SorterType.NATIVE);
    benchmark(ExternalSorter.create(options));
  }

  private static void benchmark(Sorter sorter) throws IOException {
    long start = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      sorter.add(
          KV.of(
              UUID.randomUUID().toString().getBytes(Charset.defaultCharset()),
              UUID.randomUUID().toString().getBytes(Charset.defaultCharset())));
    }
    int i = 0;
    for (KV<byte[], byte[]> ignored : sorter.sort()) {
      i++;
    }
    long end = System.currentTimeMillis();
    System.out.println(
        String.format("%s: %fs", sorter.getClass().getSimpleName(), (end - start) / 1000.0));
  }
}
