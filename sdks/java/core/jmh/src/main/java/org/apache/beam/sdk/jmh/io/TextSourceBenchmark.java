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
package org.apache.beam.sdk.jmh.io;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.TextIOReadTest;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

public class TextSourceBenchmark {
  private static final int NUM_LINES = 10_000_000;
  private static char[] data = new char[120];

  static {
    Arrays.fill(data, 'a');
  }

  @State(Scope.Benchmark)
  public static class Data {
    public Path path;
    public String pathString;
    public int length;

    /** Generates a random file with {@code NUM_LINES} between 60 and 120 characters each. */
    @Setup
    public void createFile() throws Exception {
      path = Files.createTempFile("benchmark", null).toAbsolutePath();
      pathString = path.toString();
      BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
      for (int i = 0; i < NUM_LINES; ++i) {
        String valueToAppend =
            String.valueOf(data, 0, ThreadLocalRandom.current().nextInt(60, 120));
        length += valueToAppend.length();
        writer.write(valueToAppend);
        writer.write('\n');
      }
      writer.close();
    }

    @TearDown
    public void deleteFile() throws Exception {
      Files.deleteIfExists(path);
    }
  }

  @Benchmark
  public void benchmarkTextSource(Data data) throws Exception {
    Source.Reader<String> reader =
        ((FileBasedSource<String>) TextIOReadTest.getTextSource(data.pathString, null))
            .createReader(PipelineOptionsFactory.create());
    int length = 0;
    int linesRead = 0;
    if (reader.start()) {
      linesRead += 1;
      length += reader.getCurrent().length();
    }
    while (reader.advance()) {
      linesRead += 1;
      length += reader.getCurrent().length();
    }
    if (linesRead != NUM_LINES) {
      throw new IllegalStateException();
    }
    if (length != data.length) {
      throw new IllegalStateException();
    }
    reader.close();
  }

  @Benchmark
  public void benchmarkHadoopLineReader(Data data) throws Exception {
    LineReader reader = new LineReader(new FileInputStream(data.pathString));
    int length = 0;
    int linesRead = 0;
    do {
      Text text = new Text();
      reader.readLine(text);
      // It is important to convert toString() here so that we force the decoding to UTF8 otherwise
      // Text keeps the encoded byte[] version in memory.
      length += text.toString().length();
      linesRead += 1;
    } while (length < data.length);
    if (linesRead != NUM_LINES) {
      throw new IllegalStateException();
    }
    reader.close();
  }
}
