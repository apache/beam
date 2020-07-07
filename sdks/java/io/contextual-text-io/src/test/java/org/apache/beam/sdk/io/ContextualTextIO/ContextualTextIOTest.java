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
package org.apache.beam.sdk.io.ContextualTextIO;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO.Read;
import org.apache.beam.sdk.testing.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Read}. */
@RunWith(JUnit4.class)
public class ContextualTextIOTest implements Serializable {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
  @Rule public final transient TestPipeline p = TestPipeline.create();

  public static final char CR = (char) 0x0D;
  public static final char LF = (char) 0x0A;

  public static final String CRLF = "" + CR + LF;

  public String createFiles(List<String> input) throws Exception {

    File tmpFile = tempFolder.newFile();
    String filename = tmpFile.getPath();

    try (PrintStream writer = new PrintStream(new FileOutputStream(tmpFile))) {
      for (String elem : input) {
        byte[] encodedElem = CoderUtils.encodeToByteArray(StringUtf8Coder.of(), elem);
        String line = new String(encodedElem, Charsets.UTF_8);
        writer.println(line);
      }
    }
    return filename;
  }

  @Test
  @Category(NeedsRunner.class)
  public void runBasicReadTest() throws Exception {

    List<String> input = ImmutableList.of("1", "2");

    ContextualTextIO.Read read = ContextualTextIO.read().from(createFiles(input));
    PCollection<LineContext> output = p.apply(read);

    PCollection<String> result =
        output.apply(
            MapElements.into(TypeDescriptors.strings()).via(x -> String.valueOf(x.getLine())));

    PAssert.that(result).containsInAnyOrder("1", "2");

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void runBasicReadTestWithRFC4180Set() throws Exception {

    List<String> input = ImmutableList.of("1", "2");

    ContextualTextIO.Read read =
        ContextualTextIO.read().from(createFiles(input)).withHasRFC4180MultiLineColumn(true);
    PCollection<LineContext> output = p.apply(read);

    PCollection<String> result =
        output.apply(
            MapElements.into(TypeDescriptors.strings()).via(x -> String.valueOf(x.getLine())));

    PAssert.that(result).containsInAnyOrder("1", "2");

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  /** Test to read files with using MultiLine columns as per RFC4180 */
  public void runSmallRFC4180MultiLineReadTest() throws Exception {

    // Generate lines of format "1\n1" where number changes per line.
    List<String> input =
        IntStream.range(0, 2)
            .<String>mapToObj(x -> "\"" + x + CRLF + x + "\"")
            .collect(Collectors.toList());

    ContextualTextIO.Read read =
        ContextualTextIO.read().from(createFiles(input)).withRFC4180MultiLineColumn(true);
    PCollection<LineContext> output = p.apply(read);

    PCollection<String> result =
        output.apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    x -> {
                      System.out.println(
                          x.getRangeNum() + " " + x.getRangeLineNum() + " " + x.getFile());
                      return String.valueOf(x.getLine());
                    }));

    PAssert.that(result).containsInAnyOrder(input);

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  /** Test to read files with using MultiLine columns as per RFC4180 */
  public void runSmallRFC4180EscapedCharcatersReadTest() throws Exception {

    // Generate lines of format  "aaa","b""bb","ccc" where number changes per line.
    List<String> input =
        IntStream.range(0, 2)
            .<String>mapToObj(x -> "\"aaa\",\"b\"\"bb\",\"ccc\"")
            .collect(Collectors.toList());

    ContextualTextIO.Read read =
        ContextualTextIO.read().from(createFiles(input)).withRFC4180MultiLineColumn(true);
    PCollection<LineContext> output = p.apply(read);

    PCollection<String> result =
        output.apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    x -> {
                      System.out.println(x);
                      return String.valueOf(x.getLine());
                    }));

    PAssert.that(result).containsInAnyOrder(input);

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  /** Test to read files with using MultiLine columns as per RFC4180 */
  public void runLargeRFC4180MultiLineReadTest() throws Exception {

    // Generate lines of format "1\n1" where number changes per line.
    List<String> input =
        IntStream.range(0, 1000)
            .<String>mapToObj(x -> "\"" + x + CRLF + x + "\"")
            .collect(Collectors.toList());

    ContextualTextIO.Read read =
        ContextualTextIO.read().from(createFiles(input)).withHasRFC4180MultiLineColumn(true);
    PCollection<LineContext> output = p.apply(read);

    PCollection<String> result =
        output.apply(
            MapElements.into(TypeDescriptors.strings()).via(x -> String.valueOf(x.getLine())));

    PAssert.that(result).containsInAnyOrder(input);

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  /** Test to read files with using MultiLine columns as per RFC4180 */
  public void runLargeRFC4180MultiLineAndEscapedReadTest() throws Exception {

    // Generate lines of format  "aaa","b""\nbb","ccc","""\nHello" where number changes per line.
    List<String> input =
        IntStream.range(0, 1000)
            .<String>mapToObj(
                x -> "\"a" + CRLF + "aa\",\"b\"\"" + CRLF + "bb\",\"ccc\",\"\"\"\\nHello\"")
            .collect(Collectors.toList());

    ContextualTextIO.Read read =
        ContextualTextIO.read().from(createFiles(input)).withHasRFC4180MultiLineColumn(true);
    PCollection<LineContext> output = p.apply(read);

    PCollection<String> result =
        output.apply(
            MapElements.into(TypeDescriptors.strings()).via(x -> String.valueOf(x.getLine())));

    PAssert.that(result).containsInAnyOrder(input);

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  /** Test to read files with using MultiLine columns as per RFC4180 */
  public void testFileNameIsPreserved() throws Exception {

    List<String> input =
        IntStream.range(1, 1000)
            .<String>mapToObj(x -> Integer.toString(x))
            .collect(Collectors.toList());

    ContextualTextIO.Read read =
        ContextualTextIO.read().from(createFiles(input)).withHasRFC4180MultiLineColumn(true);
    PCollection<LineContext> output = p.apply(read);

    PCollection<String> result =
        output.apply(
            MapElements.into(TypeDescriptors.strings()).via(x -> String.valueOf(x.getLine())));

    PAssert.that(result).containsInAnyOrder(input);

    p.run();
  }

  public static class TestOridinalLineNumber
      extends DoFn<Iterable<LineContext>, KV<Long, LineContext>> {
    @ProcessElement
    public void process(
        @Element Iterable<LineContext> input, OutputReceiver<KV<Long, LineContext>> o) {
      List<LineContext> lineContexts =
          StreamSupport.stream(input.spliterator(), false).collect(Collectors.toList());
      lineContexts.sort(Comparator.comparingLong(LineContext::getRangeNum));
      long lineCount = 0;
      long cumulativeRangeCount = 0;
      Long currentRange = lineContexts.get(0).getRangeNum();

      List<KV<Long, LineContext>> unMatched = new ArrayList<>();
      System.out.println(lineContexts);
      for (LineContext l : lineContexts) {

        // If the range has moved then set cumulativeRangeCount
        if (!currentRange.equals(l.getRangeNum())) {
          cumulativeRangeCount = lineCount;
        }
        lineCount++;
        if (lineCount != Long.parseLong(l.getLine()) + cumulativeRangeCount) {
          unMatched.add(KV.of(lineCount, l));
        }
      }
      unMatched.forEach(x -> o.output(x));
    }
  }

  @Test
  @Category(NeedsRunner.class)
  /** Test to read files with using MultiLine columns as per RFC4180 */
  public void testLineNumsAreCorrectWithoutRFC4180Set() throws Exception {

    List<String> input =
        IntStream.range(1, 1000)
            .<String>mapToObj(x -> Integer.toString(x))
            .collect(Collectors.toList());

    ContextualTextIO.Read read = ContextualTextIO.read().from(createFiles(input));
    PCollection<LineContext> output = p.apply(read);

    // Collapse all values into single GBK.
    // Then use offset to construct actual line position with original line position.
    // Note any exceptions.
    // RangeNum should go up in blocks, with carry forward of last block to new block for the
    // numbers.
    // <p>RangeNum 0 : Line [1-100]
    // <p>RangeNum 1 : Line [1-100]
    // <p>Translates to :
    // <p> Line [0-200]

    PCollection<KV<Long, LineContext>> result =
        output
            .apply(WithKeys.of(1))
            .apply(GroupByKey.create())
            .apply(Values.create())
            .apply(ParDo.of(new TestOridinalLineNumber()));

    PAssert.that(result);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  /** Test to read files with using MultiLine columns as per RFC4180 */
  public void testLineNumsAreCorrectWithRFC4180Set() throws Exception {

    List<String> input =
        IntStream.range(1, 1000)
            .<String>mapToObj(x -> Integer.toString(x))
            .collect(Collectors.toList());

    ContextualTextIO.Read read =
        ContextualTextIO.read().from(createFiles(input)).withHasRFC4180MultiLineColumn(true);
    PCollection<LineContext> output = p.apply(read);

    // Collapse all values into single GBK.
    // Then use offset to construct actual line position with original line position.
    // Note any exceptions.
    // RangeNum should go up in blocks, with carry forward of last block to new block for the
    // numbers.
    // <p>RangeNum 0 : Line [1-100]
    // <p>RangeNum 1 : Line [1-100]
    // <p>Translates to :
    // <p> Line [0-200]

    PCollection<KV<Long, LineContext>> result =
        output
            .apply(WithKeys.of(1))
            .apply(GroupByKey.create())
            .apply(Values.create())
            .apply(ParDo.of(new TestOridinalLineNumber()));

    PAssert.that(result);
    p.run();
  }
}
