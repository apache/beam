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
package org.apache.beam.sdk.io.thrift;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomUtils;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Resources;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ThriftIO}. */
@RunWith(JUnit4.class)
public class ThriftIOTest implements Serializable {

  private static final String RESOURCE_DIR = "ThriftIOTest/";

  private static final String THRIFT_DIR = Resources.getResource(RESOURCE_DIR).getPath();

  private static final TestThriftStruct TEST_THRIFT_STRUCT = new TestThriftStruct();
  private static List<TestThriftStruct> testThriftStructs;
  private final TProtocolFactory tBinaryProtoFactory = new TBinaryProtocol.Factory();
  private final TProtocolFactory tJsonProtocolFactory = new TJSONProtocol.Factory();

  private final TProtocolFactory tCompactProtocolFactory = new TCompactProtocol.Factory();
  @Rule public transient TestPipeline mainPipeline = TestPipeline.create();
  @Rule public transient TestPipeline readPipeline = TestPipeline.create();
  @Rule public transient TestPipeline writePipeline = TestPipeline.create();
  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    byte[] bytes = new byte[10];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    TEST_THRIFT_STRUCT.setTestByte((byte) 100);
    TEST_THRIFT_STRUCT.setTestShort((short) 200);
    TEST_THRIFT_STRUCT.setTestInt(2500);
    TEST_THRIFT_STRUCT.setTestLong(79303L);
    TEST_THRIFT_STRUCT.setTestDouble(25.007);
    TEST_THRIFT_STRUCT.setTestBool(true);
    TEST_THRIFT_STRUCT.setStringIntMap(new HashMap<>());
    TEST_THRIFT_STRUCT.getStringIntMap().put("first", (short) 1);
    TEST_THRIFT_STRUCT.getStringIntMap().put("second", (short) 2);
    TEST_THRIFT_STRUCT.setTestBinary(buffer);

    testThriftStructs = ImmutableList.copyOf(generateTestObjects(1000L));
  }

  /** Tests {@link ThriftIO#readFiles(Class)} with {@link TBinaryProtocol}. */
  @Test
  public void testReadFilesBinaryProtocol() {

    PCollection<TestThriftStruct> testThriftDoc =
        mainPipeline
            .apply(Create.of(THRIFT_DIR + "data").withCoder(StringUtf8Coder.of()))
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches())
            .apply(ThriftIO.readFiles(TestThriftStruct.class).withProtocol(tBinaryProtoFactory));

    // Assert
    PAssert.that(testThriftDoc).containsInAnyOrder(TEST_THRIFT_STRUCT);

    // Execute pipeline
    mainPipeline.run();
  }

  /**
   * Tests {@link ThriftIO#sink(TProtocolFactory)} and {@link ThriftIO#readFiles(Class)} with {@link
   * TBinaryProtocol}.
   */
  @Test
  public void testReadWriteBinaryProtocol() {

    mainPipeline
        .apply(
            Create.of(testThriftStructs)
                .withCoder(ThriftCoder.of(TestThriftStruct.class, tBinaryProtoFactory)))
        .apply(
            FileIO.<TestThriftStruct>write()
                .via(ThriftIO.sink(tBinaryProtoFactory))
                .to(temporaryFolder.getRoot().getAbsolutePath()));

    // Execute write pipeline
    mainPipeline.run().waitUntilFinish();

    // Read written files
    PCollection<TestThriftStruct> readDocs =
        readPipeline
            .apply(
                Create.of(temporaryFolder.getRoot().getAbsolutePath() + "/*")
                    .withCoder(StringUtf8Coder.of()))
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches())
            .apply(ThriftIO.readFiles(TestThriftStruct.class).withProtocol(tBinaryProtoFactory));

    // Assert
    PAssert.that(readDocs).containsInAnyOrder(testThriftStructs);

    // Execute read pipeline
    readPipeline.run().waitUntilFinish();
  }

  /**
   * Tests {@link ThriftIO#sink(TProtocolFactory)} and {@link ThriftIO#readFiles(Class)} with {@link
   * TJSONProtocol}.
   */
  @Test
  public void testReadWriteJsonProtocol() {

    mainPipeline
        .apply(
            Create.of(testThriftStructs)
                .withCoder(ThriftCoder.of(TestThriftStruct.class, tJsonProtocolFactory)))
        .apply(
            FileIO.<TestThriftStruct>write()
                .via(ThriftIO.sink(tJsonProtocolFactory))
                .to(temporaryFolder.getRoot().getAbsolutePath()));

    // Execute write pipeline
    mainPipeline.run().waitUntilFinish();

    // Read written files
    PCollection<TestThriftStruct> readDocs =
        readPipeline
            .apply(
                Create.of(temporaryFolder.getRoot().getAbsolutePath() + "/*")
                    .withCoder(StringUtf8Coder.of()))
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches())
            .apply(ThriftIO.readFiles(TestThriftStruct.class).withProtocol(tJsonProtocolFactory));

    // Assert
    PAssert.that(readDocs).containsInAnyOrder(testThriftStructs);

    // Execute read pipeline
    readPipeline.run().waitUntilFinish();
  }

  /**
   * Tests {@link ThriftIO#sink(TProtocolFactory)} and {@link ThriftIO#readFiles(Class)} with {@link
   * TCompactProtocol}.
   */
  @Test
  public void testReadWriteCompactProtocol() {

    mainPipeline
        .apply(
            Create.of(testThriftStructs)
                .withCoder(ThriftCoder.of(TestThriftStruct.class, tCompactProtocolFactory)))
        .apply(
            FileIO.<TestThriftStruct>write()
                .via(ThriftIO.sink(tCompactProtocolFactory))
                .to(temporaryFolder.getRoot().getAbsolutePath()));

    // Execute write pipeline
    mainPipeline.run().waitUntilFinish();

    // Read written files
    PCollection<TestThriftStruct> readDocs =
        readPipeline
            .apply(
                Create.of(temporaryFolder.getRoot().getAbsolutePath() + "/*")
                    .withCoder(StringUtf8Coder.of()))
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches())
            .apply(
                ThriftIO.readFiles(TestThriftStruct.class).withProtocol(tCompactProtocolFactory));

    // Assert
    PAssert.that(readDocs).containsInAnyOrder(testThriftStructs);

    // Execute read pipeline
    readPipeline.run().waitUntilFinish();
  }

  /** Tests {@link ThriftCoder} to ensure that values are being encoded/decoded correctly. */
  @Test
  public void testThriftCoder() {

    PCollection<TestThriftStruct> encodedStructs =
        mainPipeline.apply(
            Create.of(testThriftStructs)
                .withCoder(ThriftCoder.of(TestThriftStruct.class, tBinaryProtoFactory)));

    // Assert
    PAssert.that(encodedStructs).containsInAnyOrder(testThriftStructs);

    // Execute pipeline
    mainPipeline.run().waitUntilFinish();
  }

  private List<TestThriftStruct> generateTestObjects(long count) {
    List<TestThriftStruct> testThriftStructList = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      TestThriftStruct temp = new TestThriftStruct();
      byte[] bytes = RandomUtils.nextBytes(10);
      ByteBuffer buffer = ByteBuffer.wrap(bytes);

      // Generate random string
      String randomString = RandomStringUtils.random(10, true, false);
      short s = (short) RandomUtils.nextInt(0, Short.MAX_VALUE + 1);
      temp.setStringIntMap(new HashMap<>());
      temp.getStringIntMap().put(randomString, s);
      temp.setTestShort(s);
      temp.setTestBinary(buffer);
      temp.setTestBool(RandomUtils.nextBoolean());
      temp.setTestByte((byte) RandomUtils.nextInt(0, Byte.MAX_VALUE + 1));
      temp.setTestDouble(RandomUtils.nextDouble());
      temp.setTestInt(RandomUtils.nextInt());
      temp.setTestLong(RandomUtils.nextLong());

      testThriftStructList.add(temp);
    }

    return testThriftStructList;
  }
}
