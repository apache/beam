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
package org.apache.beam.sdk.io.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongoCmdOptionsBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.io.mongodb.MongoDbGridFSIO.Read.BoundedGridFSSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.bson.types.ObjectId;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test on the MongoDbGridFSIO. */
@RunWith(JUnit4.class)
public class MongoDBGridFSIOTest {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDBGridFSIOTest.class);

  @ClassRule public static final TemporaryFolder MONGODB_LOCATION = new TemporaryFolder();
  private static final String DATABASE = "gridfs";

  private static final MongodStarter mongodStarter = MongodStarter.getDefaultInstance();
  private static MongodExecutable mongodExecutable;
  private static MongodProcess mongodProcess;

  private static int port;

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void start() throws Exception {
    port = NetworkTestHelper.getAvailableLocalPort();
    LOG.info("Starting MongoDB embedded instance on {}", port);
    IMongodConfig mongodConfig =
        new MongodConfigBuilder()
            .version(Version.Main.PRODUCTION)
            .configServer(false)
            .replication(new Storage(MONGODB_LOCATION.getRoot().getPath(), null, 0))
            .net(new Net("localhost", port, Network.localhostIsIPv6()))
            .cmdOptions(
                new MongoCmdOptionsBuilder()
                    .syncDelay(10)
                    .useNoPrealloc(true)
                    .useSmallFiles(true)
                    .useNoJournal(true)
                    .verbose(false)
                    .build())
            .build();
    mongodExecutable = mongodStarter.prepare(mongodConfig);
    mongodProcess = mongodExecutable.start();

    LOG.info("Insert test data");

    MongoClient client = new MongoClient("localhost", port);
    DB database = client.getDB(DATABASE);
    GridFS gridfs = new GridFS(database);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int x = 0; x < 100; x++) {
      out.write(
          ("Einstein\nDarwin\nCopernicus\nPasteur\n"
                  + "Curie\nFaraday\nNewton\nBohr\nGalilei\nMaxwell\n")
              .getBytes(StandardCharsets.UTF_8));
    }
    for (int x = 0; x < 5; x++) {
      gridfs.createFile(new ByteArrayInputStream(out.toByteArray()), "file" + x).save();
    }

    gridfs = new GridFS(database, "mapBucket");
    long now = System.currentTimeMillis();
    Random random = new Random();
    String[] scientists = {
      "Einstein",
      "Darwin",
      "Copernicus",
      "Pasteur",
      "Curie",
      "Faraday",
      "Newton",
      "Bohr",
      "Galilei",
      "Maxwell"
    };
    for (int x = 0; x < 10; x++) {
      GridFSInputFile file = gridfs.createFile("file_" + x);
      OutputStream outf = file.getOutputStream();
      OutputStreamWriter writer = new OutputStreamWriter(outf, StandardCharsets.UTF_8);
      for (int y = 0; y < 5000; y++) {
        long time = now - random.nextInt(3600000);
        String name = scientists[y % scientists.length];
        writer.write(time + "\t");
        writer.write(name + "\t");
        writer.write(Integer.toString(random.nextInt(100)));
        writer.write("\n");
      }
      for (int y = 0; y < scientists.length; y++) {
        String name = scientists[y % scientists.length];
        writer.write(now + "\t");
        writer.write(name + "\t");
        writer.write("101");
        writer.write("\n");
      }
      writer.flush();
      writer.close();
    }
    client.close();
  }

  @AfterClass
  public static void stop() {
    LOG.info("Stopping MongoDB instance");
    mongodProcess.stop();
    mongodExecutable.stop();
  }

  @Test
  public void testFullRead() {
    PCollection<String> output =
        pipeline.apply(
            MongoDbGridFSIO.read().withUri("mongodb://localhost:" + port).withDatabase(DATABASE));

    PAssert.thatSingleton(output.apply("Count All", Count.globally())).isEqualTo(5000L);

    PAssert.that(output.apply("Count PerElement", Count.perElement()))
        .satisfies(
            input -> {
              for (KV<String, Long> element : input) {
                assertEquals(500L, element.getValue().longValue());
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testReadWithParser() {
    PCollection<KV<String, Integer>> output =
        pipeline.apply(
            MongoDbGridFSIO.read()
                .withUri("mongodb://localhost:" + port)
                .withDatabase(DATABASE)
                .withBucket("mapBucket")
                .<KV<String, Integer>>withParser(
                    (input, callback) -> {
                      try (final BufferedReader reader =
                          new BufferedReader(
                              new InputStreamReader(
                                  input.getInputStream(), StandardCharsets.UTF_8))) {
                        String line = reader.readLine();
                        while (line != null) {
                          try (Scanner scanner = new Scanner(line.trim())) {
                            scanner.useDelimiter("\\t");
                            long timestamp = scanner.nextLong();
                            String name = scanner.next();
                            int score = scanner.nextInt();
                            callback.output(KV.of(name, score), new Instant(timestamp));
                          }
                          line = reader.readLine();
                        }
                      }
                    })
                .withSkew(new Duration(3610000L))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));

    PAssert.thatSingleton(output.apply("Count All", Count.globally())).isEqualTo(50100L);

    PAssert.that(output.apply("Max PerElement", Max.integersPerKey()))
        .satisfies(
            input -> {
              for (KV<String, Integer> element : input) {
                assertEquals(101, element.getValue().longValue());
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testSplit() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    MongoDbGridFSIO.Read<String> read =
        MongoDbGridFSIO.read().withUri("mongodb://localhost:" + port).withDatabase(DATABASE);

    BoundedGridFSSource src = new BoundedGridFSSource(read, null);

    // make sure 2 files can fit in
    long desiredBundleSizeBytes = (src.getEstimatedSizeBytes(options) * 2L) / 5L + 1000;
    List<? extends BoundedSource<ObjectId>> splits = src.split(desiredBundleSizeBytes, options);

    int expectedNbSplits = 3;
    assertEquals(expectedNbSplits, splits.size());
    SourceTestUtils.assertSourcesEqualReferenceSource(src, splits, options);
    int nonEmptySplits = 0;
    int count = 0;
    for (BoundedSource<ObjectId> subSource : splits) {
      List<ObjectId> result = SourceTestUtils.readFromSource(subSource, options);
      if (result.size() > 0) {
        nonEmptySplits += 1;
      }
      count += result.size();
    }
    assertEquals(expectedNbSplits, nonEmptySplits);
    assertEquals(5, count);
  }

  @Test
  public void testWriteMessage() throws Exception {
    ArrayList<String> data = new ArrayList<>(100);
    ArrayList<Integer> intData = new ArrayList<>(100);
    for (int i = 0; i < 1000; i++) {
      data.add("Message " + i);
    }
    for (int i = 0; i < 100; i++) {
      intData.add(i);
    }
    pipeline
        .apply("String", Create.of(data))
        .apply(
            "StringInternal",
            MongoDbGridFSIO.write()
                .withUri("mongodb://localhost:" + port)
                .withDatabase(DATABASE)
                .withChunkSize(100L)
                .withBucket("WriteTest")
                .withFilename("WriteTestData"));

    pipeline
        .apply("WithWriteFn", Create.of(intData))
        .apply(
            "WithWriteFnInternal",
            MongoDbGridFSIO.<Integer>write(
                    (output, outStream) -> {
                      // one byte per output
                      outStream.write(output.byteValue());
                    })
                .withUri("mongodb://localhost:" + port)
                .withDatabase(DATABASE)
                .withBucket("WriteTest")
                .withFilename("WriteTestIntData"));

    pipeline.run();

    MongoClient client = null;
    try {
      StringBuilder results = new StringBuilder();
      client = new MongoClient("localhost", port);
      DB database = client.getDB(DATABASE);
      GridFS gridfs = new GridFS(database, "WriteTest");
      List<GridFSDBFile> files = gridfs.find("WriteTestData");
      assertTrue(files.size() > 0);
      for (GridFSDBFile file : files) {
        assertEquals(100, file.getChunkSize());
        int l = (int) file.getLength();
        try (InputStream ins = file.getInputStream()) {
          DataInputStream dis = new DataInputStream(ins);
          byte[] b = new byte[l];
          dis.readFully(b);
          results.append(new String(b, StandardCharsets.UTF_8));
        }
      }
      String dataString = results.toString();
      for (int x = 0; x < 1000; x++) {
        assertTrue(dataString.contains("Message " + x));
      }

      files = gridfs.find("WriteTestIntData");
      boolean[] intResults = new boolean[100];
      for (GridFSDBFile file : files) {
        int l = (int) file.getLength();
        try (InputStream ins = file.getInputStream()) {
          DataInputStream dis = new DataInputStream(ins);
          byte[] b = new byte[l];
          dis.readFully(b);
          for (byte aB : b) {
            intResults[aB] = true;
          }
        }
      }

      for (int x = 0; x < 100; x++) {
        assertTrue("Did not get a result for " + x, intResults[x]);
      }
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
