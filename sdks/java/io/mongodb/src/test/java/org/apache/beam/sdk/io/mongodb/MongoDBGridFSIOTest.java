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

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongoCmdOptionsBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.io.file.Files;
import de.flapdoodle.embed.process.runtime.Network;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;
import java.util.Scanner;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.mongodb.MongoDbGridFSIO.ParseCallback;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test on the MongoDbGridFSIO.
 */
public class MongoDBGridFSIOTest implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBGridFSIOTest.class);

  private static final String MONGODB_LOCATION = "target/mongodb";
  private static final int PORT = 27017;
  private static final String DATABASE = "gridfs";

  private transient MongodExecutable mongodExecutable;

  @Before
  public void setup() throws Exception {
    LOGGER.info("Starting MongoDB embedded instance");
    try {
      Files.forceDelete(new File(MONGODB_LOCATION));
    } catch (Exception e) {

    }
    new File(MONGODB_LOCATION).mkdirs();
    IMongodConfig mongodConfig = new MongodConfigBuilder()
        .version(Version.Main.PRODUCTION)
        .configServer(false)
        .replication(new Storage(MONGODB_LOCATION, null, 0))
        .net(new Net("localhost", PORT, Network.localhostIsIPv6()))
        .cmdOptions(new MongoCmdOptionsBuilder()
            .syncDelay(10)
            .useNoPrealloc(true)
            .useSmallFiles(true)
            .useNoJournal(true)
            .build())
        .build();
    mongodExecutable = MongodStarter.getDefaultInstance().prepare(mongodConfig);
    mongodExecutable.start();

    LOGGER.info("Insert test data");

    Mongo client = new Mongo("localhost", PORT);
    DB database = client.getDB(DATABASE);
    GridFS gridfs = new GridFS(database);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int x = 0; x < 100; x++) {
      out.write(("Einstein\nDarwin\nCopernicus\nPasteur\n"
                  + "Curie\nFaraday\nNewton\nBohr\nGalilei\nMaxwell\n").getBytes());
    }
    for (int x = 0; x < 5; x++) {
      gridfs.createFile(new ByteArrayInputStream(out.toByteArray()), "file" + x).save();
    }

    gridfs = new GridFS(database, "mapBucket");
    long now = System.currentTimeMillis();
    Random random = new Random();
    String[] scientists = {"Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday",
        "Newton", "Bohr", "Galilei", "Maxwell"};
    for (int x = 0; x < 10; x++) {
      GridFSInputFile file = gridfs.createFile("file_" + x);
      OutputStream outf = file.getOutputStream();
      OutputStreamWriter writer = new OutputStreamWriter(outf);
      for (int y = 0; y < 5000; y++) {
        long time = now - random.nextInt(3600000);
        String name = scientists[y % scientists.length];
        writer.write(Long.toString(time) + "\t");
        writer.write(name + "\t");
        writer.write(Integer.toString(random.nextInt(100)));
        writer.write("\n");
      }
      for (int y = 0; y < scientists.length; y++) {
        String name = scientists[y % scientists.length];
        writer.write(Long.toString(now) + "\t");
        writer.write(name + "\t");
        writer.write("101");
        writer.write("\n");
      }
      writer.flush();
      writer.close();
    }
  }

  @After
  public void stop() throws Exception {
    LOGGER.info("Stopping MongoDB instance");
    mongodExecutable.stop();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFullRead() throws Exception {
    TestPipeline pipeline = TestPipeline.create();

    PCollection<String> output = pipeline.apply(
        MongoDbGridFSIO.read()
            .withUri("mongodb://localhost:" + PORT)
            .withDatabase(DATABASE));

    PAssert.thatSingleton(
        output.apply("Count All", Count.<String>globally()))
        .isEqualTo(5000L);

    PAssert.that(
      output.apply("Count PerElement", Count.<String>perElement()))
      .satisfies(new SerializableFunction<Iterable<KV<String, Long>>, Void>() {
      @Override
      public Void apply(Iterable<KV<String, Long>> input) {
        for (KV<String, Long> element : input) {
          assertEquals(500L, element.getValue().longValue());
        }
        return null;
      }
    });

    pipeline.run();
  }


  @Test
  @Category(NeedsRunner.class)
  public void testReadWithParser() throws Exception {
    TestPipeline pipeline = TestPipeline.create();

    PCollection<KV<String, Integer>> output = pipeline.apply(
        MongoDbGridFSIO.read()
            .withUri("mongodb://localhost:" + PORT)
            .withDatabase(DATABASE)
            .withBucket("mapBucket")
            .withParsingFn(new ParseCallback<KV<String, Integer>>() {
              @Override
              public Iterator<MongoDbGridFSIO.ParseCallback.Line<KV<String, Integer>>> parse(
                  GridFSDBFile input) throws IOException {
                final BufferedReader reader =
                    new BufferedReader(new InputStreamReader(input.getInputStream()));
                return new Iterator<Line<KV<String, Integer>>>() {
                  String line = reader.readLine();
                  @Override
                  public boolean hasNext() {
                    return line != null;
                  }
                  @Override
                  public MongoDbGridFSIO.ParseCallback.Line<KV<String, Integer>> next() {
                    try (Scanner scanner = new Scanner(line.trim())) {
                      scanner.useDelimiter("\\t");
                      long timestamp = scanner.nextLong();
                      String name = scanner.next();
                      int score = scanner.nextInt();

                      try {
                        line = reader.readLine();
                      } catch (IOException e) {
                        line = null;
                      }
                      if (line == null) {
                        try {
                          reader.close();
                        } catch (IOException e) {
                          //ignore
                        }
                      }
                      return new Line<>(KV.of(name, score), new Instant(timestamp));
                    }
                  }
                  @Override
                  public void remove() {
                  }
                };
              }
            })).setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));

    PAssert.thatSingleton(output.apply("Count All", Count.<KV<String, Integer>>globally()))
        .isEqualTo(50100L);

    PAssert.that(output.apply("Max PerElement", Max.<String>integersPerKey()))
      .satisfies(new SerializableFunction<Iterable<KV<String, Integer>>, Void>() {
      @Override
      public Void apply(Iterable<KV<String, Integer>> input) {
        for (KV<String, Integer> element : input) {
          assertEquals(101, element.getValue().longValue());
        }
        return null;
      }
    });
    pipeline.run();
  }

}
