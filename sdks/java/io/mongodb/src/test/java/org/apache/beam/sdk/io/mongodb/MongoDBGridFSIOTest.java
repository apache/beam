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


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.Serializable;


import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbIOTest.class);

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

    PAssert.thatSingleton(output.apply("Count All", Count.<String>globally()))
        .isEqualTo(5000L);

    PAssert.that(output.apply("Count PerElement", Count.<String>perElement()))
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

}
