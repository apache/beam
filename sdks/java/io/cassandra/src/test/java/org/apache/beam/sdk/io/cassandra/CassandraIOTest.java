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
package org.apache.beam.sdk.io.cassandra;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.base.Objects;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Tests of {@link CassandraIO}. */
public class CassandraIOTest implements Serializable {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testEstimatedSizeBytes() throws Exception {
    final FakeCassandraService service = new FakeCassandraService();
    service.load();

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    CassandraIO.Read spec = CassandraIO.<Scientist>read().withCassandraService(service);
    CassandraIO.CassandraSource source = new CassandraIO.CassandraSource(
        spec,
        null);
    long estimatedSizeBytes = source.getEstimatedSizeBytes(pipelineOptions);
    // the size is the sum of the bytes size of the String representation of a scientist in the map
    assertEquals(113890, estimatedSizeBytes);
  }

  @Test
  public void testRead() throws Exception {
    FakeCassandraService service = new FakeCassandraService();
    service.load();

    PCollection<Scientist> output = pipeline.apply(CassandraIO
        .<Scientist>read()
        .withCassandraService(service)
        .withKeyspace("beam")
        .withTable("scientist")
        .withCoder(SerializableCoder.of(Scientist.class))
        .withEntity(Scientist.class)
    );

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(10000L);

    PCollection<KV<String, Integer>> mapped =
        output.apply(
            MapElements.via(
                new SimpleFunction<Scientist, KV<String, Integer>>() {
                  public KV<String, Integer> apply(Scientist scientist) {
                    return KV.of(scientist.name, scientist.id);
                  }
                }));
    PAssert.that(mapped.apply("Count occurrences per scientist", Count.perKey()))
        .satisfies(
            input -> {
              for (KV<String, Long> element : input) {
                assertEquals(element.getKey(), 1000, element.getValue().longValue());
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testWrite() throws  Exception {
    FakeCassandraService service = new FakeCassandraService();

    ArrayList<Scientist> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      Scientist scientist = new Scientist();
      scientist.id = i;
      scientist.name = "Name " + i;
      data.add(scientist);
    }

    pipeline
        .apply(Create.of(data))
        .apply(CassandraIO.<Scientist>write().withCassandraService(service)
            .withKeyspace("beam")
            .withEntity(Scientist.class));
    pipeline.run();

    assertEquals(service.getTable().size(), 1000);
    for (Scientist scientist : service.getTable().values()) {
      assertTrue(scientist.name.matches("Name (\\d*)"));
    }
  }

  /**
   * A {@link CassandraService} implementation that stores the entity in memory.
   */
  private static class FakeCassandraService implements CassandraService<Scientist> {

    private static final Map<Integer, Scientist> table = new ConcurrentHashMap<>();

    public void load() {
      table.clear();
      String[] scientists = {
          "Lovelace",
          "Franklin",
          "Meitner",
          "Hopper",
          "Curie",
          "Faraday",
          "Newton",
          "Bohr",
          "Galilei",
          "Maxwell"
      };
      for (int i = 0; i < 10000; i++) {
        int index = i % scientists.length;
        Scientist scientist = new Scientist();
        scientist.id = i;
        scientist.name = scientists[index];
        table.put(scientist.id, scientist);
      }
    }

    public Map<Integer, Scientist> getTable() {
      return table;
    }

    @Override
    public FakeCassandraReader createReader(CassandraIO.CassandraSource source) {
      return new FakeCassandraReader(source);
    }

    static class FakeCassandraReader extends BoundedSource.BoundedReader {

      private final CassandraIO.CassandraSource source;

      private Iterator<Scientist> iterator;
      private Scientist current;

      public FakeCassandraReader(CassandraIO.CassandraSource source) {
        this.source = source;
      }

      @Override
      public boolean start() throws IOException {
        iterator = table.values().iterator();
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        if (iterator.hasNext()) {
          current = iterator.next();
          return true;
        }
        current = null;
        return false;
      }

      @Override
      public void close() {
        iterator = null;
        current = null;
      }

      @Override
      public Scientist getCurrent() throws NoSuchElementException {
        if (current == null) {
          throw new NoSuchElementException();
        }
        return current;
      }

      @Override
      public CassandraIO.CassandraSource getCurrentSource() {
        return this.source;
      }

    }

    @Override
    public long getEstimatedSizeBytes(CassandraIO.Read spec) {
      long size = 0L;
      for (Scientist scientist : table.values()) {
        size = size + scientist.toString().getBytes().length;
      }
      return size;
    }

    @Override
    public List<BoundedSource<Scientist>> split(CassandraIO.Read spec,
                                                           long desiredBundleSizeBytes) {
      List<BoundedSource<Scientist>> sources = new ArrayList<>();
      sources.add(new CassandraIO.CassandraSource<Scientist>(spec, null));
      return sources;
    }

    static class FakeCassandraWriter implements Writer<Scientist> {

      @Override
      public void write(Scientist scientist) {
        table.put(scientist.id, scientist);
      }

      @Override
      public void close() {
        // nothing to do
      }

    }

    @Override
    public FakeCassandraWriter createWriter(CassandraIO.Write<Scientist> spec) {
      return new FakeCassandraWriter();
    }

  }

  /** Simple Cassandra entity used in test. */
  @Table(name = "scientist", keyspace = "beam")
  public static class Scientist implements Serializable {

    @Column(name = "person_name")
    public String name;

    @Column(name = "person_id")
    public int id;

    public String toString() {
      return id + ":" + name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Scientist scientist = (Scientist) o;
      return id == scientist.id && Objects.equal(name, scientist.name);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, id);
    }
  }
}
