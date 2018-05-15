/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.examples;

import com.google.common.base.Preconditions;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.hadoop.SerializableWritable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization.KeyValueSerializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Read and persist HBase table. */
public class HBaseStreamer implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseStreamer.class);
  private final URI input;
  private final URI output;
  private final SerializableWritable<Configuration> conf;
  private final AtomicReference<Serializer<KeyValue>> serializer = new AtomicReference<>();
  private final transient Executor executor;
  private HBaseStreamer(URI input, URI output, Executor executor) {
    this.input = input;
    this.output = output;
    this.executor = executor;
    this.conf = new SerializableWritable<>(HBaseConfiguration.create());
  }

  public static void main(String[] args) throws URISyntaxException, IOException {
    Preconditions.checkArgument(
        args.length == 3, "Please specify <input_uri> <output_uri> <executor>");

    URI input = new URI(args[0]);
    URI output = new URI(args[1]);
    Executor executor = Executors.createExecutor(args[2]);
    HBaseStreamer app = new HBaseStreamer(input, output, executor);
    app.run();
  }

  private void run() {

    Flow flow = Flow.create();
    Dataset<Pair<ImmutableBytesWritable, Result>> ds =
        flow.createInput(Utils.getHBaseSource(input, conf.get()));

    FlatMap.of(ds)
        .using(
            (Pair<ImmutableBytesWritable, Result> p, Collector<byte[]> c) -> {
              writeCellsAsBytes(p.getSecond(), c);
            })
        .output()
        .persist(Utils.getSink(output, conf.get()));
    LOG.info("Starting flow reading from {} and persisting to {}", input, output);
    executor.submit(flow).join();
  }

  private void writeCellsAsBytes(Result res, Collector<byte[]> c) {

    if (serializer.get() == null) {
      serializer.set(new KeyValueSerializer());
    }
    final Serializer<KeyValue> s = serializer.get();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (Cell cell : res.listCells()) {
      try {
        s.open(baos);
        s.serialize((KeyValue) cell);
        s.close();
        c.collect(baos.toByteArray());
        baos.reset();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}
